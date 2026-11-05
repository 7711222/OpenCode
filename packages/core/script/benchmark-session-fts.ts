#!/usr/bin/env bun

/**
 * Measures what the session search index costs on the write path, using the real schema, migrations, projector,
 * triggers and SessionSearchIndex primitives over temporary file databases filled with synthetic sessions.
 *
 *   bun run script/benchmark-session-fts.ts [--sessions 40] [--steps 8] [--tools 3] [--output-bytes 4096]
 *     [--repeats 3] [--batch 100] [--backlog-batches 25,100,500] [--json]
 *
 * Write latency is `bus.publish` of real durable session events (bus lock, sequence row, projector, triggers),
 * not raw SQL. The "statement" section times one bare UPDATE so the trigger cost alone is visible. The drain modes
 * stand in for a poll loop running at different rates; assistants are indexed only once their step finishes, so a
 * drain during streaming only dequeues them.
 */

import fs from "fs/promises"
import os from "os"
import path from "path"
import { parseArgs } from "util"
import { sql } from "drizzle-orm"
import { Effect, Logger } from "effect"
import { Global } from "@opencode/util/global"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { AppNodeBuilder } from "../src/effect/app-node-builder"
import { Agent } from "../src/agent"
import { Bus } from "../src/bus"
import { Database } from "../src/database/database"
import { KVTable } from "../src/kv/sql"
import { Model } from "../src/model"
import { Project } from "../src/project"
import { ProjectTable } from "../src/project/sql"
import { Provider } from "../src/provider"
import { AbsolutePath } from "../src/schema"
import { SessionEvent } from "../src/session/event"
import { SessionMessage } from "../src/session/message"
import { SessionProjector } from "../src/session/projector"
import { SessionSearchIndex } from "../src/session/search-index"
import { SessionMessageTable, SessionSearchDirtyTable, SessionTable } from "../src/session/sql"
import { SessionID } from "@opencode/schema/session-id"
import { Money } from "@opencode/schema/money"

const args = parseArgs({
  args: process.argv.slice(2),
  options: {
    sessions: { type: "string", default: "40" },
    steps: { type: "string", default: "8" },
    tools: { type: "string", default: "3" },
    "output-bytes": { type: "string", default: "4096" },
    repeats: { type: "string", default: "3" },
    batch: { type: "string", default: "100" },
    "backlog-batches": { type: "string", default: "25,100,500" },
    json: { type: "boolean", default: false },
  },
})
const positive = (name: string, value: string) => {
  const number = Number(value)
  if (!Number.isInteger(number) || number < 1) throw new Error(`--${name} must be a positive integer`)
  return number
}
const config = {
  sessions: positive("sessions", args.values.sessions),
  steps: positive("steps", args.values.steps),
  tools: positive("tools", args.values.tools),
  outputBytes: positive("output-bytes", args.values["output-bytes"]),
  repeats: positive("repeats", args.values.repeats),
  batch: positive("batch", args.values.batch),
  backlogBatches: args.values["backlog-batches"].split(",").map((value) => positive("backlog-batches", value)),
}

type Db = Database.Interface["db"]
type Drain = "none" | "after" | "step" | "event"
interface Mode {
  readonly name: string
  readonly index: boolean
  readonly drain: Drain
}

const modes: ReadonlyArray<Mode> = [
  { name: "index off", index: false, drain: "none" },
  { name: "index on, drain after", index: true, drain: "after" },
  { name: "index on, drain each step", index: true, drain: "step" },
  { name: "index on, drain each event", index: true, drain: "event" },
]

// Deterministic synthetic text: no real session data.
let seed = 0x5eed
const random = () => {
  seed = (seed * 1_103_515_245 + 12_345) & 0x7fffffff
  return seed / 0x7fffffff
}
const vocabulary = [
  "reconnect",
  "websocket",
  "migration",
  "parseConfigFile",
  "retry",
  "backoff",
  "SessionStore",
  "decodeRow",
  "timeout",
  "cache",
  "branch",
  "worktree",
  "provider",
  "streaming",
  "handler",
  "fixture",
  "snapshot",
  "permission",
  "tokenizer",
  "HTTPServer",
]
const words = (count: number) =>
  Array.from({ length: count }, () => vocabulary[Math.floor(random() * vocabulary.length)]).join(" ")
const bytes = (size: number) => {
  const chunks: string[] = []
  let length = 0
  while (length < size) {
    const chunk = words(16)
    chunks.push(chunk)
    length += chunk.length + 1
  }
  return chunks.join("\n").slice(0, size)
}

const model = { id: Model.ID.make("bench-model"), providerID: Provider.ID.make("bench") }
const tokens = { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } }
const sessionIDs = Array.from({ length: config.sessions }, (_, index) =>
  SessionID.make(`ses_bench_${String(index).padStart(5, "0")}`),
)

const stats = (samples: ReadonlyArray<number>) => {
  const sorted = samples.toSorted((a, b) => a - b)
  const percentile = (value: number) => sorted[Math.min(Math.ceil(sorted.length * value) - 1, sorted.length - 1)] ?? 0
  const total = samples.reduce((sum, sample) => sum + sample, 0)
  return { count: samples.length, total, p50: percentile(0.5), p95: percentile(0.95), max: sorted.at(-1) ?? 0 }
}

const timed = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const start = performance.now()
    const value = yield* effect
    return { value, ms: performance.now() - start }
  })

const layerFor = (directory: string) =>
  AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node]), [
    Global.node.replace(
      Global.layerWith({
        home: directory,
        data: path.join(directory, "data"),
        cache: path.join(directory, "cache"),
        config: path.join(directory, "config"),
        state: path.join(directory, "state"),
        tmp: path.join(directory, "tmp"),
        bin: path.join(directory, "bin"),
        log: path.join(directory, "log"),
        repos: path.join(directory, "repos"),
      }),
    ),
    Database.node.replace(Database.configured({ path: path.join(directory, "bench.db") })),
  ])

const withDatabase = async <A>(effect: Effect.Effect<A, unknown, Database.Service | Bus.Service>) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-session-fts-"))
  try {
    return await Effect.runPromise(
      effect.pipe(Effect.scoped, Effect.provide(layerFor(directory)), Effect.provide(Logger.layer([]))),
    )
  } finally {
    await fs.rm(directory, { recursive: true, force: true })
  }
}

/**
 * WAL frames written since the last call. Automatic checkpoints are off, so frames accumulate; a passive checkpoint
 * reports them (a truncating one reports zero) and the truncating one then resets the log.
 */
const walFrames = (db: Db) =>
  Effect.gen(function* () {
    const rows = yield* db.all<{ log: number }>(sql.raw("PRAGMA wal_checkpoint(PASSIVE)"))
    yield* db.all(sql.raw("PRAGMA wal_checkpoint(TRUNCATE)"))
    return Math.max(0, rows[0]?.log ?? 0)
  })

const pageSize = (db: Db) =>
  db.get<{ page_size: number }>(sql.raw("PRAGMA page_size")).pipe(Effect.map((row) => row?.page_size ?? 4096))

/** Bytes per table, indexes included, when this SQLite has the dbstat table; FTS5 internals are summed as `fts`. */
const tableBytes = (db: Db) =>
  db
    .all<{ name: string; bytes: number }>(
      sql.raw(`SELECT m.tbl_name AS name, sum(s.pgsize) AS bytes FROM dbstat s
        JOIN sqlite_master m ON m.name = s.name GROUP BY m.tbl_name`),
    )
    .pipe(
      Effect.map((rows) => {
        const pick = (name: string) => rows.find((row) => row.name === name)?.bytes ?? 0
        return {
          session_message: pick("session_message"),
          fts: rows
            .filter(
              (row) =>
                row.name.startsWith("session_search_") &&
                !["session_search_document", "session_search_dirty"].includes(row.name),
            )
            .reduce((sum, row) => sum + row.bytes, 0),
          session_search_document: pick("session_search_document"),
          session_search_dirty: pick("session_search_dirty"),
        }
      }),
      Effect.catch(() => Effect.succeed(undefined)),
    )

const pending = (db: Db) =>
  db
    .get<{ count: number }>(sql`SELECT count(*) AS count FROM ${SessionSearchDirtyTable}`)
    .pipe(Effect.map((row) => row?.count ?? 0))

interface DrainResult {
  readonly batches: number[]
  readonly sources: number
  readonly heapPeakMB: number
  readonly rssPeakMB: number
}

const drain = (db: Db, batch: number) =>
  Effect.gen(function* () {
    const batches: number[] = []
    let sources = 0
    const before = process.memoryUsage()
    let heapPeak = before.heapUsed
    let rssPeak = before.rss
    while (true) {
      const result = yield* timed(SessionSearchIndex.processBatch(db, batch))
      if (result.value === 0) break
      batches.push(result.ms)
      sources += result.value
      const memory = process.memoryUsage()
      heapPeak = Math.max(heapPeak, memory.heapUsed)
      rssPeak = Math.max(rssPeak, memory.rss)
    }
    return {
      batches,
      sources,
      heapPeakMB: (heapPeak - before.heapUsed) / 1_048_576,
      rssPeakMB: (rssPeak - before.rss) / 1_048_576,
    } satisfies DrainResult
  })

const seedSessions = (db: Db) =>
  db.transaction(() =>
    Effect.gen(function* () {
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/bench"), sandboxes: [] })
        .run()
      yield* db
        .insert(SessionTable)
        .values(
          sessionIDs.map((id, index) => ({
            id,
            project_id: Project.ID.global,
            slug: `bench-${index}`,
            directory: "/bench",
            title: `${words(4)} ${index}`,
            version: "bench",
          })),
        )
        .run()
    }),
  )

/** One settled agent workload through real durable events: prompts, steps with text and tools, renames, deletes. */
const workload = (db: Db, bus: Bus.Interface, mode: Mode) =>
  Effect.gen(function* () {
    const inserts: number[] = []
    const updates: number[] = []
    const deletes: number[] = []
    const drains: DrainResult[] = []
    let peakPending = 0
    let backgroundMs = 0
    let counter = 0
    const id = (suffix: string) => SessionMessage.ID.make(`msg_${String(counter++).padStart(9, "0")}${suffix}`)
    // Stands in for the poll loop: runs between writes and is timed apart from them.
    const background = Effect.gen(function* () {
      const start = performance.now()
      peakPending = Math.max(peakPending, yield* pending(db))
      drains.push(yield* drain(db, config.batch))
      backgroundMs += performance.now() - start
    })
    const publish = Effect.fnUntraced(function* (
      kind: "insert" | "update" | "delete" | "other",
      effect: Effect.Effect<unknown>,
    ) {
      const result = yield* timed(effect)
      if (kind === "insert") inserts.push(result.ms)
      if (kind === "update") updates.push(result.ms)
      if (kind === "delete") deletes.push(result.ms)
      if (mode.drain === "event") yield* background
    })

    for (const sessionID of sessionIDs) {
      for (let step = 0; step < config.steps; step++) {
        const inboxID = id("u")
        yield* publish(
          "other",
          bus.publish(SessionEvent.InboxEnqueued, {
            sessionID,
            inboxID,
            item: { type: "user", payload: { text: words(24) }, delivery: "steer" },
          }),
        )
        yield* publish("insert", bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID }))
        const assistantMessageID = id("a")
        yield* publish(
          "insert",
          bus.publish(SessionEvent.Step.Started, {
            sessionID,
            assistantMessageID,
            agent: Agent.defaultID,
            model,
            started: 0,
          }),
        )
        yield* publish(
          "update",
          bus.publish(SessionEvent.Reasoning.Started, { sessionID, assistantMessageID, ordinal: 0 }),
        )
        yield* publish(
          "update",
          bus.publish(SessionEvent.Reasoning.Ended, { sessionID, assistantMessageID, ordinal: 0, text: words(40) }),
        )
        yield* publish("update", bus.publish(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 }))
        yield* publish(
          "update",
          bus.publish(SessionEvent.Text.Ended, { sessionID, assistantMessageID, ordinal: 0, text: words(30) }),
        )
        for (let tool = 0; tool < config.tools; tool++) {
          const callID = `call_${step}_${tool}`
          yield* publish(
            "update",
            bus.publish(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: callID, name: "bash" }),
          )
          const input = { command: words(6) }
          yield* publish(
            "update",
            bus.publish(SessionEvent.Tool.Input.Ended, {
              sessionID,
              assistantMessageID,
              id: callID,
              text: JSON.stringify(input),
            }),
          )
          yield* publish(
            "update",
            bus.publish(SessionEvent.Tool.Called, { sessionID, assistantMessageID, id: callID, input, executed: true }),
          )
          yield* publish(
            "update",
            bus.publish(SessionEvent.Tool.Success, {
              sessionID,
              assistantMessageID,
              id: callID,
              content: [{ type: "text", text: bytes(config.outputBytes) }],
              executed: true,
            }),
          )
        }
        yield* publish("update", bus.publish(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 1 }))
        yield* publish(
          "update",
          bus.publish(SessionEvent.Text.Ended, { sessionID, assistantMessageID, ordinal: 1, text: words(60) }),
        )
        yield* publish("update", bus.publish(SessionEvent.Step.Streamed, { sessionID, assistantMessageID }))
        yield* publish(
          "update",
          bus.publish(SessionEvent.Step.Ended, {
            sessionID,
            assistantMessageID,
            finish: "stop",
            cost: Money.USD.make(0),
            tokens,
          }),
        )
        if (mode.drain === "step") yield* background
      }
      yield* publish("update", bus.publish(SessionEvent.Renamed, { sessionID, title: `${words(5)} renamed` }))
    }
    // Delete every tenth session, cascading its messages.
    for (const sessionID of sessionIDs.filter((_, index) => index % 10 === 9))
      yield* publish("delete", bus.publish(SessionEvent.Deleted, { sessionID }))
    peakPending = Math.max(peakPending, yield* pending(db))
    return { inserts, updates, deletes, drains, peakPending, backgroundMs }
  })

const runMode = (mode: Mode) =>
  withDatabase(
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const bus = yield* Bus.Service
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* seedSessions(db)
      if (mode.index) {
        const capability = yield* SessionSearchIndex.ensure(db)
        if (!capability.supported) return yield* Effect.die(new Error(`Index unsupported: ${capability.reason}`))
        let done = false
        while (!done) done = (yield* SessionSearchIndex.backfillBatch(db, 500)).done
        yield* drain(db, 500)
      }
      const page = yield* pageSize(db)
      yield* walFrames(db)
      const write = yield* timed(workload(db, bus, mode))
      const writeFrames = yield* walFrames(db)
      const final = mode.index ? yield* timed(drain(db, config.batch)) : undefined
      const drainFrames = mode.index ? yield* walFrames(db) : 0
      const status = mode.index ? yield* SessionSearchIndex.status(db) : undefined
      const hits = mode.index ? (yield* SessionSearchIndex.search(db, { query: "websocket", limit: 5 })).length : 0
      const messages = yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM ${SessionMessageTable}`)
      const drains = [...write.value.drains, ...(final ? [final.value] : [])]
      const dequeued = drains.reduce((sum, result) => sum + result.sources, 0)
      const drainBatches = drains.flatMap((result) => result.batches)
      const drainMs = drainBatches.reduce((sum, ms) => sum + ms, 0)
      return {
        mode: mode.name,
        events: write.value.inserts.length + write.value.updates.length + write.value.deletes.length,
        writeMs: write.ms - write.value.backgroundMs,
        insert: stats(write.value.inserts),
        update: stats(write.value.updates),
        delete: stats(write.value.deletes),
        peakPending: write.value.peakPending,
        dequeued,
        drainMs,
        drainBatch: stats(drainBatches),
        heapPeakMB: Math.max(0, ...drains.map((result) => result.heapPeakMB)),
        rssPeakMB: Math.max(0, ...drains.map((result) => result.rssPeakMB)),
        walWriteMB: (writeFrames * (page + 24)) / 1_048_576,
        walDrainMB: (drainFrames * (page + 24)) / 1_048_576,
        bytes: yield* tableBytes(db),
        documents: status?.documents ?? 0,
        messages: messages?.count ?? 0,
        hits,
        sqlite: (yield* db.get<{ version: string }>(sql.raw("SELECT sqlite_version() AS version")))?.version,
      }
    }),
  )

/** One bare UPDATE of an assistant row, with and without the triggers: the trigger cost alone. */
const statement = (index: boolean) =>
  withDatabase(
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      yield* seedSessions(db)
      if (index) yield* SessionSearchIndex.ensure(db)
      const messageID = SessionMessage.ID.make("msg_statement")
      const data = {
        agent: Agent.defaultID,
        model,
        content: [{ type: "text" as const, text: bytes(config.outputBytes) }],
        time: { created: 1 },
      }
      yield* db
        .insert(SessionMessageTable)
        .values({ id: messageID, session_id: sessionIDs[0]!, type: "assistant", seq: 1, time_created: 1, data })
        .run()
      const samples: number[] = []
      for (let iteration = 0; iteration < 2_000; iteration++) {
        const result = yield* timed(
          db
            .update(SessionMessageTable)
            .set({ data: { ...data, time: { created: iteration } } })
            .where(sql`${SessionMessageTable.id} = ${messageID}`)
            .run(),
        )
        samples.push(result.ms)
      }
      return stats(samples.slice(200))
    }),
  )

/** Backlog cleanup: requeue everything, drain at several batch sizes; then mass delete; then a full rebuild. */
const backlog = () =>
  withDatabase(
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const bus = yield* Bus.Service
      yield* seedSessions(db)
      yield* SessionSearchIndex.ensure(db)
      yield* workload(db, bus, { name: "seed", index: true, drain: "none" })
      yield* drain(db, 500)
      const requeue = sql`INSERT INTO ${SessionSearchDirtyTable} (source_id)
        SELECT id FROM ${SessionMessageTable} UNION ALL SELECT id FROM ${SessionTable}`
      const batches = []
      for (const batch of config.backlogBatches) {
        yield* db.run(requeue)
        const queued = yield* pending(db)
        const result = yield* timed(drain(db, batch))
        batches.push({
          batch,
          queued,
          ms: result.ms,
          batchMs: stats(result.value.batches),
          heapPeakMB: result.value.heapPeakMB,
        })
      }
      const half = sessionIDs.filter((_, index) => index % 2 === 0 && index % 10 !== 9)
      yield* Effect.forEach(half, (sessionID) => bus.publish(SessionEvent.Deleted, { sessionID }), { discard: true })
      const deleteQueued = yield* pending(db)
      const deleted = yield* timed(drain(db, config.batch))
      yield* db
        .update(KVTable)
        .set({ value: { version: -1, backfill: "done" } })
        .where(sql`${KVTable.key} = 'session.search'`)
        .run()
      const rebuild = yield* timed(SessionSearchIndex.ensure(db))
      const backfillBatches: number[] = []
      while (true) {
        const result = yield* timed(SessionSearchIndex.backfillBatch(db, config.batch))
        backfillBatches.push(result.ms)
        if (result.value.done) break
      }
      return {
        batches,
        deleted: { queued: deleteQueued, ms: deleted.ms, batchMs: stats(deleted.value.batches) },
        rebuild: {
          ensureMs: rebuild.ms,
          backfillMs: stats(backfillBatches),
          documents: (yield* SessionSearchIndex.status(db)).documents,
        },
      }
    }),
  )

/**
 * `ensure` on a database without the index (it builds it), then repeatedly on a current one, as every start would.
 * Each call's WAL frames and schema version changes are counted: a schema change makes every other connection
 * re-read the schema before its next statement.
 */
const ensureCost = () =>
  withDatabase(
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* seedSessions(db)
      const page = yield* pageSize(db)
      const schemaVersion = db
        .get<{ schema_version: number }>(sql.raw("PRAGMA schema_version"))
        .pipe(Effect.map((row) => row?.schema_version ?? 0))
      yield* walFrames(db)
      const coldVersion = yield* schemaVersion
      const cold = yield* timed(SessionSearchIndex.ensure(db))
      const coldFrames = yield* walFrames(db)
      const warmVersion = yield* schemaVersion
      const warm: number[] = []
      const warmKB: number[] = []
      for (let iteration = 0; iteration < 50; iteration++) {
        const result = yield* timed(SessionSearchIndex.ensure(db))
        warm.push(result.ms)
        warmKB.push(((yield* walFrames(db)) * (page + 24)) / 1024)
      }
      return {
        cold: { ms: cold.ms, walKB: (coldFrames * (page + 24)) / 1024, schemaChanges: warmVersion - coldVersion },
        warm: {
          ms: stats(warm),
          walKB: stats(warmKB),
          schemaChangesPerCall: ((yield* schemaVersion) - warmVersion) / 50,
        },
      }
    }),
  )

const results: Array<Awaited<ReturnType<typeof runMode>>> = []
for (let repeat = 0; repeat < config.repeats; repeat++)
  for (const mode of modes) {
    seed = 0x5eed
    results.push(await runMode(mode))
  }
const statementOff = await statement(false)
const statementOn = await statement(true)
seed = 0x5eed
const cleanup = await backlog()
const ensured = await ensureCost()

if (args.values.json) {
  console.log(
    JSON.stringify(
      {
        config,
        runtime: process.versions,
        results,
        statement: { off: statementOff, on: statementOn },
        backlog: cleanup,
        ensure: ensured,
      },
      null,
      2,
    ),
  )
  process.exit(0)
}

const ms = (value: number) => `${value.toFixed(3)} ms`
const median = (values: ReadonlyArray<number>) => stats(values).p50
console.log(
  `sessions ${config.sessions}, steps ${config.steps}, tools ${config.tools}, output ${config.outputBytes} B, repeats ${config.repeats}, drain batch ${config.batch}`,
)
console.log("\nFull write path (bus.publish of durable events), medians across repeats:")
for (const mode of modes) {
  const runs = results.filter((result) => result.mode === mode.name)
  const first = runs[0]!
  console.log(`\n  ${mode.name}`)
  console.log(
    `    events firing triggers ${first.events}, peak queue ${first.peakPending}, messages ${first.messages}, documents ${first.documents}, "websocket" hits ${first.hits}`,
  )
  console.log(
    `    insert p50 ${ms(median(runs.map((run) => run.insert.p50)))} p95 ${ms(median(runs.map((run) => run.insert.p95)))} | update p50 ${ms(median(runs.map((run) => run.update.p50)))} p95 ${ms(median(runs.map((run) => run.update.p95)))} | delete p50 ${ms(median(runs.map((run) => run.delete.p50)))}`,
  )
  const write = median(runs.map((run) => run.writeMs))
  const drained = median(runs.map((run) => run.drainMs))
  console.log(
    `    writes ${write.toFixed(0)} ms, drain ${drained.toFixed(0)} ms over ${median(runs.map((run) => run.drainBatch.count))} batches (p95 ${ms(median(runs.map((run) => run.drainBatch.p95)))}, max ${ms(median(runs.map((run) => run.drainBatch.max)))}), sources dequeued ${median(runs.map((run) => run.dequeued))}`,
  )
  console.log(
    `    amortized (write + drain) per event ${ms((write + drained) / first.events)}, WAL write ${median(runs.map((run) => run.walWriteMB)).toFixed(1)} MB, WAL drain ${median(runs.map((run) => run.walDrainMB)).toFixed(1)} MB, drain heap peak +${median(runs.map((run) => run.heapPeakMB)).toFixed(1)} MB`,
  )
  if (first.bytes)
    console.log(
      `    bytes: session_message ${(first.bytes.session_message / 1_048_576).toFixed(1)} MB, fts ${(first.bytes.fts / 1024).toFixed(0)} KB, documents ${(first.bytes.session_search_document / 1024).toFixed(0)} KB, dirty ${(first.bytes.session_search_dirty / 1024).toFixed(0)} KB`,
    )
}
console.log("\nStatement only (one UPDATE of a session_message row, no bus/projector):")
console.log(`  triggers off p50 ${ms(statementOff.p50)} p95 ${ms(statementOff.p95)}`)
console.log(`  triggers on  p50 ${ms(statementOn.p50)} p95 ${ms(statementOn.p95)}`)
console.log("\nBacklog cleanup:")
for (const batch of cleanup.batches)
  console.log(
    `  requeue ${batch.queued} sources, batch ${batch.batch}: ${batch.ms.toFixed(0)} ms total, batch p50 ${ms(batch.batchMs.p50)} max ${ms(batch.batchMs.max)}, heap peak +${batch.heapPeakMB.toFixed(1)} MB`,
  )
console.log(
  `  delete half the sessions: ${cleanup.deleted.queued} queued, drained in ${cleanup.deleted.ms.toFixed(0)} ms (batch max ${ms(cleanup.deleted.batchMs.max)})`,
)
console.log(
  `  rebuild: ensure ${ms(cleanup.rebuild.ensureMs)}, backfill ${cleanup.rebuild.backfillMs.total.toFixed(0)} ms over ${cleanup.rebuild.backfillMs.count} batches (max ${ms(cleanup.rebuild.backfillMs.max)}), ${cleanup.rebuild.documents} documents`,
)
console.log("\nensure:")
console.log(
  `  cold (builds the index): ${ms(ensured.cold.ms)}, WAL ${ensured.cold.walKB.toFixed(0)} KB, schema changes ${ensured.cold.schemaChanges}`,
)
console.log(
  `  warm (index current, 50 calls): p50 ${ms(ensured.warm.ms.p50)} p95 ${ms(ensured.warm.ms.p95)}, WAL p50 ${ensured.warm.walKB.p50.toFixed(0)} KB per call, schema changes per call ${ensured.warm.schemaChangesPerCall}`,
)
