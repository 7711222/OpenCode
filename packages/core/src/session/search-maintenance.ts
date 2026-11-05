export * as SessionSearchMaintenance from "./search-maintenance.js"

import { Cause, Clock, Context, Effect, Latch, Layer, Option, RcMap, Schema, Stream } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { App } from "../app.js"
import { Bus } from "../bus.js"
import { Config } from "../config.js"
import { Database } from "../database/database.js"
import { Flag } from "../flag.js"
import { LocationServiceMap } from "../location-service-map.js"
import { SessionEvent } from "./event.js"
import { SessionSearchIndex } from "./search-index.js"

export interface Interface {
  /**
   * Keeps the search index in step with `experimental.sqlite_fts`: builds it, backfills existing history in small
   * transactions, then drains changes as Sessions write. Runs until interrupted, so fork it.
   */
  readonly run: Effect.Effect<never>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionSearchMaintenance") {}

const isSessionEvent = Schema.is(SessionEvent.Durable)

/** Work this long before re-reading config, so turning the flag off stops a backfill promptly. */
const SLICE = 1_000
/** Pause between transactions so requests waiting on the JS thread or the write lock get a turn. */
const PAUSE = "5 millis"
/** Re-read config at least this often, so a location that enables the index starts it without activity. */
const IDLE = "30 seconds"
/** After activity, wait this long so a streaming reply settles into one batch. */
const SETTLE = "1 second"
const SLOW_BATCH = 250
/**
 * Backfill batches grow or shrink to take about this long, whatever the size of the messages in them: each one is a
 * synchronous transaction that holds the JS thread and the write lock.
 */
const TARGET_BATCH = { min: 10, max: 40 }
const BATCH_LIMIT = { min: 1, initial: 25, max: 200 }
/**
 * While Sessions are writing, the backfill slows to one batch per this long, so it barely competes with a streaming
 * reply but still finishes. Changes to the queue are drained at full speed either way.
 */
const BUSY_PAUSE = "250 millis"
const BUSY_FOR = 2_000

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const database = yield* Database.Service
    const db = database.db
    const bus = yield* Bus.Service
    const locations = yield* LocationServiceMap.Service
    const doorbell = yield* Latch.make()
    const resolve = yield* Flag.resolver
    const state = {
      active: false,
      unsupported: false,
      backfilled: false,
      backfill: undefined as { started: number; batches: number; slowest: number } | undefined,
      limit: BATCH_LIMIT.initial,
      activity: 0,
    }

    yield* bus.subscribe().pipe(
      Stream.filter((event) => state.active && isSessionEvent(event)),
      Stream.runForEach(() =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => {
            state.activity = now
            return doorbell.open
          }),
        ),
      ),
      Effect.forkScoped,
    )

    // Config is per location, but the index covers the whole database: it runs while any loaded location enables
    // it, and is dropped once every loaded location has it off. Undefined when no location says either way.
    const configured = Effect.gen(function* () {
      const flags = yield* Effect.forEach(Array.from(yield* RcMap.keys(locations.rcMap)), (ref) =>
        Effect.gen(function* () {
          const context = yield* locations.contextEffectOption(ref)
          if (Option.isNone(context)) return undefined
          const entries = yield* Context.get(context.value, Config.Service).entries()
          return resolve(entries).sqlite_fts.enabled
        }).pipe(
          Effect.scoped,
          Effect.orElseSucceed(() => undefined),
        ),
      )
      if (flags.includes(true)) return true
      if (flags.length > 0 && flags.every((flag) => flag === false)) return false
      return undefined
    })

    /** Runs one transaction and returns how long to pause before the next, or undefined when nothing is left. */
    const batch = Effect.gen(function* () {
      if ((yield* SessionSearchIndex.processBatch(db)) > 0) return PAUSE
      if (state.backfilled) return undefined
      const now = yield* Clock.currentTimeMillis
      const result = yield* SessionSearchIndex.backfillBatch(db, state.limit)
      const elapsed = (yield* Clock.currentTimeMillis) - now
      if (elapsed < TARGET_BATCH.min) state.limit = Math.min(BATCH_LIMIT.max, state.limit * 2)
      if (elapsed > TARGET_BATCH.max) state.limit = Math.max(BATCH_LIMIT.min, Math.floor(state.limit / 2))
      if (result.done && state.backfill === undefined) {
        state.backfilled = true
        return undefined
      }
      const backfill = state.backfill ?? { started: now, batches: 0, slowest: 0 }
      state.backfill = backfill
      backfill.batches++
      backfill.slowest = Math.max(backfill.slowest, elapsed)
      if (elapsed > SLOW_BATCH) yield* Effect.logWarning("session search backfill batch was slow", { ms: elapsed })
      if (!result.done) return now - state.activity < BUSY_FOR ? BUSY_PAUSE : PAUSE
      state.backfilled = true
      state.backfill = undefined
      const status = yield* SessionSearchIndex.status(db)
      yield* Effect.logInfo("session search backfill finished", {
        ms: now + elapsed - backfill.started,
        batches: backfill.batches,
        slowestBatchMs: backfill.slowest,
        documents: status.documents,
      })
      return undefined
    })

    /** Returns true when work remains. */
    const tick = Effect.gen(function* () {
      const enabled = yield* configured
      state.active = enabled === true
      if (enabled === false && (yield* SessionSearchIndex.disable(db))) {
        state.backfilled = false
        state.backfill = undefined
        yield* Effect.logInfo("session search index dropped")
      }
      if (enabled !== true || state.unsupported) return false
      const capability = yield* SessionSearchIndex.ensure(db)
      if (!capability.supported) {
        state.unsupported = true
        yield* Effect.logWarning("session search index is not supported by this SQLite", {
          reason: capability.reason,
        })
        return false
      }
      if (capability.rebuilt) {
        state.backfilled = false
        state.backfill = undefined
        yield* Effect.logInfo("session search backfill started")
      }
      const start = yield* Clock.currentTimeMillis
      while ((yield* Clock.currentTimeMillis) - start < SLICE) {
        const pause = yield* batch
        if (pause === undefined) return false
        yield* Effect.sleep(pause)
      }
      return true
    })

    const loop = Effect.gen(function* () {
      yield* doorbell.close
      const more = yield* tick.pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) => Effect.logError("session search maintenance failed", { cause }).pipe(Effect.as(false)),
        ),
      )
      if (more) return yield* Effect.sleep(PAUSE)
      yield* doorbell.await.pipe(Effect.timeoutOption(IDLE))
      yield* Effect.sleep(SETTLE)
    })

    return Service.of({ run: Effect.forever(loop) })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [App.node, Bus.node, Database.node, LocationServiceMap.node],
})
