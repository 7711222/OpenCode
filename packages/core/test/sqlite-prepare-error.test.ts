import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import { isSqlError } from "effect/sql/SqlError"

// Both adapters export `sqliteLayer`; bind each module whole instead of aliasing the import.
const BunSqlite = await import("@opencode/core/database/sqlite.bun")
const NodeSqlite = await import("@opencode/core/database/sqlite.node")

// SQLite rejects a missing table, bad syntax, or a virtual table it cannot open while preparing the statement, before
// any row is read. Those errors must reach callers as SqlError failures, like errors raised while stepping, so code
// such as the session search probe can catch and classify them.
const adapters = [
  ["bun:sqlite", () => BunSqlite.sqliteLayer({ filename: ":memory:", disableWAL: true })],
  ["node:sqlite", () => NodeSqlite.sqliteLayer({ filename: ":memory:", disableWAL: true })],
] as const

const run = <A, E>(layer: (typeof adapters)[number][1], effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.runPromise(effect.pipe(Effect.provide(layer()), Effect.scoped))

// Effect.flip only sees failures: a defect still rejects the promise and fails the test.
const failure = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.flip(effect).pipe(
    Effect.map((error) => {
      expect(isSqlError(error)).toBe(true)
      return isSqlError(error) ? error.reason : undefined
    }),
  )

describe.each(adapters)("%s prepare errors", (_, layer) => {
  test("fails with SqlError on a missing table for rows and values, and the connection keeps working", async () => {
    const result = await run(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const rows = yield* failure(sql`SELECT * FROM missing`)
        const values = yield* failure(sql`SELECT * FROM missing`.values)
        return {
          rows: String(rows?.cause),
          values: String(values?.cause),
          after: yield* sql<{ ok: number }>`SELECT 1 AS ok`,
          afterValues: yield* sql`SELECT 1 AS ok`.values,
        }
      }),
    )
    expect(result.rows).toContain("no such table: missing")
    expect(result.values).toContain("no such table: missing")
    expect(result.after).toEqual([{ ok: 1 }])
    expect(result.afterValues).toEqual([[1]])
  })

  test("fails with SqlError on a syntax error for rows and values", async () => {
    const result = await run(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        return {
          rows: String((yield* failure(sql.unsafe("SELEC 1")))?.cause),
          values: String((yield* failure(sql.unsafe("SELEC 1").values))?.cause),
          after: yield* sql.unsafe("SELECT 2 AS ok").values,
        }
      }),
    )
    expect(result.rows).toContain("syntax error")
    expect(result.values).toContain("syntax error")
    expect(result.after).toEqual([[2]])
  })

  test("still fails with SqlError on a constraint violation raised while stepping", async () => {
    const reason = await run(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        yield* sql`CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`
        yield* sql`INSERT INTO item (name) VALUES (${"one"})`
        return yield* failure(sql`INSERT INTO item (name) VALUES (${"one"})`)
      }),
    )
    expect(String(reason?.cause)).toContain("UNIQUE constraint failed: item.name")
  })

  test("reads integers as bigints only when SafeIntegers is set, for rows and values", async () => {
    const result = await run(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const small = sql<{ n: unknown }>`SELECT 42 AS n`
        const large = sql<{ n: unknown }>`SELECT 9007199254740993 AS n`
        const safe = Effect.provideService(SqlClient.SafeIntegers, true)
        return {
          plain: (yield* small)[0]?.n,
          plainValues: (yield* small.values)[0]?.[0],
          safe: (yield* large.pipe(safe))[0]?.n,
          safeValues: (yield* large.values.pipe(safe))[0]?.[0],
        }
      }),
    )
    expect(result).toEqual({ plain: 42, plainValues: 42, safe: 9007199254740993n, safeValues: 9007199254740993n })
  })
})
