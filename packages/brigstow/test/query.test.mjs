import assert from "node:assert/strict"
import { getEventListeners } from "node:events"
import test from "node:test"
import { DocumentUnavailableError, Repo, mapQuery, mapQueryAsync } from "../dist/index.js"

const id = new Uint8Array(16)
const tick = () => new Promise(resolve => setImmediate(resolve))

// Consumers must subscribe before reading the initial snapshot.
class TestQuery {
  listeners = new Set()
  disposed = false

  constructor(current) {
    this.current = current
  }

  id() { return id }
  state() {
    assert.ok(this.listeners.size > 0, "subscribe before reading state")
    return this.current
  }

  subscribe(callback) {
    this.listeners.add(callback)
    return () => this.listeners.delete(callback)
  }

  publish(state) {
    this.current = state
    for (const listener of [...this.listeners]) listener(state)
  }

  dispose() {
    this.disposed = true
    this.listeners.clear()
  }
}

for (const [name, map] of [
  ["mapQuery", mapQuery],
  ["mapQueryAsync", (query, f) => mapQueryAsync(query, async value => f(value))],
]) {
  test(`${name} initializes from a snapshot without replaying it to subscribers`, async () => {
    const source = new TestQuery({ type: "ready", handle: 2 })
    let calls = 0
    const query = map(source, value => { calls++; return value * 2 })
    await tick()
    assert.equal(calls, 1)

    const states = []
    const unsubscribe = query.subscribe(state => states.push(state))
    assert.deepEqual(states, [])
    assert.deepEqual(query.state(), { type: "ready", handle: 4 })
    assert.deepEqual(states, [])
    source.publish({ type: "unavailable" })
    assert.deepEqual(states.at(-1), { type: "unavailable" })

    unsubscribe()
    source.publish({ type: "finding" })
    assert.equal(states.length, 1)
    query.dispose()
    assert.equal(source.listeners.size, 0)
    assert.equal(source.disposed, false)
  })

  test(`${name} notifies only on subsequent transitions, including reentrant ones`, () => {
    const source = new TestQuery({ type: "finding" })
    const query = map(source, value => value)
    const states = []
    const unsubscribe = query.subscribe(state => {
      states.push(state.type)
      if (state.type === "finding") source.publish({ type: "unavailable" })
    })
    assert.deepEqual(states, [])
    source.publish({ type: "finding" })
    assert.deepEqual(states, ["finding", "unavailable"])
    unsubscribe()
    query.dispose()
    assert.throws(() => query.subscribe(() => {}), /disposed/)
  })
}

test("async mapping exposes finding via a snapshot, then notifies when ready", async () => {
  const source = new TestQuery({ type: "ready", handle: 1 })
  const query = mapQueryAsync(source, async value => value + 1)
  const states = []
  query.subscribe(state => states.push(state))
  assert.deepEqual(states, [])
  assert.deepEqual(query.state(), { type: "finding" })
  await tick()
  assert.deepEqual(states, [{ type: "ready", handle: 2 }])
  query.dispose()
})

class TestRepo extends Repo {
  constructor(query) {
    super({})
    this.result = query
  }
  query() { return this.result }
}

for (const type of ["ready", "unavailable", "failed"]) {
  test(`find handles an initial ${type} snapshot and cleans up`, async () => {
    const handle = {}
    const error = new Error("failed")
    const query = new TestQuery({ type, handle, error })
    const controller = new AbortController()
    const promise = new TestRepo(query).find({}, id, { signal: controller.signal })
    if (type === "ready") assert.equal(await promise, handle)
    else await assert.rejects(promise, e => type === "failed" ? e === error : e instanceof DocumentUnavailableError)
    assert.equal(query.listeners.size, 0)
    assert.equal(query.disposed, true)
    assert.equal(getEventListeners(controller.signal, "abort").length, 0)
  })
}

test("find waits for a transition and the first terminal notification wins", async () => {
  const query = new TestQuery({ type: "finding" })
  const promise = new TestRepo(query).find({}, id)
  const handle = {}
  assert.equal(query.listeners.size, 1)
  query.publish({ type: "ready", handle })
  query.publish({ type: "failed", error: new Error("too late") })
  assert.equal(await promise, handle)
  assert.equal(query.listeners.size, 0)
  assert.equal(query.disposed, true)
})

test("find rejects on abort and removes both listeners", async () => {
  const query = new TestQuery({ type: "finding" })
  const controller = new AbortController()
  const reason = new Error("stop")
  const promise = new TestRepo(query).find({}, id, { signal: controller.signal })
  controller.abort(reason)
  await assert.rejects(promise, e => e === reason)
  assert.equal(query.listeners.size, 0)
  assert.equal(query.disposed, true)
  assert.equal(getEventListeners(controller.signal, "abort").length, 0)
})

test("find with an already aborted signal does not create a query", async () => {
  const repo = new Repo({ find() { assert.fail("source should not be called") } })
  const controller = new AbortController()
  controller.abort()
  await assert.rejects(repo.find({}, id, { signal: controller.signal }), { name: "AbortError" })
})

test("repository query does not replay and find disposes its owned source", async () => {
  const source = new TestQuery({ type: "unavailable" })
  const repo = new Repo({ find: () => source })
  const query = repo.query({}, id)
  const states = []
  query.subscribe(state => states.push(state))
  assert.deepEqual(states, [])
  assert.deepEqual(query.state(), { type: "unavailable" })
  source.publish({ type: "finding" })
  assert.deepEqual(states, [{ type: "finding" }])
  query.dispose()
  assert.equal(source.disposed, true)

  const nextSource = new TestQuery({ type: "unavailable" })
  await assert.rejects(new Repo({ find: () => nextSource }).find({}, id), DocumentUnavailableError)
  assert.equal(nextSource.disposed, true)
  assert.equal(nextSource.listeners.size, 0)
})
