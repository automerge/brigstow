import assert from "node:assert/strict"
import test from "node:test"
import { DocHandle } from "../dist/DocHandle.js"
import { Repo } from "../dist/index.js"
import { RefreshScheduler } from "../dist/RefreshScheduler.js"

const tick = () => new Promise(resolve => setImmediate(resolve))

function deferred() {
  let resolve, reject
  const promise = new Promise((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

async function setup(createHandle = true) {
  // A grow-only set CRDT: each independent value is a graph head.
  const meta = n => ({ kind: "commit", head: String(n), parents: [] })
  const persisted = new Map([["0", meta(0)]])
  const batches = []
  const listeners = new Set()
  const emit = () => { for (const listener of listeners) listener() }
  const receive = (...values) => {
    for (const value of values) persisted.set(String(value), meta(value))
    emit()
  }
  const source = {
    documentId: new Uint8Array(16),
    documentType: "set",
    heads: () => [...persisted.keys()],
    metadata: () => persisted.values(),
    async materialize(metas) { return metas.map(meta => new Uint8Array([Number(meta.head)])) },
    on(event, listener) { assert.equal(event, "change"); listeners.add(listener) },
    off(event, listener) { assert.equal(event, "change"); listeners.delete(listener) },
    async apply(records) {
      batches.push(records.map(record => Number(record.head)))
      for (const record of records) persisted.set(record.head, record)
      emit()
    },
  }
  const adapter = {
    metadata: state => state.map(meta),
    materialize: (_state, metas) => metas.map(meta => new Uint8Array([Number(meta.head)])),
    apply: (state, records) => [...new Set([...state, ...records.map(record => record.bytes[0])])].sort((a, b) => a - b),
  }
  const docType = {
    name: "set",
    empty: () => [],
    init: state => state,
    view: state => state,
    heads: state => state.map(String),
    change: (state, n) => [...state, n],
    sedimentree: adapter,
  }
  const scheduler = new RefreshScheduler({ onError: error => console.error("Document refresh failed", error) })
  const handle = createHandle ? new DocHandle(source, docType, [0]) : undefined
  if (handle) await scheduler.schedule(handle, source)
  return { handle, scheduler, source, docType, adapter, batches, persisted, receive, emit, listeners }
}

test("change updates synchronously, serializes saves, and flush waits for queued edits", async t => {
  const { handle, source, batches } = await setup()
  const entered = deferred(), release = deferred()
  const apply = source.apply.bind(source)
  let calls = 0
  t.mock.method(source, "apply", async records => {
    if (++calls === 1) {
      entered.resolve()
      await release.promise
    }
    await apply(records)
  })
  const notifications = []
  handle.on("change", ({ doc }) => notifications.push(doc))
  const first = handle.change(1)
  assert.deepEqual(handle.doc(), [0, 1])
  assert.deepEqual(notifications, [[0, 1]])
  await entered.promise
  const second = handle.change(2)
  const third = handle.change(3)
  assert.deepEqual(handle.doc(), [0, 1, 2, 3])
  let flushed = false
  const flushing = handle.flush().then(() => { flushed = true })
  await Promise.resolve()
  assert.equal(flushed, false)
  assert.equal(calls, 1, "writes must not overlap")
  release.resolve()
  await Promise.all([first, second, third, flushing])
  assert.equal(flushed, true)
  assert.deepEqual(batches, [[1], [2, 3]], "do not rewrite stored records; coalesce queued edits")
})

for (const stage of ["materialize", "apply"]) {
  test(`${stage} failures reject change/flush, and later edits retry missing records`, async t => {
    const { handle, source, adapter, persisted, batches } = await setup()
    const logs = t.mock.method(console, "error", () => {})
    const error = new Error("storage unavailable")
    const target = stage === "apply" ? source : adapter
    const original = target[stage].bind(target)
    let fail = true
    t.mock.method(target, stage, async (...args) => {
      if (fail) throw error
      return original(...args)
    })
    await assert.rejects(handle.change(1), reason => reason === error)
    await assert.rejects(handle.flush(), reason => reason === error)
    assert.deepEqual(handle.doc(), [0, 1], "failed persistence must not discard the local edit")
    assert.deepEqual([...persisted.keys()], ["0"])
    assert.equal(logs.mock.callCount(), 1)

    fail = false
    await handle.change(2)
    await handle.flush()
    assert.deepEqual(batches, [[1, 2]], "retry failed edits along with the new change")
    assert.deepEqual([...persisted.keys()], ["0", "1", "2"])
  })
}

test("incoming records notify once without saving an echo; own writes and reordered heads are silent", async t => {
  const { handle, source, receive, emit, batches } = await setup()
  const notifications = []
  handle.on("change", ({ handle: changed, doc }) => {
    assert.equal(changed, handle)
    notifications.push(doc)
  })
  receive(1)
  await tick()
  assert.deepEqual(handle.doc(), [0, 1])
  assert.deepEqual(handle.heads(), ["0", "1"], "use DocType.heads, not a state method")
  assert.deepEqual(notifications, [[0, 1]])
  assert.deepEqual(batches, [])

  await handle.change(2)
  await tick()
  assert.deepEqual(notifications, [[0, 1], [0, 1, 2]])
  assert.deepEqual(batches, [[2]])
  const materialize = t.mock.method(source, "materialize")
  t.mock.method(source, "heads", () => ["2", "1", "0"])
  emit()
  emit()
  await tick()
  assert.equal(materialize.mock.callCount(), 0)
  assert.equal(notifications.length, 2)
})

test("a delayed incoming read merges into current local state, including unsaved edits", async t => {
  const { handle, source, receive, batches } = await setup()
  const entered = deferred(), readRelease = deferred(), saveRelease = deferred()
  const materialize = source.materialize.bind(source)
  t.mock.method(source, "materialize", async metas => {
    entered.resolve()
    await readRelease.promise
    return materialize(metas)
  })
  const apply = source.apply.bind(source)
  t.mock.method(source, "apply", async records => {
    await saveRelease.promise
    await apply(records)
  })
  const notifications = []
  handle.on("change", ({ doc }) => notifications.push(doc))
  receive(1)
  await entered.promise
  const saved = handle.change(2)
  assert.deepEqual(handle.doc(), [0, 2])
  readRelease.resolve()
  await tick()
  assert.deepEqual(handle.doc(), [0, 1, 2], "must not replace the state captured before IO")
  assert.deepEqual(batches, [], "local persistence is still blocked")
  saveRelease.resolve()
  await saved
  await tick()
  assert.deepEqual(notifications, [[0, 2], [0, 1, 2]])
  assert.deepEqual(batches, [[2]], "incoming record must not be echoed")
})

test("reapplying an older source snapshot does not notify for unchanged logical heads", async t => {
  const { handle, source, emit } = await setup()
  const release = deferred()
  const apply = source.apply.bind(source)
  t.mock.method(source, "apply", async records => {
    await release.promise
    await apply(records)
  })
  const notifications = []
  handle.on("change", ({ doc }) => notifications.push(doc))
  const saved = handle.change(1)
  const materialize = t.mock.method(source, "materialize")
  emit()
  await tick()
  assert.equal(materialize.mock.callCount(), 1, "source heads differ from the unsaved local heads")
  assert.deepEqual(handle.doc(), [0, 1])
  assert.deepEqual(notifications, [[0, 1]], "a new state object with identical heads is not a change")
  release.resolve()
  await saved
  await tick()
  assert.equal(notifications.length, 1)
})

test("refreshes serialize and coalesce events, including events during a read", async t => {
  const { handle, source, receive, emit } = await setup()
  const entered = deferred(), release = deferred()
  const materialize = source.materialize.bind(source)
  let calls = 0, active = 0, maxActive = 0
  t.mock.method(source, "materialize", async metas => {
    maxActive = Math.max(maxActive, ++active)
    if (++calls === 1) {
      entered.resolve()
      await release.promise
    }
    const result = await materialize(metas)
    active--
    return result
  })
  receive(1)
  emit()
  await entered.promise
  receive(2)
  receive(3)
  emit()
  assert.equal(calls, 1)
  release.resolve()
  await tick()
  assert.deepEqual(handle.doc(), [0, 1, 2, 3])
  assert.equal(calls, 2, "one follow-up read should cover all intervening events")
  assert.equal(maxActive, 1)
})

for (const stage of ["metadata", "materialize", "apply"]) {
  test(`incoming ${stage} failure is logged and a later event recovers`, async t => {
    const { handle, source, adapter, receive, emit } = await setup()
    const logs = t.mock.method(console, "error", () => {})
    const target = stage === "apply" ? adapter : source
    const original = target[stage].bind(target)
    const error = new Error("incoming unavailable")
    let fail = true
    t.mock.method(target, stage, (...args) => {
      if (fail) {
        if (stage === "materialize") return Promise.reject(error)
        throw error
      }
      return original(...args)
    })
    receive(1)
    await tick()
    assert.deepEqual(handle.doc(), [0])
    assert.equal(logs.mock.callCount(), 1)
    assert.equal(logs.mock.calls[0].arguments[1], error)
    fail = false
    emit()
    await tick()
    assert.deepEqual(handle.doc(), [0, 1])
  })
}

test("an event during failed IO is not lost", async t => {
  const { handle, source, receive } = await setup()
  const logs = t.mock.method(console, "error", () => {})
  const entered = deferred(), release = deferred()
  const materialize = source.materialize.bind(source)
  let calls = 0
  t.mock.method(source, "materialize", async metas => {
    if (++calls === 1) {
      entered.resolve()
      await release.promise
    }
    return materialize(metas)
  })
  receive(1)
  await entered.promise
  receive(2)
  release.reject(new Error("temporary read failure"))
  await tick()
  assert.deepEqual(handle.doc(), [0, 1, 2])
  assert.equal(calls, 2)
  assert.equal(logs.mock.callCount(), 1)
})

test("Repo.find subscribes before initial loading and catches arrivals during loading", async t => {
  const { source, docType, receive, listeners } = await setup(false)
  const entered = deferred(), release = deferred()
  const metadata = source.metadata.bind(source)
  t.mock.method(source, "metadata", (...args) => {
    assert.equal(listeners.size, 1, "subscribe before reading the snapshot")
    return metadata(...args)
  })
  const materialize = source.materialize.bind(source)
  let calls = 0
  t.mock.method(source, "materialize", async metas => {
    if (++calls === 1) {
      entered.resolve()
      await release.promise
    }
    return materialize(metas)
  })
  let disposed = false
  const repo = new Repo({ find: () => ({
    id: () => source.documentId,
    state: () => ({ type: "ready", handle: source }),
    subscribe: () => () => {},
    dispose: () => { disposed = true },
  }) })
  const finding = repo.find(docType, source.documentId)
  await entered.promise
  receive(1)
  release.resolve()
  const handle = await finding
  assert.deepEqual(handle.doc(), [0, 1])
  assert.equal(disposed, true)
  receive(2)
  await tick()
  assert.deepEqual(handle.doc(), [0, 1, 2], "disposing the query must not stop the returned handle")
})

test("Repo.create catches source changes before its handle is constructed", async () => {
  const { source, docType, receive } = await setup(false)
  const repo = new Repo({ async create() {
    receive(1)
    return source
  } })
  const handle = await repo.create(docType, [0])
  assert.deepEqual(handle.doc(), [0, 1])
  receive(2)
  await tick()
  assert.deepEqual(handle.doc(), [0, 1, 2])
})

test("initial load failures reject rather than returning an empty ready handle", async t => {
  const { scheduler, source, docType, listeners } = await setup(false)
  const logs = t.mock.method(console, "error", () => {})
  const error = new Error("initial read failed")
  t.mock.method(source, "materialize", async () => { throw error })
  const handle = new DocHandle(source, docType, docType.empty())
  await assert.rejects(scheduler.schedule(handle, source), reason => reason === error)
  assert.equal(listeners.size, 0)
  assert.equal(logs.mock.callCount(), 0, "initial failures belong to the caller, not the background error handler")
})

test("aborting Repo.find unschedules initial loading and discards late records", async t => {
  const { source, docType, adapter, listeners } = await setup(false)
  const entered = deferred(), release = deferred()
  const materialize = source.materialize.bind(source)
  t.mock.method(source, "materialize", async metas => {
    entered.resolve()
    await release.promise
    return materialize(metas)
  })
  const apply = t.mock.method(adapter, "apply")
  let queryDisposed = false
  const repo = new Repo({ find: () => ({
    id: () => source.documentId,
    state: () => ({ type: "ready", handle: source }),
    subscribe: () => () => {},
    dispose: () => { queryDisposed = true },
  }) })
  t.after(() => repo.dispose())
  const controller = new AbortController()
  const finding = repo.find(docType, source.documentId, { signal: controller.signal })
  await entered.promise
  assert.equal(listeners.size, 1)
  controller.abort()
  await assert.rejects(finding, { name: "AbortError" })
  assert.equal(queryDisposed, true)
  assert.equal(listeners.size, 0)
  release.resolve()
  await tick()
  assert.equal(apply.mock.callCount(), 0)
})

test("disposing a Repo stops its handle refreshes but not local saves or its source", async () => {
  const { source, docType, receive, listeners, batches } = await setup(false)
  const repo = new Repo({
    create: async () => source,
    shutdown() { assert.fail("Repo does not own the source lifecycle") },
  })
  const handle = await repo.create(docType, [0])
  assert.equal(listeners.size, 1)
  repo.dispose()
  repo.dispose()
  assert.equal(listeners.size, 0)
  receive(1)
  await tick()
  assert.deepEqual(handle.doc(), [0])
  await handle.change(2)
  assert.deepEqual(batches, [[2]])
  assert.deepEqual(handle.doc(), [0, 2])
  assert.throws(() => repo.query(docType, source.documentId), /disposed/)
  await assert.rejects(repo.find(docType, source.documentId), /disposed/)
  await assert.rejects(repo.create(docType, [0]), /disposed/)
})
