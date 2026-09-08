import assert from "node:assert/strict"
import test from "node:test"
import { DocHandle } from "../dist/DocHandle.js"

function deferred() {
  let resolve
  const promise = new Promise(r => { resolve = r })
  return { promise, resolve }
}

function setup() {
  const meta = n => ({ kind: "commit", head: String(n), parents: n ? [String(n - 1)] : [] })
  const persisted = new Map([["0", meta(0)]])
  const batches = []
  const source = {
    metadata: () => persisted.values(),
    async apply(records) {
      batches.push(records.map(record => Number(record.head)))
      for (const record of records) persisted.set(record.head, record)
    },
  }
  const adapter = {
    metadata: state => state.map(meta),
    materialize: (_state, metas) => metas.map(meta => new Uint8Array([Number(meta.head)])),
  }
  const docType = {
    view: state => state,
    change: (state, n) => [...state, n],
    sedimentree: adapter,
  }
  return { handle: new DocHandle(source, docType, [0]), source, adapter, batches, persisted }
}

test("change updates synchronously, serializes saves, and flush waits for queued edits", async t => {
  const { handle, source, batches } = setup()
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
    const { handle, source, adapter, persisted, batches } = setup()
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
