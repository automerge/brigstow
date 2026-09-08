import assert from "node:assert/strict"
import test from "node:test"
import { setTimeout as delay } from "node:timers/promises"
import { Checkpoint, CommitId, MemorySigner, MemoryStorage, SedimentreeId, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "../dist/index.js"

const documentId = new Uint8Array(32).fill(42)
const id = n => n.toString(16).padStart(2, "0").repeat(32)
const commit = (n, parents = []) => ({ kind: "commit", head: id(n), parents: parents.map(id), bytes: new Uint8Array([n]) })
const fragment = (n, boundary = [], checkpoints = []) => ({
  kind: "fragment", head: id(n), boundary: boundary.map(id),
  checkpoints: checkpoints.map(n => id(n).slice(0, 24)), bytes: new Uint8Array([n, 0]),
})
const metas = handle => Array.from(handle.metadata())
const sortedHeads = handle => metas(handle).map(m => m.head).sort()

async function until(predicate) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return
    await delay(5)
  }
  assert.fail("Timed out waiting for a handle update")
}

function setup(t, backend = new MemoryStorage()) {
  const storage = new ObservableStorage(backend)
  const sdn = new Subduction({ signer: MemorySigner.generate(), storage, defaultTimeoutMilliseconds: 1000 })
  const source = new SubductionSource(sdn, "test")
  t.after(() => sdn.disconnectAll())
  return { backend, storage, sdn, source, create: records => source.create({ documentId, documentType: "test", initialRecords: records }) }
}

async function find(source) {
  const query = source.find(documentId)
  await until(() => query.state().type !== "finding")
  const state = query.state()
  assert.equal(state.type, "ready")
  return state.handle
}

test("observable storage normalizes missing WASM records to null", async () => {
  for (const missing of [undefined, null]) {
    const backend = new MemoryStorage()
    backend.loadCommit = async () => missing
    backend.loadFragment = async () => missing
    const storage = new ObservableStorage(backend)
    const treeId = SedimentreeId.fromBytes(documentId)
    const head = CommitId.fromHexString(id(1))
    try {
      assert.equal(await storage.loadCommit(treeId, head), null)
      assert.equal(await storage.loadFragment(treeId, head), null)
    } finally {
      treeId.free(); head.free(); backend.free()
    }
  }
})

test("source requires observation, distinguishes absent/empty trees, and uses its configured type", async t => {
  const bare = new Subduction({ signer: MemorySigner.generate(), storage: new MemoryStorage() })
  t.after(() => bare.disconnectAll())
  assert.throws(() => new SubductionSource(bare), /ObservableStorage/)
  const { source, create } = setup(t)
  const query = source.find(documentId)
  await until(() => query.state().type !== "finding")
  assert.equal(query.state().type, "unavailable")
  const empty = await create([])
  assert.deepEqual(metas(empty), [])
  assert.deepEqual(empty.heads(), [])
  assert.equal((await find(source)).documentType, "test")
})

test("metadata and heads are defensive snapshots; materialize preserves kind and request order", async t => {
  const { create } = setup(t)
  const handle = await create([commit(1), commit(2, [1]), fragment(2, [1], [3])])
  assert.deepEqual(handle.heads(), [id(2)])
  assert.equal(metas(handle).length, 3)
  const requests = metas(handle).reverse()
  const expected = requests.map(m => m.kind === "fragment" ? new Uint8Array([2, 0]) : new Uint8Array([parseInt(m.head.slice(0, 2), 16)]))
  assert.deepEqual(await handle.materialize(requests), expected)
  await assert.rejects(handle.materialize([commit(99)]), /Missing commit/)
  await assert.rejects(handle.materialize([fragment(99)]), /Missing fragment/)
  assert.deepEqual(await handle.materialize([]), [])

  const snapshot = metas(handle)
  snapshot[0].head = id(99)
  snapshot.find(m => m.kind === "fragment").checkpoints[0] = "00".repeat(12)
  snapshot.find(m => m.kind === "commit").parents.push(id(99))
  handle.documentId.fill(0)
  handle.heads().push(id(99))
  assert.equal(metas(handle).find(m => m.kind === "fragment").checkpoints[0], "03".repeat(12))
  assert.ok(!sortedHeads(handle).includes(id(99)))
  assert.deepEqual(handle.heads(), [id(2)])
  assert.deepEqual(handle.documentId, documentId)
})

test("metadata filters roots and known ancestors, but retains uncertain opaque-fragment coverage", async t => {
  const { create } = setup(t)
  const handle = await create([commit(1), commit(2, [1]), commit(3, [2]), fragment(4, [2], [3]), commit(5, [4])])
  const remaining = roots => Array.from(handle.metadata({ notAncestorsOf: roots.map(id) })).map(m => m.head).sort()
  assert.deepEqual(remaining([2]), [id(3), id(4), id(5)])
  assert.deepEqual(remaining([4]), [id(5)])
  assert.deepEqual(remaining([5]), [])
  assert.deepEqual(remaining([99]), sortedHeads(handle))
  // Knowing an interior checkpoint does not prove that the whole fragment is known.
  assert.ok(remaining([3]).includes(id(4)))
})

test("apply merges batches, updates all live handles, and suppresses duplicate notifications", async t => {
  const { create, source } = setup(t)
  const first = await create([commit(1)])
  const second = await find(source)
  let events = 0
  const listener = () => { events++; assert.ok(second.heads().includes(id(2))) }
  second.on("change", listener)
  const before = metas(first)
  await first.apply([commit(2, [1]), fragment(3, [1], [])])
  await until(() => metas(second).length === 3)
  assert.deepEqual(before.map(m => m.head), [id(1)])
  assert.deepEqual(first.heads(), [id(2), id(3)])
  assert.ok(events > 0)
  const count = events
  await first.apply([commit(2, [1]), fragment(3, [1], [])])
  await delay(20)
  assert.equal(events, count)
  second.off("change", listener)
  await first.apply([commit(4, [2, 3])])
  await until(() => second.heads()[0] === id(4))
  assert.equal(events, count)
  await first.apply([])
})

test("direct Subduction single-record writes and storage deletions update handles", async t => {
  const { create, storage, sdn } = setup(t)
  const handle = await create([commit(1)])
  const treeId = SedimentreeId.fromBytes(documentId)
  const head = CommitId.fromHexString(id(2))
  const parent = CommitId.fromHexString(id(1))
  const fragmentHead = CommitId.fromHexString(id(3))
  const checkpoint = new Checkpoint(new Uint8Array(12).fill(2))
  t.after(() => { treeId.free(); head.free(); parent.free(); fragmentHead.free(); checkpoint.free() })
  await sdn.storeCommit(treeId, head, [parent], new Uint8Array([2]))
  await until(() => handle.heads()[0] === id(2))
  await sdn.storeFragment(treeId, fragmentHead, [parent], [checkpoint], new Uint8Array([3, 0]))
  await until(() => handle.heads()[0] === id(3))
  await storage.deleteCommit(treeId, head)
  await until(() => !metas(handle).some(m => m.kind === "commit" && m.head === id(2)))
  await storage.deleteFragment(treeId, fragmentHead)
  await until(() => handle.heads()[0] === id(1))
  await sdn.removeSedimentree(treeId)
  await until(() => metas(handle).length === 0 && !handle.exists)
})

test("an update during initial loading cannot be overwritten by an older snapshot", async t => {
  const { backend, create, source, sdn } = setup(t)
  await create([commit(1)])
  let release, entered
  const gate = new Promise(resolve => { release = resolve })
  const started = new Promise(resolve => { entered = resolve })
  const original = backend.loadAllCommits.bind(backend)
  let blockOnce = true
  backend.loadAllCommits = async (...args) => {
    const result = await original(...args)
    if (blockOnce) {
      blockOnce = false
      entered()
      await gate
    }
    return result
  }
  const pending = find(source)
  await started
  const treeId = SedimentreeId.fromBytes(documentId)
  const head = CommitId.fromHexString(id(2))
  const parent = CommitId.fromHexString(id(1))
  try {
    await sdn.storeCommit(treeId, head, [parent], new Uint8Array([2]))
  } finally {
    release()
    treeId.free(); head.free(); parent.free()
  }
  const handle = await pending
  assert.deepEqual(sortedHeads(handle), [id(1), id(2)])
  assert.deepEqual(handle.heads(), [id(2)])
})

test("failed persistence rejects apply without publishing unpersisted records", async t => {
  const { backend, create } = setup(t)
  const handle = await create([commit(1)])
  const error = new Error("storage unavailable")
  backend.saveBatchAll = async () => { throw error }
  let events = 0
  handle.on("change", () => { events++ })
  await assert.rejects(handle.apply([commit(2, [1])]))
  await delay(20)
  assert.deepEqual(sortedHeads(handle), [id(1)])
  assert.equal(events, 0)
})

test("apply snapshots caller buffers and listener failures do not break persistence", async t => {
  const { create } = setup(t)
  const handle = await create([])
  const errors = []
  const original = console.error
  console.error = (...args) => errors.push(args)
  let notified = false
  handle.on("change", () => { throw new Error("bad listener") })
  handle.on("change", () => { notified = true })
  try {
    const record = fragment(1, [], [2])
    const pending = handle.apply([record])
    record.bytes.fill(99)
    record.checkpoints[0] = "63".repeat(12)
    await pending
    assert.ok(notified)
    assert.ok(errors.length > 0)
    assert.deepEqual(await handle.materialize(metas(handle)), [new Uint8Array([1, 0])])
    assert.equal(metas(handle)[0].checkpoints[0], "02".repeat(12))
  } finally {
    console.error = original
  }
})

test("a partially persisted failed batch is reflected in the handle", async t => {
  const { backend, create } = setup(t)
  const handle = await create([commit(1)])
  const save = backend.saveBatchAll.bind(backend)
  backend.saveBatchAll = async (treeId, commits) => {
    await save(treeId, commits.slice(0, 1), [])
    throw new Error("partial failure")
  }
  await assert.rejects(handle.apply([commit(2, [1]), commit(3, [1])]))
  assert.equal(metas(handle).length, 2)
  assert.ok(sortedHeads(handle).includes(id(1)))
})

test("refresh failures are observable and a later write can recover", async t => {
  const { backend, create, sdn } = setup(t)
  const handle = await create([commit(1)])
  const load = backend.loadAllFragments.bind(backend)
  let failing = true
  backend.loadAllFragments = (...args) => failing ? Promise.reject(new Error("read failed")) : load(...args)
  const treeId = SedimentreeId.fromBytes(documentId)
  const head = CommitId.fromHexString(id(2))
  t.after(() => { treeId.free(); head.free() })
  await sdn.storeCommit(treeId, head, [], new Uint8Array([2]))
  await until(() => { try { handle.heads(); return false } catch { return true } })
  assert.throws(() => handle.metadata(), /read failed/)
  failing = false
  await sdn.storeCommit(treeId, head, [], new Uint8Array([2]))
  await until(() => { try { return metas(handle).length === 2 } catch { return false } })
})

test("inbound synchronization refreshes a live handle through observed batch storage", async t => {
  const a = setup(t), b = setup(t)
  const sender = await a.create([fragment(1, [], [2])])
  const receiver = await b.create([])
  let batches = 0
  const save = b.backend.saveBatchAll.bind(b.backend)
  b.backend.saveBatchAll = (...args) => { batches++; return save(...args) }
  await Subduction.link(a.sdn, b.sdn)
  const treeId = SedimentreeId.fromBytes(documentId)
  t.after(() => treeId.free())
  const result = await b.sdn.syncWithAllPeers(treeId, true, 1000)
  result.free()
  await until(() => metas(receiver).length === 1)
  assert.ok(batches > 0)
  assert.deepEqual(metas(receiver), metas(sender))
  assert.deepEqual(await receiver.materialize(metas(receiver)), [new Uint8Array([1, 0])])
  await sender.apply([commit(3, [1])])
  await until(() => receiver.heads()[0] === id(3))
})
