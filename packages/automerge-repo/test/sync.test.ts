import assert from "node:assert/strict"
import test, { type TestContext } from "node:test"
import { setTimeout as delay } from "node:timers/promises"
import { isDeepStrictEqual } from "node:util"
import * as Automerge from "@automerge/automerge"
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"
import { Repo, type AutomergeUrl, type DocHandle } from "../src/index.js"

function setup(t: TestContext) {
  const sources: SubductionSource[] = []
  t.after(async () => {
    await Promise.all(sources.map(source => source.shutdown()))
  })

  return function openRepo(retryIntervalMilliseconds = 10_000) {
    // Every node has independent storage: documents can only cross via the link.
    const storage = new MemoryStorage()
    const node = new Subduction({
      signer: MemorySigner.generate(),
      storage: new ObservableStorage(storage),
      defaultTimeoutMilliseconds: 500,
    })
    const source = new SubductionSource(node, "automerge", {
      syncTimeoutMilliseconds: 500,
      // Keep retries outside the assertions so edits exercise immediate sync,
      // and reconnecting exercises the explicit public sync method.
      retryIntervalMilliseconds,
      onSyncError: () => {}, // Disconnect races are expected; sync() still rejects.
    })
    sources.push(source)
    return { repo: new Repo(source), source, node, storage }
  }
}

function urlOf<T>(handle: DocHandle<T>): AutomergeUrl {
  return `automerge:${handle.documentId}` as AutomergeUrl
}

async function until(predicate: () => boolean | Promise<boolean>, message: string): Promise<void> {
  const deadline = Date.now() + 3000
  while (Date.now() < deadline) {
    if (await predicate()) return
    await delay(10)
  }
  assert.fail(message)
}

async function expectDoc<T>(handle: DocHandle<T>, expected: T): Promise<void> {
  // A completed network round need not have refreshed the live DocHandle yet.
  await until(
    () => isDeepStrictEqual(Automerge.toJS(handle.doc()), expected),
    `Document did not converge to ${JSON.stringify(expected)}`,
  )
  assert.deepEqual(Automerge.toJS(handle.doc()), expected)
}

test("a cold repo finds a document from a peer with independent storage", { timeout: 10_000 }, async t => {
  const openRepo = setup(t)
  const a = openRepo()
  const initial = { title: "Available from a peer", todos: ["first"] }
  const original = await a.repo.create(initial)
  // Finish the initial offline round before linking, and check that an
  // explicit sync cannot report success when there are no peers.
  await assert.rejects(a.source.sync())

  const b = openRepo()
  await Subduction.link(a.node, b.node)
  assert.deepEqual(await b.storage.loadAllSedimentreeIds(), [])
  // No sync after linking: find must request this previously unknown document.
  const found = await b.repo.find<typeof initial>(urlOf(original), {
    signal: AbortSignal.timeout(3000),
  })

  assert.notStrictEqual(found, original)
  assert.equal(found.documentId, original.documentId)
  await expectDoc(found, initial)
  assert.deepEqual(Automerge.getHeads(found.doc()), Automerge.getHeads(original.doc()))
})

test("edits automatically update existing live handles in both directions", { timeout: 10_000 }, async t => {
  const openRepo = setup(t)
  const a = openRepo()
  const b = openRepo()
  await Subduction.link(a.node, b.node)
  const original = await a.repo.create({ title: "Initial", todos: [] as string[] })
  await a.source.sync()
  const found = await b.repo.find<{ title: string; todos: string[] }>(urlOf(original), {
    signal: AbortSignal.timeout(3000),
  })
  await expectDoc(found, { title: "Initial", todos: [] })

  // change() waits only for local persistence, not network delivery. Neither
  // direction below calls sync() or opens a replacement handle.
  await original.change(doc => { doc.todos.push("written by A") })
  await expectDoc(found, { title: "Initial", todos: ["written by A"] })

  await found.change(doc => { doc.title = "Edited by B" })
  await expectDoc(original, { title: "Edited by B", todos: ["written by A"] })
  assert.deepEqual(Automerge.getHeads(original.doc()), Automerge.getHeads(found.doc()))
})

test("a server relays a newly created document after its creator disconnects", { timeout: 10_000 }, async t => {
  const openRepo = setup(t)
  const a = openRepo()
  const server = openRepo()
  await Subduction.link(a.node, server.node)
  const initial = { title: "Uploaded without an edit", count: 1 }
  const original = await a.repo.create(initial)
  // A create must upload its initial records without requiring change().
  await a.source.sync()
  // MessagePort.close() doesn't notify its remote end like a WebSocket does;
  // explicitly drop both ends of this test transport.
  await Promise.all([a.source.shutdown(), server.node.disconnectAll()])

  const b = openRepo()
  assert.deepEqual(await b.storage.loadAllSedimentreeIds(), [])
  await Subduction.link(b.node, server.node)
  const found = await b.repo.find<typeof initial>(urlOf(original), {
    signal: AbortSignal.timeout(3000),
  })

  await expectDoc(found, initial)
  assert.deepEqual(Automerge.getHeads(found.doc()), Automerge.getHeads(original.doc()))
  assert.deepEqual(await a.node.getConnectedPeerIds(), [])
})

test("live documents catch up through a shared server, including periodic reconciliation", { timeout: 10_000 }, async t => {
  const openRepo = setup(t)
  const a = openRepo(100)
  const b = openRepo(100)
  const server = openRepo()
  await Subduction.link(a.node, server.node)
  await Subduction.link(b.node, server.node)
  const original = await a.repo.create({ fromA: 0, fromB: 0 })
  await a.source.sync()
  const found = await b.repo.find<{ fromA: number; fromB: number }>(urlOf(original))
  await original.change(doc => { doc.fromA = 1 })
  await expectDoc(found, { fromA: 1, fromB: 0 })
  await found.change(doc => { doc.fromB = 2 })
  await expectDoc(original, { fromA: 1, fromB: 2 })
})

test("offline edits persist locally and converge after reconnecting and syncing", { timeout: 10_000 }, async t => {
  const openRepo = setup(t)
  const a = openRepo()
  const b = openRepo()
  await Subduction.link(a.node, b.node)
  const original = await a.repo.create({ fromA: "initial", fromB: "initial" })
  await a.source.sync()
  const found = await b.repo.find<{ fromA: string; fromB: string }>(urlOf(original), {
    signal: AbortSignal.timeout(3000),
  })
  await expectDoc(found, { fromA: "initial", fromB: "initial" })
  await Promise.all([a.node.disconnectAll(), b.node.disconnectAll()])
  assert.deepEqual(await a.node.getConnectedPeerIds(), [])
  assert.deepEqual(await b.node.getConnectedPeerIds(), [])

  // Awaited changes must succeed with no peers: they commit locally and leave
  // network work for a later round. Distinct fields preserve both edits.
  await original.change(doc => { doc.fromA = "edited offline by A" })
  await found.change(doc => { doc.fromB = "edited offline by B" })
  assert.deepEqual(Automerge.toJS(original.doc()), { fromA: "edited offline by A", fromB: "initial" })
  assert.deepEqual(Automerge.toJS(found.doc()), { fromA: "initial", fromB: "edited offline by B" })

  await Subduction.link(a.node, b.node)
  await Promise.all([a.source.sync(), b.source.sync()])
  const expected = { fromA: "edited offline by A", fromB: "edited offline by B" }
  await Promise.all([expectDoc(original, expected), expectDoc(found, expected)])
  assert.deepEqual(Automerge.getHeads(original.doc()), Automerge.getHeads(found.doc()))
})
