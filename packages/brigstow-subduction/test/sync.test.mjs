import assert from "node:assert/strict"
import test from "node:test"
import { setTimeout as delay } from "node:timers/promises"
// The full entry point initializes WASM for DocumentSync's slim import.
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { DocumentSync, NoSyncPeersError } from "../dist/DocumentSync.js"
import { ObservableStorage, SubductionSource } from "../dist/index.js"

const documentId = new Uint8Array(32).fill(42)
const otherId = new Uint8Array(32).fill(43)
const server = "wss://sync.example.test/"

function deferred() {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}

async function bounded(promise, message = "operation did not finish", milliseconds = 1000) {
  let timer
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), milliseconds) }),
    ])
  } finally { clearTimeout(timer) }
}

async function until(predicate, message = "condition did not become true") {
  const deadline = Date.now() + 1000
  while (!predicate()) {
    assert.ok(Date.now() < deadline, message)
    await delay(2)
  }
}

function owned(value = {}) {
  return { ...value, frees: 0, free() { this.frees++ } }
}

function result(outcomes) {
  const entries = outcomes.map(outcome => owned({ success: true, transportErrors: [], ...outcome }))
  return { entries, wrapper: owned({ entries: () => entries }) }
}

class FakeSubduction {
  connected = new Set()
  peerWrappers = []
  connections = []
  rounds = []
  results = []
  disconnected = []
  disconnects = 0
  connect = async () => "server-peer"
  run = async () => result([...this.connected].map(() => ({})))

  peer(name) {
    const peer = owned({ toString: () => name })
    this.peerWrappers.push(peer)
    return peer
  }

  async getConnectedPeerIds() { return [...this.connected].map(name => this.peer(name)) }

  async connectDiscover(url) {
    this.connections.push(url.href)
    const name = await this.connect(url)
    this.connected.add(name)
    return this.peer(name)
  }

  async syncWithAllPeers(id, subscribe, timeout) {
    this.rounds.push({ id: id.toBytes(), subscribe, timeout })
    const response = await this.run()
    this.results.push(response)
    return response.wrapper
  }

  async disconnectFromPeer(peer) {
    const name = peer.toString()
    this.disconnected.push(name)
    this.connected.delete(name)
  }

  async disconnectAll() { this.disconnects++; this.connected.clear() }
}

function setup(t, options = {}) {
  const fake = new FakeSubduction()
  const errors = [], synced = [], cleanup = []
  const manager = new DocumentSync(fake, {
    syncTimeoutMilliseconds: 20,
    retryIntervalMilliseconds: 10000,
    onSyncError: error => errors.push(error),
    onSynced: id => synced.push(id),
    ...options,
  })
  t.after(async () => {
    for (const release of cleanup) release()
    await bounded(manager.shutdown(), "manager shutdown did not finish")
  })
  return { fake, manager, errors, synced, cleanup }
}

function assertFreed(fake) {
  for (const peer of fake.peerWrappers) assert.equal(peer.frees, 1, "peer wrapper freed exactly once")
  for (const response of fake.results) {
    assert.equal(response.wrapper.frees, 1, "result wrapper freed exactly once")
    for (const entry of response.entries) assert.equal(entry.frees, 1, "entry freed exactly once")
  }
}

test("connects configured servers, reuses live peers, and reconnects a dropped server", async t => {
  const { fake, manager, synced } = setup(t, { syncServers: [server] })
  await manager.sync(documentId)
  await manager.sync(documentId)
  assert.deepEqual(fake.connections, [server])
  fake.connected.clear()
  await manager.sync(documentId)
  assert.deepEqual(fake.connections, [server, server])
  assert.equal(synced.length, 3)
  for (const round of fake.rounds) {
    assert.deepEqual(round, { id: documentId, subscribe: true, timeout: 20 })
  }
  assertFreed(fake)
})

test("handshake waits are bounded without duplicate attempts; shutdown disconnects late completion", async t => {
  const { fake, manager, errors, synced, cleanup } = setup(t, { syncServers: [server] })
  const handshake = deferred()
  fake.connect = () => handshake.promise
  cleanup.push(() => handshake.resolve("late-peer"))
  const timedOut = error => error instanceof AggregateError && /timed out/.test(error.errors[0].message)

  // Concurrent documents and a later retry must all share the unresolved handshake.
  await bounded(Promise.all([
    assert.rejects(manager.sync(documentId), timedOut),
    assert.rejects(manager.sync(otherId), timedOut),
  ]), "handshake timeout did not bound sync")
  await bounded(assert.rejects(manager.sync(documentId), timedOut))
  assert.deepEqual(fake.connections, [server])
  assert.equal(fake.rounds.length, 0)

  await bounded(manager.shutdown(), "shutdown waited for an uncancellable handshake")
  assert.equal(fake.disconnects, 1)
  handshake.resolve("late-peer")
  await until(() => fake.disconnected.includes("late-peer"), "late peer was not disconnected")
  await until(() => fake.peerWrappers.every(peer => peer.frees === 1))
  assert.equal(fake.connected.size, 0)
  assert.deepEqual(errors, [])
  assert.deepEqual(synced, [])
  await assert.rejects(manager.sync(documentId), /shut down/)
  manager.schedule(otherId)
  assert.equal(fake.connections.length, 1)
  assertFreed(fake)
})

test("a rejected server handshake can be retried and does not poison the connection cache", async t => {
  const { fake, manager } = setup(t, { syncServers: [server] })
  const offline = new Error("server offline")
  fake.connect = async () => { throw offline }
  await assert.rejects(manager.sync(documentId), error => {
    assert.deepEqual(error.errors, [offline])
    return true
  })
  fake.connect = async () => "recovered-peer"
  await manager.sync(documentId)
  assert.deepEqual(fake.connections, [server, server])
  assert.equal(fake.rounds.length, 1)
  assertFreed(fake)
})

test("an unavailable server is reported without blocking synchronization with an existing peer", async t => {
  const { fake, manager, errors, synced } = setup(t, { syncServers: [server] })
  fake.connected.add("local-peer")
  fake.connect = async () => { throw new Error("server offline") }
  await manager.sync(documentId)
  assert.equal(fake.rounds.length, 1)
  assert.equal(synced.length, 1)
  assert.equal(errors.length, 1)
  assert.match(errors[0].message, /Unable to connect/)
  assertFreed(fake)
})

test("offline tracked documents retry automatically when a peer appears, and stop on shutdown", async t => {
  const { fake, manager, errors, synced } = setup(t, { retryIntervalMilliseconds: 10 })
  manager.schedule(documentId)
  await until(() => errors.some(error => error instanceof NoSyncPeersError))
  fake.connected.add("new-peer")
  await until(() => synced.length > 0, "tracked offline document was not retried")
  assert.ok(fake.rounds.length >= 2)
  assert.ok(fake.rounds.every(round => Buffer.from(round.id).equals(documentId)))
  await manager.shutdown()
  const rounds = fake.rounds.length
  await delay(35)
  assert.equal(fake.rounds.length, rounds)
  assertFreed(fake)
})

test("automatic retries reconnect unavailable servers for tracked documents", async t => {
  const { fake, manager, errors, synced } = setup(t, { syncServers: [server], retryIntervalMilliseconds: 10 })
  let online = false
  fake.connect = async () => {
    if (!online) throw new Error("offline")
    return "server-peer"
  }
  manager.schedule(documentId)
  await until(() => errors.length > 0)
  online = true
  await until(() => synced.length > 0)
  assert.ok(fake.connections.length >= 2)
  assert.ok(fake.rounds.length >= 1)
  assertFreed(fake)
})

test("sync queued during a running round runs another round; retry ticks do not prolong it", async t => {
  const { fake, manager, synced, cleanup } = setup(t, { retryIntervalMilliseconds: 10 })
  fake.connected.add("peer")
  const first = deferred(), second = deferred()
  cleanup.push(() => first.resolve(), () => second.resolve())
  fake.run = async () => {
    await (fake.rounds.length === 1 ? first.promise : second.promise)
    return result([{}])
  }
  const initial = manager.sync(documentId)
  await until(() => fake.rounds.length === 1)
  let queuedFinished = false
  const queued = manager.sync(documentId).then(() => { queuedFinished = true })
  manager.schedule(documentId)
  await delay(30)
  assert.equal(fake.rounds.length, 1, "rounds must not run concurrently for one document")
  first.resolve()
  await until(() => fake.rounds.length === 2)
  assert.equal(queuedFinished, false, "queued caller must wait for the new snapshot's round")
  await delay(30)
  second.resolve()
  await bounded(Promise.all([initial, queued]))
  // Stop the retry timer before inspecting the final count.
  await manager.shutdown()
  assert.equal(fake.rounds.length, 2, "timer ticks must not request extra rounds while pending")
  assert.equal(synced.length, 2)
  assertFreed(fake)
})

test("sync() only synchronizes tracked documents and snapshots caller IDs", async t => {
  const { fake, manager } = setup(t)
  fake.connected.add("peer")
  await manager.sync()
  assert.equal(fake.rounds.length, 0)
  const mutable = documentId.slice()
  const pending = manager.sync(mutable)
  mutable.fill(0)
  await pending
  await manager.sync(otherId)
  fake.rounds.length = 0
  await manager.sync()
  assert.deepEqual(fake.rounds.map(round => round.id), [documentId, otherId])
  assertFreed(fake)
})

for (const mode of ["success", "partial failure", "all failed", "no peers"]) {
  test(`${mode}: per-peer results determine outcome and all wrappers are freed`, async t => {
    const { fake, manager, errors, synced } = setup(t)
    const transportError = new Error("transport failed")
    const failed = { success: false, transportErrors: [transportError] }
    const outcomes = mode === "success" ? [{}]
      : mode === "partial failure" ? [{}, failed]
      : mode === "all failed" ? [failed, failed] : []
    fake.run = async () => result(outcomes)
    if (mode === "all failed") {
      await assert.rejects(manager.sync(documentId), error => {
        assert.ok(error instanceof AggregateError)
        assert.deepEqual(error.errors, [transportError, transportError])
        return true
      })
    } else if (mode === "no peers") {
      await assert.rejects(manager.sync(documentId), NoSyncPeersError)
    } else {
      await manager.sync(documentId)
    }
    assert.equal(synced.length, mode === "success" || mode === "partial failure" ? 1 : 0)
    assert.equal(errors.length, mode === "partial failure" ? 1 : 0)
    if (errors.length) {
      assert.ok(errors[0] instanceof AggregateError)
      assert.deepEqual(errors[0].errors, [transportError])
    }
    assert.equal(fake.results.length, 1)
    assertFreed(fake)
  })
}

function sourceSetup(t, options = {}) {
  const sdn = new Subduction({ signer: MemorySigner.generate(), storage: new ObservableStorage(new MemoryStorage()) })
  const source = new SubductionSource(sdn, "test", { retryIntervalMilliseconds: 10000, onSyncError: () => {}, ...options })
  t.after(() => bounded(source.shutdown(), "source shutdown did not finish"))
  return { sdn, source }
}

async function queryState(t, source, id = documentId) {
  const query = source.find(id)
  t.after(() => query.dispose())
  await until(() => query.state().type !== "finding", "source query did not settle")
  return query.state()
}

test("source does not return a fake-ready handle for a document absent from connected peers", async t => {
  const a = sourceSetup(t), b = sourceSetup(t)
  await Subduction.link(a.sdn, b.sdn)
  const state = await queryState(t, a.source)
  assert.equal(state.type, "unavailable")
})

test("source distinguishes an unavailable server from an absent local-only document", async t => {
  const local = sourceSetup(t)
  assert.equal((await queryState(t, local.source)).type, "unavailable")
  const remote = sourceSetup(t, { syncServers: [server], syncTimeoutMilliseconds: 20 })
  remote.sdn.connectDiscover = async () => { throw new Error("server offline") }
  const state = await queryState(t, remote.source)
  assert.equal(state.type, "failed")
  assert.ok(state.error instanceof AggregateError)
})

test("local creation, reads and writes do not wait for an unavailable server", async t => {
  const { sdn, source } = sourceSetup(t, { syncServers: [server], syncTimeoutMilliseconds: 1000 })
  // This promise deliberately never settles: source shutdown must not await it.
  let connections = 0
  sdn.connectDiscover = () => { connections++; return new Promise(() => {}) }
  // Each local operation must finish before the network wait's deadline, not
  // merely swallow its timeout and eventually return the local data.
  const handle = await bounded(source.create({ documentId, documentType: "test", initialRecords: [] }), "create waited for network", 500)
  await until(() => connections === 1)
  const state = await bounded(queryState(t, source), "local find waited for network", 500)
  assert.equal(state.type, "ready")
  const record = { kind: "commit", head: "01".repeat(32), parents: [], bytes: new Uint8Array([1]) }
  await bounded(handle.apply([record]), "local apply waited for network", 500)
  assert.deepEqual(handle.heads(), [record.head])
  assert.deepEqual(await state.handle.materialize([record]), [record.bytes])
  assert.equal(connections, 1)
})
