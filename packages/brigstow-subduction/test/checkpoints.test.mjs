import assert from "node:assert/strict"
import test from "node:test"
import { MemorySigner, MemoryStorage, SedimentreeId, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "../dist/index.js"

const documentId = new Uint8Array(32).fill(42)
const head = "01".repeat(32)
const boundary = "02".repeat(32)
const bytes = new Uint8Array([10, 20, 30])
const record = checkpoints => ({ kind: "fragment", head, boundary: [boundary], checkpoints, bytes })
const makeNode = (storage = new MemoryStorage()) => new Subduction({ signer: MemorySigner.generate(), storage: new ObservableStorage(storage) })

test("12-byte checkpoints survive create, storage reload, and reuse as record metadata", async () => {
  const storage = new MemoryStorage()
  const writer = makeNode(storage)
  const reader = makeNode(storage)
  const target = makeNode()
  const checkpoint = new Uint8Array(12).fill(3)
  const treeId = SedimentreeId.fromBytes(documentId)

  try {
    await new SubductionSource(writer).create({
      documentId,
      documentType: "test",
      initialRecords: [record([checkpoint])],
    })

    // Read through a different instance, so this exercises storage rather than
    // just returning the metadata in the writer's resident tree.
    const [loaded] = await reader.getFragments(treeId)
    assert.deepEqual(loaded.checkpoints.map(cp => cp.toBytes()), [checkpoint])
    const persisted = await storage.loadFragment(treeId, loaded.head)
    assert.deepEqual(persisted.blob, bytes)

    // Metadata read from WASM becomes the same plain-byte representation that
    // Brigstow accepts on writes. No full commit IDs need to be recovered.
    await new SubductionSource(target).create({
      documentId,
      documentType: "test",
      initialRecords: [{
        kind: "fragment",
        head: loaded.head.toHexString(),
        boundary: loaded.boundary.map(id => id.toHexString()),
        checkpoints: loaded.checkpoints.map(cp => cp.toBytes()),
        bytes: persisted.blob,
      }],
    })
    const [copied] = await target.getFragments(treeId)
    assert.deepEqual(copied.checkpoints.map(cp => cp.toBytes()), [checkpoint])
    assert.deepEqual(copied.boundary.map(id => id.toHexString()), [boundary])
    assert.equal(copied.head.toHexString(), head)

    // Regression for the old WASM heads bug: the fragment contributes its head,
    // not its boundary.
    assert.deepEqual((await target.getAllHeads())[0].heads.map(id => id.toHexString()), [head])
  } finally {
    treeId.free()
    await Promise.all([writer, reader, target].map(node => node.disconnectAll()))
  }
})

test("empty checkpoint sets remain valid", async () => {
  const node = makeNode()
  const treeId = SedimentreeId.fromBytes(documentId)
  try {
    await new SubductionSource(node).create({ documentId, documentType: "test", initialRecords: [record([])] })
    const [loaded] = await node.getFragments(treeId)
    assert.deepEqual(loaded.checkpoints, [])
  } finally {
    treeId.free()
    await node.disconnectAll()
  }
})

test("checkpoint lengths other than 12 bytes are rejected, including full commit IDs", async () => {
  const storage = new MemoryStorage()
  const node = makeNode(storage)
  const source = new SubductionSource(node)
  const treeId = SedimentreeId.fromBytes(documentId)
  try {
    for (const length of [0, 11, 13, 32]) {
      await assert.rejects(source.create({
        documentId,
        documentType: "test",
        initialRecords: [record([new Uint8Array(length)])],
      }), /expected 12 bytes/)
    }
    assert.equal(await storage.containsSedimentreeId(treeId), false)
  } finally {
    treeId.free()
    await node.disconnectAll()
  }
})
