import assert from "node:assert/strict"
import test from "node:test"
import { MemorySigner, MemoryStorage, SedimentreeId, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSedimentreeHandle, SubductionSource } from "../dist/index.js"

const documentId = new Uint8Array(32).fill(42)
const head = "01".repeat(32)
const boundary = "02".repeat(32)
const bytes = new Uint8Array([10, 20, 30])
const record = checkpoints => ({ kind: "fragment", head, boundary: [boundary], checkpoints, bytes })
const makeNode = (storage = new MemoryStorage()) => new Subduction({ signer: MemorySigner.generate(), storage: new ObservableStorage(storage) })

test("hex checkpoints survive create, storage reload, JSON serialization, and reuse as record metadata", async () => {
  const storage = new MemoryStorage()
  const writer = makeNode(storage)
  const reader = makeNode(storage)
  const target = makeNode()
  const checkpoint = "000123456789abcdef012345"
  const checkpointBytes = Uint8Array.from(Buffer.from(checkpoint, "hex"))
  const treeId = SedimentreeId.fromBytes(documentId)

  try {
    await new SubductionSource(writer).create({
      documentId,
      documentType: "test",
      initialRecords: [record([checkpoint.toUpperCase()])],
    })

    // Read through a different instance, so this exercises storage rather than
    // just returning the metadata in the writer's resident tree.
    const [loaded] = await reader.getFragments(treeId)
    assert.deepEqual(loaded.checkpoints.map(cp => cp.toBytes()), [checkpointBytes])
    const persisted = await storage.loadFragment(treeId, loaded.head)
    assert.deepEqual(persisted.blob, bytes)

    // Brigstow exposes lowercase hex even when writes use uppercase. Metadata
    // can round-trip through JSON and be reused without recovering full IDs.
    const handle = await SubductionSedimentreeHandle.open(reader, documentId, "test")
    const metadata = Array.from(handle.metadata())
    assert.deepEqual(metadata, [{ kind: "fragment", head, boundary: [boundary], checkpoints: [checkpoint] }])
    const [meta] = JSON.parse(JSON.stringify(metadata))
    const copiedHandle = await new SubductionSource(target).create({
      documentId,
      documentType: "test",
      initialRecords: [{ ...meta, bytes: persisted.blob }],
    })
    assert.deepEqual(Array.from(copiedHandle.metadata()), metadata)
    const [copied] = await target.getFragments(treeId)
    assert.deepEqual(copied.checkpoints.map(cp => cp.toBytes()), [checkpointBytes])
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

test("invalid checkpoint hex is rejected without persisting, including full commit IDs", async () => {
  const storage = new MemoryStorage()
  const node = makeNode(storage)
  const source = new SubductionSource(node)
  const treeId = SedimentreeId.fromBytes(documentId)
  try {
    const invalid = [
      ...[0, 11, 13, 32].map(length => "00".repeat(length)),
      "0".repeat(23), "0".repeat(25), // odd lengths
      "gg".repeat(12), "0g".repeat(12), // parseInt must not silently accept these
      "0x" + "00".repeat(11), " " + "0".repeat(23), "0".repeat(23) + "\n",
      "0".repeat(24) + "\n", "0".repeat(24) + "\r\n",
      new Uint8Array(12), null, 123,
    ]
    for (const checkpoint of invalid) {
      await assert.rejects(source.create({
        documentId,
        documentType: "test",
        initialRecords: [record([checkpoint])],
      }), /expected 24 hex characters/)
    }
    assert.equal(await storage.containsSedimentreeId(treeId), false)
  } finally {
    treeId.free()
    await node.disconnectAll()
  }
})
