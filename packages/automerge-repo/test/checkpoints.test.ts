import assert from "node:assert/strict"
import test from "node:test"
import * as Automerge from "@automerge/automerge"
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { stringifyDocId } from "@brigstow/brigstow"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"
import { amDocType } from "../src/AutomergeDocType.js"
import { Repo, type AutomergeUrl } from "../src/index.js"

type CheckboxDocument = { completed: boolean; revision: number }

function checkpointDocument() {
  const actor = "0123456789abcdef0123456789abcdef"
  let doc = Automerge.init<CheckboxDocument>({ actor })
  // Fixed actor, timestamps, and message give a level-1 head at revision 39,
  // followed by a level-2 head at revision 40. No random hashes or mining at test
  // time: this exercises an interior checkpoint as well as the fragment's head.
  for (let revision = 0; revision < 40; revision++) {
    doc = Automerge.change(doc, { time: 0 }, draft => {
      draft.completed = revision % 2 === 0
      draft.revision = revision
    })
  }
  return Automerge.change(doc, { time: 0, message: "12872" }, draft => {
    draft.completed = true
    draft.revision = 40
  })
}

test("Automerge fragment checkpoints become 12-byte prefixes without referencing their own head", () => {
  const doc = checkpointDocument()
  const [raw] = Automerge.getFragmentMetadata(doc, { start: 1 })
  assert.ok(raw)
  assert.equal(raw.level, 2)
  assert.ok(raw.checkpoints.includes(raw.head), "fixture must exercise a self checkpoint")
  const interior = raw.checkpoints.filter(checkpoint => checkpoint !== raw.head)
  assert.ok(interior.length > 0, "fixture must exercise non-head checkpoints too")
  assert.ok(raw.checkpoints.every(checkpoint => /^[0-9a-f]{64}$/.test(checkpoint)))

  const [meta] = Array.from(amDocType<CheckboxDocument>().sedimentree.metadata(doc))
  assert.equal(meta?.kind, "fragment")
  if (meta?.kind !== "fragment") assert.fail("Expected fragment metadata")
  assert.equal(meta.head, raw.head)
  assert.deepEqual(meta.boundary, raw.boundary)
  assert.deepEqual(meta.checkpoints, interior.map(checkpoint => checkpoint.slice(0, 24)))
  assert.ok(meta.checkpoints.every(checkpoint => /^[0-9a-f]{24}$/.test(checkpoint)))
  assert.ok(raw.checkpoints.every(checkpoint => checkpoint.length === 64), "do not mutate Automerge metadata")
})

test("a checkpoint-bearing document can be stored, edited through a handle, and reloaded", { timeout: 5000 }, async t => {
  const backend = new MemoryStorage()
  const sources: SubductionSource[] = []
  t.after(() => Promise.all(sources.map(source => source.shutdown())))
  const openSource = () => {
    const node = new Subduction({ signer: MemorySigner.generate(), storage: new ObservableStorage(backend) })
    const source = new SubductionSource(node)
    sources.push(source)
    return source
  }
  const source = openSource()
  const adapter = amDocType<CheckboxDocument>().sedimentree
  const doc = checkpointDocument()
  const metas = Array.from(adapter.metadata(doc))
  const bytes = await adapter.materialize(doc, metas)
  const stored = await source.create({
    documentType: "automerge",
    initialRecords: metas.map((meta, i) => ({ ...meta, bytes: bytes[i]! })),
  })
  assert.deepEqual(stored.heads(), Automerge.getHeads(doc), "a fragment must not cover its own head")
  assert.deepEqual(Array.from(stored.metadata()), metas)

  const url = `automerge:${stringifyDocId(stored.documentId)}` as AutomergeUrl
  const original = await new Repo(source).find<CheckboxDocument>(url)
  assert.deepEqual(Automerge.toJS(original.doc()), { completed: true, revision: 40 })
  await original.change(draft => { draft.completed = false })
  const heads = original.heads()
  await source.shutdown()

  const reloaded = await new Repo(openSource()).find<CheckboxDocument>(url)
  assert.deepEqual(Automerge.toJS(reloaded.doc()), { completed: false, revision: 40 })
  assert.deepEqual(reloaded.heads(), heads)
})
