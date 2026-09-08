import assert from "node:assert/strict"
import test from "node:test"
import * as Automerge from "@automerge/automerge"
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"
import { Repo, type AutomergeUrl } from "../src/index.js"

test("a document created by one repo reloads from storage in a different repo", { timeout: 5000 }, async t => {
  const backend = new MemoryStorage()
  const nodes: Subduction[] = []
  t.after(async () => {
    await Promise.all(nodes.map(node => node.disconnectAll()))
  })

  function openRepo() {
    const node = new Subduction({
      signer: MemorySigner.generate(),
      storage: new ObservableStorage(backend),
    })
    nodes.push(node)
    return { repo: new Repo(new SubductionSource(node)), node }
  }

  const initial = {
    title: "Stored document",
    todos: [{ id: "first", title: "Survive a refresh", completed: false }],
    settings: { archived: false, priority: 3 },
  }
  const writer = openRepo()
  const original = await writer.repo.create(initial)
  assert.deepEqual(Automerge.toJS(original.doc()), initial)
  const url = `automerge:${original.documentId}` as AutomergeUrl
  const heads = Automerge.getHeads(original.doc())
  assert.ok(heads.length > 0)

  // Only the storage backend and URL survive the simulated refresh. The reader
  // has its own Repo, source, Subduction instance, signer, and storage wrapper;
  // it cannot recover the document from the writer's resident tree or peers.
  await writer.node.disconnectAll()
  const reader = openRepo()
  const reloaded = await reader.repo.find<typeof initial>(url, {
    signal: AbortSignal.timeout(2000),
  })

  assert.notStrictEqual(reloaded, original)
  assert.equal(reloaded.documentId, original.documentId)
  assert.deepEqual(Automerge.toJS(reloaded.doc()), initial)
  assert.deepEqual(Automerge.getHeads(reloaded.doc()), heads)
})
