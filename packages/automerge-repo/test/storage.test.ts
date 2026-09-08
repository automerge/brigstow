import assert from "node:assert/strict"
import test, { type TestContext } from "node:test"
import * as Automerge from "@automerge/automerge"
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"
import { Repo, type AutomergeUrl } from "../src/index.js"

function setup(t: TestContext) {
  const backend = new MemoryStorage()
  const nodes: Subduction[] = []
  t.after(async () => {
    await Promise.all(nodes.map(node => node.disconnectAll()))
  })

  return function openRepo() {
    const node = new Subduction({
      signer: MemorySigner.generate(),
      storage: new ObservableStorage(backend),
    })
    nodes.push(node)
    return { repo: new Repo(new SubductionSource(node)), node }
  }
}

test("a document created by one repo reloads from storage in a different repo", { timeout: 5000 }, async t => {
  const openRepo = setup(t)
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

test("rapid todo edits survive reload and a reopened handle can save further edits", { timeout: 5000 }, async t => {
  const openRepo = setup(t)
  type TodoDocument = { todos: { id: string; title: string; completed: boolean }[] }
  const writer = openRepo()
  const handle = await writer.repo.create<TodoDocument>({ todos: [] })
  const url = `automerge:${handle.documentId}` as AutomergeUrl

  // Like the UI, apply several changes synchronously without waiting for IO.
  const saves = ["first", "second", "third"].map(id => handle.change(doc => {
    doc.todos.push({ id, title: id, completed: false })
  }))
  saves.push(handle.change(doc => {
    doc.todos[0]!.completed = true
    doc.todos[1]!.title = "Edited title"
    doc.todos.splice(2, 1)
  }))
  const expected = {
    todos: [
      { id: "first", title: "first", completed: true },
      { id: "second", title: "Edited title", completed: false },
    ],
  }
  assert.deepEqual(Automerge.toJS(handle.doc()), expected)
  await Promise.all(saves)
  const heads = Automerge.getHeads(handle.doc())
  await writer.node.disconnectAll()

  const reader = openRepo()
  const reloaded = await reader.repo.find<TodoDocument>(url, { signal: AbortSignal.timeout(2000) })
  assert.deepEqual(Automerge.toJS(reloaded.doc()), expected)
  assert.deepEqual(Automerge.getHeads(reloaded.doc()), heads)

  await reloaded.change(doc => { doc.todos[1]!.completed = true })
  await reader.node.disconnectAll()
  const third = openRepo()
  const reopened = await third.repo.find<TodoDocument>(url, { signal: AbortSignal.timeout(2000) })
  expected.todos[1]!.completed = true
  assert.deepEqual(Automerge.toJS(reopened.doc()), expected)
  assert.deepEqual(Automerge.getHeads(reopened.doc()), Automerge.getHeads(reloaded.doc()))
})
