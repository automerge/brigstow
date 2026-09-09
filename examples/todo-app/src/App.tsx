import { createMemo, createSignal, For, onCleanup, onMount, Show } from "solid-js"
import { IndexedDbStorage, MemorySigner, Subduction } from "@automerge/subduction"
import { Repo, type AutomergeUrl, type DocHandle } from "@brigstow/automerge-repo"
import { type StringDocumentId } from "@brigstow/brigstow"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"

interface Todo {
  id: string
  title: string
  completed: boolean
}

type TodoDocument = {
  todos: Todo[]
}

type Filter = "all" | "active" | "completed"
type Phase = "loading" | "ready" | "error"
type TodoHandle = DocHandle<TodoDocument>

export default function App() {
  let handle: TodoHandle | undefined
  let source: SubductionSource | undefined
  let repo: Repo | undefined
  let disposed = false
  let input: HTMLInputElement | undefined
  let removeHandleListener: (() => void) | undefined

  const [phase, setPhase] = createSignal<Phase>("loading")
  const [failure, setFailure] = createSignal<string>()
  const [pendingSaves, setPendingSaves] = createSignal(0)
  const [saveFailure, setSaveFailure] = createSignal<string>()
  const [syncFailure, setSyncFailure] = createSignal<string>()
  const [activeFilter, setActiveFilter] = createSignal<Filter>("all")
  const [documentRevision, setDocumentRevision] = createSignal(0)

  // DocHandle isn't a reactive value, so every handle change advances a Solid
  // signal. Any memo that reads the document will then get a fresh view.
  const document = createMemo(() => {
    documentRevision()
    return handle?.doc()
  })

  const todos = createMemo(() => [...(document()?.todos ?? [])])
  const visibleTodos = createMemo(() => {
    const filter = activeFilter()
    return todos().filter(todo => {
      if (filter === "active") return !todo.completed
      if (filter === "completed") return todo.completed
      return true
    })
  })
  const itemsLeft = createMemo(() => todos().filter(todo => !todo.completed).length)
  const hasCompleted = createMemo(() => todos().some(todo => todo.completed))
  const documentLabel = createMemo(() => {
    documentRevision()
    return handle ? shortDocumentId(handle.documentId) : "No document yet"
  })

  onMount(() => {
    window.addEventListener("beforeunload", warnUnsavedChanges)
    void start()
  })

  onCleanup(() => {
    disposed = true
    removeHandleListener?.()
    repo?.dispose()
    window.removeEventListener("beforeunload", warnUnsavedChanges)
    void source?.shutdown().catch(error => console.error("Unable to stop synchronization", error))
  })

  function warnUnsavedChanges(event: BeforeUnloadEvent) {
    if (pendingSaves() > 0 || saveFailure()) {
      event.preventDefault()
      event.returnValue = ""
    }
  }

  async function changeDocument(change: Parameters<TodoHandle["change"]>[0]) {
    if (!handle) return
    setPendingSaves(count => count + 1)
    try {
      await handle.change(change)
      setSaveFailure(undefined)
    } catch (error) {
      setSaveFailure(errorMessage(error))
    } finally {
      setPendingSaves(count => count - 1)
    }
  }

  async function start() {
    setPhase("loading")
    try {
      if (!source) {
        const backend = await IndexedDbStorage.setup(window.indexedDB, "brigstow-todo")
        const subduction = new Subduction({
          signer: MemorySigner.generate(),
          storage: new ObservableStorage(backend),
        })
        const server = import.meta.env.VITE_SUBDUCTION_SYNC_SERVER ?? "wss://subduction.sync.inkandswitch.com"
        source = new SubductionSource(subduction, "automerge", {
          syncServers: server ? [server] : [],
          onSyncError: error => setSyncFailure(errorMessage(error)),
          onSynced: () => setSyncFailure(undefined),
        })
      }
      if (disposed) { await source.shutdown(); return }
      repo?.dispose()
      repo = new Repo(source)
      const existingDocumentId = documentIdFromHash(window.location.hash)
      const resolvedHandle = existingDocumentId
        ? await repo.find<TodoDocument>("automerge:" + existingDocumentId as AutomergeUrl)
        : await repo.create<TodoDocument>({ todos: [] })
      if (disposed) return
      const onChange = () => setDocumentRevision(revision => revision + 1)

      handle = resolvedHandle
      resolvedHandle.on("change", onChange)
      removeHandleListener = () => resolvedHandle.off("change", onChange)

      setDocumentHash(resolvedHandle.documentId)
      setDocumentRevision(revision => revision + 1)
      setPhase("ready")
      queueMicrotask(() => input?.focus())
    } catch (error) {
      console.error("Unable to initialize the Brigstow todo document", error)
      setFailure(errorMessage(error))
      setPhase("error")
    }
  }

  function addTodo(event: SubmitEvent) {
    event.preventDefault()

    const title = input?.value.trim()
    if (!title || !handle) return

    void changeDocument(doc => {
      doc.todos.push({
        id: randomUUID(),
        title,
        completed: false,
      })
    })

    const form = event.currentTarget as HTMLFormElement
    form.reset()
    input?.focus()
  }

  function toggleTodo(id: string) {
    void changeDocument(doc => {
      const todo = doc.todos.find(candidate => candidate.id === id)
      if (todo) todo.completed = !todo.completed
    })
  }

  function removeTodo(id: string) {
    void changeDocument(doc => {
      const index = doc.todos.findIndex(todo => todo.id === id)
      if (index >= 0) doc.todos.splice(index, 1)
    })
  }

  function clearCompleted() {
    void changeDocument(doc => {
      for (let index = doc.todos.length - 1; index >= 0; index -= 1) {
        if (doc.todos[index]?.completed) doc.todos.splice(index, 1)
      }
    })
  }

  return (
    <main class="app-shell">
      <header class="app-header">
        <div class="brand-mark" aria-hidden="true">
          <span></span><span></span><span></span>
        </div>
        <div>
          <p class="eyebrow">Local-first by design</p>
          <h1>Things to do</h1>
        </div>
      </header>

      <section class="todo-card" aria-labelledby="todo-heading">
        <h2 id="todo-heading" class="visually-hidden">Todo list</h2>

        <form class="todo-form" onSubmit={addTodo}>
          <label class="visually-hidden" for="new-todo">Add a todo</label>
          <input
            ref={input}
            id="new-todo"
            name="title"
            type="text"
            autocomplete="off"
            maxLength={120}
            placeholder="What needs doing?"
            disabled={phase() !== "ready"}
          />
          <button type="submit" disabled={phase() !== "ready"}>Add task</button>
        </form>

        <Show when={phase() !== "ready"}>
          <div
            class="status"
            data-state={phase() === "error" ? "error" : undefined}
            role="status"
            aria-live="polite"
          >
            <span class="status-dot" aria-hidden="true"></span>
            <span>
              {phase() === "error"
                ? `Could not start the todo document: ${failure() ?? "Unknown error"}`
                : "Starting your local document…"}
            </span>
          </div>
        </Show>

        <div class="toolbar" aria-label="Filter todos">
          <div class="filters" role="group" aria-label="Todo status">
            <FilterButton value="all" active={activeFilter()} onSelect={setActiveFilter}>All</FilterButton>
            <FilterButton value="active" active={activeFilter()} onSelect={setActiveFilter}>Active</FilterButton>
            <FilterButton value="completed" active={activeFilter()} onSelect={setActiveFilter}>Done</FilterButton>
          </div>
          <span class="document-label">{documentLabel()}</span>
        </div>

        <ul class="todo-list">
          <For each={visibleTodos()}>
            {todo => (
              <li class="todo-item" data-completed={todo.completed ? "true" : undefined}>
                <input
                  type="checkbox"
                  checked={todo.completed}
                  aria-label={`Mark ${todo.title} as ${todo.completed ? "active" : "done"}`}
                  onChange={() => toggleTodo(todo.id)}
                />
                <span class="todo-title">{todo.title}</span>
                <button
                  type="button"
                  class="remove-todo"
                  aria-label={`Delete ${todo.title}`}
                  onClick={() => removeTodo(todo.id)}
                >
                  ×
                </button>
              </li>
            )}
          </For>
        </ul>

        <Show when={phase() === "ready" && visibleTodos().length === 0}>
          <p class="empty-state">
            <span aria-hidden="true">✓</span>
            Nothing here. Enjoy the clear view.
          </p>
        </Show>

        <footer class="list-footer">
          <span>{itemsLeft()} {itemsLeft() === 1 ? "item" : "items"} left</span>
          <button type="button" disabled={!hasCompleted()} onClick={clearCompleted}>
            Clear completed
          </button>
        </footer>
      </section>

      <Show when={phase() === "ready"}>
        <p class="app-footer" role="status" aria-live="polite">
          {saveFailure()
            ? `Could not save changes: ${saveFailure()}`
            : pendingSaves() > 0 ? "Saving…" : "All changes saved locally"}
        </p>
      </Show>

      <Show when={syncFailure()}>
        <p class="app-footer" role="status">Peer sync unavailable: {syncFailure()}. Retrying in the background.</p>
      </Show>
      <Show when={phase() === "error"}>
        <button type="button" onClick={() => void start()}>Retry opening document</button>
      </Show>

      <p class="app-footer">
        Demo documents are shared via a public sync server. Do not enter sensitive data.
      </p>
      <p class="app-footer">
        Built with SolidJS and <code>@brigstow/automerge-repo</code>.
      </p>
    </main>
  )
}

interface FilterButtonProps {
  value: Filter
  active: Filter
  onSelect: (filter: Filter) => void
  children: string
}

function FilterButton(props: FilterButtonProps) {
  return (
    <button
      type="button"
      aria-pressed={props.active === props.value}
      onClick={() => props.onSelect(props.value)}
    >
      {props.children}
    </button>
  )
}

function documentIdFromHash(hash: string): StringDocumentId | undefined {
  const documentId = decodeURIComponent(hash.replace(/^#/, "")).trim()
  return documentId.length > 0 ? documentId as StringDocumentId : undefined
}

function setDocumentHash(documentId: string) {
  const url = new URL(window.location.href)
  url.hash = encodeURIComponent(documentId)
  window.history.replaceState(null, "", url)
}

function shortDocumentId(documentId: string) {
  return documentId.length > 30
    ? `${documentId.slice(0, 18)}…${documentId.slice(-7)}`
    : documentId
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

/** Necessary because crypto.randomUUID is not supported in insecure contexts */
function randomUUID() {
  if (crypto.randomUUID) return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6]! & 0x0f) | 0x40; // version 4
  b[8] = (b[8]! & 0x3f) | 0x80; // variant 10
  const hex = [...b].map(x => x.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}
