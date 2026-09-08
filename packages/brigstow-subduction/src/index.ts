import type { Subduction } from "@automerge/subduction/slim"
import type { DocumentId, SedimentreeSource, SedimentreeCreateRequest, SedimentreeHandle, Query, QueryState } from "@brigstow/brigstow"
import { SubductionSedimentreeHandle } from "./SubductionSedimentreeHandle.js"
import { ObservableStorage } from "./ObservableStorage.js"
import { writeRecords } from "./records.js"
import { DocumentSync, NoSyncPeersError, type SubductionSourceOptions } from "./DocumentSync.js"

export { ObservableStorage } from "./ObservableStorage.js"
export { SubductionSedimentreeHandle } from "./SubductionSedimentreeHandle.js"
export { NoSyncPeersError, type SubductionSourceOptions } from "./DocumentSync.js"

export class SubductionSource implements SedimentreeSource {
  #sync: DocumentSync
  #hasServers: boolean
  #closed = false

  /** Subduction does not persist document types; this type is used when reopening. */
  constructor(private subduction: Subduction, private documentType = "automerge", options: SubductionSourceOptions = {}) {
    if (!(subduction.storage instanceof ObservableStorage)) {
      throw new Error("Construct Subduction with an ObservableStorage to use SubductionSource")
    }
    this.#sync = new DocumentSync(subduction, options)
    this.#hasServers = !!options.syncServers?.length
  }

  /** Local-first lookup; fetch and subscribe via peers if the local copy is absent. */
  find(id: DocumentId): Query<SedimentreeHandle> {
    if (this.#closed) throw new Error("SubductionSource is shut down")
    const documentId = id.slice() as DocumentId
    return new PromiseQuery(documentId, this.#findHandle(documentId))
  }

  async create(request: SedimentreeCreateRequest): Promise<SedimentreeHandle> {
    if (this.#closed) throw new Error("SubductionSource is shut down")
    const documentId = (request.documentId?.slice() ?? crypto.getRandomValues(new Uint8Array(32))) as DocumentId
    await writeRecords(this.subduction, documentId, request.initialRecords, false)
    const handle = await this.#open(documentId, request.documentType)
    this.#sync.schedule(documentId)
    return handle
  }

  /** Explicitly synchronize one document, or all documents opened/created by this source. */
  sync(id?: DocumentId): Promise<void> { return this.#sync.sync(id) }

  /** Stop retries and disconnect this source's Subduction instance. */
  shutdown(): Promise<void> {
    this.#closed = true
    return this.#sync.shutdown()
  }

  #open(id: DocumentId, documentType = this.documentType): Promise<SubductionSedimentreeHandle> {
    return SubductionSedimentreeHandle.open(this.subduction, id, documentType, () => this.#sync.schedule(id))
  }

  async #findHandle(id: DocumentId): Promise<SedimentreeHandle | null> {
    const local = await this.#open(id)
    if (local.exists) {
      this.#sync.schedule(id)
      return local
    }
    try {
      await this.#sync.sync(id)
    } catch (error) {
      // Preserve local-only sources' unavailable behavior. Connection/transport
      // errors remain failures, not evidence that the document doesn't exist.
      if (error instanceof NoSyncPeersError && !this.#hasServers) return null
      throw error
    }
    const fetched = await this.#open(id)
    return fetched.exists ? fetched : null
  }
}

class PromiseQuery<F> implements Query<F> {
  private listeners: Set<((state: QueryState<F>) => void)> = new Set()
  private value: F | null | undefined
  private error: Error | undefined
  private disposed = false

  constructor(private docId: DocumentId, private promise: Promise<F | null>) {
    this.promise
      .then(value => {
        this.value = value
      })
      .catch(error => {
        this.error = error instanceof Error ? error : new Error(String(error))
      })
      .finally(() => {
        this.#onchange()
      })
  }

  id(): DocumentId {
    return this.docId.slice() as DocumentId
  }

  state(): QueryState<F> {
    if (this.value !== undefined) {
      if (this.value == null) {
        return { type: "unavailable" }
      } else {
        return { type: "ready", handle: this.value }
      }
    }
    if (this.error !== undefined) return { type: "failed", error: this.error }
    return { type: "finding" }
  }

  subscribe(callback: (state: QueryState<F>) => void): () => void {
    if (this.disposed) throw new Error("Query is disposed")
    this.listeners.add(callback)
    return () => {
      this.listeners.delete(callback)
    }
  }

  dispose(): void {
    this.disposed = true
    this.listeners.clear()
    // WASM sync calls cannot be aborted by dropping a Promise. Their configured
    // deadline bounds the work; disposing a query stops its notifications.
  }

  #onchange = () => {
    if (this.disposed) return
    const state = this.state()
    for (const listener of [...this.listeners]) {
      try { listener(state) } catch (error) { console.error("Subduction query listener failed", error) }
    }
  }
}
