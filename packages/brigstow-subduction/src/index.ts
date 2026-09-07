import type { Subduction } from "@automerge/subduction/slim"
import type { DocumentId, SedimentreeSource, SedimentreeCreateRequest, SedimentreeHandle, Query, QueryState } from "@brigstow/brigstow"
import { SubductionSedimentreeHandle } from "./SubductionSedimentreeHandle.js"
import { ObservableStorage } from "./ObservableStorage.js"
import { writeRecords } from "./records.js"

export { ObservableStorage } from "./ObservableStorage.js"
export { SubductionSedimentreeHandle } from "./SubductionSedimentreeHandle.js"

export class SubductionSource implements SedimentreeSource {
  /** Subduction does not persist document types; this type is used when reopening. */
  constructor(private subduction: Subduction, private documentType = "automerge") {
    if (!(subduction.storage instanceof ObservableStorage)) {
      throw new Error("Construct Subduction with an ObservableStorage to use SubductionSource")
    }
  }

  /** Find locally persisted data. The host establishes peer synchronization separately. */
  find(id: DocumentId): Query<SedimentreeHandle> {
    return new PromiseQuery(id, this.#findHandle(id))
  }

  async create(request: SedimentreeCreateRequest): Promise<SedimentreeHandle> {
    const documentId = (request.documentId?.slice() ?? crypto.getRandomValues(new Uint8Array(32))) as DocumentId
    await writeRecords(this.subduction, documentId, request.initialRecords, false)
    return SubductionSedimentreeHandle.open(this.subduction, documentId, request.documentType)
  }

  async #findHandle(id: DocumentId): Promise<SedimentreeHandle | null> {
    const handle = await SubductionSedimentreeHandle.open(this.subduction, id, this.documentType)
    return handle.exists ? handle : null
  }
}

class PromiseQuery<F> implements Query<F> {
  private listeners: Set<((state: QueryState<F>) => void)> = new Set()
  private value: F | null | undefined
  private error: Error | undefined

  constructor(private docId: DocumentId, private promise: Promise<F | null>) {
    this.promise
      .then(value => {
        this.value = value
      })
      .catch(error => {
        this.error = error
      })
      .finally(() => {
        this.#onchange()
      })
  }

  id(): DocumentId {
    return this.docId
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
    this.listeners.add(callback)
    return () => {
      this.listeners.delete(callback)
    }
  }

  #onchange = () => {
    const state = this.state()
    for (const listener of [...this.listeners]) {
      listener(state)
    }
  }
}
