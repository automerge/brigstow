import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import { type StringDocumentId, type DocumentId, stringifyDocId } from "./DocumentId.js"
import { sedimentreeRecordKey, type SedimentreeHandle } from "./SedimentreeSource.js"

export class DocHandle<D extends DocType<any, any, any, any>> {
  readonly #doctype: D
  #document: DocState<D>
  #sedimentreeHandle: SedimentreeHandle
  #changeListeners: Set<DocHandleEvents<D>["change"]> = new Set()
  #pendingSave: Promise<void> = Promise.resolve()

  get documentId(): StringDocumentId {
    return stringifyDocId(this.#sedimentreeHandle.documentId)
  }

  /** @hidden */
  constructor(sedimentreeHandle: SedimentreeHandle, doctype: D, document: DocState<D>) {
    this.#doctype = doctype
    this.#document = document
    this.#sedimentreeHandle = sedimentreeHandle
  }

  doc(): DocView<D> {
    return this.#doctype.view(this.#document)
  }

  heads(): [string] {
    return this.#document.heads()
  }

  viewAt(heads: [string]): DocView<D> {
    return this.#doctype.viewAt(this.#document, heads)
  }

  /** Update the local view synchronously; resolve once the change is persisted/synced. */
  change(
    change: DocChange<D>,
  ): Promise<void> {
    this.#document = this.#doctype.change(this.#document, change)
    // Serialize writes. A later change retries any still-missing records after
    // a failed save, rather than permanently poisoning the queue.
    const saved = this.#pendingSave.catch(() => {}).then(() => this.#save())
    this.#pendingSave = saved
    // Existing callers may ignore the promise; still report background failures.
    void saved.catch(error => console.error("Document save failed", error))
    for (const listener of this.#changeListeners) {
      listener({
        handle: this,
        doc: this.#document
      })
    }
    return saved
  }

  /** Wait for changes queued so far. Rejects if the latest save failed. */
  flush(): Promise<void> {
    return this.#pendingSave
  }

  async #save(): Promise<void> {
    // Read the latest state when the queued write starts. Rapid edits can share
    // one persisted snapshot, and we need not retain old CRDT states while IO runs.
    const document = this.#document
    const stored = new Set(Array.from(this.#sedimentreeHandle.metadata(), sedimentreeRecordKey))
    const metas = Array.from(this.#doctype.sedimentree.metadata(document))
      .filter(meta => !stored.has(sedimentreeRecordKey(meta)))
    if (!metas.length) return
    const data = await this.#doctype.sedimentree.materialize(document, metas)
    await this.#sedimentreeHandle.apply(metas.map((meta, i) => ({ ...meta, bytes: data[i]! })))
  }

  on<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn: DocHandleEvents<D>[E]
  ): (() => void) {
    if (event === "change") {
      this.#changeListeners.add(fn as DocHandleEvents<D>["change"])
      return () => {
        this.#changeListeners.delete(fn as DocHandleEvents<D>["change"])
      }
    }
    return () => { }
  }

  off<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn: DocHandleEvents<D>[E]
  ) {
    if (event === "change") {
      this.#changeListeners.delete(fn as DocHandleEvents<D>["change"])
    }
  }
}
