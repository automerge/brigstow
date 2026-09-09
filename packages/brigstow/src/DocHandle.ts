import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import { type StringDocumentId, stringifyDocId } from "./DocumentId.js"
import { sedimentreeRecordKey, type SedimentreeHandle, type SedimentreeRecord } from "./SedimentreeSource.js"
import { sameHeads } from "./helpers/sameHeads.js"

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

  /** @internal Merge scheduler-loaded records into the current state, without saving an echo. */
  applyRecords(records: SedimentreeRecord[]): void {
    // Local edits may have happened while the scheduler was materializing bytes.
    const before = [...this.#doctype.heads(this.#document)]
    this.#document = this.#doctype.sedimentree.apply(this.#document, records)
    if (!sameHeads(before, this.#doctype.heads(this.#document))) this.#emitChange()
  }

  #emitChange(): void {
    for (const listener of this.#changeListeners) {
      listener({ handle: this, doc: this.#document })
    }
  }

  doc(): DocView<D> {
    return this.#doctype.view(this.#document)
  }

  heads(): string[] {
    return this.#doctype.heads(this.#document)
  }

  viewAt(heads: string[]): DocView<D> {
    return this.#doctype.viewAt(this.#document, heads)
  }

  /** Update the local view synchronously; resolve once the source has persisted the change. */
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
    this.#emitChange()
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
