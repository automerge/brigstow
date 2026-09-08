import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import { type StringDocumentId, stringifyDocId } from "./DocumentId.js"
import { sedimentreeRecordKey, type SedimentreeHandle } from "./SedimentreeSource.js"

export class DocHandle<D extends DocType<any, any, any, any>> {
  readonly #doctype: D
  #document: DocState<D>
  #sedimentreeHandle: SedimentreeHandle
  #changeListeners: Set<DocHandleEvents<D>["change"]> = new Set()
  #pendingSave: Promise<void> = Promise.resolve()
  #pendingRefresh: Promise<void> | undefined
  #refreshRequested = false

  get documentId(): StringDocumentId {
    return stringifyDocId(this.#sedimentreeHandle.documentId)
  }

  /** @hidden */
  constructor(sedimentreeHandle: SedimentreeHandle, doctype: D, document: DocState<D>) {
    this.#doctype = doctype
    this.#document = document
    this.#sedimentreeHandle = sedimentreeHandle
    DocHandle.#subscribe(sedimentreeHandle, new WeakRef(this))
  }

  /** @hidden Subscribe before reading the initial source snapshot. */
  static async load<D extends DocType<any, any, any, any>>(
    source: SedimentreeHandle, doctype: D, document: DocState<D> = doctype.empty(),
  ): Promise<DocHandle<D>> {
    const handle = new DocHandle(source, doctype, document)
    await handle.#requestRefresh()
    return handle
  }

  // Keep the listener's closure separate from the constructor: the source must
  // not retain the document (or its application listeners) after it is dropped.
  static #subscribe<D extends DocType<any, any, any, any>>(
    source: SedimentreeHandle, reference: WeakRef<DocHandle<D>>,
  ): void {
    source.on("change", function listener() {
      const handle = reference.deref()
      if (handle) void handle.#requestRefresh()
      else source.off("change", listener)
    })
  }

  #requestRefresh(): Promise<void> {
    this.#refreshRequested = true
    if (!this.#pendingRefresh) {
      const refreshed = Promise.resolve().then(async () => {
        while (this.#refreshRequested) {
          this.#refreshRequested = false
          await this.#refresh()
        }
      }).finally(() => {
        this.#pendingRefresh = undefined
        // An event arriving during failed IO still deserves another attempt.
        if (this.#refreshRequested) void this.#requestRefresh()
      })
      this.#pendingRefresh = refreshed
      void refreshed.catch(error => console.error("Document refresh failed", error))
    }
    return this.#pendingRefresh
  }

  async #refresh(): Promise<void> {
    const heads = this.#doctype.heads(this.#document)
    if (sameHeads(heads, this.#sedimentreeHandle.heads())) return
    const metas = Array.from(this.#sedimentreeHandle.metadata({ notAncestorsOf: heads }))
    if (!metas.length) return
    const data = await this.#sedimentreeHandle.materialize(metas)
    // Local changes may have happened during materialization. Merge into the
    // CURRENT state, never the snapshot used to request the records.
    const before = [...this.#doctype.heads(this.#document)]
    this.#document = this.#doctype.sedimentree.apply(
      this.#document, metas.map((meta, i) => ({ ...meta, bytes: data[i]! })),
    )
    if (!sameHeads(before, this.#doctype.heads(this.#document))) this.#emitChange()
    // Incoming records are already stored by the source; do not save an echo.
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

function sameHeads(left: string[], right: string[]): boolean {
  const sorted = [...right].sort()
  return left.length === right.length && [...left].sort().every((head, i) => head === sorted[i])
}
