import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import { type StringDocumentId, type DocumentId, stringifyDocId } from "./DocumentId.js"
import type { SedimentreeHandle } from "./SedimentreeSource.js"

export class DocHandle<D extends DocType<any, any, any, any>> {

  readonly #doctype: D
  #document: DocState<D>
  #sedimentreeHandle: SedimentreeHandle
  #changeListeners: Set<DocHandleEvents<D>["change"]> = new Set()

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

  change(
    change: DocChange<D>,
  ) {
    this.#document = this.#doctype.change(this.#document, change)
    for (const listener of this.#changeListeners) {
      listener({
        handle: this,
        doc: this.#document
      })
    }
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
