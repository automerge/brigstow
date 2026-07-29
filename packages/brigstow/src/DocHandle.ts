import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import type { BinaryDocumentId, DocumentId } from "./DocumentId.js"
import type { SedimentreeHandle } from "./SedimentreeSource.js"

export class DocHandle<D extends DocType<any, any, any, any>> {

  readonly #doctype: D
  #document: DocState<D>
  #sedimentreeHandle: SedimentreeHandle

  get documentId(): DocumentId {
    return this.#sedimentreeHandle.documentId
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
  }

  on<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn: DocHandleEvents<D>[E]
  ): this {
    return this
  }

  off<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn?: DocHandleEvents<D>[E]
  ): this {
    return this
  }
}
