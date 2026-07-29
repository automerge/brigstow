import { decodeHeads, encodeHeads, stringifyAutomergeUrl, type AutomergeUrl, type UrlHeads, type UrlOptions } from "./AutomergeUrl.js"
import type { DocHandleEvents } from "./DocHandleEvents.js"
import type { DocChange, DocState, DocType, DocView } from "./DocType.js"
import type { BinaryDocumentId, DocumentId } from "./DocumentId.js"

export class DocHandle<D extends DocType<any, any, any, any>> {
  #fixedHeads?: UrlHeads | undefined

  readonly #doctype: D
  #document: DocState<D>

  get documentId(): DocumentId {
    return this.#document.documentId
  }

  /** @hidden */
  constructor(doctype: D, document: DocState<D>, options: {heads?: UrlHeads} = {}) {
    this.#doctype = doctype
    this.#document = document

    if ("heads" in options && options.heads) {
      this.#fixedHeads = options.heads
    }
  }

  get url(): AutomergeUrl {
    let arg: UrlOptions = { documentId: this.documentId }
    if (this.#fixedHeads) {
      arg.heads = this.#fixedHeads
    }
    return stringifyAutomergeUrl(arg)
  }

  doc(): DocView<D> {
    return this.#document.view(this.#document)
  }

  heads(): UrlHeads {
    if (this.#fixedHeads) return this.#fixedHeads
    return encodeHeads(this.#document.heads())
  }

  viewAt(heads: UrlHeads): DocView<D> {
    let decoded = decodeHeads(heads)
    return this.#document.viewAt(decoded)
  }

  change(
    change: DocChange<D>,
  ) {
    this.#throwIfFixedHeads("change")
    this.#document = this.#doctype.change(this.#document, change)
  }

  isReadOnly() {
    return !!this.#fixedHeads
  }

  equals(other: DocHandle<any>): boolean {
    return this.url === other.url
  }

  on<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn: DocHandleEvents<D>[E]
  ): this {
    return this
  }

  addListener<E extends keyof DocHandleEvents<D>>(
    event: E,
    fn: DocHandleEvents<D>[E]
  ): this {
    return this.on(event, fn)
  }

  once<E extends keyof DocHandleEvents<D>>(
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

  #throwIfFixedHeads(operation: string) {
    if (this.#fixedHeads) {
      throw new Error(
        `Cannot ${operation} on DocHandle#${this.documentId}: it is in view-only mode at specific heads.`
      )
    }
  }
}
