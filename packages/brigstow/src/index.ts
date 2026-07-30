import { DocHandle } from "./DocHandle.js";
import type { DocInit, DocState, DocType } from "./DocType.js";
import type { DocumentId } from "./DocumentId.js";
import type { SedimentreeSource } from "./SedimentreeSource.js";

export { type DocumentId, type StringDocumentId, stringifyDocId } from "./DocumentId.js"
export type { SedimentreeQuery, SedimentreeMeta, SedimentreeSource, SedimentreeHandle, SedimentreeCreateRequest, SedimentreeRecord } from "./SedimentreeSource.js"
export { type DocType } from "./DocType.js"

export class Repo {
  constructor(private source: SedimentreeSource) { }
  #handles: Set<DocHandle<any>> = new Set()

  find<D extends DocType<any, any, any, any>>(docType: D, docId: DocumentId): Query<D> {
    throw new Error("not implemented")
  }

  async create<D extends DocType<any, any, any, any>>(docType: D, value: DocInit<D>): Promise<DocHandle<D>> {
    const sedimentreeHandle = await this.source.create({
      documentType: docType.name,
      initialRecords: []
    })
    return new DocHandle(sedimentreeHandle, docType, docType.init(value))
  }
}

export interface Query<D extends DocType<any, any, any, any>> {
  id(): DocumentId
  state(): QueryState<D>
  subscribe(callback: (state: QueryState<D>) => void): () => void
}

export type QueryState<D extends DocType<any, any, any, any>> =
  | { type: "finding" }
  | { type: "unavailable" }
  | { type: "failed"; error: Error }
  | { type: "ready"; handle: DocHandle<D> }
