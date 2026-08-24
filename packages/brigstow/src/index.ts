import { DocHandle } from "./DocHandle.js";
import type { DocInit, DocState, DocType } from "./DocType.js";
import type { DocumentId } from "./DocumentId.js";
import { type Query, mapQueryAsync } from "./query.js";
import type { SedimentreeRecord, SedimentreeSource } from "./SedimentreeSource.js";

export { type DocumentId, type StringDocumentId, stringifyDocId } from "./DocumentId.js"
export type { SedimentreeMeta, SedimentreeSource, SedimentreeHandle, SedimentreeCreateRequest, SedimentreeRecord } from "./SedimentreeSource.js"
export { type DocType } from "./DocType.js"
export { mapQuery, mapQueryAsync, type Query, type QueryState } from "./query.js"


export class Repo {
  constructor(private source: SedimentreeSource) { }
  #handles: Set<DocHandle<any>> = new Set()

  find<D extends DocType<any, any, any, any>>(docType: D, docId: DocumentId): Query<DocHandle<D>> {
    const query = this.source.find(docId)
    return mapQueryAsync(query, async sedimentreeHandle => {
      const metas = Array.from(sedimentreeHandle.metadata())
      const data = await sedimentreeHandle.materialize(metas)
      const sedimentreeRecords: SedimentreeRecord[] = metas.map((meta, i) => ({ ...meta, bytes: data[i]! }))
      const init = docType.sedimentree.apply(docType.empty(), sedimentreeRecords)
      return new DocHandle(sedimentreeHandle, docType, init)
    })
  }

  async create<D extends DocType<any, any, any, any>>(docType: D, value: DocInit<D>): Promise<DocHandle<D>> {
    const sedimentreeHandle = await this.source.create({
      documentType: docType.name,
      initialRecords: []
    })
    return new DocHandle(sedimentreeHandle, docType, docType.init(value))
  }
}
