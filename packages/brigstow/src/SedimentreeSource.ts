import type { DocumentId } from "./DocumentId.js"

export type SedimentreeMeta =
  | { kind: "commit"; head: string; parents: string[] }
  | {
      kind: "fragment"
      head: string
      boundary: string[]
      checkpoints: string[]
    }

/** Records are identified by the pair `(kind, head)`. */
export type SedimentreeRecord = SedimentreeMeta & { bytes: Uint8Array }

export interface SedimentreeSource {
  find(id: DocumentId): SedimentreeQuery
  create(request: SedimentreeCreateRequest): Promise<SedimentreeHandle>

  flush?(ids?: DocumentId[]): Promise<void>
  shutdown?(): Promise<void>
}

export interface SedimentreeQuery {
  id(): DocumentId
  state(): SedimentreeQueryState
  subscribe(callback: (state: SedimentreeQueryState) => void): () => void
}

export type SedimentreeQueryState =
  | { type: "finding" }
  | { type: "unavailable" }
  | { type: "failed"; error: Error }
  | { type: "ready"; handle: SedimentreeHandle }

export interface SedimentreeHandle {
  readonly documentId: DocumentId
  readonly documentType: string

  /** Source's current graph heads. For encrypted sources, expose cleartext heads. */
  heads(): string[]

  metadata(opts?: { notAncestorsOf?: string[] }): Iterable<SedimentreeMeta>
  materialize(metas: SedimentreeMeta[]): Promise<Uint8Array[]>

  /** Persist/sync already-materialized records. */
  apply(records: SedimentreeRecord[]): Promise<void>

  on(event: "change", listener: () => void): void
  off(event: "change", listener: () => void): void
}

export interface SedimentreeCreateRequest {
  /** Stable name of the document type, e.g. "automerge". */
  documentType: string

  /** Optional requested document ID. Sources may reject IDs they cannot honor. */
  documentId?: DocumentId

  /** Initial records ready to persist immediately. */
  initialRecords: SedimentreeRecord[]
}


export function sedimentreeRecordKey(record: SedimentreeMeta): string {
  return `${record.kind}:${record.head}`
}
