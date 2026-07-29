import { Subduction } from "@automerge/subduction";
import type { SedimentreeSource } from "@brigstow/brigstow";
import type { DocumentId } from "@brigstow/brigstow/src/DocumentId.js";
import type { SedimentreeQuery, SedimentreeCreateRequest, SedimentreeHandle, SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow/src/SedimentreeSource.js";
import * as uuid from "uuid"

export class SubductionSource implements SedimentreeSource {
  constructor(private subduction: Subduction) {

  }
  find(id: DocumentId): SedimentreeQuery {
    throw new Error("Method not implemented.");
  }
  create(request: SedimentreeCreateRequest): Promise<SedimentreeHandle> {
    const handle: SedimentreeHandle = {
      documentId: uuid.v4() as DocumentId,
      documentType: "automerge",
      heads: function (): string[] {
        throw new Error("Function not implemented.");
      },
      metadata: function (opts?: { notAncestorsOf?: string[]; }): Iterable<SedimentreeMeta> {
        throw new Error("Function not implemented.");
      },
      materialize: function (metas: SedimentreeMeta[]): Promise<Uint8Array[]> {
        throw new Error("Function not implemented.");
      },
      apply: function (records: SedimentreeRecord[]): Promise<void> {
        throw new Error("Function not implemented.");
      },
      on: function (event: "change", listener: () => void): void {
        throw new Error("Function not implemented.");
      },
      off: function (event: "change", listener: () => void): void {
        throw new Error("Function not implemented.");
      }
    }
    return Promise.resolve(handle)
  }
  flush?(ids?: DocumentId[]): Promise<void> {
    throw new Error("Method not implemented.");
  }
  shutdown?(): Promise<void> {
    throw new Error("Method not implemented.");
  }
}
