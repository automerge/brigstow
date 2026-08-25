import type { SedimentreeHandle } from "@brigstow/brigstow";
import type { DocumentId } from "@brigstow/brigstow/src/DocumentId.js";
import type { SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow/src/SedimentreeSource.js";

export class SubductionSedimentreeHandle implements SedimentreeHandle {
  readonly documentId: DocumentId;
  readonly documentType: string;

  constructor(private sdn: Subduction, private tree: Sedimentree, documentId: DocumentId, documentType: string) {
    this.documentId = documentId
    this.documentType = documentType
  }

  heads(): string[] {
    throw new Error("Method not implemented.");
  }
  metadata(opts?: { notAncestorsOf?: string[]; }): Iterable<SedimentreeMeta> {
    throw new Error("Method not implemented.");
  }
  materialize(metas: SedimentreeMeta[]): Promise<Uint8Array[]> {
    throw new Error("Method not implemented.");
  }
  apply(records: SedimentreeRecord[]): Promise<void> {
    throw new Error("Method not implemented.");
  }
  on(event: "change", listener: () => void): void {
    throw new Error("Method not implemented.");
  }
  off(event: "change", listener: () => void): void {
    throw new Error("Method not implemented.");
  }

}
