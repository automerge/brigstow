import type { SedimentreeMeta, SedimentreeRecord } from "./SedimentreeSource.js"

export interface SedimentreeAdapter<State> {
  /**
   * Get metadata about the exportable CRDT as a sedimentree graph.
   *
   * The `notAncestorsOf` option can be used to filter the results to only
   * records which are not ancestors of the given heads.
   */
  metadata(
    state: State,
    opts?: { notAncestorsOf?: string[] }
  ): Iterable<SedimentreeMeta>

  /** Materialize bytes only for the metadata entries a source is missing. */
  materialize(
    state: State,
    metas: SedimentreeMeta[]
  ): Promise<Uint8Array[]> | Uint8Array[]

  /** Apply inbound cleartext records and return the new CRDT state. */
  apply(state: State, records: SedimentreeRecord[]): State
}
