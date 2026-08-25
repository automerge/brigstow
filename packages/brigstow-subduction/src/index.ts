import { Subduction, Sedimentree, Fragment, BlobMeta, SedimentreeId, CommitId, LooseCommit } from "@automerge/subduction/slim";
import type { DocumentId, SedimentreeMeta, SedimentreeRecord, SedimentreeSource, SedimentreeCreateRequest, SedimentreeHandle, Query, QueryState } from "@brigstow/brigstow";
import * as uuid from "uuid"
import { SubductionSedimentreeHandle } from "./SubductionSedimentreeHandle.js";

export class SubductionSource implements SedimentreeSource {
  constructor(private subduction: Subduction) {

  }

  find(id: DocumentId): Query<SedimentreeHandle> {
    const treeId = SedimentreeId.fromBytes(id)
    return new PromiseQuery(id, this.#findHandle(treeId))
  }

  async create(request: SedimentreeCreateRequest): Promise<SedimentreeHandle> {
    let { documentId, initialRecords } = request
    if (!documentId) {
      const bytes = new Uint8Array(32)
      // TODO: Subduction requires 32 byte document IDs, so concatenate two UUIDs. This is obviously
      // a bad way to do this.
      uuid.v4(undefined, bytes, 0)
      uuid.v4(undefined, bytes, 16)
      documentId = bytes as DocumentId
    } else {
      if (documentId.length != 32) {
        // TODO: ensure this invariant in brigstow, not here
        throw new Error("subduction requires 32 byte document IDs")
      }
    }
    const sedimentreeId = SedimentreeId.fromBytes(documentId)
    const fragments = []
    const commits = []
    const blobs = []
    for (const r of initialRecords) {
      blobs.push(r.bytes)
      const blob = new BlobMeta(r.bytes)
      const head = CommitId.fromHexString(r.head)
      if (r.kind === "fragment") {
        const boundary = r.boundary.map(b => CommitId.fromHexString(b))
        const checkpoints = r.checkpoints.map(c => CommitId.fromHexString(c))
        fragments.push(new Fragment(sedimentreeId, head, boundary, checkpoints, blob))
      } else if (r.kind === "commit") {
        const parents = r.parents.map(b => CommitId.fromHexString(b))
        commits.push(new LooseCommit(sedimentreeId, head, parents, blob))
      } else {
        throw new Error(`Unknown record type`)
      }
    }
    const tree = new Sedimentree(fragments, commits)
    await this.subduction.storeSedimentree(sedimentreeId, tree, blobs)

    return new SubductionSedimentreeHandle(documentId, request.documentType)
  }
  flush?(ids?: DocumentId[]): Promise<void> {
    throw new Error("Method not implemented.");
  }
  shutdown?(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async #findHandle(docId: SedimentreeId): Promise<SedimentreeHandle> {
    const [fragments, commits] = await Promise.all([
      this.subduction.getFragments(docId),
      this.subduction.getCommits(docId),
    ])

  }
}

class PromiseQuery<F> implements Query<F> {
  private listeners: Set<((state: QueryState<F>) => void)> = new Set()
  private value: F | null | undefined
  private error: Error | undefined

  constructor(private docId: DocumentId, private promise: Promise<F | null>) {
    this.promise
      .then(value => {
        this.value = value
      })
      .catch(error => {
        this.error = error
      })
      .finally(() => {
        this.#onchange()
      })
  }

  id(): DocumentId {
    return this.docId
  }

  state(): QueryState<F> {
    if (this.value !== undefined) {
      if (this.value == null) {
        return { type: "unavailable" }
      } else {
        return { type: "ready", handle: this.value }
      }
    }
    if (this.error !== undefined) return { type: "failed", error: this.error }
    return { type: "finding" }
  }

  subscribe(callback: (state: QueryState<F>) => void): () => void {
    this.listeners.add(callback)
    return () => {
      this.listeners.delete(callback)
    }
  }

  #onchange = () => {
    const state = this.state()
    for (const listener of this.listeners) {
      listener(state)
    }
  }
}
