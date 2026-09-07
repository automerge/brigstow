import {
  BlobMeta, Checkpoint, CommitId, Fragment, LooseCommit, Sedimentree, SedimentreeId,
  type Subduction,
} from "@automerge/subduction/slim"
import type { DocumentId, SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow"
import type { ObservableStorage } from "./ObservableStorage.js"

/** Own only JS-created/returned wrappers, never borrowed storage arguments. */
export class WasmScope {
  #values: { free(): void }[] = []
  own<T extends { free(): void }>(value: T): T { this.#values.push(value); return value }
  free(): void { for (const value of this.#values.reverse()) value.free() }
}

export const hex = (bytes: Uint8Array): string => Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("")
export const recordKey = (meta: SedimentreeMeta): string => `${meta.kind}:${meta.head}`
export const copyMeta = (meta: SedimentreeMeta): SedimentreeMeta => meta.kind === "commit"
  ? { ...meta, parents: [...meta.parents] }
  : { ...meta, boundary: [...meta.boundary], checkpoints: meta.checkpoints.map(cp => cp.slice()) }

export interface Snapshot {
  records: SedimentreeMeta[]
  heads: string[]
  fingerprint: string
  exists: boolean
}

export async function readSnapshot(storage: ObservableStorage, documentId: DocumentId): Promise<Snapshot> {
  const scope = new WasmScope()
  try {
    const id = scope.own(SedimentreeId.fromBytes(documentId))
    const commits = await storage.loadAllCommits(id)
    commits.forEach(c => scope.own(c))
    const fragments = await storage.loadAllFragments(id)
    fragments.forEach(f => scope.own(f))
    const entries: { meta: SedimentreeMeta; digest: string }[] = []
    for (const stored of [...commits, ...fragments]) {
      const signed = scope.own(stored.signed)
      const payload = scope.own(signed.payload)
      const blobMeta = scope.own(payload.blobMeta)
      const digest = scope.own(blobMeta.digest()).toHexString()
      const ids = (values: CommitId[]) => values.map(value => scope.own(value).toHexString())
      const meta: SedimentreeMeta = "commitId" in payload
        ? { kind: "commit", head: scope.own(payload.commitId).toHexString(), parents: ids(payload.parents).sort() }
        : {
          kind: "fragment", head: scope.own(payload.head).toHexString(), boundary: ids(payload.boundary).sort(),
          checkpoints: payload.checkpoints.map(cp => scope.own(cp).toBytes()),
        }
      entries.push({ meta, digest })
    }
    entries.sort((a, b) => recordKey(a.meta).localeCompare(recordKey(b.meta)))
    const records = entries.map(entry => entry.meta)
    const referenced = new Set<string>()
    const checkpoints = new Set<string>()
    for (const meta of records) {
      for (const dependency of meta.kind === "commit" ? meta.parents : meta.boundary) referenced.add(dependency)
      if (meta.kind === "fragment") for (const cp of meta.checkpoints) checkpoints.add(hex(cp))
    }
    const heads = [...new Set(records.map(meta => meta.head))]
      .filter(head => !referenced.has(head) && !checkpoints.has(head.slice(0, 24))).sort()
    const exists = await storage.containsSedimentreeId(id)
    // Include blob digests: replacing bytes without changing causal metadata
    // must still invalidate consumers. Signatures alone are not data changes.
    return { records, heads, exists, fingerprint: JSON.stringify([exists, entries]) }
  } finally {
    scope.free()
  }
}

/** Batch persistence merges these records into the existing tree. */
export async function writeRecords(
  subduction: Subduction, documentId: DocumentId, records: SedimentreeRecord[], broadcast: boolean,
): Promise<void> {
  const scope = new WasmScope()
  // Copy before the first await, including bytes consumed asynchronously by WASM.
  records = records.map(record => ({ ...copyMeta(record), bytes: record.bytes.slice() }))
  try {
    const id = scope.own(SedimentreeId.fromBytes(documentId))
    const fragments: Fragment[] = []
    const commits: LooseCommit[] = []
    const ids = (values: string[]) => values.map(value => scope.own(CommitId.fromHexString(value)))
    for (const record of records) {
      const head = scope.own(CommitId.fromHexString(record.head))
      const blob = scope.own(new BlobMeta(record.bytes))
      if (record.kind === "commit") {
        commits.push(scope.own(new LooseCommit(id, head, ids(record.parents), blob)))
      } else {
        const checkpoints = record.checkpoints.map(cp => scope.own(new Checkpoint(cp)))
        fragments.push(scope.own(new Fragment(id, head, ids(record.boundary), checkpoints, blob)))
      }
    }
    const tree = scope.own(new Sedimentree(fragments, commits))
    const blobs = records.map(record => record.bytes)
    if (broadcast) {
      scope.own(await subduction.addSedimentree(id, tree, blobs))
    } else {
      await subduction.storeSedimentree(id, tree, blobs)
    }
  } finally {
    scope.free()
  }
}

/**
 * Walk known causal edges, including checkpoint-covered records. When a root
 * lies inside an opaque fragment we cannot reconstruct all of its ancestors;
 * keep uncertain records rather than risk omitting data the caller needs.
 */
export function withoutAncestors(records: SedimentreeMeta[], roots: string[]): SedimentreeMeta[] {
  const byHead = new Map<string, SedimentreeMeta[]>()
  const byPrefix = new Map<string, string[]>()
  for (const meta of records) {
    byHead.set(meta.head, [...(byHead.get(meta.head) ?? []), meta])
    const prefix = meta.head.slice(0, 24)
    byPrefix.set(prefix, [...(byPrefix.get(prefix) ?? []), meta.head])
  }
  const visited = new Set<string>()
  const stack = [...roots]
  while (stack.length) {
    const head = stack.pop()!
    if (visited.has(head)) continue
    visited.add(head)
    for (const meta of byHead.get(head) ?? []) {
      stack.push(...(meta.kind === "commit" ? meta.parents : meta.boundary))
      if (meta.kind === "fragment") {
        for (const cp of meta.checkpoints) stack.push(...(byPrefix.get(hex(cp)) ?? []))
      }
    }
  }
  return records.filter(meta => !visited.has(meta.head))
}
