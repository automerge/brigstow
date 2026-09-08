import { CommitId, SedimentreeId, type Subduction } from "@automerge/subduction/slim"
import type { DocumentId, SedimentreeHandle, SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow"
import { ObservableStorage } from "./ObservableStorage.js"
import { copyMeta, readSnapshot, withoutAncestors, writeRecords, WasmScope, type Snapshot } from "./records.js"

/**
 * A live, synchronous metadata view of persisted records. Storage notifications
 * trigger coalesced asynchronous reads; change is emitted only after publishing
 * the new snapshot. Metadata includes redundant records retained by storage,
 * not just Subduction's minimized resident tree. Blobs are loaded on demand.
 */
export class SubductionSedimentreeHandle implements SedimentreeHandle {
  #documentId: DocumentId
  #snapshot: Snapshot = { records: [], heads: [], fingerprint: "", exists: false }
  #error: Error | undefined
  #listeners = new Set<() => void>()
  #revision = 0
  #completedRevision = -1
  #pending: Promise<void> | undefined

  private constructor(
    private sdn: Subduction,
    private storage: ObservableStorage,
    documentId: DocumentId,
    readonly documentType: string,
    private onPersisted?: () => void,
  ) {
    this.#documentId = documentId.slice() as DocumentId
    const id = SedimentreeId.fromBytes(documentId)
    try { storage.watch(id.toString(), this.#invalidate) } finally { id.free() }
  }

  static async open(sdn: Subduction, documentId: DocumentId, documentType: string, onPersisted?: () => void): Promise<SubductionSedimentreeHandle> {
    if (!(sdn.storage instanceof ObservableStorage)) {
      throw new Error("Construct Subduction with an ObservableStorage to use SubductionSource")
    }
    // Install the watcher before loading. Any concurrent mutation invalidates
    // that load, preventing an older snapshot from replacing newer metadata.
    const handle = new SubductionSedimentreeHandle(sdn, sdn.storage, documentId, documentType, onPersisted)
    await handle.#refresh()
    return handle
  }

  get documentId(): DocumentId { return this.#documentId.slice() as DocumentId }

  /** @internal Distinguish an empty persisted document from an absent one. */
  get exists(): boolean { this.#checkError(); return this.#snapshot.exists }

  heads(): string[] {
    this.#checkError()
    return [...this.#snapshot.heads]
  }

  /** The supplied heads themselves are excluded as well as their known ancestors. */
  metadata(opts?: { notAncestorsOf?: string[] }): Iterable<SedimentreeMeta> {
    this.#checkError()
    const records = opts?.notAncestorsOf?.length
      ? withoutAncestors(this.#snapshot.records, opts.notAncestorsOf)
      : this.#snapshot.records
    return records.map(copyMeta)
  }

  async materialize(metas: SedimentreeMeta[]): Promise<Uint8Array[]> {
    const scope = new WasmScope()
    try {
      const id = scope.own(SedimentreeId.fromBytes(this.#documentId))
      const blobs: Uint8Array[] = []
      for (const meta of metas) {
        const head = scope.own(CommitId.fromHexString(meta.head))
        const stored = meta.kind === "commit"
          ? await this.storage.loadCommit(id, head)
          : await this.storage.loadFragment(id, head)
        if (!stored) throw new Error(`Missing ${meta.kind} ${meta.head}`)
        scope.own(stored)
        blobs.push(stored.blob.slice())
      }
      return blobs
    } finally {
      scope.free()
    }
  }

  /**
   * Source-managed handles persist locally and schedule background sync, so
   * offline peers cannot delay or fail a local save. Standalone handles retain
   * the store-and-sync behavior. Storage observations also cover remote writes.
   */
  async apply(records: SedimentreeRecord[]): Promise<void> {
    if (!records.length) return
    try {
      await writeRecords(this.sdn, this.#documentId, records, !this.onPersisted)
    } catch (error) {
      // A failed write/sync can still have persisted part or all of the batch.
      await this.#refresh().catch(() => {})
      throw error
    }
    await this.#refresh()
    this.onPersisted?.()
  }

  on(event: "change", listener: () => void): void { this.#listeners.add(listener) }
  off(event: "change", listener: () => void): void { this.#listeners.delete(listener) }

  #checkError(): void { if (this.#error) throw this.#error }

  #invalidate = (): void => {
    this.#revision++
    // Refresh records failures in the handle: snapshot methods then throw, and
    // change listeners are notified. A later storage change retries the load.
    void this.#refresh().catch(() => {})
  }

  async #refresh(): Promise<void> {
    do {
      if (!this.#pending) {
        const pending = Promise.resolve().then(() => this.#reload())
        this.#pending = pending
        const finished = () => { this.#pending = undefined }
        void pending.then(finished, finished)
      }
      try {
        await this.#pending
      } catch (error) {
        if (this.#completedRevision === this.#revision) throw error
      }
      // An invalidation can arrive after reload returns but before its promise
      // settles. Both background refreshes and apply/open callers must wait for
      // that last update, rather than merely schedule it for later.
    } while (this.#completedRevision !== this.#revision)
    this.#checkError()
  }

  async #reload(): Promise<void> {
    while (this.#completedRevision !== this.#revision) {
      const revision = this.#revision
      let snapshot: Snapshot
      try {
        snapshot = await readSnapshot(this.storage, this.#documentId)
      } catch (reason) {
        if (revision !== this.#revision) continue
        this.#completedRevision = revision
        this.#error = reason instanceof Error ? reason : new Error(String(reason))
        this.#notify()
        throw this.#error
      }
      if (revision !== this.#revision) continue
      this.#completedRevision = revision
      const changed = this.#error !== undefined || snapshot.fingerprint !== this.#snapshot.fingerprint
      this.#error = undefined
      this.#snapshot = snapshot
      if (changed) this.#notify()
    }
    this.#checkError()
  }

  #notify(): void {
    for (const listener of [...this.#listeners]) {
      try { listener() } catch (error) { console.error("Sedimentree change listener failed", error) }
    }
  }
}
