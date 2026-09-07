import type { CommitWithBlob, FragmentWithBlob, SedimentreeId, SedimentreeStorage } from "@automerge/subduction/slim"

// WASM IndexedDbStorage uses undefined for missing records, whereas the JS
// storage protocol specifies null. Accept either and normalize at the boundary.
type StorageBackend = Omit<SedimentreeStorage, "loadCommit" | "loadFragment"> & {
  loadCommit(...args: Parameters<SedimentreeStorage["loadCommit"]>): Promise<CommitWithBlob | null | undefined>
  loadFragment(...args: Parameters<SedimentreeStorage["loadFragment"]>): Promise<FragmentWithBlob | null | undefined>
}

type Listener = () => void

/**
 * Pass this wrapper as Subduction's storage, rather than wrapping the backend
 * after constructing Subduction. All writers sharing a backend must use the
 * same wrapper to observe one another's changes.
 */
export class ObservableStorage implements SedimentreeStorage {
  #listeners = new Map<string, Set<WeakRef<Listener>>>()
  #collected = new FinalizationRegistry<{ key: string; ref: WeakRef<Listener> }>(({ key, ref }) => {
    const listeners = this.#listeners.get(key)
    listeners?.delete(ref)
    if (listeners?.size === 0) this.#listeners.delete(key)
  })

  constructor(private backend: StorageBackend) { }

  /** @internal The handle keeps its callback alive; storage must not retain handles. */
  watch(key: string, listener: Listener): void {
    let listeners = this.#listeners.get(key)
    if (!listeners) this.#listeners.set(key, listeners = new Set())
    const ref = new WeakRef(listener)
    listeners.add(ref)
    this.#collected.register(listener, { key, ref })
  }

  async #mutate<T>(id: SedimentreeId, action: () => Promise<T>): Promise<T> {
    const key = id.toString()
    try {
      return await action()
    } finally {
      // A backend can partially persist a failed batch. Invalidate on failure
      // too, but only the subsequent read determines what actually changed.
      const listeners = this.#listeners.get(key)
      for (const ref of listeners ?? []) {
        const listener = ref.deref()
        if (listener) {
          try { listener() } catch (error) { console.error("Storage observer failed", error) }
        } else {
          listeners?.delete(ref)
        }
      }
      if (listeners?.size === 0) this.#listeners.delete(key)
    }
  }

  saveSedimentreeId(...args: Parameters<SedimentreeStorage["saveSedimentreeId"]>) {
    return this.#mutate(args[0], () => this.backend.saveSedimentreeId(...args))
  }
  deleteSedimentreeId(...args: Parameters<SedimentreeStorage["deleteSedimentreeId"]>) {
    return this.#mutate(args[0], () => this.backend.deleteSedimentreeId(...args))
  }
  loadAllSedimentreeIds() { return this.backend.loadAllSedimentreeIds() }
  async containsSedimentreeId(id: SedimentreeId): Promise<boolean> {
    if (this.backend.containsSedimentreeId) return this.backend.containsSedimentreeId(id)
    const ids = await this.backend.loadAllSedimentreeIds()
    try {
      const key = id.toString()
      return ids.some(candidate => candidate.toString() === key)
    } finally {
      for (const candidate of ids) candidate.free()
    }
  }

  saveCommit(...args: Parameters<SedimentreeStorage["saveCommit"]>) {
    return this.#mutate(args[0], () => this.backend.saveCommit(...args))
  }
  async loadCommit(...args: Parameters<SedimentreeStorage["loadCommit"]>) { return await this.backend.loadCommit(...args) ?? null }
  listCommitIds(...args: Parameters<SedimentreeStorage["listCommitIds"]>) { return this.backend.listCommitIds(...args) }
  loadAllCommits(...args: Parameters<SedimentreeStorage["loadAllCommits"]>) { return this.backend.loadAllCommits(...args) }
  deleteCommit(...args: Parameters<SedimentreeStorage["deleteCommit"]>) {
    return this.#mutate(args[0], () => this.backend.deleteCommit(...args))
  }
  deleteAllCommits(...args: Parameters<SedimentreeStorage["deleteAllCommits"]>) {
    return this.#mutate(args[0], () => this.backend.deleteAllCommits(...args))
  }

  saveFragment(...args: Parameters<SedimentreeStorage["saveFragment"]>) {
    return this.#mutate(args[0], () => this.backend.saveFragment(...args))
  }
  async loadFragment(...args: Parameters<SedimentreeStorage["loadFragment"]>) { return await this.backend.loadFragment(...args) ?? null }
  listFragmentIds(...args: Parameters<SedimentreeStorage["listFragmentIds"]>) { return this.backend.listFragmentIds(...args) }
  loadAllFragments(...args: Parameters<SedimentreeStorage["loadAllFragments"]>) { return this.backend.loadAllFragments(...args) }
  deleteFragment(...args: Parameters<SedimentreeStorage["deleteFragment"]>) {
    return this.#mutate(args[0], () => this.backend.deleteFragment(...args))
  }
  deleteAllFragments(...args: Parameters<SedimentreeStorage["deleteAllFragments"]>) {
    return this.#mutate(args[0], () => this.backend.deleteAllFragments(...args))
  }
  saveBatchAll(...args: Parameters<SedimentreeStorage["saveBatchAll"]>) {
    return this.#mutate(args[0], () => this.backend.saveBatchAll(...args))
  }
}
