import { SedimentreeId, type Subduction } from "@automerge/subduction/slim"
import type { DocumentId } from "@brigstow/brigstow"

export interface SubductionSourceOptions {
  /** Optional discovery-mode WebSocket servers. No network endpoints are implicit. */
  syncServers?: string[]
  syncTimeoutMilliseconds?: number
  /** Retry tracked documents and reconnect missing server peers. Default: 5 seconds. */
  retryIntervalMilliseconds?: number
  onSyncError?: (error: unknown) => void
  /** A round reached at least one peer; not an acknowledgement from every replica. */
  onSynced?: (id: DocumentId) => void
}

export class NoSyncPeersError extends Error {
  constructor() { super("No Subduction peers are connected"); this.name = "NoSyncPeersError" }
}

type Tracked = { id: DocumentId; revision: number; pending?: Promise<void> }

/** Source-local sync policy. Never uploads documents that this source hasn't opened/created. */
export class DocumentSync {
  #documents = new Map<string, Tracked>()
  #servers: URL[]
  #serverPeers = new Map<string, string>()
  #connecting = new Map<string, Promise<void>>()
  #timer: ReturnType<typeof setTimeout> | undefined
  #closed = false
  #timeout: number
  #retry: number

  constructor(private subduction: Subduction, private options: SubductionSourceOptions) {
    this.#servers = (options.syncServers ?? []).map(address => {
      const url = new URL(address)
      if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("Sync servers must use ws: or wss:")
      return url
    })
    this.#timeout = options.syncTimeoutMilliseconds ?? 5000
    this.#retry = options.retryIntervalMilliseconds ?? 5000
    for (const value of [this.#timeout, this.#retry]) {
      if (!Number.isFinite(value) || value <= 0 || value > 2 ** 31 - 1) throw new Error("Sync timeouts must be positive timer durations")
    }
  }

  schedule(id: DocumentId): void {
    if (!this.#closed) void this.sync(id).catch(error => this.#report(error))
  }

  async sync(id?: DocumentId): Promise<void> {
    if (this.#closed) throw new Error("SubductionSource is shut down")
    if (!id) {
      await Promise.all([...this.#documents.values()].map(document => this.sync(document.id)))
      return
    }
    const key = Array.from(id, byte => byte.toString(16).padStart(2, "0")).join("")
    let document = this.#documents.get(key)
    if (!document) {
      document = { id: id.slice() as DocumentId, revision: 0 }
      this.#documents.set(key, document)
    }
    this.#startRetryTimer()
    const tracked = document
    tracked.revision++
    if (!tracked.pending) {
      // A write during an in-flight round requests another round, not just the
      // old promise: that round may already have read its outbound snapshot.
      const pending = Promise.resolve().then(async () => {
        let revision: number
        do {
          revision = tracked.revision
          await this.#ensureConnections()
          if (this.#closed) throw new Error("SubductionSource is shut down")
          await this.#syncDocument(tracked.id)
          if (!this.#closed) {
            try { this.options.onSynced?.(tracked.id.slice() as DocumentId) }
            catch (error) { console.error("Subduction sync listener failed", error) }
          }
        } while (!this.#closed && revision !== tracked.revision)
      })
      tracked.pending = pending
      const finished = () => { delete tracked.pending }
      void pending.then(finished, finished)
    }
    await tracked.pending
  }

  async shutdown(): Promise<void> {
    this.#closed = true
    if (this.#timer !== undefined) clearTimeout(this.#timer)
    this.#timer = undefined
    await this.subduction.disconnectAll()
    await Promise.allSettled([...this.#documents.values()].map(doc => doc.pending))
    this.#documents.clear()
  }

  #startRetryTimer(): void {
    if (this.#timer !== undefined || this.#closed) return
    this.#timer = setTimeout(() => {
      this.#timer = undefined
      // Do not prolong an already-running round on every retry tick.
      for (const document of this.#documents.values()) {
        if (!document.pending) this.schedule(document.id)
      }
      this.#startRetryTimer()
    }, this.#retry)
    // Background network maintenance must not keep Node programs alive.
    ;(this.#timer as unknown as { unref?(): void }).unref?.()
  }

  async #ensureConnections(): Promise<void> {
    const peers = await this.subduction.getConnectedPeerIds()
    const connected = new Set(peers.map(peer => peer.toString()))
    peers.forEach(peer => peer.free())
    const outcomes = await Promise.allSettled(this.#servers.map(async url => {
      const key = url.href
      const peer = this.#serverPeers.get(key)
      if (peer && connected.has(peer)) return
      let connecting = this.#connecting.get(key)
      if (!connecting) {
        connecting = this.subduction.connectDiscover(url).then(async peer => {
          try {
            if (this.#closed) await this.subduction.disconnectFromPeer(peer)
            else this.#serverPeers.set(key, peer.toString())
          } finally { peer.free() }
        })
        this.#connecting.set(key, connecting)
        const finished = () => { this.#connecting.delete(key) }
        void connecting.then(finished, finished)
      }
      // The WASM connection API has no cancellation. Bound the caller's wait,
      // retain the in-flight handshake to avoid duplicates, and close a late
      // connection if shutdown has already happened.
      await withTimeout(connecting, this.#timeout)
    }))
    const errors = outcomes.flatMap(result => result.status === "rejected" ? [result.reason] : [])
    if (errors.length) {
      const peers = await this.subduction.getConnectedPeerIds()
      const available = peers.length > 0
      peers.forEach(peer => peer.free())
      const error = new AggregateError(errors, "Unable to connect to Subduction sync server")
      if (!available) throw error
      this.#report(error) // One failed server must not prevent syncing with others.
    }
  }

  async #syncDocument(documentId: DocumentId): Promise<void> {
    const id = SedimentreeId.fromBytes(documentId)
    try {
      const results = await this.subduction.syncWithAllPeers(id, true, this.#timeout)
      try {
        const entries = results.entries()
        try {
          if (!entries.length) throw new NoSyncPeersError()
          const failed = entries.filter(entry => !entry.success)
          if (failed.length) {
            const error = new AggregateError(failed.flatMap(entry => entry.transportErrors), "Subduction peer synchronization failed")
            if (failed.length === entries.length) throw error
            this.#report(error)
          }
        } finally { entries.forEach(entry => entry.free()) }
      } finally { results.free() }
    } finally { id.free() }
  }

  #report(error: unknown): void {
    if (this.#closed) return
    try {
      if (this.options.onSyncError) this.options.onSyncError(error)
      else if (!(error instanceof NoSyncPeersError)) console.warn("Subduction sync failed; will retry", error)
    } catch (listenerError) { console.error("Subduction sync error listener failed", listenerError) }
  }
}

function withTimeout<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Subduction connection timed out")), milliseconds)
    promise.then(value => { clearTimeout(timer); resolve(value) }, error => { clearTimeout(timer); reject(error) })
  })
}
