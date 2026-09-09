import type { DocHandle } from "./DocHandle.js"
import type { SedimentreeHandle, SedimentreeRecord } from "./SedimentreeSource.js"
import { transition, type RefreshEvent, type RefreshOutcome, type RefreshState } from "./RefreshSchedulerState.js"
import { sameHeads } from "./helpers/sameHeads.js"

export interface RefreshSchedulerOptions {
  /** Background failures only; initial loading failures reject schedule(). */
  onError: (error: unknown, handle: DocHandle<any>) => void
}

type InitialLoad = {
  promise: Promise<void>
  resolve(): void
  reject(error: unknown): void
}

type RegistrationRow = RefreshState & {
  handle: WeakRef<DocHandle<any>>
  source: SedimentreeHandle
  load: InitialLoad
  unsubscribe(): void
}

/**
 * Event-driven source-to-document refreshes owned by one Repo, not network sync.
 * Registrations use handle identity: multiple handles for one document each get
 * updates. Only weak references to handles are retained between refreshes.
 *
 * transition() owns row state; this class owns the table and executes effects.
 */
export class RefreshScheduler {
  #handles = new WeakMap<DocHandle<any>, RegistrationRow>()
  #registrations = new Set<RegistrationRow>()
  #disposed = false
  #collected = new FinalizationRegistry<RegistrationRow>(row => {
    this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
  })

  constructor(private options: RefreshSchedulerOptions) {}

  /**
   * Subscribe before reading the initial snapshot, and resolve once caught up,
   * including notifications received during loading. Initial failure rejects and
   * removes the registration; later failures go to onError and can be retried by
   * subsequent notifications. Repeated scheduling of the same pair is idempotent.
   */
  schedule(handle: DocHandle<any>, source: SedimentreeHandle): Promise<void> {
    if (this.#disposed) return Promise.reject(new Error("RefreshScheduler is disposed"))
    const existing = this.#handles.get(handle)
    if (existing) {
      return existing.source === source
        ? existing.load.promise
        : Promise.reject(new Error("Unschedule the handle before changing its source"))
    }

    const row: RegistrationRow = {
      handle: new WeakRef(handle), source, load: initialLoad(), unsubscribe() {},
      active: true, initial: "pending", work: { phase: "idle" },
    }
    this.#handles.set(handle, row)
    this.#registrations.add(row)
    this.#collected.register(handle, row, row)
    // Subscription and promise closures are created outside schedule() so they
    // cannot capture its handle parameter and turn this into a strong reference.
    this.#subscribe(row)
    return row.load.promise
  }

  /**
   * Detach a handle. Reject pending initial loading with AbortError and ignore
   * late IO, even if this handle is registered again. Local saves are unaffected.
   * No-op for a handle that isn't registered.
   */
  unschedule(handle: DocHandle<any>): void {
    const row = this.#handles.get(handle)
    if (row) this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
  }

  /** Permanently stop refreshes; does not dispose handles, sources, or peers. */
  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true
    for (const row of [...this.#registrations]) {
      this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
    }
  }

  #subscribe(row: RegistrationRow): void {
    const listener = () => this.#dispatch(row, { type: "requested" })
    row.unsubscribe = () => row.source.off("change", listener)
    try {
      row.source.on("change", listener)
      this.#dispatch(row, { type: "requested" })
    } catch (error) {
      this.#dispatch(row, { type: "removed", initial: "failed", reason: error })
    }
  }

  #dispatch(row: RegistrationRow, event: RefreshEvent): void {
    // Commit all row changes before executing any effect that can call back.
    for (const effect of transition(row, event)) {
      switch (effect.type) {
        case "queue-start":
          queueMicrotask(() => this.#dispatch(row, { type: "start" }))
          break
        case "read":
          void this.#read(row)
          break
        case "apply":
          this.#apply(row, effect.records)
          break
        case "queue-completion":
          this.#complete(row, effect.outcome)
          break
        case "continue":
          // No new microtask here: re-read the row after error/detach callbacks,
          // then start a dirty follow-up in this same drain continuation.
          this.#dispatch(row, { type: "continue" })
          break
        case "resolve":
          row.load.resolve()
          break
        case "reject":
          row.load.reject(effect.error)
          break
        case "detach":
          this.#detach(row)
          break
        case "report":
          this.#report(row, effect.error)
          break
      }
    }
  }

  async #read(row: RegistrationRow): Promise<void> {
    let records: SedimentreeRecord[] = []
    try {
      // Do not retain a strong reference to the document across materialization.
      const heads = row.handle.deref()?.heads()
      if (!heads) {
        this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
      } else if (row.active) {
        const sourceHeads = row.source.heads()
        if (row.active && !sameHeads(heads, sourceHeads)) {
          const metas = Array.from(row.source.metadata({ notAncestorsOf: heads }))
          if (row.active && metas.length) {
            const data = await row.source.materialize(metas)
            if (row.active) records = metas.map((meta, i) => ({ ...meta, bytes: data[i]! }))
          }
        }
      }
    } catch (error) {
      this.#complete(row, { ok: false, error })
      return
    }
    this.#dispatch(row, { type: "loaded", records })
  }

  #apply(row: RegistrationRow, records: SedimentreeRecord[]): void {
    let outcome: RefreshOutcome = { ok: true }
    try {
      // Always check THIS row, never the current row for its handle. Application
      // merges into current local state and may synchronously notify or remove.
      if (row.active) {
        const handle = row.handle.deref()
        if (handle) handle.applyRecords(records)
        else this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
      }
    } catch (error) {
      outcome = { ok: false, error }
    }
    this.#complete(row, outcome)
  }

  #complete(row: RegistrationRow, outcome: RefreshOutcome): void {
    // This is the original drain's await boundary, including empty snapshots
    // and synchronous source failures. The callback retains only the old row.
    queueMicrotask(() => this.#dispatch(row, { type: "completed", outcome }))
  }

  #detach(row: RegistrationRow): void {
    this.#registrations.delete(row)
    this.#collected.unregister(row)
    const handle = row.handle.deref()
    if (handle && this.#handles.get(handle) === row) this.#handles.delete(handle)
    try { row.unsubscribe() }
    catch (error) { console.error("Unable to unsubscribe document refresh", error) }
  }

  #report(row: RegistrationRow, error: unknown): void {
    const handle = row.handle.deref()
    if (!handle) {
      this.#dispatch(row, { type: "removed", initial: "aborted", reason: abortError() })
      return
    }
    try { this.options.onError(error, handle) }
    catch (listenerError) { console.error("Document refresh error listener failed", listenerError) }
  }
}

// Separate scope: promise resolver closures must not retain a scheduled handle.
function initialLoad(): InitialLoad {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

function abortError(): Error {
  const error = new Error("Document refresh was unscheduled")
  error.name = "AbortError"
  return error
}
