import type { DocHandle } from "./DocHandle.js"
import type { SedimentreeHandle, SedimentreeMeta } from "./SedimentreeSource.js"
import { sameHeads } from "./helpers/sameHeads.js"

export interface RefreshSchedulerOptions {
  /** Background failures only; initial loading failures reject schedule(). */
  onError: (error: unknown, handle: DocHandle<any>) => void
}

/**
 * A SedimentreeHandle synchronously tells us that its contents have changed, but
 * reading the corresponding records is asynchronous. RefreshScheduler bridges
 * that gap so a DocHandle's in-memory document catches up with its source. Each
 * Repo owns one; it coordinates source-to-document reads, not network sync or
 * the persistence of local edits.
 *
 * While a read is outstanding, more notifications may arrive. Rather than start
 * overlapping reads for the same registration, we coalesce those notifications
 * into a dirty flag and make another pass afterward. Meanwhile, the application
 * may edit the document or remove its registration. Loaded records therefore
 * merge into the document's current state, and results from a removed registration
 * are ignored—even if the same handle has since been registered again. Unrelated
 * registrations can progress independently.
 *
 * Initial loading is the same process, with a promise attached. We subscribe
 * before the first read so arrivals during loading aren't missed, and resolve
 * only once the notified follow-up work has also been drained. The explicit row
 * state and the transition functions below keep these decisions separate from
 * the IO that eventually supplies their outcomes.
 */
export class RefreshScheduler {
  #handles = new WeakMap<DocHandle<any>, RegistrationRow>()
  #registrations = new Set<RegistrationRow>()
  #disposed = false
  #collected = new FinalizationRegistry<RegistrationRow>(row => {
    this.#update(row, onRemoved, abortError(), "aborted")
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
    if (row) this.#update(row, onRemoved, abortError(), "aborted")
  }

  /** Permanently stop refreshes; does not dispose handles, sources, or peers. */
  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true
    for (const row of [...this.#registrations]) {
      this.#update(row, onRemoved, abortError(), "aborted")
    }
  }

  #subscribe(row: RegistrationRow): void {
    const listener = () => this.#update(row, onRequested)
    row.unsubscribe = () => row.source.off("change", listener)
    try {
      row.source.on("change", listener)
      this.#update(row, onRequested)
    } catch (error) {
      this.#update(row, onRemoved, error, "failed")
    }
  }

  #update<Args extends unknown[]>(
    row: RegistrationRow,
    change: (row: RefreshState, effects: RefreshEffects, ...args: Args) => void,
    ...args: Args
  ): void {
    // Record calls directly, bound to THIS row and outside schedule()'s strong
    // handle scope. No external code runs until the state function returns.
    const pending: (() => void)[] = []
    const effects: RefreshEffects = {
      queueStart: () => { pending.push(() => queueMicrotask(() => this.#update(row, onStart))) },
      read: () => { pending.push(() => this.#read(row)) },
      // Reconsider the row after report/detach callbacks, without a new microtask.
      continue: () => { pending.push(() => this.#update(row, onContinue)) },
      resolve: () => { pending.push(() => row.load.resolve()) },
      reject: error => { pending.push(() => row.load.reject(error)) },
      detach: () => { pending.push(() => this.#detach(row)) },
      report: error => { pending.push(() => this.#report(row, error)) },
    }
    change(row, effects, ...args)
    for (const run of pending) run()
  }

  #read(row: RegistrationRow): void {
    try {
      // Only heads and metadata, not the document, survive until IO completes.
      let metas: SedimentreeMeta[] = []
      const heads = row.handle.deref()?.heads()
      if (!heads) {
        this.#update(row, onRemoved, abortError(), "aborted")
      } else if (row.active) {
        const sourceHeads = row.source.heads()
        if (row.active && !sameHeads(heads, sourceHeads)) {
          metas = Array.from(row.source.metadata({ notAncestorsOf: heads }))
        }
      }
      if (row.active && metas.length) {
        void row.source.materialize(metas).then(
          data => this.#apply(row, metas, data),
          error => this.#complete(row, { ok: false, error }),
        )
      } else {
        // Empty snapshots and obsolete work still cross the completion boundary,
        // but must not call into the document.
        this.#complete(row, { ok: true })
      }
    } catch (error) {
      this.#complete(row, { ok: false, error })
    }
  }

  #apply(row: RegistrationRow, metas: SedimentreeMeta[], data: Uint8Array[]): void {
    let outcome: RefreshOutcome = { ok: true }
    try {
      const records = row.active ? metas.map((meta, i) => ({ ...meta, bytes: data[i]! })) : []
      // Always check THIS row, never the current row for its handle. Application
      // merges into current local state and may synchronously notify or remove.
      if (row.active && records.length) {
        const handle = row.handle.deref()
        if (handle) handle.applyRecords(records)
        else this.#update(row, onRemoved, abortError(), "aborted")
      }
    } catch (error) {
      outcome = { ok: false, error }
    }
    this.#complete(row, outcome)
  }

  #complete(row: RegistrationRow, outcome: RefreshOutcome): void {
    // Preserve the drain's completion boundary, including empty snapshots and
    // synchronous source failures. The callback retains only the original row.
    queueMicrotask(() => this.#update(row, onCompleted, outcome))
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
      this.#update(row, onRemoved, abortError(), "aborted")
      return
    }
    try { this.options.onError(error, handle) }
    catch (listenerError) { console.error("Document refresh error listener failed", listenerError) }
  }
}

/**
 * These transition functions mutate one row and record effects in the supplied
 * buffer. No IO, promises, callbacks, or table operations happen inside them.
 * The scheduler's update boundary executes effects only after a function returns.
 * See model/RefreshScheduler.tla: Requested, Start, Complete, Continue, Removed.
 *
 * `reading` covers loading AND applying records. Application can notify, remove
 * the row, or throw; only its outcome completes the pass. The completion runs in
 * a microtask (the old drain's await continuation). `after` lets an error callback
 * request/remove work before Continue decides whether to read again or settle.
 *
 * Row identity is registration identity. Removal invalidates a row but leaves
 * its outstanding work alive; a replacement always gets a different row.
 */
export function onRequested(row: RefreshState, effects: RefreshEffects): void {
  if (!row.active) return
  switch (row.work.phase) {
    case "idle":
      row.work = { phase: "queued" }
      effects.queueStart()
      return
    case "queued":
      return
    case "reading":
    case "after":
      row.work.dirty = true
      return
  }
}

export function onStart(row: RefreshState, effects: RefreshEffects): void {
  if (row.work.phase !== "queued") return
  row.work = row.active ? { phase: "reading", dirty: false } : { phase: "idle" }
  if (row.active) effects.read()
}

export function onCompleted(row: RefreshState, effects: RefreshEffects, outcome: RefreshOutcome): void {
  if (row.work.phase !== "reading") return
  row.work = { phase: "after", dirty: row.active && row.work.dirty }
  if (row.active && !outcome.ok) {
    if (row.initial === "pending") onRemoved(row, effects, outcome.error, "failed")
    else effects.report(outcome.error)
  }
  // When flushed, report/detach callbacks run before this fresh transition
  // decides whether to read again. Do not precompute that decision here.
  effects.continue()
}

export function onContinue(row: RefreshState, effects: RefreshEffects): void {
  if (row.work.phase !== "after") return
  if (row.active && row.work.dirty) {
    row.work = { phase: "reading", dirty: false }
    effects.read()
    return
  }
  row.work = { phase: "idle" }
  if (row.active && row.initial === "pending") {
    row.initial = "ready"
    effects.resolve()
  }
}

export function onRemoved(
  row: RefreshState, effects: RefreshEffects, reason: unknown, initial: "failed" | "aborted",
): void {
  if (!row.active) return
  row.active = false
  if (row.work.phase === "reading" || row.work.phase === "after") row.work.dirty = false
  if (row.initial === "pending") {
    row.initial = initial
    effects.reject(reason)
  }
  effects.detach()
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

/** The control state of one row in the scheduler's registrations table. */
interface RefreshState {
  /**
   * Whether this registration still accepts notifications and applies records.
   * Removal sets this to false permanently. Queued work or IO may still finish,
   * but its results are ignored; re-registering the handle creates a new row.
   */
  active: boolean

  /**
   * Status of the one-time initial-loading promise returned by schedule().
   * - pending: still loading, including any notified follow-up passes.
   * - ready: initial loading caught up and the promise can resolve.
   * - failed: subscription setup or an initial refresh failed.
   * - aborted: the registration was removed before initial loading finished.
   * Once terminal, this never changes. Background refresh failures and removal
   * of an already-ready registration do not alter its initial promise outcome.
   */
  initial: "pending" | "ready" | "failed" | "aborted"

  /**
   * Current refresh work, independent of registration lifetime and initial
   * readiness. A non-idle phase owns the runner, so requests cannot start
   * overlapping reads for this row. No separate `running` flag is needed.
   */
  work:
    | {
        /** No queued or outstanding refresh; an active row waits for a request. */
        phase: "idle"
      }
    | {
        /** A start microtask is queued. Further requests coalesce into that pass. */
        phase: "queued"
      }
    | {
        /**
         * reading: loading/applying records, or awaiting the completion microtask.
         * after: processing the outcome, before Continue decides whether to read
         * again or become idle. Error callbacks may still request or remove work.
         */
        phase: "reading" | "after"
        /**
         * A notification requested another pass since the current read started.
         * This is a coalesced wakeup, not an indication of unsaved local edits.
         * Cleared when the next read starts or the row is removed—not when IO
         * completes. Initial readiness must wait until no follow-up is needed.
         */
        dirty: boolean
      }
}

export type RefreshOutcome = { ok: true } | { ok: false; error: unknown }

/**
 * Effects for one registration row. During a transition these methods must only
 * record work, never execute it. The scheduler flushes the buffer synchronously
 * after the transition returns, in call order. queueStart enqueues a microtask;
 * continue invokes a fresh transition synchronously.
 */
export interface RefreshEffects {
  queueStart(): void
  read(): void
  continue(): void
  resolve(): void
  reject(error: unknown): void
  detach(): void
  report(error: unknown): void
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

