/**
 * Keeps each `DocHandle` up to date with its `SedimentreeHandle` source.
 *
 * The source's `metadata()` and change notifications are synchronous, but
 * `materialize()` (loading bytes) is asynchronous. This is annoying for a 
 * few reasons. The most important of which is that it's hard to know when
 * we have "finished" loading a document. To elaborate, when we query the
 * tree from the sedimentreesource we get back a sedimentreehandle, but we
 * don't want to report that thing available to the application until we
 * have read it's content as well. To that end we have an initial state 
 * where we read until there is nothing left to read and _then_ report the
 * handle ready to the application.
 *
 * Once we have done this we then continue coalescing read passes so we only
 * ever have one in flight read.
 *
 * The state of a registered handle is roughly:
 *
 *
 *   schedule -> queued -> reading -> settling -> idle
 *                           ^           |
 *                           +-----------+  active and dirty: read again
 *
 * How to read this file:
 *
 * 1. The types describe one registration: its lifetime (`active`), the
 *    one-time initial-loading promise, and the current work phase.
 * 2. The `on*` transition functions are pure: each mutates the state of one
 *    registration and returns the effects it wants, as data. They mirror the
 *    actions in model/RefreshScheduler.tla and are unit tested on their own.
 * 3. The `RefreshScheduler` class owns the registration table and does the IO.
 *    Its `#update` method runs a transition and then executes the returned
 *    effects, so no external code runs while state is being changed.
 *
 * Registration identity is row identity. Removal invalidates a row but leaves
 * its outstanding IO alive; a re-registered handle always gets a fresh row, so
 * a late completion can never be confused with the replacement's work.
 */

import type { DocHandle } from "./DocHandle.js"
import type { SedimentreeHandle, SedimentreeMeta } from "./SedimentreeSource.js"
import { sameHeads } from "./helpers/sameHeads.js"

export interface RefreshSchedulerOptions {
  /** Background failures only; initial loading failures reject schedule(). */
  onError: (error: unknown, handle: DocHandle<any>) => void
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/**
 * Status of the one-time initial-loading promise returned by schedule().
 * - pending: still loading, including any notified follow-up passes.
 * - ready: initial loading caught up and the promise has resolved.
 * - failed: subscription setup or an initial refresh failed.
 * - aborted: the registration was removed before initial loading finished.
 * Once terminal, this never changes. Background refresh failures and removal
 * of an already-ready registration do not alter the promise's outcome.
 */
export type InitialLoadStatus = "pending" | "ready" | "failed" | "aborted"

/** Current refresh work for one registration. See the diagram at the top. */
export type RefreshWork =
  | {
      /** No queued or outstanding refresh; an active row waits for a request. */
      phase: "idle"
    }
  | {
      /**
       * A start microtask is queued. Further requests coalesce into that pass,
       * so a queued row is implicitly dirty.
       */
      phase: "queued"
    }
  | {
      /**
       * reading: loading and applying records, or awaiting the completion
       * microtask.
       * settling: the outcome is known and its error callbacks are running,
       * before `onContinue` decides whether to read again or become idle.
       */
      phase: "reading" | "settling"
      /**
       * A notification requested another pass since the current read started.
       * This is a coalesced wakeup, not an indication of unsaved local edits.
       * Cleared when the next read starts or the row is removed, never when IO
       * completes. Initial readiness must wait until no follow-up is needed.
       */
      dirty: boolean
    }

/** The control state of one registration, as seen by the transition functions. */
export interface RefreshState {
  /**
   * Whether this registration still accepts notifications and applies records.
   * Removal sets this to false permanently. Queued work or IO may still finish,
   * but its results are ignored; re-registering the handle creates a new row.
   */
  active: boolean
  initialLoad: { status: InitialLoadStatus }
  /**
   * A non-idle phase owns the read slot, so requests cannot start overlapping
   * reads for this row. No separate `running` flag is needed.
   */
  work: RefreshWork
}

export type RefreshOutcome = { ok: true } | { ok: false; error: unknown }

/** Why a registration is being removed, and how a pending initial load ends. */
export type Removal = { status: "failed" | "aborted"; error: unknown }

/**
 * Effects a transition asks the scheduler to perform, in order, after the
 * transition has returned. Transitions never perform IO, settle promises, or
 * call listeners themselves; they only describe the work as data.
 *
 * - queueStart: run `onStart` in a microtask.
 * - read: capture the missing metadata and start loading it.
 * - continue: run `onContinue` synchronously, after any preceding effects. Its
 *   decision must be made fresh, because report/detach callbacks can request or
 *   remove work in between.
 * - resolve / reject: settle the initial-loading promise.
 * - detach: drop the row from the registration table and unsubscribe.
 * - report: deliver a background error to the `onError` option.
 */
export type RefreshEffect =
  | { type: "queueStart" }
  | { type: "read" }
  | { type: "continue" }
  | { type: "resolve" }
  | { type: "reject"; error: unknown }
  | { type: "detach" }
  | { type: "report"; error: unknown }

// ---------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------

/** A notification, or the initial request made by schedule(). */
export function onRequested(row: RefreshState): RefreshEffect[] {
  if (!row.active) return []
  switch (row.work.phase) {
    case "idle":
      row.work = { phase: "queued" }
      return [{ type: "queueStart" }]
    case "queued":
      return []
    case "reading":
    case "settling":
      row.work.dirty = true
      return []
  }
}

/** The queued start microtask. A removed row drops its queued work here. */
export function onStart(row: RefreshState): RefreshEffect[] {
  if (row.work.phase !== "queued") return []
  if (!row.active) {
    row.work = { phase: "idle" }
    return []
  }
  row.work = { phase: "reading", dirty: false }
  return [{ type: "read" }]
}

/** A read pass finished, successfully or not. Always followed by `onContinue`. */
export function onCompleted(row: RefreshState, outcome: RefreshOutcome): RefreshEffect[] {
  if (row.work.phase !== "reading") return []
  row.work = { phase: "settling", dirty: row.active && row.work.dirty }
  const effects: RefreshEffect[] = []
  if (row.active && !outcome.ok) {
    // An initial failure removes the row in this same atomic step, so the
    // rejection and detachment are ordered before `continue` below.
    if (row.initialLoad.status === "pending") {
      effects.push(...onRemoved(row, { status: "failed", error: outcome.error }))
    } else {
      effects.push({ type: "report", error: outcome.error })
    }
  }
  effects.push({ type: "continue" })
  return effects
}

/** Decide, after error callbacks have run, whether to read again or settle. */
export function onContinue(row: RefreshState): RefreshEffect[] {
  if (row.work.phase !== "settling") return []
  if (row.active && row.work.dirty) {
    row.work = { phase: "reading", dirty: false }
    return [{ type: "read" }]
  }
  row.work = { phase: "idle" }
  if (row.active && row.initialLoad.status === "pending") {
    row.initialLoad.status = "ready"
    return [{ type: "resolve" }]
  }
  return []
}

/** Unschedule, dispose, garbage collection, or an initial failure. */
export function onRemoved(row: RefreshState, removal: Removal): RefreshEffect[] {
  if (!row.active) return []
  row.active = false
  if (row.work.phase === "reading" || row.work.phase === "settling") row.work.dirty = false
  const effects: RefreshEffect[] = []
  if (row.initialLoad.status === "pending") {
    row.initialLoad.status = removal.status
    effects.push({ type: "reject", error: removal.error })
  }
  effects.push({ type: "detach" })
  return effects
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

type InitialLoad = {
  status: InitialLoadStatus
  promise: Promise<void>
  resolve(): void
  reject(error: unknown): void
}

/**
 * One row of the registration table.
 *
 * The scheduler must not keep a `DocHandle` alive: the handle is held through
 * a WeakRef, and nothing else in the row, its promise, or its effects may
 * close over the handle. `schedule()` is the only method that receives the
 * handle as a strong parameter, so the closures created on its behalf live in
 * other functions.
 */
type Registration = RefreshState & {
  handle: WeakRef<DocHandle<any>>
  source: SedimentreeHandle
  initialLoad: InitialLoad
  unsubscribe(): void
}

export class RefreshScheduler {
  #handles = new WeakMap<DocHandle<any>, Registration>()
  #registrations = new Set<Registration>()
  #disposed = false
  #collected = new FinalizationRegistry<Registration>(row => {
    this.#update(row, onRemoved, aborted())
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
        ? existing.initialLoad.promise
        : Promise.reject(new Error("Unschedule the handle before changing its source"))
    }

    const row: Registration = {
      handle: new WeakRef(handle), source, initialLoad: initialLoad(), unsubscribe() {},
      active: true, work: { phase: "idle" },
    }
    this.#handles.set(handle, row)
    this.#registrations.add(row)
    this.#collected.register(handle, row, row)
    this.#subscribe(row)
    return row.initialLoad.promise
  }

  /**
   * Detach a handle. Reject pending initial loading with AbortError and ignore
   * late IO, even if this handle is registered again. Local saves are unaffected.
   * No-op for a handle that isn't registered.
   */
  unschedule(handle: DocHandle<any>): void {
    const row = this.#handles.get(handle)
    if (row) this.#update(row, onRemoved, aborted())
  }

  /** Permanently stop refreshes; does not dispose handles, sources, or peers. */
  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true
    for (const row of [...this.#registrations]) {
      this.#update(row, onRemoved, aborted())
    }
  }

  #subscribe(row: Registration): void {
    const listener = () => this.#update(row, onRequested)
    row.unsubscribe = () => row.source.off("change", listener)
    try {
      row.source.on("change", listener)
      this.#update(row, onRequested)
    } catch (error) {
      this.#update(row, onRemoved, { status: "failed", error })
    }
  }

  /**
   * The update boundary: run one transition, then execute the effects it
   * returned, in order. Effects may re-enter `#update` for the same row (for
   * example `continue`, or a listener that unschedules), but only after the
   * original transition has finished changing state.
   */
  #update<Args extends unknown[]>(
    row: Registration,
    transition: (row: RefreshState, ...args: Args) => RefreshEffect[],
    ...args: Args
  ): void {
    for (const effect of transition(row, ...args)) this.#run(row, effect)
  }

  #run(row: Registration, effect: RefreshEffect): void {
    switch (effect.type) {
      case "queueStart":
        queueMicrotask(() => this.#update(row, onStart))
        return
      case "read":
        this.#read(row)
        return
      case "continue":
        this.#update(row, onContinue)
        return
      case "resolve":
        row.initialLoad.resolve()
        return
      case "reject":
        row.initialLoad.reject(effect.error)
        return
      case "detach":
        this.#detach(row)
        return
      case "report":
        this.#report(row, effect.error)
        return
    }
  }

  /**
   * One read pass. Synchronous source calls may notify or remove the row, so
   * `row.active` is re-checked after each; an inactive row completes without
   * touching the document. Empty reads still cross the completion boundary.
   */
  #read(row: Registration): void {
    const done = (outcome: RefreshOutcome) => this.#complete(row, outcome)
    try {
      // Only heads and metadata, not the handle, survive until IO completes.
      const heads = row.handle.deref()?.heads()
      if (!heads) {
        this.#update(row, onRemoved, aborted())
        return done({ ok: true })
      }
      if (!row.active) return done({ ok: true })
      const sourceHeads = row.source.heads()
      if (!row.active || sameHeads(heads, sourceHeads)) return done({ ok: true })
      const metas = Array.from(row.source.metadata({ notAncestorsOf: heads }))
      if (!row.active || metas.length === 0) return done({ ok: true })
      void row.source.materialize(metas).then(
        data => this.#apply(row, metas, data),
        error => done({ ok: false, error }),
      )
    } catch (error) {
      done({ ok: false, error })
    }
  }

  /**
   * Materialized bytes arrived. Application merges into the current document
   * state and may synchronously notify, remove this row, or throw. Always check
   * THIS row, never the handle's current registration: an old row's late bytes
   * must not reach a replacement.
   */
  #apply(row: Registration, metas: SedimentreeMeta[], data: Uint8Array[]): void {
    if (!row.active) return this.#complete(row, { ok: true })
    let outcome: RefreshOutcome = { ok: true }
    try {
      const records = metas.map((meta, i) => ({ ...meta, bytes: data[i]! }))
      const handle = row.handle.deref()
      if (handle) handle.applyRecords(records)
      else this.#update(row, onRemoved, aborted())
    } catch (error) {
      outcome = { ok: false, error }
    }
    this.#complete(row, outcome)
  }

  /**
   * Completion always runs in a microtask, so empty reads and synchronous
   * failures behave like asynchronous ones: anything already queued, such as an
   * unschedule, is observed before the outcome is processed.
   */
  #complete(row: Registration, outcome: RefreshOutcome): void {
    queueMicrotask(() => this.#update(row, onCompleted, outcome))
  }

  #detach(row: Registration): void {
    this.#registrations.delete(row)
    this.#collected.unregister(row)
    const handle = row.handle.deref()
    if (handle && this.#handles.get(handle) === row) this.#handles.delete(handle)
    try { row.unsubscribe() }
    catch (error) { console.error("Unable to unsubscribe document refresh", error) }
  }

  #report(row: Registration, error: unknown): void {
    const handle = row.handle.deref()
    if (!handle) {
      this.#update(row, onRemoved, aborted())
      return
    }
    try { this.options.onError(error, handle) }
    catch (listenerError) { console.error("Document refresh error listener failed", listenerError) }
  }
}

// Separate function: the resolver closures must not retain a scheduled handle.
function initialLoad(): InitialLoad {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((res, rej) => { resolve = res; reject = rej })
  return { status: "pending", promise, resolve, reject }
}

function aborted(): Removal {
  const error = new Error("Document refresh was unscheduled")
  error.name = "AbortError"
  return { status: "aborted", error }
}
