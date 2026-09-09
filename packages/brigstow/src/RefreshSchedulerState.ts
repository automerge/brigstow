import type { SedimentreeRecord } from "./SedimentreeSource.js"

/** The control state of one row in the scheduler's registrations table. */
export interface RefreshState {
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

export type RefreshEvent =
  | { type: "requested" }
  | { type: "start" }
  | { type: "loaded"; records: SedimentreeRecord[] }
  | { type: "completed"; outcome: RefreshOutcome }
  | { type: "continue" }
  | { type: "removed"; reason: unknown; initial: "failed" | "aborted" }

export type RefreshEffect =
  | { type: "queue-start" }
  | { type: "read" }
  | { type: "apply"; records: SedimentreeRecord[] }
  | { type: "queue-completion"; outcome: RefreshOutcome }
  | { type: "continue" }
  | { type: "resolve" }
  | { type: "reject"; error: unknown }
  | { type: "detach" }
  | { type: "report"; error: unknown }

/**
 * Mutate one registration row, then return effects for the scheduler to execute.
 * No IO, promises, callbacks, or table operations happen inside this function.
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
export function transition(row: RefreshState, event: RefreshEvent): RefreshEffect[] {
  switch (event.type) {
    case "requested": return onRequested(row)
    case "start": return onStart(row)
    case "loaded": return onLoaded(row, event.records)
    case "completed": return onCompleted(row, event.outcome)
    case "continue": return onContinue(row)
    case "removed": return onRemoved(row, event.reason, event.initial)
  }
}

function onRequested(row: RefreshState): RefreshEffect[] {
  if (!row.active) return []
  switch (row.work.phase) {
    case "idle":
      row.work = { phase: "queued" }
      return [{ type: "queue-start" }]
    case "queued":
      return []
    case "reading":
    case "after":
      row.work.dirty = true
      return []
  }
}

function onStart(row: RefreshState): RefreshEffect[] {
  if (row.work.phase !== "queued") return []
  row.work = row.active ? { phase: "reading", dirty: false } : { phase: "idle" }
  return row.active ? [{ type: "read" }] : []
}

function onLoaded(row: RefreshState, records: SedimentreeRecord[]): RefreshEffect[] {
  if (row.work.phase !== "reading") return []
  // Empty snapshots and obsolete reads still have a completion to drain,
  // but must not call into the document.
  return row.active && records.length > 0
    ? [{ type: "apply", records }]
    : [{ type: "queue-completion", outcome: { ok: true } }]
}

function onCompleted(row: RefreshState, outcome: RefreshOutcome): RefreshEffect[] {
  if (row.work.phase !== "reading") return []
  row.work = { phase: "after", dirty: row.active && row.work.dirty }
  const next: RefreshEffect = { type: "continue" }
  if (!row.active || outcome.ok) return [next]
  const error = outcome.error
  if (row.initial === "pending") {
    return [...onRemoved(row, error, "failed"), next]
  }
  // Continue is an effect, not a transition performed here: onError can
  // synchronously request another pass or remove this registration.
  return [{ type: "report", error }, next]
}

function onContinue(row: RefreshState): RefreshEffect[] {
  if (row.work.phase !== "after") return []
  if (row.active && row.work.dirty) {
    row.work = { phase: "reading", dirty: false }
    return [{ type: "read" }]
  }
  row.work = { phase: "idle" }
  if (row.active && row.initial === "pending") {
    row.initial = "ready"
    return [{ type: "resolve" }]
  }
  return []
}

function onRemoved(row: RefreshState, reason: unknown, initial: "failed" | "aborted"): RefreshEffect[] {
  if (!row.active) return []
  row.active = false
  if (row.work.phase === "reading" || row.work.phase === "after") row.work.dirty = false
  const effects: RefreshEffect[] = []
  if (row.initial === "pending") {
    row.initial = initial
    effects.push({ type: "reject", error: reason })
  }
  effects.push({ type: "detach" })
  return effects
}
