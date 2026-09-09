import assert from "node:assert/strict"
import test from "node:test"
import { onRequested, onStart, onCompleted, onContinue, onRemoved } from "../dist/RefreshScheduler.js"

const row = () => ({ active: true, initial: "pending", work: { phase: "idle" } })
const ok = { ok: true }
const abort = new Error("unscheduled")

// A recording-only effects interface tests the state functions without IO.
// Scheduler integration tests exercise the real buffered update boundary.
function update(state, change, ...args) {
  const calls = []
  const effects = {
    queueStart: () => calls.push({ type: "queue-start" }),
    read: () => calls.push({ type: "read" }),
    continue: () => calls.push({ type: "continue" }),
    resolve: () => calls.push({ type: "resolve" }),
    reject: error => calls.push({ type: "reject", error }),
    detach: () => calls.push({ type: "detach" }),
    report: error => calls.push({ type: "report", error }),
  }
  assert.equal(change(state, effects, ...args), undefined)
  return calls
}

function start(state) {
  assert.deepEqual(update(state, onRequested), [{ type: "queue-start" }])
  assert.deepEqual(update(state, onStart), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
}

function ready(state) {
  start(state)
  assert.deepEqual(update(state, onCompleted, ok), [{ type: "continue" }])
  assert.deepEqual(update(state, onContinue), [{ type: "resolve" }])
  assert.equal(state.initial, "ready")
}

test("requests coalesce in queued, reading, and after phases without overlapping reads", () => {
  const state = row()
  assert.deepEqual(update(state, onRequested), [{ type: "queue-start" }])
  for (let i = 0; i < 5; i++) {
    assert.deepEqual(update(state, onRequested), [])
    assert.deepEqual(state.work, { phase: "queued" })
  }
  assert.deepEqual(update(state, onStart), [{ type: "read" }])
  assert.deepEqual(update(state, onStart), [], "a second start cannot replace a read")
  for (const phase of ["reading", "after"]) {
    for (let i = 0; i < 5; i++) assert.deepEqual(update(state, onRequested), [])
    assert.deepEqual(state.work, { phase, dirty: true })
    if (phase === "reading") assert.deepEqual(update(state, onCompleted, ok), [{ type: "continue" }])
  }
  assert.deepEqual(update(state, onContinue), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
  assert.equal(state.initial, "pending")
  assert.deepEqual(update(state, onCompleted, ok), [{ type: "continue" }])
  assert.deepEqual(update(state, onContinue), [{ type: "resolve" }])
  assert.deepEqual(state, { active: true, initial: "ready", work: { phase: "idle" } })
})

test("a read cannot continue before application has delivered its outcome", () => {
  const state = row()
  start(state)
  assert.deepEqual(update(state, onContinue), [])
  update(state, onRequested) // A notification from application or outstanding IO.
  update(state, onCompleted, ok)
  assert.deepEqual(state.work, { phase: "after", dirty: true })
  assert.deepEqual(update(state, onContinue), [{ type: "read" }])
  assert.equal(state.initial, "pending")
})

test("an initial failure invalidates the row before rejection and detachment effects", () => {
  const state = row()
  const error = new Error("read/apply failed")
  start(state)
  update(state, onRequested)
  assert.deepEqual(update(state, onCompleted, { ok: false, error }), [
    { type: "reject", error }, { type: "detach" }, { type: "continue" },
  ])
  assert.deepEqual(state, { active: false, initial: "failed", work: { phase: "after", dirty: false } })
  assert.deepEqual(update(state, onRequested), [])
  assert.deepEqual(update(state, onContinue), [])
  assert.deepEqual(update(state, onRemoved, abort, "aborted"), [])
  assert.equal(state.initial, "failed")
})

test("background failure reports without retrying itself or changing initial readiness", () => {
  const state = row()
  ready(state)
  start(state)
  const error = new Error("background failure")
  assert.deepEqual(update(state, onCompleted, { ok: false, error }), [
    { type: "report", error }, { type: "continue" },
  ])
  assert.equal(state.initial, "ready")
  assert.equal(state.active, true)
  assert.deepEqual(update(state, onContinue), [])
  assert.deepEqual(state.work, { phase: "idle" })
  start(state) // Only another notification starts the retry.
})

for (const callback of ["request", "remove"]) {
  test(`Continue observes an onError callback that will ${callback}`, () => {
    const state = row()
    ready(state)
    start(state)
    update(state, onCompleted, { ok: false, error: new Error("failure") })
    // Flushing report can trigger a separate update before continue is flushed.
    if (callback === "request") update(state, onRequested)
    else assert.deepEqual(update(state, onRemoved, abort, "aborted"), [{ type: "detach" }])
    assert.deepEqual(update(state, onContinue), callback === "request" ? [{ type: "read" }] : [])
    assert.equal(state.initial, "ready")
  })
}

test("removal keeps queued work identifiable but start performs no IO", () => {
  const state = row()
  update(state, onRequested)
  assert.deepEqual(update(state, onRemoved, abort, "aborted"), [
    { type: "reject", error: abort }, { type: "detach" },
  ])
  assert.deepEqual(state.work, { phase: "queued" })
  assert.deepEqual(update(state, onStart), [])
  assert.deepEqual(state.work, { phase: "idle" })
  assert.equal(state.initial, "aborted")
})

for (const outcome of [ok, { ok: false, error: new Error("obsolete") }]) {
  test(`old-row completion (${outcome.ok ? "success" : "failure"}) cannot affect a replacement`, () => {
    const old = row(), replacement = row()
    start(old)
    update(old, onRequested)
    update(old, onRemoved, abort, "aborted")
    start(replacement)
    assert.deepEqual(update(old, onCompleted, outcome), [{ type: "continue" }])
    assert.deepEqual(update(old, onContinue), [])
    assert.deepEqual(old, { active: false, initial: "aborted", work: { phase: "idle" } })
    assert.deepEqual(replacement, { active: true, initial: "pending", work: { phase: "reading", dirty: false } })
  })
}
