import assert from "node:assert/strict"
import test from "node:test"
import { onRequested, onStart, onCompleted, onContinue, onRemoved } from "../dist/RefreshScheduler.js"

const row = () => ({ active: true, initialLoad: { status: "pending" }, work: { phase: "idle" } })
const ok = { ok: true }
const abort = { status: "aborted", error: new Error("unscheduled") }

// The transitions return their effects as data, so they can be checked without
// IO. Scheduler integration tests exercise the real update boundary.
function start(state) {
  assert.deepEqual(onRequested(state), [{ type: "queueStart" }])
  assert.deepEqual(onStart(state), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
}

function ready(state) {
  start(state)
  assert.deepEqual(onCompleted(state, ok), [{ type: "continue" }])
  assert.deepEqual(onContinue(state), [{ type: "resolve" }])
  assert.equal(state.initialLoad.status, "ready")
}

test("requests coalesce in queued, reading, and settling phases without overlapping reads", () => {
  const state = row()
  assert.deepEqual(onRequested(state), [{ type: "queueStart" }])
  for (let i = 0; i < 5; i++) {
    assert.deepEqual(onRequested(state), [])
    assert.deepEqual(state.work, { phase: "queued" })
  }
  assert.deepEqual(onStart(state), [{ type: "read" }])
  assert.deepEqual(onStart(state), [], "a second start cannot replace a read")
  for (const phase of ["reading", "settling"]) {
    for (let i = 0; i < 5; i++) assert.deepEqual(onRequested(state), [])
    assert.deepEqual(state.work, { phase, dirty: true })
    if (phase === "reading") assert.deepEqual(onCompleted(state, ok), [{ type: "continue" }])
  }
  assert.deepEqual(onContinue(state), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
  assert.equal(state.initialLoad.status, "pending")
  assert.deepEqual(onCompleted(state, ok), [{ type: "continue" }])
  assert.deepEqual(onContinue(state), [{ type: "resolve" }])
  assert.deepEqual(state, { active: true, initialLoad: { status: "ready" }, work: { phase: "idle" } })
})

test("a read cannot continue before application has delivered its outcome", () => {
  const state = row()
  start(state)
  assert.deepEqual(onContinue(state), [])
  onRequested(state) // A notification from application or outstanding IO.
  onCompleted(state, ok)
  assert.deepEqual(state.work, { phase: "settling", dirty: true })
  assert.deepEqual(onContinue(state), [{ type: "read" }])
  assert.equal(state.initialLoad.status, "pending")
})

test("an initial failure invalidates the row before rejection and detachment effects", () => {
  const state = row()
  const error = new Error("read/apply failed")
  start(state)
  onRequested(state)
  assert.deepEqual(onCompleted(state, { ok: false, error }), [
    { type: "reject", error }, { type: "detach" }, { type: "continue" },
  ])
  assert.deepEqual(state, {
    active: false, initialLoad: { status: "failed" }, work: { phase: "settling", dirty: false },
  })
  assert.deepEqual(onRequested(state), [])
  assert.deepEqual(onContinue(state), [])
  assert.deepEqual(onRemoved(state, abort), [])
  assert.equal(state.initialLoad.status, "failed")
})

test("background failure reports without retrying itself or changing initial readiness", () => {
  const state = row()
  ready(state)
  start(state)
  const error = new Error("background failure")
  assert.deepEqual(onCompleted(state, { ok: false, error }), [
    { type: "report", error }, { type: "continue" },
  ])
  assert.equal(state.initialLoad.status, "ready")
  assert.equal(state.active, true)
  assert.deepEqual(onContinue(state), [])
  assert.deepEqual(state.work, { phase: "idle" })
  start(state) // Only another notification starts the retry.
})

for (const callback of ["request", "remove"]) {
  test(`Continue observes an onError callback that will ${callback}`, () => {
    const state = row()
    ready(state)
    start(state)
    onCompleted(state, { ok: false, error: new Error("failure") })
    // Running the report effect can trigger a separate update before continue runs.
    if (callback === "request") onRequested(state)
    else assert.deepEqual(onRemoved(state, abort), [{ type: "detach" }])
    assert.deepEqual(onContinue(state), callback === "request" ? [{ type: "read" }] : [])
    assert.equal(state.initialLoad.status, "ready")
  })
}

test("removal keeps queued work identifiable but start performs no IO", () => {
  const state = row()
  onRequested(state)
  assert.deepEqual(onRemoved(state, abort), [
    { type: "reject", error: abort.error }, { type: "detach" },
  ])
  assert.deepEqual(state.work, { phase: "queued" })
  assert.deepEqual(onStart(state), [])
  assert.deepEqual(state.work, { phase: "idle" })
  assert.equal(state.initialLoad.status, "aborted")
})

for (const outcome of [ok, { ok: false, error: new Error("obsolete") }]) {
  test(`old-row completion (${outcome.ok ? "success" : "failure"}) cannot affect a replacement`, () => {
    const old = row(), replacement = row()
    start(old)
    onRequested(old)
    onRemoved(old, abort)
    start(replacement)
    assert.deepEqual(onCompleted(old, outcome), [{ type: "continue" }])
    assert.deepEqual(onContinue(old), [])
    assert.deepEqual(old, { active: false, initialLoad: { status: "aborted" }, work: { phase: "idle" } })
    assert.deepEqual(replacement, {
      active: true, initialLoad: { status: "pending" }, work: { phase: "reading", dirty: false },
    })
  })
}
