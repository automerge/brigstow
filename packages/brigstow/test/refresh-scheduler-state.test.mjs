import assert from "node:assert/strict"
import test from "node:test"
import { transition } from "../dist/RefreshSchedulerState.js"

const row = () => ({ active: true, initial: "pending", work: { phase: "idle" } })
const ok = { ok: true }
const records = [{ kind: "commit", head: "1", parents: [], bytes: new Uint8Array([1]) }]
const remove = { type: "removed", initial: "aborted", reason: new Error("unscheduled") }

function start(state) {
  assert.deepEqual(transition(state, { type: "requested" }), [{ type: "queue-start" }])
  assert.deepEqual(transition(state, { type: "start" }), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
}

function complete(state, outcome = ok) {
  return transition(state, { type: "completed", outcome })
}

function ready(state) {
  start(state)
  assert.deepEqual(complete(state), [{ type: "continue" }])
  assert.deepEqual(transition(state, { type: "continue" }), [{ type: "resolve" }])
  assert.equal(state.initial, "ready")
}

test("requests coalesce in queued, reading, and after phases without overlapping reads", () => {
  const state = row()
  assert.deepEqual(transition(state, { type: "requested" }), [{ type: "queue-start" }])
  for (let i = 0; i < 5; i++) {
    assert.deepEqual(transition(state, { type: "requested" }), [])
    assert.deepEqual(state.work, { phase: "queued" })
  }
  assert.deepEqual(transition(state, { type: "start" }), [{ type: "read" }])
  assert.deepEqual(transition(state, { type: "start" }), [], "a second start cannot replace a read")
  for (const phase of ["reading", "after"]) {
    for (let i = 0; i < 5; i++) assert.deepEqual(transition(state, { type: "requested" }), [])
    assert.deepEqual(state.work, { phase, dirty: true })
    if (phase === "reading") assert.deepEqual(complete(state), [{ type: "continue" }])
  }
  assert.deepEqual(transition(state, { type: "continue" }), [{ type: "read" }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
  assert.equal(state.initial, "pending")
  assert.deepEqual(complete(state), [{ type: "continue" }])
  assert.deepEqual(transition(state, { type: "continue" }), [{ type: "resolve" }])
  assert.deepEqual(state, { active: true, initial: "ready", work: { phase: "idle" } })
})

test("loaded bytes do not complete a pass before the apply effect returns its outcome", () => {
  const state = row()
  start(state)
  assert.deepEqual(transition(state, { type: "loaded", records }), [{ type: "apply", records }])
  assert.deepEqual(state.work, { phase: "reading", dirty: false })
  assert.deepEqual(transition(state, { type: "continue" }), [])
  transition(state, { type: "requested" }) // An applyRecords callback.
  complete(state)
  assert.deepEqual(state.work, { phase: "after", dirty: true })
  assert.deepEqual(transition(state, { type: "continue" }), [{ type: "read" }])
  assert.equal(state.initial, "pending")
})

test("empty snapshots still pass through the completion boundary", () => {
  const state = row()
  start(state)
  assert.deepEqual(transition(state, { type: "loaded", records: [] }), [
    { type: "queue-completion", outcome: ok },
  ])
  assert.equal(state.initial, "pending")
  transition(state, { type: "requested" })
  complete(state)
  assert.deepEqual(transition(state, { type: "continue" }), [{ type: "read" }])
})

test("an initial failure invalidates the row before rejection and detachment effects", () => {
  const state = row()
  const error = new Error("read/apply failed")
  start(state)
  transition(state, { type: "requested" })
  assert.deepEqual(complete(state, { ok: false, error }), [
    { type: "reject", error }, { type: "detach" }, { type: "continue" },
  ])
  assert.deepEqual(state, { active: false, initial: "failed", work: { phase: "after", dirty: false } })
  assert.deepEqual(transition(state, { type: "requested" }), [])
  assert.deepEqual(transition(state, { type: "continue" }), [])
  assert.deepEqual(transition(state, remove), [])
  assert.equal(state.initial, "failed")
})

test("background failure reports without retrying itself or changing initial readiness", () => {
  const state = row()
  ready(state)
  start(state)
  const error = new Error("background failure")
  assert.deepEqual(complete(state, { ok: false, error }), [
    { type: "report", error }, { type: "continue" },
  ])
  assert.equal(state.initial, "ready")
  assert.equal(state.active, true)
  assert.deepEqual(transition(state, { type: "continue" }), [])
  assert.deepEqual(state.work, { phase: "idle" })
  start(state) // Only another notification starts the retry.
})

for (const callback of ["request", "remove"]) {
  test(`Continue observes an onError callback that will ${callback}`, () => {
    const state = row()
    ready(state)
    start(state)
    complete(state, { ok: false, error: new Error("failure") })
    // The driver executes report before continue, with reentrant transitions.
    if (callback === "request") transition(state, { type: "requested" })
    else assert.deepEqual(transition(state, remove), [{ type: "detach" }])
    assert.deepEqual(transition(state, { type: "continue" }), callback === "request" ? [{ type: "read" }] : [])
    assert.equal(state.initial, "ready")
  })
}

test("removal keeps queued work identifiable but start performs no IO", () => {
  const state = row()
  transition(state, { type: "requested" })
  assert.deepEqual(transition(state, remove), [
    { type: "reject", error: remove.reason }, { type: "detach" },
  ])
  assert.deepEqual(state.work, { phase: "queued" })
  assert.deepEqual(transition(state, { type: "start" }), [])
  assert.deepEqual(state.work, { phase: "idle" })
  assert.equal(state.initial, "aborted")
})

for (const outcome of [ok, { ok: false, error: new Error("obsolete") }]) {
  test(`old-row completion (${outcome.ok ? "success" : "failure"}) cannot affect a replacement`, () => {
    const old = row(), replacement = row()
    start(old)
    transition(old, { type: "requested" })
    transition(old, remove)
    start(replacement)
    assert.deepEqual(transition(old, { type: "loaded", records }), [
      { type: "queue-completion", outcome: ok },
    ], "obsolete bytes never produce an apply effect")
    assert.deepEqual(complete(old, outcome), [{ type: "continue" }])
    assert.deepEqual(transition(old, { type: "continue" }), [])
    assert.deepEqual(old, { active: false, initial: "aborted", work: { phase: "idle" } })
    assert.deepEqual(replacement, { active: true, initial: "pending", work: { phase: "reading", dirty: false } })
  })
}
