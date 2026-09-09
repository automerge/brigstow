import assert from "node:assert/strict"
import test from "node:test"
import { RefreshScheduler } from "../dist/RefreshScheduler.js"

const tick = () => new Promise(resolve => setImmediate(resolve))
const options = { timeout: 3000 }

function deferred() {
  let resolve, reject
  const promise = new Promise((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

function observe(promise) {
  const result = { status: "pending" }
  promise.then(
    () => { result.status = "fulfilled" },
    error => { result.status = "rejected"; result.error = error },
  )
  return result
}

function fakeHandle(documentId = new Uint8Array(16), initial = []) {
  const values = new Set(initial)
  const applied = []
  return {
    documentId,
    values,
    applied,
    heads: () => [...values].map(String),
    applyRecords(records) {
      applied.push(records)
      for (const record of records) values.add(record.bytes[0])
    },
  }
}

function fakeSource(initial = [1], documentId = new Uint8Array(16)) {
  const stored = new Map()
  const listeners = new Set()
  const trace = []
  const reads = []
  const gates = []
  let active = 0, maxActive = 0, subscriptions = 0, unsubscriptions = 0
  const put = (...values) => {
    for (const value of values) {
      stored.set(String(value), { kind: "commit", head: String(value), parents: [] })
    }
  }
  put(...initial)
  const source = {
    documentId,
    documentType: "set",
    heads() { trace.push("heads"); return [...stored.keys()] },
    metadata({ notAncestorsOf = [] } = {}) {
      trace.push("metadata")
      const known = new Set(notAncestorsOf)
      return [...stored.values()].filter(meta => !known.has(meta.head))
    },
    async materialize(metas) {
      trace.push("materialize")
      reads.push(metas)
      maxActive = Math.max(maxActive, ++active)
      const gate = gates.shift()
      try {
        if (gate) {
          gate.entered.resolve(metas)
          await gate.release.promise
        }
        return metas.map(meta => new Uint8Array([Number(meta.head)]))
      } finally {
        active--
      }
    },
    async apply() { assert.fail("refreshing must not persist incoming records") },
    on(event, listener) {
      assert.equal(event, "change")
      trace.push("on")
      subscriptions++
      listeners.add(listener)
    },
    off(event, listener) {
      assert.equal(event, "change")
      trace.push("off")
      unsubscriptions++
      assert.ok(listeners.delete(listener), "remove the exact subscribed listener once")
    },
  }
  const emit = () => { for (const listener of [...listeners]) listener() }
  return {
    source, listeners, trace, reads, put, emit,
    receive(...values) { put(...values); emit() },
    blockNextRead() {
      const gate = { entered: deferred(), release: deferred() }
      gates.push(gate)
      return gate
    },
    get maxActive() { return maxActive },
    get subscriptions() { return subscriptions },
    get unsubscriptions() { return unsubscriptions },
  }
}

function setup(t, onError) {
  const errors = []
  const scheduler = new RefreshScheduler({
    onError: onError ?? ((error, handle) => errors.push({ error, handle })),
  })
  t.after(() => scheduler.dispose())
  return { scheduler, errors }
}

const contents = handle => [...handle.values].sort((a, b) => a - b)
const isAbortError = error => error?.name === "AbortError"

test("subscribes before initial source reads and catches notifications during metadata", options, async t => {
  const { scheduler } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  const metadata = f.source.metadata.bind(f.source)
  let first = true
  t.mock.method(f.source, "metadata", (...args) => {
    const snapshot = metadata(...args)
    if (first) {
      first = false
      assert.equal(f.listeners.size, 1)
      f.receive(2)
    }
    return snapshot
  })
  await scheduler.schedule(handle, f.source)
  assert.equal(f.trace[0], "on", "subscription must precede heads and metadata")
  assert.ok(f.trace.includes("metadata"))
  assert.deepEqual(contents(handle), [1, 2])
})

test("initial promise includes follow-up reads and duplicate schedules share that exact promise", options, async t => {
  const { scheduler } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  const first = f.blockNextRead()
  const loading = scheduler.schedule(handle, f.source)
  const state = observe(loading)
  assert.strictEqual(scheduler.schedule(handle, f.source), loading)
  await first.entered.promise
  const second = f.blockNextRead()
  f.receive(2)
  f.emit()
  first.release.resolve()
  await second.entered.promise
  await tick()
  assert.equal(state.status, "pending", "initial load must await notifications received during loading")
  assert.strictEqual(scheduler.schedule(handle, f.source), loading)
  assert.equal(f.subscriptions, 1)
  second.release.resolve()
  await loading
  assert.deepEqual(contents(handle), [1, 2])
  const reads = f.reads.length
  await scheduler.schedule(handle, f.source)
  assert.equal(f.subscriptions, 1)
  assert.equal(f.reads.length, reads, "an already loaded registration is idempotent")
})

test("distinct handles for the same document ID are independently registered", options, async t => {
  const { scheduler } = setup(t)
  const id = new Uint8Array(16)
  const f = fakeSource([1], id)
  const first = fakeHandle(id), second = fakeHandle(id)
  await Promise.all([scheduler.schedule(first, f.source), scheduler.schedule(second, f.source)])
  assert.equal(f.listeners.size, 2)
  assert.deepEqual(contents(first), [1])
  assert.deepEqual(contents(second), [1])
  scheduler.unschedule(first)
  f.receive(2)
  await tick()
  assert.deepEqual(contents(first), [1])
  assert.deepEqual(contents(second), [1, 2])
  assert.equal(f.listeners.size, 1)
})

test("bursts coalesce into one follow-up read without overlapping reads for a handle", options, async t => {
  const { scheduler } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  await scheduler.schedule(handle, f.source)
  const before = f.reads.length
  const first = f.blockNextRead()
  f.receive(2)
  await first.entered.promise
  const second = f.blockNextRead()
  for (let i = 0; i < 20; i++) f.emit()
  f.receive(3, 4)
  await tick()
  assert.equal(f.reads.length, before + 1)
  first.release.resolve()
  await second.entered.promise
  second.release.resolve()
  await tick()
  assert.deepEqual(contents(handle), [1, 2, 3, 4])
  assert.equal(f.reads.length, before + 2, "all queued notifications share one follow-up")
  assert.equal(f.maxActive, 1)
})

test("unrelated handles load and refresh while another handle is blocked", options, async t => {
  const { scheduler } = setup(t)
  const slow = fakeSource(), fast = fakeSource([2])
  const a = fakeHandle(), b = fakeHandle()
  const gate = slow.blockNextRead()
  const loading = scheduler.schedule(a, slow.source)
  const state = observe(loading)
  await gate.entered.promise
  await scheduler.schedule(b, fast.source)
  fast.receive(3)
  await tick()
  assert.deepEqual(contents(b), [2, 3])
  assert.equal(state.status, "pending")
  gate.release.resolve()
  await loading
})

for (const stage of ["heads", "metadata", "materialize", "applyRecords"]) {
  test(`initial ${stage} failure rejects, unsubscribes, and allows fresh registration`, options, async t => {
    const { scheduler, errors } = setup(t)
    const f = fakeSource()
    const handle = fakeHandle()
    const target = stage === "applyRecords" ? handle : f.source
    const original = target[stage].bind(target)
    const error = new Error(`initial ${stage} failed`)
    const mock = t.mock.method(target, stage, () => {
      if (stage === "materialize") return Promise.reject(error)
      throw error
    })
    await assert.rejects(scheduler.schedule(handle, f.source), reason => reason === error)
    assert.equal(f.listeners.size, 0)
    assert.equal(f.unsubscriptions, 1)
    assert.deepEqual(errors, [])
    assert.deepEqual(contents(handle), [])
    mock.mock.mockImplementation(original)
    await scheduler.schedule(handle, f.source)
    assert.deepEqual(contents(handle), [1])
    assert.equal(f.listeners.size, 1)
    assert.equal(f.subscriptions, 2)
  })
}

test("a failure in an initial follow-up still rejects initial loading without onError", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  const first = f.blockNextRead()
  const loading = scheduler.schedule(handle, f.source)
  const error = new Error("follow-up failed")
  const rejected = assert.rejects(loading, reason => reason === error)
  await first.entered.promise
  const second = f.blockNextRead()
  f.receive(2)
  first.release.resolve()
  await second.entered.promise
  second.release.reject(error)
  await rejected
  assert.equal(f.listeners.size, 0)
  assert.deepEqual(errors, [])
})

for (const stage of ["heads", "metadata", "materialize", "applyRecords"]) {
  test(`background ${stage} failure reports the handle and retries on the next notification`, options, async t => {
    const { scheduler, errors } = setup(t)
    const f = fakeSource()
    const handle = fakeHandle()
    const loading = scheduler.schedule(handle, f.source)
    await loading
    const target = stage === "applyRecords" ? handle : f.source
    const original = target[stage].bind(target)
    const error = new Error(`background ${stage} failed`)
    let attempts = 0
    const mock = t.mock.method(target, stage, () => {
      attempts++
      if (stage === "materialize") return Promise.reject(error)
      throw error
    })
    f.receive(2)
    await tick()
    assert.deepEqual(errors, [{ error, handle }])
    assert.deepEqual(contents(handle), [1])
    assert.equal(f.listeners.size, 1)
    await tick()
    assert.equal(attempts, 1, "do not spin retrying without another notification")
    await loading // A settled initial promise must not become a background failure.
    mock.mock.mockImplementation(original)
    f.emit()
    await tick()
    assert.deepEqual(contents(handle), [1, 2])
    assert.equal(errors.length, 1)
  })
}

test("a notification during a failing background read still requests a follow-up", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  await scheduler.schedule(handle, f.source)
  const gate = f.blockNextRead()
  f.receive(2)
  await gate.entered.promise
  f.receive(3)
  const error = new Error("in-flight failure")
  gate.release.reject(error)
  await tick()
  assert.deepEqual(errors, [{ error, handle }])
  assert.deepEqual(contents(handle), [1, 2, 3], "retry without requiring one more notification")
  assert.equal(f.reads.length, 3)
  assert.equal(f.maxActive, 1)
})

test("throwing onError callbacks do not block retries or unrelated handles", options, async t => {
  t.mock.method(console, "error", () => {})
  const reports = []
  const { scheduler } = setup(t, (error, handle) => {
    reports.push({ error, handle })
    throw new Error("reporter failed")
  })
  const f = fakeSource(), other = fakeSource([9])
  const handle = fakeHandle(), otherHandle = fakeHandle()
  await scheduler.schedule(handle, f.source)
  const gate = f.blockNextRead()
  f.receive(2)
  await gate.entered.promise
  const error = new Error("read failed")
  gate.release.reject(error)
  await tick()
  assert.deepEqual(reports, [{ error, handle }])
  await scheduler.schedule(otherHandle, other.source)
  f.emit()
  await tick()
  assert.deepEqual(contents(handle), [1, 2])
  assert.deepEqual(contents(otherHandle), [9])
})

test("unschedule promptly aborts initial loading and ignores late successful bytes", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  const gate = f.blockNextRead()
  const loading = scheduler.schedule(handle, f.source)
  const state = observe(loading)
  await gate.entered.promise
  f.receive(2) // Queued work must be discarded too.
  scheduler.unschedule(handle)
  scheduler.unschedule(handle)
  scheduler.unschedule(fakeHandle())
  await tick()
  assert.equal(state.status, "rejected", "abort must not wait for materialize to complete")
  assert.ok(isAbortError(state.error))
  assert.equal(f.listeners.size, 0)
  assert.equal(f.unsubscriptions, 1)
  gate.release.resolve()
  f.emit()
  await tick()
  assert.deepEqual(handle.applied, [])
  assert.equal(f.reads.length, 1)
  assert.deepEqual(errors, [])
})

for (const lateResult of ["resolve", "reject"]) {
  test(`re-registering a handle isolates a late old read that will ${lateResult}`, options, async t => {
    const { scheduler, errors } = setup(t)
    const old = fakeSource([1]), fresh = fakeSource([2])
    const handle = fakeHandle()
    const gate = old.blockNextRead()
    const loading = scheduler.schedule(handle, old.source)
    const aborted = assert.rejects(loading, isAbortError)
    await gate.entered.promise
    scheduler.unschedule(handle)
    await aborted
    await scheduler.schedule(handle, fresh.source)
    const applied = handle.applied.length
    gate.release[lateResult](lateResult === "reject" ? new Error("obsolete read failed") : undefined)
    await tick()
    assert.deepEqual(contents(handle), [2])
    assert.equal(handle.applied.length, applied, "old registration must never apply late records")
    assert.deepEqual(errors, [], "obsolete failures are not background failures of the new registration")
    fresh.receive(3)
    await tick()
    assert.deepEqual(contents(handle), [2, 3])
    assert.equal(old.listeners.size, 0)
    assert.equal(fresh.listeners.size, 1)
  })
}

test("re-registering with the same source also discards the old registration's late records", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  const gate = f.blockNextRead()
  const oldLoading = scheduler.schedule(handle, f.source)
  const aborted = assert.rejects(oldLoading, isAbortError)
  await gate.entered.promise
  scheduler.unschedule(handle)
  await aborted
  f.put(2)
  const newLoading = scheduler.schedule(handle, f.source)
  assert.notStrictEqual(newLoading, oldLoading)
  await newLoading
  const applied = handle.applied.length
  gate.release.resolve()
  await tick()
  assert.equal(handle.applied.length, applied, "identity of handle/source is not enough to identify a registration")
  assert.deepEqual(contents(handle), [1, 2])
  assert.equal(f.listeners.size, 1)
  f.receive(3)
  await tick()
  assert.deepEqual(contents(handle), [1, 2, 3])
  assert.deepEqual(errors, [])
})

test("unscheduling a loaded handle discards an in-flight refresh and queued events", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource()
  const handle = fakeHandle()
  await scheduler.schedule(handle, f.source)
  const gate = f.blockNextRead()
  f.receive(2)
  await gate.entered.promise
  f.receive(3)
  scheduler.unschedule(handle)
  gate.release.resolve()
  await tick()
  assert.deepEqual(contents(handle), [1])
  assert.equal(f.reads.length, 2)
  assert.equal(f.listeners.size, 0)
  assert.deepEqual(errors, [])
})

test("different source registrations reject while initial or loaded until unscheduled", options, async t => {
  const { scheduler } = setup(t)
  const first = fakeSource([1]), second = fakeSource([2])
  const handle = fakeHandle()
  const gate = first.blockNextRead()
  const loading = scheduler.schedule(handle, first.source)
  await gate.entered.promise
  await assert.rejects(scheduler.schedule(handle, second.source))
  assert.strictEqual(scheduler.schedule(handle, first.source), loading)
  assert.equal(second.subscriptions, 0)
  assert.equal(first.listeners.size, 1)
  gate.release.resolve()
  await loading
  await assert.rejects(scheduler.schedule(handle, second.source))
  first.receive(3)
  await tick()
  assert.deepEqual(contents(handle), [1, 3], "rejection must not disturb the existing registration")
  scheduler.unschedule(handle)
  await scheduler.schedule(handle, second.source)
  assert.deepEqual(contents(handle), [1, 2, 3])
  assert.equal(first.listeners.size, 0)
  assert.equal(second.listeners.size, 1)
})

test("dispose is idempotent, aborts all initial loads, stops loaded handles, and permanently rejects schedule", options, async t => {
  const { scheduler, errors } = setup(t)
  const sources = [fakeSource([1]), fakeSource([2]), fakeSource([3])]
  const handles = sources.map(() => fakeHandle())
  await scheduler.schedule(handles[2], sources[2].source)
  const gates = sources.slice(0, 2).map(f => f.blockNextRead())
  const states = sources.slice(0, 2).map((f, i) => observe(scheduler.schedule(handles[i], f.source)))
  await Promise.all(gates.map(gate => gate.entered.promise))
  const background = sources[2].blockNextRead()
  sources[2].receive(4)
  await background.entered.promise
  scheduler.dispose()
  scheduler.dispose()
  await tick()
  for (const state of states) {
    assert.equal(state.status, "rejected")
    assert.ok(isAbortError(state.error))
  }
  for (const f of sources) {
    assert.equal(f.listeners.size, 0)
    assert.equal(f.unsubscriptions, 1)
    f.emit()
  }
  for (const gate of [...gates, background]) gate.release.resolve()
  await tick()
  assert.deepEqual(handles.map(contents), [[], [], [3]])
  const fresh = fakeSource()
  await assert.rejects(scheduler.schedule(fakeHandle(), fresh.source))
  await assert.rejects(scheduler.schedule(handles[2], sources[2].source))
  assert.equal(fresh.subscriptions, 0)
  assert.deepEqual(errors, [])
})

test("unscheduling before the start microtask detaches without reading", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource(), handle = fakeHandle()
  const loading = scheduler.schedule(handle, f.source)
  const rejected = assert.rejects(loading, isAbortError)
  scheduler.unschedule(handle)
  assert.equal(f.listeners.size, 0)
  await rejected
  await tick()
  assert.deepEqual(f.trace, ["on", "off"])
  assert.deepEqual(handle.applied, [])
  assert.deepEqual(errors, [])
})

for (const fails of [false, true]) {
  test(`synchronous subscription notifications ${fails ? "followed by setup failure cancel queued work" : "coalesce with the initial request"}`, options, async t => {
    const { scheduler, errors } = setup(t)
    const f = fakeSource(), handle = fakeHandle()
    const on = f.source.on.bind(f.source)
    const error = new Error("subscribe failed")
    t.mock.method(f.source, "on", (...args) => {
      on(...args)
      f.receive(2)
      f.emit()
      if (fails) throw error
    })
    const loading = scheduler.schedule(handle, f.source)
    if (fails) {
      await assert.rejects(loading, reason => reason === error)
      await tick()
      assert.deepEqual(f.trace, ["on", "off"])
      assert.deepEqual(handle.applied, [])
    } else {
      await loading
      assert.deepEqual(contents(handle), [1, 2])
      assert.equal(f.reads.length, 1)
    }
    assert.deepEqual(errors, [])
  })
}

for (const notify of ["synchronous", "microtask"]) {
  test(`${notify} notification from applyRecords is included in initial readiness`, options, async t => {
    const { scheduler } = setup(t)
    const f = fakeSource(), handle = fakeHandle()
    const first = f.blockNextRead()
    const loading = scheduler.schedule(handle, f.source)
    const state = observe(loading)
    await first.entered.promise
    const followUp = f.blockNextRead()
    const apply = handle.applyRecords.bind(handle)
    let initial = true
    t.mock.method(handle, "applyRecords", records => {
      apply(records)
      if (initial) {
        initial = false
        if (notify === "synchronous") f.receive(2)
        else queueMicrotask(() => f.receive(2))
      }
    })
    first.release.resolve()
    await followUp.entered.promise
    await tick()
    assert.equal(state.status, "pending")
    assert.deepEqual(contents(handle), [1])
    followUp.release.resolve()
    await loading
    assert.deepEqual(contents(handle), [1, 2])
    assert.equal(f.maxActive, 1)
    assert.equal(f.reads.length, 2)
  })
}

test("an initial apply that merges, notifies, then throws still rejects and detaches", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource(), handle = fakeHandle()
  const apply = handle.applyRecords.bind(handle)
  const error = new Error("document listener failed")
  t.mock.method(handle, "applyRecords", records => {
    apply(records)
    f.receive(2)
    throw error
  })
  await assert.rejects(scheduler.schedule(handle, f.source), reason => reason === error)
  await tick()
  assert.deepEqual(contents(handle), [1], "a listener failure does not roll back a merge")
  assert.equal(f.listeners.size, 0)
  assert.equal(f.reads.length, 1, "initial failure discards the notified follow-up")
  assert.deepEqual(errors, [])
})

test("applyRecords can replace its registration and throw without failing the replacement", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource(), handle = fakeHandle()
  const apply = handle.applyRecords.bind(handle)
  let replacement, initial = true
  t.mock.method(handle, "applyRecords", records => {
    apply(records)
    if (initial) {
      initial = false
      scheduler.unschedule(handle)
      f.put(2)
      replacement = scheduler.schedule(handle, f.source)
      throw new Error("obsolete document callback")
    }
  })
  const loading = scheduler.schedule(handle, f.source)
  await assert.rejects(loading, isAbortError)
  assert.ok(replacement)
  assert.notStrictEqual(replacement, loading)
  await replacement
  assert.deepEqual(contents(handle), [1, 2])
  assert.equal(f.listeners.size, 1)
  assert.equal(f.subscriptions, 2)
  assert.equal(f.unsubscriptions, 1)
  assert.deepEqual(errors, [])
})

for (const throws of [false, true]) {
  test(`onError can request a retry${throws ? " and throw" : ""} before Continue runs`, options, async t => {
    t.mock.method(console, "error", () => {})
    const f = fakeSource(), handle = fakeHandle(), reports = []
    const { scheduler } = setup(t, (error, reportedHandle) => {
      reports.push({ error, handle: reportedHandle })
      f.receive(3)
      if (throws) throw new Error("reporter failed")
    })
    const loading = scheduler.schedule(handle, f.source)
    await loading
    const gate = f.blockNextRead()
    f.receive(2)
    await gate.entered.promise
    const error = new Error("background read failed")
    gate.release.reject(error)
    await tick()
    assert.deepEqual(reports, [{ error, handle }])
    assert.deepEqual(contents(handle), [1, 2, 3])
    assert.equal(f.reads.length, 3)
    assert.equal(f.maxActive, 1)
    assert.strictEqual(scheduler.schedule(handle, f.source), loading)
  })
}

test("onError can remove and re-register a handle before the old Continue runs", options, async t => {
  const old = fakeSource(), fresh = fakeSource([9]), handle = fakeHandle()
  let replacement, reports = 0
  const { scheduler } = setup(t, () => {
    reports++
    scheduler.unschedule(handle)
    replacement = scheduler.schedule(handle, fresh.source)
  })
  const loading = scheduler.schedule(handle, old.source)
  await loading
  const gate = old.blockNextRead()
  old.receive(2)
  await gate.entered.promise
  old.receive(3) // The old dirty follow-up must not run after removal.
  gate.release.reject(new Error("replace source"))
  await tick()
  assert.ok(replacement)
  await replacement
  await loading // Removal must not retroactively reject initial readiness.
  assert.deepEqual(contents(handle), [1, 9])
  assert.equal(old.reads.length, 2)
  assert.equal(old.listeners.size, 0)
  assert.equal(fresh.listeners.size, 1)
  assert.equal(reports, 1)
})

test("synchronous read failure keeps the await boundary so queued removal suppresses reporting", options, async t => {
  const { scheduler, errors } = setup(t)
  const f = fakeSource(), handle = fakeHandle()
  await scheduler.schedule(handle, f.source)
  t.mock.method(f.source, "heads", () => {
    queueMicrotask(() => scheduler.unschedule(handle))
    throw new Error("obsolete before drain resumes")
  })
  f.receive(2)
  await tick()
  assert.equal(f.listeners.size, 0)
  assert.deepEqual(contents(handle), [1])
  assert.deepEqual(errors, [])
})

test("detachment removes the old row before unsubscribe can re-register and throw", options, async t => {
  const logs = t.mock.method(console, "error", () => {})
  const { scheduler, errors } = setup(t)
  const f = fakeSource(), handle = fakeHandle()
  const off = f.source.off.bind(f.source)
  let replacement, initial = true
  t.mock.method(f.source, "off", (...args) => {
    off(...args)
    if (initial) {
      initial = false
      replacement = scheduler.schedule(handle, f.source)
      throw new Error("unsubscribe callback failed")
    }
  })
  const loading = scheduler.schedule(handle, f.source)
  const rejected = assert.rejects(loading, isAbortError)
  scheduler.unschedule(handle)
  await rejected
  assert.ok(replacement)
  assert.notStrictEqual(replacement, loading)
  await replacement
  assert.deepEqual(contents(handle), [1])
  assert.equal(f.reads.length, 1, "the cancelled start must not read")
  assert.equal(f.listeners.size, 1)
  assert.equal(f.subscriptions, 2)
  assert.equal(f.unsubscriptions, 1)
  assert.deepEqual(errors, [])
  assert.equal(logs.mock.callCount(), 1)
})
