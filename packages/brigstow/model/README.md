# RefreshScheduler model

[`RefreshScheduler.tla`](./RefreshScheduler.tla) is a direct TLA+ model of
[`RefreshScheduler.ts`](../src/RefreshScheduler.ts), without PlusCal.
It models initial loading and ongoing source-to-document refreshes, not network
synchronization or the local-save queue.

## Properties worth preserving

The key distinction is **safety** (nothing bad happens, even if IO never finishes)
versus **progress** (something eventually happens, under stated assumptions).

### Safety

| Property | Meaning |
| --- | --- |
| `OneActiveRegistration` | At most one active registration per handle identity, not per document ID. Different handles may subscribe to the same source. |
| `Lifecycle` | Active registrations have subscriptions; removed ones do not. Pending initial promises belong to active registrations. Disposal removes all active registrations. Initial failures detach; background error reporting only belongs to handles whose initial load succeeded. |
| `RunnerOwnsWork` | Dirty work always has a runner. Pending loading has a runner. `running` covers the queued microtask, the outstanding refresh, and its continuation. |
| `NoForgottenInitialWork` | If an initial load is not dirty, every announced record is already in the document or in the captured read batch. A notification cannot disappear between passes. |
| `InitialReadiness` | At initial resolution there was no dirty follow-up, and all records announced up to resolution have been merged. This is historical readiness, not a promise that the document will never lag afterward. |
| `IdleCaughtUpUnlessFailed` | An active, idle registration has incorporated its announced records, unless its last refresh failed. |
| `DocumentsOnlyGrow` | Applying incoming records cannot discard local edits, including edits made during a read. |
| `NoStaleApplication` | Once a registration is inactive, its completions cannot contribute further records—even if its handle has been re-registered against the same source. |
| `PromiseSettlesOnce` | Ready, failed, and aborted initial promises never change outcome. In particular, unscheduling an already-ready handle does not retroactively reject its initial promise. |
| `NoUnrequestedRetry` | A background failure with no intervening notification does not start another read by itself. |

`Safety` groups the state invariants, including `TypeOK`. The last four properties
are two-state safety formulas and are checked as TLC `PROPERTY` entries.

There is **one read slot per registration**, enforced structurally by the phases
and action guards. Notifications only set a dirty bit; they do not start another
read while that registration is running.

This is intentionally *not* a global single-flight rule per handle. Unscheduling
does not cancel materialization. An old inactive registration can still have IO
outstanding while its replacement reads. The old result must be ignored, rather
than preventing the replacement from progressing.

### Conditional progress

* `InitialEventuallySettles`: after notifications stop, an initial load whose IO
  can finish eventually becomes ready, failed, or aborted.
* `EventuallyCaughtUp`: with refresh failures disabled, after notifications stop,
  an active registration whose IO can finish eventually incorporates its source's
  records, or is removed.

The specification assumes weak fairness for queued microtasks and `Continue`
steps. It assumes weak fairness for IO completion only for handles outside
`BlockedHandles`. Completion can still fail when `AllowFailures = TRUE`.

`Quiesce` is an environment action that permanently stops source updates and
notifications. It is **not** a scheduler feature, and there is no assumption that
it eventually occurs. Progress obligations apply on behaviors where it does.
Local edits and lifecycle operations remain possible afterward.

These conditions matter:

* An unending stream of notifications can keep initial loading dirty indefinitely,
  even if the notifications announce no new data.
* A background failure can leave a ready handle stale indefinitely if there is no
  later notification. Fair scheduling alone does not repair it.
* A blocked read should not stop another handle. The multi-handle configurations
  leave `h1`'s IO completion unfair while checking progress for `h2`, both when
  sources are shared and when they differ.

## State and implementation correspondence

A registration is identified by `<<handle, generation>>`. IDs are never reused,
including after removal. `MaxRegistrations` bounds registrations per handle for
finite checking; it is not an implementation limit.

The core state is `active`, `subscribed`, `dirty`, `running`, `result` (the initial
promise outcome), and `phase`:

```text
schedule -> queued -> reading -> settling -> idle
                         ^            |
                         +------------+  active and dirty: another pass
```

Removal sets `active = FALSE`, detaches the subscription, and aborts a pending
initial promise. It does **not** erase the continuation. A late read can complete,
but its records and errors are ignored. This is the important distinction between
registration lifetime and asynchronous-work lifetime.

| Model action | Implementation |
| --- | --- |
| `Schedule` | `schedule()` creates a row and subscribes before updating it with `onRequested` |
| `Receive` / `Notify` | `onRequested` queues idle work or marks an outstanding pass dirty |
| `Start` | `onStart` runs in a microtask; the `read` effect captures missing metadata |
| `Complete` | The IO adapter loads/applies records, then queues an update with `onCompleted` |
| `Continue` | `onContinue` starts another read, or resolves initial loading and becomes idle |
| `Unschedule` | `onRemoved` records rejection/detachment effects; also abstracts collection |
| `Dispose` | Scheduler `dispose()`; sources and local edits remain operational |
| `LocalEdit` | A document edit, independently of refresh IO |

The TypeScript state types and transition functions live alongside the scheduler
in [`RefreshScheduler.ts`](../src/RefreshScheduler.ts).
Each transition is an ordinary pure function taking `(row, ...args)` and returning
the effects it wants as data (`RefreshEffect[]`); there is no event union or
dispatcher switch. The scheduler invokes them through its update boundary:

```ts
this.#update(row, onRequested)
this.#update(row, onCompleted, outcome)
this.#update(row, onRemoved, { status: "aborted", error })
```

`#update` runs the transition and then executes the returned effects synchronously,
in order. Because transitions only describe effects such as `report` and
`continue`, no external code can run during a state change. There is no
forwarding adapter, additional microtask, or global event queue. Callbacks during
effect execution may cause subsequent updates for the same row, but cannot
interrupt the original transition's state changes.

An async completion retains its original row; removal invalidates that row before
detaching it, and re-registration creates a fresh object. The runtime effects are
bound to that particular row, not looked up through the handle's current registration.

The runtime row uses `initialLoad.status` for the model's `result`, and
`work.phase` for `phase`. It derives `running` from the phase rather than storing
another boolean. An active `queued` row is implicitly dirty; `reading` and
`settling` have an explicit dirty flag. The stable initial promise lives beside its
status in `initialLoad`.

The model's `Complete` action is split in TypeScript to handle callbacks safely.
The scheduler applies materialized records in a promise callback, skipping empty
snapshots and obsolete rows. Application may notify, remove the row, or throw;
there is no separate `loaded` transition or `apply` effect. The row remains
`reading` until a microtask runs `onCompleted`. Completion always goes through
that microtask, including for empty snapshots and synchronous errors, so anything
already queued (such as an unschedule) is observed first. `onCompleted` enters
`settling`, handles failure, and returns a `continue` effect. That effect reads the
row again *after* error or detachment callbacks have run; it does not add another
microtask. Thus the model's `settling` phase is a synchronous effect boundary in
the implementation, not an additional asynchronous wait. These are correspondence
notes, not a refinement proof; callback reentrancy is covered by implementation
tests rather than TLC.

`docs` and `stored` map handles/sources to sets of records. A read captures
`stored[source] \ docs[handle]`; successful completion unions that batch into the
**current** document, not into the document from when reading began.

`demand`, `readyDemand`, `resolvedWithWork`, and `delivered` are specification-only
observers. They remember notification obligations, obligations/dirty state at
initial resolution, and records applied by each registration. They do not drive
scheduling. `reported` observes background error reporting; `lastFailed` records
whether being stale at idle is permitted.

Repeated scheduling of the same pair, rejection of a different source while
registered, unscheduling an unregistered handle, and repeated disposal are
state-preserving API operations: represented by TLA+ stuttering, not separate
transitions. Promise object identity and exception messages are not modeled.

## Abstraction boundaries

* Records are a grow-only set, and CRDT application is set union. The model assumes
  correct heads, ancestry filtering, and merging; it does not verify Automerge,
  fragment/checkpoint coverage, compaction, or record deletion. Equal-head and
  empty-metadata fast paths are abstracted as an empty batch.
* Source updates and their notification dispatch are atomic. Initial subscription
  precedes reading. Synchronous source calls and callbacks are non-reentrant in
  this model; subscription installation/removal succeeds. Throwing subscription
  setup, ineffective/throwing detachment, and arbitrary callback-driven lifecycle
  reentrancy are outside its scope. In particular, the subscription invariant is
  not a guarantee that a broken source implementation physically removes listeners.
* Refresh failures abstract heads/metadata/materialization/application errors.
  A failure can occur before merging or after merging (for example, a document
  change listener throws). Error-reporting exceptions have no modeled effect.
* There is no heap or garbage collector. `Unschedule` captures collection's
  scheduler-visible cleanup, but this model cannot establish absence of strong
  reference leaks or timely finalization.
* Storage receives records independently of refresh. It may represent inbound
  sync or publication of a local save; the scheduler has no storage-write action.

This is an executable design model, **not a refinement proof of the TypeScript**.
A passing finite check validates these modeled transitions and assumptions, not
all implementations, parameter sizes, or CRDT behaviors.

## Running TLC

From the repository root, with `tla` and its tools JAR installed:

```sh
tla check --workers 2 \
  --config packages/brigstow/model/lifecycle.cfg \
  packages/brigstow/model/RefreshScheduler.tla

# All four configurations:
for config in packages/brigstow/model/*.cfg; do
  tla check --workers 2 --config "$config" \
    packages/brigstow/model/RefreshScheduler.tla || exit
done
```

Deadlock checking is disabled deliberately: an idle or disposed scheduler is a
valid terminal/stuttering state. Temporal properties check the required progress
instead. `SafetySpec` is also available without fairness assumptions for safety
experiments (omit the progress properties when using it).

Checked with TLC 2.19 through `tla` (managed tools release 1.7.4):

| Configuration | Handles | Sources | Records | Registrations/handle | Failures | Distinct states | Result |
| --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| `lifecycle.cfg` | 1 | 1 | 2 | 2 | Enabled | 708,884 | Pass |
| `source-switch.cfg` | 1 | 2 | 1 | 2 | Enabled | 391,552 | Pass |
| `shared-source.cfg` | 2 | 1 | 2 | 1 | Disabled | 951,550 | Pass |
| `independent-sources.cfg` | 2 | 2 | 1 | 1 | Disabled | 209,740 | Pass |

### Sanity-checking the properties

Four deliberate mutations were checked in temporary copies with `SafetySpec`,
the safety invariants/properties, and the `lifecycle.cfg` constants. All produced
counterexamples; none of these mutations is in the model here:

| Mutation | Detected by |
| --- | --- |
| Clear `dirty` when a read completes, losing notifications during IO | `Safety` (`NoForgottenInitialWork`) |
| Permit a completion whenever *any* registration for its handle is active, rather than checking its own registration | `NoStaleApplication` |
| Replace the document with a nonempty loaded batch instead of unioning it into current state | `DocumentsOnlyGrow` |
| Resolve initial loading at successful read completion, before draining notified follow-ups | `Safety` (`InitialReadiness`) |

The stale-generation counterexample is particularly instructive:
`schedule -> receive -> start read -> unschedule -> re-schedule -> old completion`.
Handle identity is insufficient to determine whether the old result is still valid.
