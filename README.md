# Brigstow

> Brigstow : The place by the bridge, also the town Alex Good lives in

This repository is an experiment. The goal is to identify a modular 
decomposition of [automerge-repo](git@github.com:automerge/automerge-repo.git).
We would like to express the automerge specific parts of `automerge-repo` as a
composition of capabilities that are useful for any CRDT, with the automerge
specific parts. This will allow us to experiment with different CRDTs and to
share infrastructure with non-automerge applications.

The strategy we are taking is to rewrite automerge-repo from scratch, paying a
lot of attention to the conceptual integrity of the modules we are introducing.
There are two concepts which underly the decomposition in `brigstow`. Firstly
[`sedimentree`s](https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md).
as a general mechanism for synchronizing local first data. Secondly  
"handles" as an API for building UIs on top of CRDTs.

The structure of the codebase at the moment is roughly this:

* `@brigstow/brigstow` - defines a CRDT independent `Repo` class, 
  the `SedimentreeSource` interface which abstracts over sync, and
  a `DocType` interface which abstracts over the specific CRDT 
  a `DocHandle` contains
* `@brigstow/subduction` - an implementation of `SedimentreeSource`
  using subduction for sync
* `@brigstow/automerge-repo` an API meant to be compatible with
  `@automerge/automerge-repo`, implemented by defining an implementation
  of `DocType` for automerge documents and wrapping that around the
  core brigstow `Repo`

## Local Subduction build

The workspace overrides `@automerge/subduction` with the built package at
`../subduction/subduction_wasm`. Build that sibling checkout before running
`pnpm install`; its package exports use `dist/`, not the legacy `pkg-node/`.

Fragment metadata uses `string[]` checkpoints, each exactly 24 hex characters
encoding a 12-byte commit-ID prefix, not a full commit ID. Heads and boundaries
remain full hex-encoded IDs. The Subduction adapter accepts either hex case and
returns lowercase hex, converting to bytes only at the WASM boundary. Metadata
can be serialized as JSON and reused as Brigstow record metadata directly;
checkpoints obtained from WASM directly can be converted with `cp.toHexString()`.

## Saving document changes

`Repo.create()` persists the initial document before returning. `handle.change(fn)`
updates the local view and emits `change` synchronously, then queues persistence.
Its returned promise resolves after persistence/sync; use `await handle.change(fn)`
or `await handle.flush()` before reopening the document elsewhere. Saves are
serialized and only missing records are exported. Failures are logged and reject
these promises; local edits remain in memory, and a later change retries missing
records. `flush()` waits for queued work; it does not itself retry a failed save.

The todo demo displays save status and warns before leaving with pending or failed
saves. Browser shutdown cannot be relied upon to finish asynchronous writes.

## Subduction handles

Wrap the backend **before** constructing Subduction so that local writes and
inbound synchronization pass through the same observation layer:

```ts
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"

const storage = new ObservableStorage(new MemoryStorage())
const subduction = new Subduction({ signer: MemorySigner.generate(), storage })
const source = new SubductionSource(subduction) // reopened documents use type "automerge"
```

The todo example instead opens `IndexedDbStorage` asynchronously with database
name `brigstow-todo`, then wraps it in `ObservableStorage`. This requires the local
Subduction build to include the `idb` Cargo feature.

All writers sharing a backend must share the wrapper; writes directly to the
underlying backend (including from another tab/process) are not observed.
`SubductionSource` rejects an unwrapped backend rather than returning stale handles.
Pass a second constructor argument to configure the document type used on reopen;
Subduction does not persist Brigstow's `documentType` field. Source lookup is local;
the host is responsible for connecting peers and initiating document discovery/sync.

Handles load an initial metadata snapshot before becoming available. Subsequent
storage mutations, including batches and deletions, trigger coalesced reloads.
`heads()` and `metadata()` are synchronous, defensive snapshots of the last
completed load. `on("change", listener)` observes future snapshot changes; it does
not replay the current state. An unchanged reload does not emit an event. A reload
failure also emits change and makes snapshot reads throw until a later successful
reload. Listener exceptions are logged without failing persistence or other listeners.
Storage watchers use weak references, so handles require no separate disposal.

`materialize()` loads blobs by `(kind, head)` in the supplied order, rejecting
missing records. `apply()` merges a batch through Subduction, awaits its bounded
sync round, then waits for the updated snapshot. Failed writes/syncs may still have
persisted data; per-peer transport failures do not undo local persistence.

The current implementation reloads metadata from storage rather than duplicating
Subduction's minimization logic. It can expose redundant persisted records and
loads compound storage entries (including their blobs) during refresh, but does
not retain a blob cache. `notAncestorsOf` excludes the supplied heads and their
known ancestors, following commit parents, fragment boundaries, and checkpoint
coverage. It conservatively retains records when an opaque fragment prevents
establishing ancestry; checkpoint-prefix coverage uses Subduction's compact
matching semantics, not full commit identity equality.
