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
Automerge exposes full hashes in its fragment checkpoints; the Automerge adapter
truncates them to 24 characters and omits the redundant checkpoint for the
fragment's own head, which is already represented by `head`.

## Saving document changes

`Repo.create()` persists the initial document before returning. `handle.change(fn)`
updates the local view and emits `change` synchronously, then queues persistence.
With `SubductionSource`, its returned promise resolves after **local persistence**;
use `await handle.change(fn)` or `await handle.flush()` before reopening from the
same storage. Network sync runs separately and cannot delay an offline save. Saves are
serialized and only missing records are exported. Failures are logged and reject
these promises; local edits remain in memory, and a later change retries missing
records. `flush()` waits for queued work; it does not itself retry a failed save.

The todo demo displays save status and warns before leaving with pending or failed
local saves. Browser shutdown cannot be relied upon to finish asynchronous writes.
A local save is not proof that a remote peer has received the document.

## Subduction handles

Wrap the backend **before** constructing Subduction so that local writes and
inbound synchronization pass through the same observation layer:

```ts
import { MemorySigner, MemoryStorage, Subduction } from "@automerge/subduction"
import { ObservableStorage, SubductionSource } from "@brigstow/brigstow-subduction"

const storage = new ObservableStorage(new MemoryStorage())
const subduction = new Subduction({ signer: MemorySigner.generate(), storage })
const source = new SubductionSource(subduction, "automerge", {
  syncServers: ["wss://subduction.sync.inkandswitch.com"],
  syncTimeoutMilliseconds: 5000,
  retryIntervalMilliseconds: 5000,
  onSyncError: error => console.warn("Peer sync unavailable", error),
})
```

The todo example instead opens `IndexedDbStorage` asynchronously with database
name `brigstow-todo`, then wraps it in `ObservableStorage`. This requires the local
Subduction build to include the `idb` Cargo feature. It connects to the public
server above by default; set `VITE_SUBDUCTION_SYNC_SERVER` to override the endpoint,
or set it to an empty string for local-only use. These are public demo documents:
do not enter sensitive information. Sharing the full demo URL (including its hash)
lets another browser request the same document.

All writers sharing a backend must share the wrapper; writes directly to the
underlying backend (including from another tab/process) are not observed.
`SubductionSource` rejects an unwrapped backend rather than returning stale handles.
Pass a second constructor argument to configure the document type used on reopen;
Subduction does not persist Brigstow's `documentType` field.

The third argument configures synchronization; libraries have no implicit server.
Connections use Subduction's discovery handshake. Without `syncServers`, callers
can supply connections themselves (e.g. `Subduction.link()` in tests).

`create()` saves locally and schedules upload. `find()` returns a local copy
immediately and synchronizes in the background; for an unknown document it asks
peers and subscribes before returning a handle. A successful round with no document
returns unavailable; connection/transport failures are not treated as proof that
a remote document is absent. With no servers or connected peers, lookup retains
local-only unavailable behavior.

The source tracks requested/created documents for its lifetime, retries them every
five seconds by default, reconnects missing server peers, and re-establishes
subscriptions. Periodic rounds also catch up missed relayed updates; public-server
updates may take a retry interval rather than arriving immediately. Unopened local
documents are not automatically uploaded.

Use `await source.sync()` to request a round for all tracked documents, or
`source.sync(documentId)` for one. It rejects if no peer is reachable or every
peer fails; partial failures go to `onSyncError`. `onSynced(documentId)` reports a
round reaching at least one peer, not delivery to every replica. Live document
refreshes can complete after the round. Call `await source.shutdown()` to stop
retries and disconnect its Subduction instance; do not share that instance between
independently managed sources. Disposing a query stops notifications, not a WASM
sync already in flight; native sync deadlines bound that work. Connection wait
timeouts do not cancel handshakes either: late connections are closed after shutdown.

Handles load an initial metadata snapshot before becoming available. Subsequent
storage mutations, including batches and deletions, trigger coalesced reloads.
`heads()` and `metadata()` are synchronous, defensive snapshots of the last
completed load. `on("change", listener)` observes future snapshot changes; it does
not replay the current state. An unchanged reload does not emit an event. A reload
failure also emits change and makes snapshot reads throw until a later successful
reload. Listener exceptions are logged without failing persistence or other listeners.
Storage watchers use weak references, so handles require no separate disposal.

`materialize()` loads blobs by `(kind, head)` in the supplied order, rejecting
missing records. On source-managed handles, `apply()` persists the batch, waits
for the updated local snapshot, and schedules background sync. Standalone handles
opened directly with `SubductionSedimentreeHandle.open()` retain store-and-sync
behavior. Failed writes may still have persisted part of a batch.

Core Brigstow subscribes to the sedimentree handle before initial loading. Incoming
records merge into the current CRDT state, preserving concurrent local edits, and
emit document changes only when logical heads change. Incoming data is already
persisted by Subduction; applying it to the document does not write an echo.

The current implementation reloads metadata from storage rather than duplicating
Subduction's minimization logic. It can expose redundant persisted records and
loads compound storage entries (including their blobs) during refresh, but does
not retain a blob cache. `notAncestorsOf` excludes the supplied heads and their
known ancestors, following commit parents, fragment boundaries, and checkpoint
coverage. It conservatively retains records when an opaque fragment prevents
establishing ancestry; checkpoint-prefix coverage uses Subduction's compact
matching semantics, not full commit identity equality.
