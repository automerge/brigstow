-------------------------- MODULE RefreshScheduler --------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
 An executable abstraction of src/RefreshScheduler.ts; no PlusCal.

 A document is a grow-only set of records. Heads/ancestry filtering is modeled
 by set difference, and DocType.apply by union into the CURRENT document.
 Sources emit notifications atomically with updates; Notify also permits
 redundant notifications. Source snapshots are assumed coherent and monotone.

 Each registration ID is <<handle, generation>> and is used at most once.
 Inactive registrations remain in the model so old IO can finish after a new
 registration starts. Handles may share a source, or change sources only after
 removal. There is deliberately no document-ID deduplication.

 phase describes the asynchronous control flow:
   queued  -- microtask scheduled, running already true
   reading -- captured a batch; source IO / refresh outcome outstanding
   after   -- refresh settled; drain's await continuation is pending
   idle    -- no drain running (also used before registration)
 Synchronous source calls and callbacks are abstracted as non-reentrant;
 subscription installation/removal is assumed to succeed. A failed refresh may
 throw before merging, or after merging (e.g. a document change listener throws).
 Error-reporting exceptions have no modeled effect.

 quiescent is an ENVIRONMENT assumption, not a scheduler flag. Quiesce stops
 source updates and notifications, allowing meaningful termination checks.
 BlockedHandles removes the IO-completion fairness assumption for chosen
 handles: the other handles must still make progress.
*)

CONSTANTS Handles, Sources, Records, MaxRegistrations, AllowFailures,
          BlockedHandles

ASSUME /\ Handles # {} /\ Sources # {} /\ Records # {}
       /\ MaxRegistrations \in Nat \ {0}
       /\ AllowFailures \in BOOLEAN
       /\ BlockedHandles \subseteq Handles

Regs == Handles \X (1..MaxRegistrations)
Owner(r) == r[1]
NoSource == "no-source"
Terminal == {"ready", "failed", "aborted"}

VARIABLES reg, docs, stored, disposed, quiescent
vars == <<reg, docs, stored, disposed, quiescent>>

EmptyRegistration ==
  [active |-> FALSE, subscribed |-> FALSE, source |-> NoSource,
   dirty |-> FALSE, running |-> FALSE, phase |-> "idle",
   result |-> "unused", batch |-> {},
   demand |-> {}, readyDemand |-> {}, delivered |-> {},
   resolvedWithWork |-> FALSE, lastFailed |-> FALSE, reported |-> FALSE]

Init ==
  /\ reg = [r \in Regs |-> EmptyRegistration]
  /\ docs = [h \in Handles |-> {}]
  /\ stored = [s \in Sources |-> {}]
  /\ disposed = FALSE
  /\ quiescent = FALSE

ActiveFor(h) == {r \in Regs : Owner(r) = h /\ reg[r].active}

(* #request: a dirty bit, not a queue of notifications. *)
Requested(x, contents) ==
  [x EXCEPT !.dirty = TRUE,
            !.running = TRUE,
            !.phase = IF x.running THEN x.phase ELSE "queued",
            !.demand = x.demand \cup contents]

(* #remove leaves a queued/running continuation alive, but invalidates it. *)
Removed(x, reason) ==
  [x EXCEPT !.active = FALSE, !.subscribed = FALSE, !.dirty = FALSE,
            !.result = IF x.result = "pending" THEN reason ELSE x.result]

(* Start one #refresh using the current document heads / source snapshot. *)
Reading(x, h) ==
  [x EXCEPT !.dirty = FALSE, !.phase = "reading",
            !.batch = stored[x.source] \ docs[h]]

Schedule(r, s) ==
  /\ ~disposed
  /\ reg[r].result = "unused"
  /\ ActiveFor(Owner(r)) = {}
  /\ reg' = [reg EXCEPT ![r] =
       [EmptyRegistration EXCEPT
         !.active = TRUE, !.subscribed = TRUE, !.source = s,
         !.dirty = TRUE, !.running = TRUE, !.phase = "queued",
         !.result = "pending", !.demand = stored[s]]]
  /\ UNCHANGED <<docs, stored, disposed, quiescent>>

(* Storage changes are already persisted; the scheduler never writes them.
   This also abstracts publication of a local save, independently of refresh. *)
Receive(s, c) ==
  /\ ~quiescent
  /\ c \notin stored[s]
  /\ stored' = [stored EXCEPT ![s] = @ \cup {c}]
  /\ reg' = [r \in Regs |->
       IF reg[r].active /\ reg[r].source = s
       THEN Requested(reg[r], stored[s] \cup {c}) ELSE reg[r]]
  /\ UNCHANGED <<docs, disposed, quiescent>>

Notify(s) ==
  /\ ~quiescent
  /\ reg' = [r \in Regs |->
       IF reg[r].active /\ reg[r].source = s
       THEN Requested(reg[r], stored[s]) ELSE reg[r]]
  /\ UNCHANGED <<docs, stored, disposed, quiescent>>

LocalEdit(h, c) ==
  /\ c \notin docs[h]
  /\ docs' = [docs EXCEPT ![h] = @ \cup {c}]
  /\ UNCHANGED <<reg, stored, disposed, quiescent>>

Start(r) ==
  /\ reg[r].phase = "queued"
  /\ reg' = [reg EXCEPT ![r] =
       IF reg[r].active THEN Reading(reg[r], Owner(r))
       ELSE [reg[r] EXCEPT !.phase = "idle", !.running = FALSE]]
  /\ UNCHANGED <<docs, stored, disposed, quiescent>>

(* Completion is tied to THIS registration, not the current registration of
   its handle. A stale success and a stale failure are both ignored.
   mergeOnFailure accounts for applyRecords merging before a listener throws. *)
Complete(r, ok, mergeOnFailure) ==
  /\ reg[r].phase = "reading"
  /\ ok \/ AllowFailures
  /\ LET x == reg[r]
         merge == x.active /\ (ok \/ mergeOnFailure)
         done == [x EXCEPT !.phase = "after", !.batch = {},
                           !.delivered = IF merge
                             THEN x.delivered \cup x.batch ELSE x.delivered]
         outcome == IF ~x.active THEN done
                    ELSE IF ok THEN [done EXCEPT !.lastFailed = FALSE]
                    ELSE IF x.result = "pending"
                         THEN Removed([done EXCEPT !.lastFailed = TRUE], "failed")
                         ELSE [done EXCEPT !.lastFailed = TRUE, !.reported = TRUE]
     IN /\ docs' = IF merge
                   THEN [docs EXCEPT ![Owner(r)] = @ \cup x.batch]
                   ELSE docs
        /\ reg' = [reg EXCEPT ![r] = outcome]
  /\ UNCHANGED <<stored, disposed, quiescent>>

CompleteRead(r) ==
  Complete(r, TRUE, FALSE)
  \/ Complete(r, FALSE, FALSE)
  \/ Complete(r, FALSE, TRUE)

(* #drain after await: consume the next dirty pass, or settle initial loading
   and release running. A failed background read retains any intervening dirty
   notification; it does not itself request a retry. *)
Continue(r) ==
  /\ reg[r].phase = "after"
  /\ LET x == reg[r]
         settle == x.active /\ ~x.dirty /\ x.result = "pending"
     IN reg' = [reg EXCEPT ![r] =
          IF x.active /\ x.dirty THEN Reading(x, Owner(r))
          ELSE [x EXCEPT !.phase = "idle", !.running = FALSE,
                        !.result = IF settle THEN "ready" ELSE x.result,
                        !.readyDemand = IF settle THEN x.demand ELSE x.readyDemand,
                        !.resolvedWithWork = IF settle THEN x.dirty ELSE x.resolvedWithWork]]
  /\ UNCHANGED <<docs, stored, disposed, quiescent>>

Unschedule(r) ==
  /\ reg[r].active
  /\ reg' = [reg EXCEPT ![r] = Removed(@, "aborted")]
  /\ UNCHANGED <<docs, stored, disposed, quiescent>>

Dispose ==
  /\ ~disposed
  /\ disposed' = TRUE
  /\ reg' = [r \in Regs |-> IF reg[r].active
                              THEN Removed(reg[r], "aborted") ELSE reg[r]]
  /\ UNCHANGED <<docs, stored, quiescent>>

Quiesce ==
  /\ ~quiescent
  /\ quiescent' = TRUE
  /\ UNCHANGED <<reg, docs, stored, disposed>>

Next ==
  \/ \E r \in Regs, s \in Sources : Schedule(r, s)
  \/ \E s \in Sources, c \in Records : Receive(s, c)
  \/ \E s \in Sources : Notify(s)
  \/ \E h \in Handles, c \in Records : LocalEdit(h, c)
  \/ \E r \in Regs : Start(r) \/ CompleteRead(r) \/ Continue(r) \/ Unschedule(r)
  \/ Dispose
  \/ Quiesce

SafetySpec == Init /\ [][Next]_vars

(* Event-loop work is weakly fair. IO need only finish for unblocked handles;
   completion may still fail when AllowFailures is TRUE. No fairness for
   Notify, Receive, Schedule, Unschedule, Dispose, or Quiesce is assumed. *)
Spec ==
  /\ SafetySpec
  /\ \A r \in Regs : WF_vars(Start(r)) /\ WF_vars(Continue(r))
  /\ \A r \in Regs : Owner(r) \notin BlockedHandles => WF_vars(CompleteRead(r))

-----------------------------------------------------------------------------
(* State invariants. demand/readyDemand/delivered are specification-only
   observers: records announced, obligations at resolution, records applied. *)

TypeOK ==
  /\ docs \in [Handles -> SUBSET Records]
  /\ stored \in [Sources -> SUBSET Records]
  /\ disposed \in BOOLEAN /\ quiescent \in BOOLEAN
  /\ reg \in [Regs ->
       [active : BOOLEAN, subscribed : BOOLEAN, source : Sources \cup {NoSource},
        dirty : BOOLEAN, running : BOOLEAN,
        phase : {"idle", "queued", "reading", "after"},
        result : {"unused", "pending", "ready", "failed", "aborted"},
        batch : SUBSET Records, demand : SUBSET Records,
        readyDemand : SUBSET Records, delivered : SUBSET Records,
        resolvedWithWork : BOOLEAN, lastFailed : BOOLEAN, reported : BOOLEAN]]

OneActiveRegistration == \A h \in Handles : Cardinality(ActiveFor(h)) <= 1

Lifecycle ==
  /\ disposed => \A r \in Regs : ~reg[r].active
  /\ \A r \in Regs :
       /\ reg[r].subscribed = reg[r].active
       /\ reg[r].active => reg[r].source \in Sources
       /\ reg[r].active => reg[r].result \in {"pending", "ready"}
       /\ reg[r].result = "pending" => reg[r].active
       /\ reg[r].result = "failed" => ~reg[r].active
       /\ reg[r].reported => reg[r].result = "ready"

RunnerOwnsWork ==
  \A r \in Regs :
    /\ reg[r].running = (reg[r].phase # "idle")
    /\ reg[r].result = "pending" => reg[r].running
    /\ reg[r].dirty => reg[r].active /\ reg[r].running
    /\ reg[r].phase = "queued" /\ reg[r].active => reg[r].dirty
    /\ reg[r].phase # "reading" => reg[r].batch = {}

(* When an initial load isn't dirty, its obligations must already be applied
   or be in the captured read. In particular, clearing dirty on IO completion
   would lose notifications and violate this invariant. *)
NoForgottenInitialWork ==
  \A r \in Regs :
    reg[r].result = "pending" /\ ~reg[r].dirty =>
      reg[r].demand \subseteq (docs[Owner(r)] \cup reg[r].batch)

InitialReadiness ==
  \A r \in Regs : reg[r].result = "ready" =>
    /\ ~reg[r].resolvedWithWork
    /\ reg[r].readyDemand \subseteq docs[Owner(r)]

IdleCaughtUpUnlessFailed ==
  \A r \in Regs :
    reg[r].active /\ ~reg[r].running /\ ~reg[r].lastFailed =>
      reg[r].demand \subseteq docs[Owner(r)]

Safety == TypeOK /\ OneActiveRegistration /\ Lifecycle /\ RunnerOwnsWork
          /\ NoForgottenInitialWork /\ InitialReadiness /\ IdleCaughtUpUnlessFailed

(* Two-state safety properties: check these as PROPERTY, not INVARIANT. *)
DocumentsOnlyGrow == [][\A h \in Handles : docs[h] \subseteq docs'[h]]_vars

NoStaleApplication ==
  [][\A r \in Regs : ~reg[r].active =>
       reg'[r].delivered = reg[r].delivered]_vars

PromiseSettlesOnce ==
  [][\A r \in Regs : reg[r].result \in Terminal =>
       reg'[r].result = reg[r].result]_vars

NoUnrequestedRetry ==
  [][\A r \in Regs :
       reg[r].active /\ reg[r].phase = "after"
       /\ reg[r].lastFailed /\ ~reg[r].dirty => reg'[r].phase # "reading"]_vars

(* A read is a single immutable slot per registration, not a global lock.
   These temporal properties additionally exercise independent progress. *)
InitialEventuallySettles ==
  \A r \in Regs : Owner(r) \notin BlockedHandles =>
    (quiescent /\ reg[r].result = "pending") ~> (reg[r].result \in Terminal)

EventuallyCaughtUp ==
  ~AllowFailures =>
    \A r \in Regs : Owner(r) \notin BlockedHandles =>
      (quiescent /\ reg[r].active) ~>
        (~reg[r].active \/ stored[reg[r].source] \subseteq docs[Owner(r)])

=============================================================================
