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
