import type { SedimentreeAdapter } from "./SedimentreeAdapter.js"

export interface DocType<
  State,
  View = State,
  Change = unknown,
  Init = unknown,
> {
  /** Stable document type name, e.g. "automerge", "yjs", "counter". */
  readonly name: string

  /** Blank local state for find/load before any records have arrived. */
  empty(): State

  /** Initial state for a locally-created document. */
  init(init: Init): State

  /** What handles expose as their current document/view. */
  view(state: State): View

  /** Apply a typed local change. */
  change(state: State, change: Change): State

  /** Publishable logical heads. */
  heads(state: State): string[]

  /** Used by document queries; default is `heads(state).length > 0`. */
  hasData?(state: State): boolean

  /** Used by find(url#heads); default false for non-Automerge types. */
  hasHeads?(state: State, heads: string[]): boolean

  /** Optional point-in-time support. */
  viewAt?(state: State, heads: string[]): State

  /** Optional event payload generation. Automerge returns patches here. */
  diff?(before: State, after: State): unknown[]

  /** Sedimentree adapter for this document type. */
  sedimentree: SedimentreeAdapter<State>
}

export type DocState<D extends DocType<any, any, any, any>> =
  D extends DocType<infer State, any, any, any> ? State : never

export type DocView<D extends DocType<any, any, any, any>> =
  D extends DocType<any, infer View, any, any> ? View : never

export type DocChange<D extends DocType<any, any, any, any>> =
  D extends DocType<any, any, infer Change, any> ? Change : never

export type DocInit<D extends DocType<any, any, any, any>> =
  D extends DocType<any, any, any, infer Init> ? Init : never
