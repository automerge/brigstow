import { type DocType } from "@brigstow/brigstow"
import  * as Automerge from "@automerge/automerge"
import type { SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow/src/SedimentreeSource.js"

export type ChangeFn<T> = (doc: Automerge.Doc<T>) => void

export type AutomergeDocType<T> = DocType<Automerge.Doc<T>, Automerge.Doc<T>, ChangeFn<T>, T>

export function amDocType<T extends Record<string, unknown>>(): AutomergeDocType<T> {
  return {
    name: "automerge",
    empty: function (): Automerge.next.Doc<T> {
      throw new Error("Function not implemented.")
    },
    init: function (init: T): Automerge.next.Doc<T> {
      return Automerge.from(init)
    },
    view: function (state: Automerge.next.Doc<T>): Automerge.next.Doc<T> {
      return state
    },
    change: function (state: Automerge.next.Doc<T>, change: ChangeFn<T>): Automerge.next.Doc<T> {
      return Automerge.change(state, change)
    },
    heads: function (state: Automerge.next.Doc<T>): string[] {
      return Automerge.getHeads(state)
    },
    viewAt: function (state: Automerge.next.Doc<T>, heads: string[]): Automerge.next.Doc<T> {
      throw new Error("Function not implemented.")
    },
    sedimentree: {
      metadata: function (state: Automerge.next.Doc<T>, opts?: { notAncestorsOf?: string[] }): Iterable<SedimentreeMeta> {
        throw new Error("Function not implemented.")
      },
      materialize: function (state: Automerge.next.Doc<T>, metas: SedimentreeMeta[]): Promise<Uint8Array[]> | Uint8Array[] {
        throw new Error("Function not implemented.")
      },
      apply: function (state: Automerge.next.Doc<T>, records: SedimentreeRecord[]): Automerge.next.Doc<T> {
        throw new Error("Function not implemented.")
      }
    }
  }
}
