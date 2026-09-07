import { type DocType } from "@brigstow/brigstow"
import * as Automerge from "@automerge/automerge"
import type { SedimentreeMeta, SedimentreeRecord } from "@brigstow/brigstow/src/SedimentreeSource.js"

export type ChangeFn<T> = (doc: Automerge.Doc<T>) => void

export type AutomergeDocType<T> = DocType<Automerge.Doc<T>, Automerge.Doc<T>, ChangeFn<T>, T>

export function amDocType<T extends Record<string, unknown>>(): AutomergeDocType<T> {
    return {
        name: "automerge",
        empty: function(): Automerge.next.Doc<T> {
            return Automerge.init()
        },
        init: function(init: T): Automerge.next.Doc<T> {
            return Automerge.from(init)
        },
        view: function(state: Automerge.next.Doc<T>): Automerge.next.Doc<T> {
            return state
        },
        change: function(state: Automerge.next.Doc<T>, change: ChangeFn<T>): Automerge.next.Doc<T> {
            return Automerge.change(state, change)
        },
        heads: function(state: Automerge.next.Doc<T>): string[] {
            return Automerge.getHeads(state)
        },
        viewAt: function(state: Automerge.next.Doc<T>, heads: string[]): Automerge.next.Doc<T> {
            throw new Error("Function not implemented.")
        },
        sedimentree: {
            metadata: function(state: Automerge.next.Doc<T>, opts?: { notAncestorsOf?: string[] }): Iterable<SedimentreeMeta> {
                const fragments = Automerge.getFragmentMetadata(state).map(f => ({
                    kind: "fragment",
                    boundary: f.boundary,
                    head: f.head,
                    checkpoints: f.checkpoints
                }))
                const commits = Automerge.getCommits(state).map(c => ({
                    kind: "commit",
                    head: c.head,
                    parents: c.parents 
                }))
                //export type SedimentreeMeta =
                //| { kind: "commit"; head: string; parents: string[] }
                //| {
                //kind: "fragment"
                //head: string
                //boundary: string[]
                //[>* 12-byte commit-ID prefixes, not full commit IDs. <]
                //checkpoints: Uint8Array[]
                //}

                return [...fragments, ...commits]
            },
            materialize: function(state: Automerge.next.Doc<T>, metas: SedimentreeMeta[]): Promise<Uint8Array[]> | Uint8Array[] {
                throw new Error("Function not implemented.")
            },
            apply: function(state: Automerge.next.Doc<T>, records: SedimentreeRecord[]): Automerge.next.Doc<T> {
                throw new Error("Function not implemented.")
            }
        }
    }
}
