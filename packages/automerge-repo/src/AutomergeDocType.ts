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
                // Level 0 represents loose commits, exported separately below.
                const fragments = Automerge.getFragmentMetadata(state, { start: 1 }).map<SedimentreeMeta>(f => ({
                    kind: "fragment",
                    boundary: f.boundary,
                    head: f.head,
                    checkpoints: f.checkpoints
                }))
                const commits = Automerge.getCommits(state).map<SedimentreeMeta>(c => ({
                    kind: "commit",
                    head: c.head,
                    parents: c.parents 
                }))
                return [...fragments, ...commits]
            },
            materialize: function(state: Automerge.next.Doc<T>, metas: SedimentreeMeta[]): Promise<Uint8Array[]> | Uint8Array[] {
                const fragsByHead = new Map(Automerge.getFragments(state).map(f => [f.head, f]))
                const commitsByHead = new Map(Automerge.getCommits(state).map(c => [c.head, c]))
                const result: Uint8Array[] = []
                for (const meta of metas) {
                    if (meta.kind === "fragment") {
                        const frag = fragsByHead.get(meta.head)
                        if (!frag) {
                            throw new Error("unknown fragment")
                        }
                        result.push(frag.bytes)
                    } else if (meta.kind === "commit") {
                        const commit = commitsByHead.get(meta.head)
                        if (!commit) {
                            throw new Error("unknown commit")
                        }
                        result.push(commit.bytes)
                    }
                }
                return Promise.resolve(result)
            },
            apply: function(state: Automerge.next.Doc<T>, records: SedimentreeRecord[]): Automerge.next.Doc<T> {
                const concatenated = concatBytes(records.map(r => r.bytes))
                return Automerge.loadIncremental(state, concatenated)
            }
        }
    }
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
    const total = chunks.reduce((n, c) => n + c.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const c of chunks) {
        out.set(c, offset);
        offset += c.length;
    }
    return out;
}
