import { DocHandle } from "./DocHandle.js";
import type { DocInit, DocType } from "./DocType.js";
import { RefreshScheduler } from "./RefreshScheduler.js";
import { stringifyDocId, type DocumentId } from "./DocumentId.js";
import { type Query, type QueryState, mapQueryAsync } from "./query.js";
import type { SedimentreeRecord, SedimentreeSource } from "./SedimentreeSource.js";

export { type DocumentId, type StringDocumentId, stringifyDocId } from "./DocumentId.js"
export type { SedimentreeMeta, SedimentreeSource, SedimentreeHandle, SedimentreeCreateRequest, SedimentreeRecord } from "./SedimentreeSource.js"
export { type DocType } from "./DocType.js"
export { mapQuery, mapQueryAsync, type Query, type QueryState } from "./query.js"

export interface FindOptions {
  signal?: AbortSignal
}

export class DocumentUnavailableError extends Error {
  constructor(readonly documentId: DocumentId) {
    super(`Document ${stringifyDocId(documentId)} is unavailable`)
    this.name = "DocumentUnavailableError"
  }
}

export class Repo {
  constructor(private source: SedimentreeSource) { }
  #disposed = false
  #refreshScheduler = new RefreshScheduler({
    onError: error => console.error("Document refresh failed", error),
  })

  query<D extends DocType<any, any, any, any>>(docType: D, docId: DocumentId): Query<DocHandle<D>> {
    this.#checkOpen()
    const sourceQuery = this.source.find(docId)
    const loading = new Set<DocHandle<D>>()
    const query = mapQueryAsync(sourceQuery, async sedimentreeHandle => {
      const handle = new DocHandle(sedimentreeHandle, docType, docType.empty())
      loading.add(handle)
      try {
        await this.#refreshScheduler.schedule(handle, sedimentreeHandle)
        return handle
      } finally { loading.delete(handle) }
    })
    return new OwnedQuery(query, sourceQuery, () => {
      // Disposing a find/query cancels unfinished loading, not the subscriptions
      // of ready handles which have already been handed to the application.
      for (const handle of loading) this.#refreshScheduler.unschedule(handle)
      loading.clear()
    })
  }

  /** Stop document refreshes. The externally supplied source retains its own lifecycle. */
  dispose(): void {
    this.#disposed = true
    this.#refreshScheduler.dispose()
  }

  #checkOpen(): void {
    if (this.#disposed) throw new Error("Repo is disposed")
  }

  async find<D extends DocType<any, any, any, any>>(
    docType: D,
    docId: DocumentId,
    options: FindOptions = {},
  ): Promise<DocHandle<D>> {
    if (options.signal?.aborted) throw abortReason(options.signal)

    const query = this.query(docType, docId)
    try {
      return await findQuery(query, options.signal)
    } finally {
      query.dispose?.()
    }
  }

  async create<D extends DocType<any, any, any, any>>(docType: D, value: DocInit<D>): Promise<DocHandle<D>> {
    this.#checkOpen()
    const document = docType.init(value)
    const metas = Array.from(docType.sedimentree.metadata(document))
    const data = await docType.sedimentree.materialize(document, metas)
    const initialRecords: SedimentreeRecord[] = metas.map((meta, i) => ({ ...meta, bytes: data[i]! }))
    const sedimentreeHandle = await this.source.create({
      documentType: docType.name,
      initialRecords
    })
    const handle = new DocHandle(sedimentreeHandle, docType, document)
    await this.#refreshScheduler.schedule(handle, sedimentreeHandle)
    return handle
  }
}

/**
 * Disposes both the mapped query and the private source query created by Repo.
 * Mapping helpers alone do not dispose their sources, which may be shared.
 */
class OwnedQuery<D> implements Query<D> {
  #disposed = false

  constructor(
    private query: Query<D>,
    private ownedQuery: Query<unknown>,
    private onDispose: () => void,
  ) { }

  id(): DocumentId {
    return this.query.id()
  }

  state(): QueryState<D> {
    return this.query.state()
  }

  subscribe(callback: (state: QueryState<D>) => void): () => void {
    return this.query.subscribe(callback)
  }

  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true

    try {
      this.query.dispose?.()
    } finally {
      try { this.onDispose() }
      finally { this.ownedQuery.dispose?.() }
    }
  }
}

async function findQuery<D>(query: Query<D>, signal?: AbortSignal): Promise<D> {
  let unsubscribe: (() => void) | undefined
  let onAbort: (() => void) | undefined

  try {
    return await new Promise<D>((resolve, reject) => {
      if (signal) {
        onAbort = () => reject(abortReason(signal))
        if (signal.aborted) {
          onAbort()
          return
        }
        signal.addEventListener("abort", onAbort, { once: true })
      }

      const onState = (state: QueryState<D>): void => {
        switch (state.type) {
          case "finding":
            return
          case "unavailable":
            reject(new DocumentUnavailableError(query.id()))
            return
          case "failed":
            reject(state.error)
            return
          case "ready":
            resolve(state.handle)
        }
      }
      unsubscribe = query.subscribe(onState)
      onState(query.state())
    })
  } finally {
    try {
      unsubscribe?.()
    } finally {
      if (onAbort) signal?.removeEventListener("abort", onAbort)
    }
  }
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? makeAbortError()
}

function makeAbortError(): Error {
  const error = new Error("The operation was aborted")
  error.name = "AbortError"
  return error
}
