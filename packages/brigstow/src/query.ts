import type { DocumentId } from "./DocumentId.js"

export interface Query<D> {
  id(): DocumentId
  state(): QueryState<D>
  subscribe(callback: (state: QueryState<D>) => void): () => void

  /** Release resources owned by this query. The query must not be used afterwards. */
  dispose?(): void
}

export type QueryState<D> =
  | { type: "finding" }
  | { type: "unavailable" }
  | { type: "failed"; error: Error }
  | { type: "ready"; handle: D }

type QueryListener<D> = (state: QueryState<D>) => void

export function mapQuery<D, F>(query: Query<D>, f: (before: D) => F): Query<F> {
  return new MappedQuery(query, f)
}

export function mapQueryAsync<D, F>(
  query: Query<D>,
  f: (before: D, signal: AbortSignal) => PromiseLike<F>,
): Query<F> {
  return new AsyncQuery(query, f)
}

export function mapQueryState<D, F>(state: QueryState<D>, f: (before: D) => F): QueryState<F> {
  switch (state.type) {
    case "finding":
      return { type: "finding" }
    case "unavailable":
      return { type: "unavailable" }
    case "failed":
      return { type: "failed", error: state.error }
    case "ready":
      return { type: "ready", handle: f(state.handle) }
  }
}

class MappedQuery<D, F> implements Query<F> {
  #state: QueryState<F> = { type: "finding" }
  #listeners = new Set<QueryListener<F>>()
  #unsubWrapped: (() => void) | undefined
  #generation = 0
  #disposed = false

  constructor(private query: Query<D>, private f: (before: D) => F) {
    // Subscribe before reading state so a transition cannot be missed between
    // the initial read and installation of the subscription. Some Query
    // implementations may synchronously replay their state from subscribe().
    let replayedState = false
    this.#unsubWrapped = query.subscribe(state => {
      replayedState = true
      this.#onchange(state)
    })
    if (!replayedState) {
      this.#onchange(query.state())
    }
  }

  id(): DocumentId {
    return this.query.id()
  }

  state(): QueryState<F> {
    return this.#state
  }

  subscribe(callback: QueryListener<F>): () => void {
    if (this.#disposed) {
      throw new Error("Cannot subscribe to a disposed query")
    }
    this.#listeners.add(callback)
    return () => {
      this.#listeners.delete(callback)
    }
  }

  dispose(): void {
    if (this.#disposed) return

    this.#disposed = true
    this.#generation++
    this.#unsubWrapped?.()
    this.#unsubWrapped = undefined
    this.#listeners.clear()
  }

  #onchange = (wrappedState: QueryState<D>): void => {
    if (this.#disposed) return

    // The mapper is synchronous, but guard against it causing a re-entrant
    // source transition before it returns.
    const generation = ++this.#generation
    let state: QueryState<F>
    try {
      state = mapQueryState(wrappedState, this.f)
    } catch (reason) {
      state = { type: "failed", error: normalizeError(reason) }
    }

    if (this.#disposed || generation !== this.#generation) return
    this.#setState(state)
  }

  #setState(state: QueryState<F>): void {
    this.#state = state
    for (const listener of [...this.#listeners]) {
      listener(state)
    }
  }
}

/**
 * A switch-to-latest asynchronous projection of another Query.
 *
 * Every source transition invalidates the previous projection. Obsolete work
 * is aborted where possible and its result is always ignored.
 */
export class AsyncQuery<D, F> implements Query<F> {
  #state: QueryState<F> = { type: "finding" }
  #listeners = new Set<QueryListener<F>>()
  #unsubWrapped: (() => void) | undefined
  #controller: AbortController | undefined
  #generation = 0
  #disposed = false

  constructor(
    private query: Query<D>,
    private f: (before: D, signal: AbortSignal) => PromiseLike<F>,
  ) {
    // See MappedQuery's constructor for why subscription precedes the read.
    let replayedState = false
    this.#unsubWrapped = query.subscribe(state => {
      replayedState = true
      this.#onchange(state)
    })
    if (!replayedState) {
      this.#onchange(query.state())
    }
  }

  id(): DocumentId {
    return this.query.id()
  }

  state(): QueryState<F> {
    return this.#state
  }

  subscribe(callback: QueryListener<F>): () => void {
    if (this.#disposed) {
      throw new Error("Cannot subscribe to a disposed query")
    }
    this.#listeners.add(callback)
    return () => {
      this.#listeners.delete(callback)
    }
  }

  dispose(): void {
    if (this.#disposed) return

    this.#disposed = true
    this.#generation++
    this.#controller?.abort()
    this.#controller = undefined
    this.#unsubWrapped?.()
    this.#unsubWrapped = undefined
    this.#listeners.clear()
  }

  #onchange = (wrappedState: QueryState<D>): void => {
    if (this.#disposed) return

    const generation = ++this.#generation
    const previousController = this.#controller
    this.#controller = undefined
    previousController?.abort()

    // abort() can synchronously invoke user code, so a newer source state may
    // already have superseded this one.
    if (this.#disposed || generation !== this.#generation) return

    if (wrappedState.type !== "ready") {
      this.#setState(projectNonReadyState(wrappedState))
      return
    }

    const controller = new AbortController()
    this.#controller = controller
    this.#setState({ type: "finding" })

    // A listener can synchronously cause another source transition.
    if (this.#disposed || generation !== this.#generation) return

    // Starting through a promise boundary also turns a synchronous mapper
    // throw into a failed QueryState.
    void Promise.resolve()
      .then(() => this.f(wrappedState.handle, controller.signal))
      .then(
        value => {
          if (this.#disposed || generation !== this.#generation) return
          this.#controller = undefined
          this.#setState({ type: "ready", handle: value })
        },
        reason => {
          if (this.#disposed || generation !== this.#generation) return
          this.#controller = undefined
          this.#setState({ type: "failed", error: normalizeError(reason) })
        },
      )
  }

  #setState(state: QueryState<F>): void {
    this.#state = state
    for (const listener of [...this.#listeners]) {
      listener(state)
    }
  }
}

function projectNonReadyState<F>(state: Exclude<QueryState<unknown>, { type: "ready" }>): QueryState<F> {
  switch (state.type) {
    case "finding":
      return { type: "finding" }
    case "unavailable":
      return { type: "unavailable" }
    case "failed":
      return { type: "failed", error: state.error }
  }
}

function normalizeError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error(String(reason))
}
