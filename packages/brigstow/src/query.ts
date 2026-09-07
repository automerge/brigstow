import type { DocumentId } from "./DocumentId.js"

export interface Query<D> {
  id(): DocumentId
  state(): QueryState<D>
  /**
   * Observe future state changes until unsubscribed; the current state is not
   * replayed. To obtain both, subscribe first and then read state().
   */
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

/**
 * Map the handle in a `Query` while continuing to follow that query's state.
 *
 * The important thing to understand about `mapQuery` is that `ready` is not a
 * final state. The source query can find a handle and later go back to
 * `finding`, become `unavailable`, fail, or produce another handle. The mapped
 * query follows all of those transitions; it is not a promise which settles
 * after the first value.
 *
 * The mapping function is called whenever the source publishes `ready`, and
 * only then. Its return value becomes the mapped query's `ready` handle. If the
 * source later publishes a non-`ready` state, the mapped query immediately
 * publishes the corresponding state and stops exposing the mapped handle. The
 * handle is not disposed for you. A later `ready` state calls `f` again, even
 * if the source publishes the same handle again.
 *
 * For example:
 *
 * ```text
 * source: finding -> ready(A)    -> unavailable -> ready(B)
 * mapped: finding -> ready(f(A)) -> unavailable -> ready(f(B))
 * ```
 *
 * The source's `failed` state keeps its original error. If `f` itself throws,
 * the mapped query becomes `failed`; that failure is also temporary if the
 * source later publishes another state. If `f` causes a re-entrant source
 * transition, only the result for the newest transition is published.
 *
 * The source's current state is processed before `mapQuery` returns, so an
 * initially-ready source calls `f` immediately. `state()` is then a
 * side-effect-free snapshot read. `subscribe()` observes only future
 * publications; to obtain both, subscribe first and then call `state()`.
 *
 * Calling `dispose()` stops following the source and clears the mapped query's
 * listeners. It does not dispose either the source query or mapped handles.
 */
export function mapQuery<D, F>(query: Query<D>, f: (before: D) => F): Query<F> {
  return new MappedQuery(query, f)
}

/**
 * Asynchronously map the handle in a `Query` while continuing to follow that
 * query's state.
 *
 * As with {@link mapQuery}, the source's states are not final. The extra
 * question here is what should happen when the source changes while `f` is
 * still running. `mapQueryAsync` uses "latest source state wins" semantics.
 * A source `ready` state makes the mapped query `finding` and starts `f`. If
 * that invocation resolves while it is still the newest one, the mapped query
 * becomes `ready` with its result. A throw or rejection instead makes it
 * `failed`.
 *
 * If the source changes before `f` finishes, the in-progress invocation is no
 * longer allowed to affect the mapped query. Its `AbortSignal` is aborted and:
 *
 * - a non-`ready` source state is immediately reflected by the mapped query;
 * - another `ready` source state keeps the mapped query at `finding` and starts
 *   a fresh invocation of `f`; and
 * - any eventual result or error from the old invocation is ignored.
 *
 * For example:
 *
 * ```text
 * source publishes ready(A)       mapped becomes finding; f(A) starts
 * source publishes unavailable    mapped becomes unavailable; f(A) is signalled
 * f(A) later resolves             nothing happens; that result is stale
 * source publishes ready(B)       mapped becomes finding; f(B) starts
 * f(B) resolves                   mapped becomes ready with f(B)'s result
 * ```
 *
 * Cancellation is cooperative. Aborting the signal tells `f` that its result
 * is no longer wanted, but cannot forcibly stop work which ignores the signal.
 * Stale results are ignored either way. The mapper starts at a promise
 * boundary, so rapid source changes can mean that `f` receives a signal which
 * is already aborted.
 *
 * A previously mapped handle is not retained while a new mapping is running:
 * the mapped query exposes `finding`, not stale data. Nor is that old handle
 * disposed for you. Every source `ready` notification starts a new mapping,
 * even if it contains the same handle as the previous notification.
 *
 * The source's current state is processed before `mapQueryAsync` returns. For
 * an initially-ready source, that means the mapped query returns as `finding`
 * with `f` scheduled to start. `state()` is a side-effect-free snapshot read.
 * `subscribe()` observes only future publications; to obtain both, subscribe
 * first and then call `state()`.
 *
 * Non-`Error` throws and rejections are wrapped in an `Error`. Mapping failures
 * are not final: a later source state is processed normally. Calling
 * `dispose()` aborts any pending mapping, stops following the source, and
 * clears the mapped query's listeners. It does not dispose the source query or
 * mapped handles.
 */
export function mapQueryAsync<D, F>(
  query: Query<D>,
  f: (before: D, signal: AbortSignal) => PromiseLike<F>,
): Query<F> {
  return new AsyncQuery(query, f)
}

/**
 * Map a single snapshot of query state rather than following a live `Query`.
 *
 * `f` is called only when this particular snapshot is `ready`. Non-`ready`
 * states are copied as-is, retaining the original error for `failed`. Because
 * there is no subscription, later source transitions are not relevant here.
 * Unlike {@link mapQuery}, an exception from `f` is allowed to propagate to the
 * caller rather than being turned into a `failed` state.
 */
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
    this.#unsubWrapped = query.subscribe(this.#onchange)
    this.#onchange(query.state())
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
    this.#unsubWrapped = query.subscribe(this.#onchange)
    this.#onchange(query.state())
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
