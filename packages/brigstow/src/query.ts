import type { DocumentId } from "./DocumentId.js"

export interface Query<D> {
  id(): DocumentId
  state(): QueryState<D>
  subscribe(callback: (state: QueryState<D>) => void): () => void
}

export type QueryState<D> =
  | { type: "finding" }
  | { type: "unavailable" }
  | { type: "failed"; error: Error }
  | { type: "ready"; handle: D }

export function mapQuery<D, F>(query: Query<D>, f: (before: D) => F): Query<F> {
    let state: QueryState<F> = { type: "finding" }
    let promise: Promise<F> | undefined
    return {
        id: () => query.id(),
        state: () => state,
        subscribe: (callback: (state: QueryState<F>) => void) => {
            return (() => { })
        }
    }
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

export class AsyncQuery<D, F> implements Query<F> {
  private wrappedState: QueryState<D>
  #promise: Promise<F> | undefined
  private value: F | undefined
  private error: Error | undefined
  #listeners: ((state: QueryState<F>) => void)[] = []
  #unsubWrapped: (() => void)

  constructor(private query: Query<D>, private f: (before: D) => Promise<F>) {
    this.wrappedState = query.state()
    this.#unsubWrapped = query.subscribe(this.#onchange)
  }

  id(): DocumentId {
    return this.query.id()
  }

  state(): QueryState<F> {
    if (this.value) {
      return { type: "ready", handle: this.value }
    }
    if (this.error) {
      return { type: "failed", error: this.error }
    }
    if (this.#promise) {
      return { type: "finding" }
    }
    const wrapped = this.query.state()
    if (wrapped.type === "ready") {
      this.#promise = this.f(wrapped.handle)
      this.#promise.then(value => {
        this.value = value
        this.#onchange()
      }).catch(error => {
          this.error = error
          this.#onchange()
      })
      return { type: "finding" }
    }
    return wrapped
  }

  subscribe(callback: ((state: QueryState<F>) => void)): () => void {
    this.#listeners.push(callback)
    return () => this.unsubscribe(callback)
  }

  unsubscribe(subs: (state: QueryState<F>) => void) {
    this.#listeners = this.#listeners.filter(l => l !== subs)
  }

  #onchange() {
    const state = this.state()
    for (const listener of this.#listeners) {
      listener(state)
    }
  }
}
