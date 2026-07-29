# Brigstow todo example

A SolidJS and TypeScript todo app built with Vite. It wires together
`@brigstow/automerge-repo`, `@brigstow/brigstow-subduction`, and an in-memory
Subduction node. Solid updates are driven by the document handle's `change`
event rather than by manually re-rendering after mutations.

```sh
pnpm --filter @brigstow/example-todo-app dev
```

The example intentionally uses the packages as they currently exist. There is
no fallback implementation: until the remaining repository APIs are
implemented, initialization displays their error and the todo controls remain
disabled.
