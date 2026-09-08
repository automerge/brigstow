import { fileURLToPath, URL } from "node:url"
import { defineConfig } from "vite"
import solid from "vite-plugin-solid"
import wasm from "vite-plugin-wasm"

export default defineConfig({
  plugins: [solid(), wasm()],
  optimizeDeps: {
    // Local WASM rebuilds can change exports without changing the package version.
    // Keep both entry points out of Vite's long-lived prebundled dependency cache.
    exclude: ["@automerge/subduction", "@automerge/subduction/slim"],
  },
  resolve: {
    alias: [
      {
        find: /^@brigstow\/automerge-repo$/,
        replacement: fileURLToPath(
          new URL("../../packages/automerge-repo/src/index.ts", import.meta.url),
        ),
      },
      {
        find: /^@brigstow\/brigstow-subduction$/,
        replacement: fileURLToPath(
          new URL("../../packages/brigstow-subduction/src/index.ts", import.meta.url),
        ),
      },
      {
        find: /^@brigstow\/brigstow$/,
        replacement: fileURLToPath(
          new URL("../../packages/brigstow/src/index.ts", import.meta.url),
        ),
      },
    ],
  },
})
