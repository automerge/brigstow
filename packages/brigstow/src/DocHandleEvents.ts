import type { DocHandle } from "./DocHandle.js"
import type { DocType, DocView } from "./DocType.js"

export interface DocHandleEvents<D extends DocType<any, any, any, any>> {
  change: (payload: DocHandleChangePayload<D>) => void
}

/** Emitted when this document has changed */
export interface DocHandleChangePayload<D extends DocType<any, any, any, any>> {
  /** The handle that changed */
  handle: DocHandle<D>
  /**
   * The value after the change, scoped to this handle. For a root handle
   * this is the whole document; for a sub-handle it is the value at the
   * handle's path (i.e. equal to `handle.doc()`). `undefined` when the
   * change removed the handle's scope.
   */
  doc: DocView<D> | undefined
}
