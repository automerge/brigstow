import type { Query, SedimentreeSource } from "@brigstow/brigstow"
import { Repo as BrigstowRepo } from "@brigstow/brigstow"
import { amDocType, type AutomergeDocType } from "./AutomergeDocType.js"
import type { DocHandle as BrigstowDocHandle } from "@brigstow/brigstow/src/DocHandle.js"
import { parseAutomergeUrl, type AutomergeUrl } from "./AutomergeUrl.js"
export { type AutomergeUrl } from "./AutomergeUrl.js"
export type DocHandle<T> = BrigstowDocHandle<AutomergeDocType<T>>

export class Repo {
  #repo: BrigstowRepo

  constructor(private source: SedimentreeSource) {
    this.#repo = new BrigstowRepo(source)
  }

  find<T extends Record<string, unknown>>(url: AutomergeUrl): Query<AutomergeDocType<T>> {
    const { binaryDocumentId: documentId } = parseAutomergeUrl(url)
    return this.#repo.find(amDocType(), documentId)
  }

  create<T extends Record<string, unknown>>(val: T): Promise<DocHandle<T>> {
    return this.#repo.create(amDocType(), val)
  }
}
