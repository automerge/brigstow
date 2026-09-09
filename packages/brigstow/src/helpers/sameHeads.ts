/** Compare logical heads without depending on their order or mutating inputs. */
export function sameHeads(left: string[], right: string[]): boolean {
  const sorted = [...right].sort()
  return left.length === right.length && [...left].sort().every((head, i) => head === sorted[i])
}
