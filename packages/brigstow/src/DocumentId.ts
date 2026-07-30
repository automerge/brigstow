import bs58check from 'bs58check'

/**
 * The base58check-encoded byte array of a document ID. This is the string following the `automerge:`
 * protocol prefix in an AutomergeUrl; for example, `4NMNnkMhL8jXrdJ9jamS58PAVdXu`. When recording
 * links to an Automerge document in another Automerge document, you should store a
 * {@link AutomergeUrl} instead.
 */
export type StringDocumentId = string & { __documentId: true } // for logging

/** The unencoded UUID of a document. Typically you should use a {@link AutomergeUrl} instead. */
export type DocumentId = Uint8Array & { __binaryDocumentId: true } // for storing / syncing

/**
 * A UUID encoded as a hex string. As of v1.0, a {@link DocumentId} is stored as a base58-encoded string with a checksum.
 * Support for this format will be removed in a future version.
 */
export type LegacyDocumentId = string & { __legacyDocumentId: true }

export type AnyDocumentId =
  | DocumentId
  | StringDocumentId
  | LegacyDocumentId

export function stringifyDocId(documentId: DocumentId): StringDocumentId {
  return bs58check.encode(documentId) as StringDocumentId
}
