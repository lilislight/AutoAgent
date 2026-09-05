# Publication Approval

Build a publication service that prepares an article, waits for an editor
decision, and then publishes or rejects it.

## Initial request

The initial request contains:

- `article_id`: non-empty string
- `title`: non-empty string
- `body`: non-empty string
- `risk_level`: `low`, `medium`, or `high`

Validate and normalize the article before requesting approval. Low-risk
articles still require editor approval; risk only changes the summary presented
to the editor.

## Editor decision

An editor may respond hours later with:

- `approved`: boolean
- `comment`: optional string
- `requested_title`: optional non-empty string

The service may restart while it is waiting. After restart, the editor must
still be able to continue the matching article request. Several articles may
wait at the same time, and a response for one article must never be consumed by
another article.

Reject an invalid, duplicate, or mismatched response clearly without changing
the state of an unrelated article.

## Result

- Approval publishes the normalized article.
- When an approved response contains `requested_title`, use it as the final
  title.
- Rejection preserves the editor comment.
- Return `article_id`, `status`, `title`, `risk_level`, and `editor_comment`.
