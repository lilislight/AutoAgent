# Publication Approval Workflow

Build a deterministic AutoAgent project that prepares an article for
publication, waits for an editor decision, and resumes in a later process.

## Inputs

The initial request accepts:

- `article_id`: non-empty string
- `title`: non-empty string
- `body`: non-empty string
- `risk_level`: `low`, `medium`, or `high`

The editor response accepts:

- `approved`: boolean
- `comment`: optional string
- `requested_title`: optional non-empty string

## Behavior

- Validate and normalize the initial article.
- Low-risk articles still require editor approval; risk only affects the
  summary presented for review.
- Pause at a stable Wait point whose key is derived from `article_id`.
- Resume with the editor response in a separate CLI process using SQLite
  persistence.
- An approval publishes the normalized article. If `requested_title` is
  present, use it as the final title.
- A rejection returns a rejected result and preserves the editor comment.
- An invalid or mismatched response must fail clearly and must not resume a
  different article.
- The final result includes `article_id`, `status`, `title`, `risk_level`, and
  `editor_comment`.

## Project Contract

- Create one `auto-agent.toml` at the project root.
- Expose exactly one Workflow from the manifest.
- Use public AutoAgent authoring APIs only.
- Use deterministic local Operators; no network services or credentials.
- Include initial-request and resume-response JSON fixtures for approval,
  renamed approval, rejection, and invalid response.
- Include automated tests for Wait identity, SQLite persistence, process
  restart, Resume, duplicate or mismatched Resume handling, and compiler
  validation.
- Document the exact install, check, initial run, resume, and test commands
  without assuming a particular package manager.

## Acceptance

The project passes `autoagent project check`; a waiting Invocation can be
resumed by a new CLI process using the same database; all tests pass; and no
Invocation can consume another article's response.
