# Purchase Exception Workflow

Build a deterministic AutoAgent project that evaluates a purchase request and
returns a final approval decision.

## Inputs

The Workflow accepts:

- `request_id`: non-empty string
- `amount`: positive number
- `supplier_tier`: `trusted`, `standard`, or `new`
- `risk_flags`: list of strings
- `available_budget`: non-negative number

Invalid input must produce a clear failed Invocation rather than silently
approving or coercing the request.

## Behavior

- Trusted suppliers with an amount no greater than 1,000 and no risk flags may
  use a fast approval path.
- Every other request needs both a budget review and a compliance review. These
  reviews are independent and should be able to run concurrently.
- Budget review passes only when the amount does not exceed the available
  budget.
- Compliance review passes only when there are no risk flags. A new supplier
  must also be reported as needing manual review.
- When the request needs manual review, perform one deterministic reassessment
  after adding a `manual_review_completed` fact. Do not allow an unbounded
  loop.
- The final result must include `request_id`, `decision`, `reasons`,
  `review_rounds`, and the individual review outcomes.
- The Workflow may have multiple internal branches, but its public result must
  be consistent for fast approval, normal approval, rejection, and manual
  review.

## Project Contract

- Create one `auto-agent.toml` at the project root.
- Expose exactly one Workflow from the manifest.
- Use public AutoAgent authoring APIs only.
- Use deterministic local Operators; no network services or credentials.
- Include JSON input fixtures and expected outputs for at least:
  fast approval, concurrent approval, budget rejection, risk rejection, and
  manual reassessment.
- Include automated tests for the branching, concurrency, bounded loop, output
  shape, and compiler validation.
- Document the exact install, check, run, and test commands without assuming a
  particular package manager.

## Acceptance

The project passes `autoagent project check`, all automated tests pass, every
fixture has a deterministic result, and a repeated run does not leak Runtime
state between Invocations.
