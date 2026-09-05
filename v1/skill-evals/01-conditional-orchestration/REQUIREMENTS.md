# Purchase Exception Review

Build a purchase-review service that returns a final decision for each request.

## Request

Each request contains:

- `request_id`: non-empty string
- `amount`: positive number
- `supplier_tier`: `trusted`, `standard`, or `new`
- `risk_flags`: list of strings
- `available_budget`: non-negative number

Reject invalid requests with a clear reason. Do not silently coerce invalid
values or approve an incomplete request.

## Review rules

- A trusted supplier with an amount no greater than 1,000 and no risk flags
  receives fast approval.
- Every other request needs a budget review and a compliance review.
- Budget and compliance reviews are independent and should run concurrently so
  their processing time does not add together.
- Budget review passes only when `amount` does not exceed `available_budget`.
- Compliance review passes only when `risk_flags` is empty.
- A new supplier requires manual review even when the budget and compliance
  checks pass.
- For this prototype, manual review is represented by one deterministic
  reassessment after adding the fact `manual_review_completed`.
- A request may be reassessed at most once.

## Result

Return:

- `request_id`
- `decision`
- `reasons`
- `review_rounds`
- the individual budget and compliance outcomes

The result must be consistent for fast approval, normal approval, budget
rejection, compliance rejection, and new-supplier reassessment. Every request
must be isolated; facts or review results from one request must never affect
another request.
