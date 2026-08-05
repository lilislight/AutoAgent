# Incorrect Fulfillment Decision

The completed Invocation below returned automatic approval for order
`order-1007`, but any order above 1,000 must be sent to manual review.

```text
<INVOCATION_ID>
```

Investigate the recorded execution, identify the cause, and make the smallest
project-code repair. Preserve the behavior of ordinary low-value orders and the
other manual-review rules.

Verify the repair by rerunning the original Invocation boundary, comparing the
original and candidate executions, and running the registered business
evaluation. Return the evidence supporting the cause and repair, the two
Invocation IDs, the commands and results, and any remaining uncertainty.
