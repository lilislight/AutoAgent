# Workflow graph and data contracts

Start with the input, expected output, and business decisions. Give the
Workflow and its Nodes stable IDs. Assign explicit Edge IDs wherever a
mapping or a business check refers to an Edge.

## Operators and routing

Use a typed callable directly as the Node's executable. Use `Operator` when
the definition needs an explicit Operator identity or version. Use `Capability`
when the project expects a runtime-provided implementation with a declared
contract.

An ordinary Operator takes zero or one positional input and declares its
output. Annotate a business input when present. Use module-level
`TypedDict` or Pydantic `BaseModel` for structured values, and `None`
for an intentionally empty contract. For Pydantic models, set
`ConfigDict(extra="forbid")` and use schema-compatible fields. Convert
between different contracts with Input Mapping. A single Complete Edge can
pass the upstream output directly when its contract matches the target input.

An Entry Node has no incoming Edges. Start the Workflow at an explicit Entry
when it exposes more than one. An Exit Node has no outgoing Edges. With one
structural Exit, the Invocation returns that Node's output directly. With
multiple structural Exits, it returns a mapping from completed Exit Node IDs
to their outputs. Use a shared final Node when callers need one output shape
across branches.

An Edge condition returns a boolean. Select complementary conditions for
exclusive routing. Multiple eligible Complete Edges can activate parallel
branches.

```python
from typing import Literal, cast

from typing_extensions import TypedDict

from autoagent import ConditionContext, Edge, Node, Workflow


class QuoteRequest(TypedDict):
    amount: int


class Quote(TypedDict):
    amount: int
    requires_review: bool


class QuoteDecision(TypedDict):
    amount: int
    status: Literal["review", "accepted"]


def classify(value: QuoteRequest) -> Quote:
    return {"amount": value["amount"], "requires_review": value["amount"] >= 100}


def needs_review(context: ConditionContext) -> bool:
    return cast(Quote, context.output)["requires_review"]


def can_continue(context: ConditionContext) -> bool:
    return not needs_review(context)


def request_review(value: Quote) -> QuoteDecision:
    return {"amount": value["amount"], "status": "review"}


def accept_quote(value: Quote) -> QuoteDecision:
    return {"amount": value["amount"], "status": "accepted"}


def quote_workflow() -> Workflow:
    return Workflow(
        "quote",
        nodes=[
            Node("classify", classify),
            Node("review", request_review),
            Node("accept", accept_quote),
        ],
        edges=[
            Edge("classify", "review", needs_review, id="needs-review"),
            Edge("classify", "accept", can_continue, id="can-continue"),
        ],
    )
```

For an input amount of `40`, the example returns
`{"accept": {"amount": 40, "status": "accepted"}}`.

Use `Edge(..., on="error")` for a defined failure path. Its Condition can
inspect `context.error`. Give the target an Input Mapping that turns the
incoming error into the target's typed input.

## Hooks and Context

| Hook | Receives | Returns |
| --- | --- | --- |
| Input Mapping | `InputMappingContext` with original Invocation input and incoming values keyed by Edge ID | The target's input, or `list[ItemInput]` for an ordinary Map Node |
| Condition | `ConditionContext` with the source output or error | `bool` |
| Map aggregation | `AggregationContext` with item inputs and outputs | The declared aggregate output |
| Output Binding | `OutputBindingContext` with the Node output after aggregation | `ContextPatch` or `None` |

Treat hook values as read-only views. Construct a fresh value when mapping
into a strict contract. At a join, use named Edge IDs for the inputs. If the
join follows exclusive branches, handle the available keys for either path.

Return `ContextPatch(invocation=(ContextOperation.set("quote", value),))`
from Output Binding for state used later in this Invocation. Use
`ContextPatch(session=(ContextOperation.set("quote", value),))` for data
an explicitly reused Session needs in later Invocations. Use a tuple of
keys for nested paths. Give parallel branches separate write paths or have
one join Node commit the combined state.

Use `@workflow_hook(version=...)` when a hook's semantics depend on behavior
outside its body, such as a changed helper or ruleset. Keep the version
aligned with that behavior.

## Map and loops

Choose explicit parallel Nodes for a fixed set of different tasks. Choose
`Map(max_parallelism=...)` when one Node should apply the same Operator
to a variable collection. The Input Mapping returns the items, and each
Call receives one typed item. Downstream Edges see the Node's combined output.
Without an aggregator, the output is the list of item results. With an
aggregator, its declared result becomes the Node's output contract.

```python
from typing import cast

from typing_extensions import TypedDict

from autoagent import (
    AggregationContext, Edge, InputMappingContext, Map, Node, Workflow,
)


class Line(TypedDict):
    unit_price: int
    quantity: int


class Order(TypedDict):
    lines: list[Line]


class Total(TypedDict):
    amount: int


def receive_order(order: Order) -> Order:
    return order


def line_items(context: InputMappingContext) -> list[Line]:
    order = cast(Order, context.incoming["received"])
    return [
        {"unit_price": line["unit_price"], "quantity": line["quantity"]}
        for line in order["lines"]
    ]


def price_line(line: Line) -> Total:
    return {"amount": line["unit_price"] * line["quantity"]}


def sum_lines(context: AggregationContext) -> Total:
    return {"amount": sum(cast(Total, item)["amount"] for item in context.outputs)}


def total_workflow() -> Workflow:
    return Workflow(
        "order-total",
        nodes=[
            Node("receive", receive_order),
            Node(
                "price-lines",
                price_line,
                input_mapping=line_items,
                map=Map(aggregate=sum_lines, max_parallelism=4),
            ),
        ],
        edges=[Edge("receive", "price-lines", id="received")],
    )
```

For business repetition, use a back Edge into the loop header and an exit
Edge controlled by explicit business conditions. Carry the iteration value
in the Operator output or Invocation Context. Give the loop a business
bound and use `ResourcePolicy(max_executions_per_invocation=...)` as an
execution guard. See [interaction-recovery.md](interaction-recovery.md) for
durable suspension, policies, and parent–child coordination.

For incremental results and observations, see
[streaming Operators and User Events](streaming-user-events.md).
