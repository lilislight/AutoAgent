"""Minimal two-Node Workflow: normalize a name, then greet it."""

from typing import NotRequired, TypedDict

from autoagent import Edge, Node, Operator, Workflow
from autoagent.host import workflow


class GreetingInput(TypedDict):
    name: NotRequired[str]


class NormalizedName(TypedDict):
    name: str


class Greeting(TypedDict):
    greeting: str


def normalize(value: GreetingInput) -> NormalizedName:
    """Trim the name; a blank or missing name becomes World."""
    name = (value.get("name") or "").strip()
    return {"name": name or "World"}


def greet(value: NormalizedName) -> Greeting:
    return {"greeting": f"Hello, {value['name']}!"}


@workflow
def build_workflow():
    return Workflow(
        "demo_greeting",
        nodes=[
            Node("normalize", Operator(normalize)),
            Node("greet", Operator(greet)),
        ],
        edges=[Edge("normalize", "greet")],
    )
