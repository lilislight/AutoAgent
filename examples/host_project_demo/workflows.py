"""Decorator discovery needs no manifest or project entry module."""

from typing_extensions import TypedDict

from autoagent import Edge, Node, Wait, Workflow
from autoagent.host import workflow


class Document(TypedDict):
    text: str


def finish(value: Document) -> Document:
    return {"text": "approved: " + value["text"]}


@workflow
def approval():
    return Workflow(
        "approval",
        nodes=[Node("approve", Wait(Document, Document)), Node("finish", finish)],
        edges=[Edge("approve", "finish")],
    )
