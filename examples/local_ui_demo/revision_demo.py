"""Two graph Revisions used by the Revision Compare and Session examples."""

from typing_extensions import TypedDict
from autoagent import Edge, Node, Workflow
from autoagent.host import workflow


class RevisionDocument(TypedDict):
    text: str


def revision_identity(value: RevisionDocument) -> RevisionDocument:
    return value


def build_revision_review(version="2"):
    stage = "legacy-review" if version == "1" else "review"
    return Workflow(
        "revision-review-demo", version=version,
        nodes=[Node("prepare", revision_identity), Node(stage, revision_identity), Node("finish", revision_identity)],
        edges=[Edge("prepare", stage, id="review-route"), Edge(stage, "finish", id="finish-route")],
    )


@workflow()
def revision_review():
    return build_revision_review()
