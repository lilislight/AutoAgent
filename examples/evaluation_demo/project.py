"""Small offline Workflows for business regression Evaluation."""

from typing_extensions import TypedDict

from autoagent import Node, Wait, Workflow


class Message(TypedDict):
    text: str


def normalize(value: Message) -> Message:
    return {'text': value['text'].strip().lower()}


def normalize_workflow():
    return Workflow('normalize', nodes=[Node('normalize', normalize)])


def approval_workflow():
    return Workflow('approval', nodes=[Node('approve', Wait(Message, Message))])
