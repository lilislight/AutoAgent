"""Typed human questions and a Workflow-local, permission-controlled Tool."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from ..models import LLMToolResult
from ..models._base import Model, NonEmpty
from ..resources import AgentResource
from ..tools import Tool
from ..tools.execution import error_result


class AskUserOption(Model):
    id: NonEmpty
    label: NonEmpty
    description: NonEmpty | None = None


class AskUserQuestion(Model):
    """One independent question; choice questions permit custom text by default."""

    id: NonEmpty
    type: Literal["text", "single_choice", "multiple_choice"]
    text: NonEmpty
    options: tuple[AskUserOption, ...] = ()
    allow_custom_answer: bool | None = Field(default=None,
        description="Choice questions allow custom text by default. Omit for text questions.")

    @model_validator(mode="after")
    def _choices(self):
        if self.type == "text":
            if self.options or self.allow_custom_answer is not None:
                raise ValueError("Text questions cannot configure choices or custom-answer options.")
        elif not self.options:
            raise ValueError("Choice questions require options.")
        elif self.allow_custom_answer is None:
            object.__setattr__(self, "allow_custom_answer", True)
        if len({option.id for option in self.options}) != len(self.options):
            raise ValueError("Option IDs must be unique within a question.")
        return self


class AskUserArguments(Model):
    questions: tuple[AskUserQuestion, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self):
        if len({question.id for question in self.questions}) != len(self.questions):
            raise ValueError("Question IDs must be unique within an ask_user call.")
        return self


class AskUserRequest(AskUserArguments):
    """Persisted Wait payload; runtime identities are never model-supplied arguments."""

    kind: Literal["agent.ask_user"] = "agent.ask_user"
    request_id: NonEmpty
    call_id: NonEmpty


class AskUserAnswer(Model):
    question_id: NonEmpty
    status: Literal["answered", "skipped"] = "answered"
    selected_option_ids: tuple[NonEmpty, ...] = ()
    text: NonEmpty | None = None

    @model_validator(mode="after")
    def _consistent(self):
        if len(set(self.selected_option_ids)) != len(self.selected_option_ids):
            raise ValueError("Selected option IDs must be unique.")
        if self.status == "skipped" and (self.selected_option_ids or self.text is not None):
            raise ValueError("Skipped questions cannot contain an answer.")
        return self


class AskUserResponse(Model):
    """One atomic reply covering every question, including explicitly skipped ones."""

    answers: tuple[AskUserAnswer, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self):
        if len({answer.question_id for answer in self.answers}) != len(self.answers):
            raise ValueError("Answer question IDs must be unique.")
        return self

    def validate_request(self, request: AskUserRequest):
        questions = {question.id: question for question in request.questions}
        if {answer.question_id for answer in self.answers} != set(questions):
            raise ValueError("Answers must match every question in the pending request exactly once.")
        for answer in self.answers:
            question = questions[answer.question_id]
            if answer.status == "skipped":
                continue
            selected = set(answer.selected_option_ids)
            if not selected <= {option.id for option in question.options}:
                raise ValueError("Answer contains an unknown option ID.")
            if question.type == "text":
                if selected or answer.text is None:
                    raise ValueError("Text questions require a text answer and no selections.")
            else:
                if question.type == "single_choice" and len(selected) > 1:
                    raise ValueError("Single-choice questions accept at most one option.")
                if answer.text is not None and question.allow_custom_answer is False:
                    raise ValueError("This question does not allow a custom answer.")
                if not selected and answer.text is None:
                    raise ValueError("Answered choice questions require a selection or custom text.")

    def tool_result(self, request: AskUserRequest) -> LLMToolResult:
        self.validate_request(request)
        answers = {answer.question_id: answer for answer in self.answers}
        result = []
        for question in request.questions:
            answer = answers[question.id]
            record = answer.model_dump(mode="json")
            record["question"] = question.text
            record["selected_options"] = [option.model_dump(mode="json") for option in question.options
                if option.id in answer.selected_option_ids]
            result.append(record)
        return LLMToolResult(call_id=request.call_id, structured_content={"answers": result})


def _question_arguments(questions: Annotated[list[AskUserQuestion], Field(min_length=1)]) -> AskUserArguments:
    """Ask the user for clarification, choices or missing information.

    Provide independent questions with stable question and option IDs. Users
    may skip any question. Use subsequent calls for dependent questions.
    """
    return AskUserArguments(questions=tuple(questions))


class _AskUserTool(Tool):
    def _prepare(self, call):
        prepared = super()._prepare(call)
        if isinstance(prepared, LLMToolResult):
            return prepared
        try:
            _question_arguments(**prepared[0])
        except ValueError as error:
            return error_result(call, "invalid_arguments", str(error))
        return prepared


ASK_USER_TOOL = _AskUserTool(_question_arguments, name="ask_user")
ASK_USER_RESOURCE = AgentResource("ask_user", "agent.ask_user.1", ASK_USER_TOOL, resource_id="builtin:ask_user")


def with_ask_user(catalog):
    """Overlay only this Workflow's builtin; never mutate the Host Registry."""
    if "ask_user" in catalog:
        raise ValueError("The enabled builtin ask_user collides with a registered Tool.")
    return {**catalog, "ask_user": ASK_USER_RESOURCE}


def question_request(task) -> AskUserRequest:
    arguments = AskUserArguments.model_validate_json(task.call.arguments)
    return AskUserRequest(request_id=task.request_id, call_id=task.call.call_id, questions=arguments.questions)


class QuestionResults(Model):
    results: tuple[LLMToolResult, ...] = ()
