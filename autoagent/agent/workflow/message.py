"""Text and images that can enter an Agent conversation."""

from pydantic import model_validator

from ..models import ImageContent, LLMMessage, TextContent
from ..models._base import Model


class AgentUserMessage(Model):
    message: str
    images: tuple[ImageContent, ...] = ()

    @model_validator(mode="after")
    def _require_content(self):
        if not self.message.strip() and not self.images:
            raise ValueError("Provide a message or an image.")
        return self

    def user_message(self, prefix=""):
        text = prefix + self.message
        content = (TextContent(text=text), *self.images) if text else self.images
        return LLMMessage(role="user", content=content)

    def display_content(self):
        text = ({"type": "text", "text": self.message},) if self.message else ()
        return (*text, *(image.model_dump(mode="json", exclude_none=True) for image in self.images))
