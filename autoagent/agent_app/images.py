"""Validated image uploads and references owned by the Agent application."""

import base64
import binascii
import hashlib
from dataclasses import dataclass, field
from typing import Literal

from pydantic import Field

from autoagent.agent.models import AssetSource, DataSource, ImageContent
from autoagent.agent.models._base import Model


MAX_IMAGES = 4
MAX_IMAGE_BYTES = 8 * 1024 * 1024
MAX_MESSAGE_IMAGE_BYTES = 16 * 1024 * 1024
IMAGE_URI_PREFIX = "asset://agent-app-images/"


class ImageUpload(Model):
    mime_type: Literal["image/png", "image/jpeg", "image/gif", "image/webp"]
    data: str = Field(min_length=1, max_length=4 * ((MAX_IMAGE_BYTES + 2) // 3), repr=False)


@dataclass(frozen=True, slots=True)
class UploadedImage:
    id: str
    mime_type: str
    data: bytes = field(repr=False)

    def content(self):
        return ImageContent(source=AssetSource(uri=IMAGE_URI_PREFIX + self.id, mime_type=self.mime_type))

    def inline(self):
        return DataSource(data=base64.b64encode(self.data).decode("ascii"), mime_type=self.mime_type)


def prepare_uploads(session_id, images):
    if len(images) > MAX_IMAGES:
        raise ValueError(f"Attach up to {MAX_IMAGES} images per message.")
    result, total = [], 0
    for image in images:
        try:
            data = base64.b64decode(image.data, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ValueError("Image data must be valid base64.") from error
        if len(data) > MAX_IMAGE_BYTES:
            raise ValueError("Each image must be 8 MiB or smaller.")
        total += len(data)
        if total > MAX_MESSAGE_IMAGE_BYTES:
            raise ValueError("Images must total 16 MiB or smaller per message.")
        matches = {
            "image/png": data.startswith(b"\x89PNG\r\n\x1a\n"),
            "image/jpeg": data.startswith(b"\xff\xd8\xff"),
            "image/gif": data.startswith((b"GIF87a", b"GIF89a")),
            "image/webp": data.startswith(b"RIFF") and data[8:12] == b"WEBP",
        }
        if not matches[image.mime_type]:
            raise ValueError("Image contents do not match the declared format.")
        digest = hashlib.sha256(session_id.encode("utf-8") + b"\0" + image.mime_type.encode("ascii") + b"\0")
        digest.update(data)
        result.append(UploadedImage(digest.hexdigest(), image.mime_type, data))
    return tuple(result)
