"""Expected Skill format, lookup and filesystem failures."""


class SkillError(ValueError):
    """An expected Skill failure, usable as a Tool error observation."""


class SkillFormatError(SkillError):
    """SKILL.md does not satisfy the supported Agent Skills format."""

    def __init__(self, message: str, *, field: str | None = None, line: int | None = None):
        super().__init__(message)
        self.field = field
        self.line = line


class SkillNotFoundError(SkillError):
    """The requested Skill is not in the selected collection."""


class SkillResourceError(SkillError):
    """A resource cannot be accessed within its Skill directory."""


class SkillLimitError(SkillResourceError):
    """A document, resource or directory listing exceeds configured limits."""


class SkillChangedError(SkillError):
    """Skill metadata or its root changed after discovery; rediscover explicitly."""


class SkillAccessError(SkillError):
    """A Skill is unavailable for model-directed calls."""


class SkillScriptError(SkillError):
    """Script configuration, launch or timeout failure."""


__all__ = [
    "SkillError", "SkillFormatError", "SkillNotFoundError", "SkillResourceError",
    "SkillLimitError", "SkillChangedError",
    "SkillAccessError", "SkillScriptError",
]
