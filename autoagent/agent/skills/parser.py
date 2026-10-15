"""Bounded YAML frontmatter parsing with no constructors, aliases or duplicate keys."""

from __future__ import annotations

from pydantic import ValidationError

from .errors import SkillFormatError
from .models import SkillMetadata


def parse_skill(data: bytes, *, directory_name: str) -> tuple[SkillMetadata, str]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeError as error:
        raise SkillFormatError("SKILL.md must be UTF-8 text.") from error
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].strip() != "---":
        raise SkillFormatError("SKILL.md must start with YAML frontmatter delimited by ---.")
    end = next((index for index in range(1, len(lines)) if lines[index].strip() == "---"), None)
    if end is None:
        raise SkillFormatError("SKILL.md frontmatter is missing its closing --- delimiter.")
    try:
        import yaml
    except ImportError as error:
        raise ImportError("Install autoagent[skills] to parse Skill YAML frontmatter.") from error

    class Loader(yaml.SafeLoader):
        def __init__(self, stream):
            super().__init__(stream)
            self.nodes = 0
            self.depth = 0

        def compose_node(self, parent, index):
            if self.check_event(yaml.AliasEvent):
                raise SkillFormatError("Skill YAML aliases are not supported.")
            self.nodes += 1
            self.depth += 1
            try:
                if self.nodes > 4096 or self.depth > 32:
                    raise SkillFormatError("Skill YAML exceeds nesting or node limits.")
                return super().compose_node(parent, index)
            finally:
                self.depth -= 1

        def construct_mapping(self, node, deep=False):
            if not isinstance(node, yaml.MappingNode):
                raise SkillFormatError("Skill YAML requires a mapping.")
            result = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if not isinstance(key, str):
                    raise SkillFormatError("Skill YAML mapping keys must be strings.")
                if key in result:
                    raise SkillFormatError(f"Duplicate Skill YAML key: {key!r}.")
                result[key] = self.construct_object(value_node, deep=deep)
            return result

    try:
        raw = yaml.load("".join(lines[1:end]), Loader=Loader)
        if not isinstance(raw, dict):
            raise SkillFormatError("Skill YAML frontmatter must be a mapping.")
        fields = {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
        values = {key: value for key, value in raw.items() if key in fields}
        if "allowed-tools" in values:
            values["allowed_tools"] = values.pop("allowed-tools")
        values["extensions"] = {key: value for key, value in raw.items() if key not in fields}
        metadata = SkillMetadata.model_validate(values)
    except ValidationError as error:
        detail = error.errors(include_input=False, include_url=False, include_context=False)[0]
        field = ".".join(str(part) for part in detail["loc"]) or None
        if "disable-model-invocation" in raw and not isinstance(raw["disable-model-invocation"], bool):
            field = "disable-model-invocation"
        raise SkillFormatError("Invalid Skill metadata.", field=field) from error
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        raise SkillFormatError("Invalid Skill YAML.", line=None if mark is None else mark.line + 2) from error
    except (TypeError, RecursionError) as error:
        raise SkillFormatError("Invalid Skill YAML or metadata.") from error
    if metadata.name != directory_name:
        raise SkillFormatError("Skill name must match its directory name.", field="name")
    return metadata, "".join(lines[end + 1:]).strip()
