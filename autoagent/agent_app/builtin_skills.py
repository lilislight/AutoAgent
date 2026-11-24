"""Read-only authoring guides shipped with the Agent application."""

from autoagent.agent import PackageSkillSource, Skill


NAMES = ("autoagent-author-skill", "autoagent-debug-skill")


def is_bundled(skill):
    location = getattr(getattr(skill.source, "snapshot", None), "source_location", None) or skill.source.location
    return location.startswith("package:autoagent.agent_app/skill_assets/")


def install(registry):
    from .workflow import BUILTIN_AGENT_WORKFLOW_ID

    for name in NAMES:
        skill = Skill(PackageSkillSource("autoagent.agent_app", "skill_assets/" + name)).freeze()
        registry.register_skill(skill, version=skill.snapshot_revision, workflows={BUILTIN_AGENT_WORKFLOW_ID})
