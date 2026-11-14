"""Evaluation entrypoints are loaded only by Evaluation commands."""

from dataclasses import dataclass, replace

from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.project import environment_scope, import_scope, resolve_entrypoint

from .models import EvalSuite, EvaluationError


@dataclass(frozen=True)
class ProjectHostFactory:
    project: str
    environment: dict

    def evaluation_conditions(self):
        # Only non-secret dependency identities are automatic. Application-specific
        # services/data remain the Suite's explicit conditions declaration.
        keys = ('AUTOAGENT_PROVIDERS_FILE',)
        return {key: self.environment[key] for key in keys if key in self.environment}

    def __call__(self, environment):
        settings = replace(HostSettings.from_environment(self.environment), database_url=environment.database_url)
        return AutoAgentHost.from_project(self.project, settings=settings,
            environment=self.environment, use_env_file=False)


def load_suites(project, environment):
    suites = []
    with environment_scope(environment), import_scope(project.root):
        for entrypoint in project.evaluation_entrypoints:
            value = resolve_entrypoint(entrypoint, project.root)
            if callable(value) and not isinstance(value, EvalSuite):
                value = value()
            if not isinstance(value, EvalSuite):
                raise EvaluationError(f'{entrypoint} must export an EvalSuite or zero-argument factory.')
            if any(suite.id == value.id for suite in suites):
                raise EvaluationError(f'Duplicate Suite id: {value.id}')
            suites.append(value)
    return tuple(suites)
