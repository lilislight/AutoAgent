"""Business Evaluation through Host execution and immutable historical evidence.

Scenarios control execution; Evaluators receive read-only observations. Reports
retain references to the normal Host history, rather than duplicating it.
"""

from .context import EvaluatorContext, ScenarioContext
from .evaluators import NodeExecuted, RouteTaken, OutputEquals, StatusIs
from autoagent.host.execution_view import ExecutionView, ViewUnavailable
from .models import (CheckResult, EvalCase, EvalSuite, EvaluationEnvironment,
                     EvaluationError, RunOptions)
from .report import compare_runs, load_report
from .runner import run_suite

__all__ = ['CheckResult', 'EvalCase', 'EvalSuite', 'EvaluationEnvironment', 'EvaluationError',
    'RunOptions', 'EvaluatorContext', 'ScenarioContext', 'ExecutionView', 'ViewUnavailable',
    'NodeExecuted', 'RouteTaken', 'OutputEquals', 'StatusIs', 'compare_runs', 'load_report', 'run_suite']
