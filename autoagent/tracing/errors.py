"""Stable failures raised while assembling optional Tracing components."""


class TracingDependencyError(RuntimeError):
    """Required local Server dependencies are not installed."""


__all__ = ["TracingDependencyError"]
