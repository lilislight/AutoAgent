"""Host Persistence; install AutoAgent's ``persistence`` dependency extra."""

from .database import DatabaseSink
from .models import (
    CapacityError,
    ClosedError,
    ConflictError,
    FormatError,
    OperatorDefinition,
    Page,
    PersistenceConfig,
    PersistenceError,
    PersistenceHealth,
    ReadView,
    WriteMode,
)
from .ports import Persistence

__all__ = [
    "CapacityError",
    "ClosedError",
    "ConflictError",
    "DatabaseSink",
    "FormatError",
    "OperatorDefinition",
    "Page",
    "Persistence",
    "PersistenceConfig",
    "PersistenceError",
    "PersistenceHealth",
    "ReadView",
    "WriteMode",
]
