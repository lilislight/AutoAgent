"""Standalone Server, embeddable API router, and optional UI mount."""

from .app import create_app, create_router
from .settings import ServerSettings
from .ui import mount_ui

__all__ = ["ServerSettings", "create_app", "create_router", "mount_ui"]
