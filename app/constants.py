"""Shared constants and enums."""
from __future__ import annotations

from enum import StrEnum


class ItemStatus(StrEnum):
    """Lifecycle status for a tracked item in a collection.

    Values are stringly-serialized (StrEnum) so they can be compared to raw
    strings read from SQLite without extra conversion.
    """
    STAGED = "staged"
    ACTIONED = "actioned"
    MIGRATED = "migrated"
    KEPT = "kept"
