"""Settings management with env-var fallback and SQLite persistence.

Keys cover the media servers MCM integrates with (Plex + Jellyfin for
library metadata and watch history), the companion services used for
rules (Radarr, Sonarr, Overseerr), the debrid providers (TorBox,
Real-Debrid), notification routing (Apprise), and scheduling. On first
boot ``init_settings`` seeds defaults from environment variables; after
that the SQLite ``settings`` table is the source of truth and values
flow through the in-memory ``_cache``.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from .database import get_db

log = logging.getLogger(__name__)

# Default settings with their types and env var names
DEFAULTS = {
    "plex_url": ("", "PLEX_URL"),
    "plex_token": ("", "PLEX_TOKEN"),
    "plex_movies_section": (1, "PLEX_MOVIES_SECTION"),
    "plex_tv_section": (2, "PLEX_TV_SECTION"),
    # NOTE: Radarr/Sonarr connection settings live in the ``arr_instances``
    # table (see app/core/arr_instances.py), not here. Existing radarr_*/
    # sonarr_* settings rows are migrated on first boot by database._migrate
    # and those keys are deleted from the settings table.
    "jellyfin_url": ("", "JELLYFIN_URL"),
    "jellyfin_api_key": ("", "JELLYFIN_API_KEY"),
    "jellyfin_movies_section": ("", "JELLYFIN_MOVIES_SECTION"),
    "jellyfin_tv_section": ("", "JELLYFIN_TV_SECTION"),
    "jellyfin_movies_keep_collection": ("", "JELLYFIN_MOVIES_KEEP_COLLECTION"),
    "jellyfin_tv_keep_collection": ("", "JELLYFIN_TV_KEEP_COLLECTION"),
    "overseerr_url": ("", "OVERSEERR_URL"),
    "overseerr_api_key": ("", "OVERSEERR_API_KEY"),
    # Public URLs for browser links (fallback to API URLs if blank).
    # radarr_public_url / sonarr_public_url moved to arr_instances.
    "plex_public_url": ("", "PLEX_PUBLIC_URL"),
    "overseerr_public_url": ("", "OVERSEERR_PUBLIC_URL"),
    "jellyfin_public_url": ("", "JELLYFIN_PUBLIC_URL"),
    "protected_requesters": ("", "PROTECTED_REQUESTERS"),
    "plex_movies_keep_collection": ("", "PLEX_MOVIES_KEEP_COLLECTION"),
    "plex_tv_keep_collection": ("", "PLEX_TV_KEEP_COLLECTION"),
    "movies_action": ("none", "MOVIES_ACTION"),
    "tv_action": ("none", "TV_ACTION"),
    "ended_action": ("none", "ENDED_ACTION"),
    "movies_grace_days": (30, "MOVIES_GRACE_DAYS"),
    "tv_grace_days": (30, "TV_GRACE_DAYS"),
    "ended_grace_days": (30, "ENDED_GRACE_DAYS"),
    "delete_files": (True, "DELETE_FILES"),
    "add_import_exclusion": (True, "ADD_IMPORT_EXCLUSION"),
    "apprise_url": ("", "APPRISE_URL"),
    "schedule_hour": (2, None),
    "schedule_minute": (30, None),
    # Debrid settings
    "torbox_api_key": ("", "TORBOX_API_KEY"),
    "rd_api_key": ("", "RD_API_KEY"),
    "user_sync_interval_hours": (6, "USER_SYNC_INTERVAL_HOURS"),
}

# Keys that are sensitive (redacted in API responses). Arr API keys live
# in the ``arr_instances`` table and are redacted separately by the
# instance API layer.
SENSITIVE_KEYS = {
    "plex_token",
    "overseerr_api_key",
    "torbox_api_key", "rd_api_key",
    "jellyfin_api_key",
}

_cache: dict[str, Any] = {}


def _coerce(key: str, value: str) -> Any:
    """Coerce a string value to the correct type based on defaults."""
    default_val = DEFAULTS.get(key, ("",))[0]
    if isinstance(default_val, bool):
        return value.lower() in ("true", "1", "yes")
    if isinstance(default_val, int):
        try:
            return int(value)
        except ValueError:
            return default_val
    return value


def init_settings() -> None:
    """Seed settings from env vars if the settings table is empty."""
    conn = get_db()
    existing = conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0]

    if existing == 0:
        log.info("Seeding settings from environment variables")
        now = datetime.now().isoformat()
        for key, (default, env_name) in DEFAULTS.items():
            if env_name:
                value = os.environ.get(env_name, "")
                if value:
                    conn.execute(
                        "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                        (key, json.dumps(_coerce(key, value)), now),
                    )
                elif default is not None:
                    conn.execute(
                        "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                        (key, json.dumps(default), now),
                    )
            elif default is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                    (key, json.dumps(default), now),
                )
        conn.commit()
    else:
        # Fill any missing keys from env vars
        for key, (default, env_name) in DEFAULTS.items():
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            if row is None:
                if env_name:
                    value = os.environ.get(env_name, "")
                    val = _coerce(key, value) if value else default
                else:
                    val = default
                conn.execute(
                    "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                    (key, json.dumps(val), datetime.now().isoformat()),
                )
        conn.commit()

    conn.close()
    _reload_cache()


def _reload_cache() -> None:
    global _cache
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    _cache = {row["key"]: json.loads(row["value"]) for row in rows}


def get(key: str) -> Any:
    if key in _cache:
        return _cache[key]
    default = DEFAULTS.get(key, (None,))[0]
    return default


def get_all(redact: bool = False) -> dict[str, Any]:
    result = dict(_cache)
    if redact:
        for k in SENSITIVE_KEYS:
            if k in result and result[k]:
                result[k] = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"
    return result


def update(updates: dict[str, Any]) -> None:
    conn = get_db()
    now = datetime.now().isoformat()
    for key, value in updates.items():
        if key not in DEFAULTS:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, json.dumps(value), now),
        )
    conn.commit()
    conn.close()
    _reload_cache()
    log.info("Settings updated: %s", ", ".join(updates.keys()))
