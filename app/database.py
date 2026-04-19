"""SQLite database setup, schema, and migration from JSON state."""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH = Path("/app/data/reclaimer.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_key      TEXT NOT NULL,
    collection      TEXT NOT NULL,
    title           TEXT,
    media_type      TEXT NOT NULL DEFAULT 'movie',
    tmdb_id         INTEGER,
    tvdb_id         INTEGER,
    imdb_id         TEXT,
    arr_id          INTEGER,
    season_number   INTEGER,
    show_rating_key TEXT,
    size_bytes      INTEGER DEFAULT 0,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    grace_expires   TEXT NOT NULL,
    status          TEXT DEFAULT 'staged',
    action_taken    TEXT,
    action_date     TEXT,
    override        TEXT,
    override_by     TEXT,
    UNIQUE(rating_key, collection)
);

CREATE TABLE IF NOT EXISTS rule_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_key      TEXT NOT NULL,
    collection      TEXT NOT NULL,
    rule_name       TEXT NOT NULL,
    passed          BOOLEAN NOT NULL,
    detail          TEXT,
    severity        TEXT DEFAULT 'info',
    evaluated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    plex_user_id    INTEGER UNIQUE,
    username        TEXT NOT NULL,
    email           TEXT,
    thumb           TEXT,
    is_protected    BOOLEAN DEFAULT 0,
    last_synced     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS watch_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    rating_key      TEXT NOT NULL,
    title           TEXT,
    grandparent_title TEXT,
    media_type      TEXT,
    season_number   INTEGER,
    episode_number  INTEGER,
    watched_at      TEXT NOT NULL,
    play_duration   INTEGER DEFAULT 0,
    media_duration  INTEGER DEFAULT 0,
    percent_complete INTEGER DEFAULT 0,
    UNIQUE(user_id, rating_key, watched_at)
);

CREATE TABLE IF NOT EXISTS activity_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
    event_type      TEXT NOT NULL,
    collection      TEXT,
    rating_key      TEXT,
    title           TEXT,
    detail          TEXT
);

CREATE TABLE IF NOT EXISTS debrid_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_key      TEXT NOT NULL,
    info_hash       TEXT,
    provider        TEXT NOT NULL,
    is_cached       BOOLEAN NOT NULL,
    checked_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(rating_key, info_hash, provider)
);

CREATE TABLE IF NOT EXISTS collection_config (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT UNIQUE NOT NULL,
    media_type     TEXT NOT NULL DEFAULT 'movie',
    action         TEXT NOT NULL DEFAULT 'none',
    grace_days     INTEGER NOT NULL DEFAULT 30,
    criteria       TEXT,
    enabled        BOOLEAN NOT NULL DEFAULT 1,
    schedule_cron  TEXT,
    priority       INTEGER NOT NULL DEFAULT 100,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ratings_cache (
    imdb_id     TEXT PRIMARY KEY,
    imdb_rating REAL,
    rt_score    INTEGER,
    metacritic  INTEGER,
    fetched_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS arr_instances (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('radarr', 'sonarr')),
    name        TEXT NOT NULL UNIQUE,
    url         TEXT NOT NULL,
    api_key     TEXT NOT NULL,
    public_url  TEXT NOT NULL DEFAULT '',
    is_default  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_arr_instances_kind ON arr_instances(kind);
-- Only one row per kind may carry is_default=1. Partial unique index
-- lets multiple is_default=0 rows coexist.
CREATE UNIQUE INDEX IF NOT EXISTS idx_arr_instances_default
    ON arr_instances(kind) WHERE is_default = 1;

CREATE INDEX IF NOT EXISTS idx_items_collection ON items(collection);
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);
CREATE INDEX IF NOT EXISTS idx_items_rating_key ON items(rating_key);
CREATE INDEX IF NOT EXISTS idx_items_collection_rk ON items(collection, rating_key);
CREATE INDEX IF NOT EXISTS idx_items_status_grace ON items(status, grace_expires);
CREATE INDEX IF NOT EXISTS idx_rule_results_rk ON rule_results(rating_key, collection);
CREATE INDEX IF NOT EXISTS idx_watch_history_user ON watch_history(user_id);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk ON watch_history(rating_key);
CREATE INDEX IF NOT EXISTS idx_watch_history_grandparent ON watch_history(grandparent_title COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk_grandparent ON watch_history(rating_key, grandparent_title);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk_user ON watch_history(rating_key, user_id);
CREATE INDEX IF NOT EXISTS idx_watch_history_user_rk ON watch_history(user_id, rating_key);
CREATE INDEX IF NOT EXISTS idx_watch_history_grandparent_watched
    ON watch_history(grandparent_title COLLATE NOCASE, watched_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_log_type ON activity_log(event_type);
CREATE INDEX IF NOT EXISTS idx_activity_log_event_ts ON activity_log(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_activity_log_rk_ts ON activity_log(rating_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ratings_imdb ON ratings_cache(imdb_id);
"""

DEFAULT_COLLECTIONS = [
    {
        "name": "Movies Leaving Soon",
        "media_type": "movie",
        "action": "none",
        "grace_days": 30,
        "criteria": json.dumps({
            "rules": {
                "never_watched": {
                    "enabled": True,
                    "min_days_unwatched": 0,
                    "check_plex_views": True,
                    "check_db_plays": True,
                    "exclude_users": [],
                },
                "no_keep_tag": {"enabled": True},
                "no_active_request": {"enabled": True},
                "no_protected_request": {"enabled": True},
                "not_in_keep_collection": {"enabled": True},
                "show_ended": {"enabled": False},
            },
            "action": "none",
            "grace_days": 30,
            "action_pipeline": [],
        }),
        "enabled": True,
    },
    {
        "name": "TV Shows Leaving Soon",
        "media_type": "show",
        "action": "none",
        "grace_days": 30,
        "criteria": json.dumps({
            "rules": {
                "never_watched": {
                    "enabled": True,
                    "min_days_unwatched": 0,
                    "check_plex_views": True,
                    "check_db_plays": True,
                    "exclude_users": [],
                },
                "no_keep_tag": {"enabled": True},
                "no_active_request": {"enabled": True},
                "no_protected_request": {"enabled": True},
                "not_in_keep_collection": {"enabled": True},
                "show_ended": {"enabled": False},
            },
            "action": "none",
            "grace_days": 30,
            "action_pipeline": [],
        }),
        "enabled": True,
    },
]


def get_db() -> sqlite3.Connection:
    """Get a database connection with WAL mode and row factory."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    # journal_mode is a per-database setting (persisted in the DB file header)
    # but the others below are per-connection and must be set each open.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # synchronous=NORMAL is safe with WAL and ~2x faster than FULL for writes.
    conn.execute("PRAGMA synchronous=NORMAL")
    # Keep temp tables/sort buffers in RAM.
    conn.execute("PRAGMA temp_store=MEMORY")
    # 20 MB page cache (negative => KB). Small enough to be safe under the
    # container mem_limit, big enough to keep hot pages warm.
    conn.execute("PRAGMA cache_size=-20000")
    # 256 MB mmap window -- read-only paths get to skip the page cache.
    conn.execute("PRAGMA mmap_size=268435456")
    return conn


def _seed_default_collections(conn: sqlite3.Connection) -> None:
    """Seed defaults only on first install (empty collection_config table)."""
    count = conn.execute("SELECT COUNT(*) FROM collection_config").fetchone()[0]
    if count > 0:
        return
    for col in DEFAULT_COLLECTIONS:
        conn.execute(
            """INSERT INTO collection_config
               (name, media_type, action, grace_days, criteria, enabled)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (col["name"], col["media_type"], col["action"],
             col["grace_days"], col["criteria"], col["enabled"]),
        )


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply incremental schema migrations for existing databases."""
    # Add media_duration column if missing
    cols = {row[1] for row in conn.execute("PRAGMA table_info(watch_history)").fetchall()}
    if "media_duration" not in cols:
        conn.execute("ALTER TABLE watch_history ADD COLUMN media_duration INTEGER DEFAULT 0")
        log.info("Migration: added media_duration column to watch_history")
    if "percent_complete" not in cols:
        conn.execute("ALTER TABLE watch_history ADD COLUMN percent_complete INTEGER DEFAULT 0")
        log.info("Migration: added percent_complete column to watch_history")

    # Add season_number and show_rating_key columns to items if missing
    item_cols = {row[1] for row in conn.execute("PRAGMA table_info(items)").fetchall()}
    if "season_number" not in item_cols:
        conn.execute("ALTER TABLE items ADD COLUMN season_number INTEGER")
        log.info("Migration: added season_number column to items")
    if "show_rating_key" not in item_cols:
        conn.execute("ALTER TABLE items ADD COLUMN show_rating_key TEXT")
        log.info("Migration: added show_rating_key column to items")

    # Users: track which media server the row came from (plex, jellyfin, or both).
    user_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "source" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN source TEXT DEFAULT 'plex'")
        log.info("Migration: added source column to users")

    # collection_config: per-rule cron schedule + priority ordering.
    cfg_cols = {row[1] for row in conn.execute("PRAGMA table_info(collection_config)").fetchall()}
    if "schedule_cron" not in cfg_cols:
        conn.execute("ALTER TABLE collection_config ADD COLUMN schedule_cron TEXT")
        log.info("Migration: added schedule_cron column to collection_config")
    if "priority" not in cfg_cols:
        # SQLite doesn't allow adding a NOT NULL column without a default to
        # an existing table; the plain DEFAULT here also backfills existing
        # rows with the sentinel 100.
        conn.execute("ALTER TABLE collection_config ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
        log.info("Migration: added priority column to collection_config")

    # arr_instances: one-shot conversion from legacy radarr_*/sonarr_*
    # key-value settings into proper instance rows. Runs only when the
    # instance table is empty, so it's idempotent across restarts.
    inst_count = conn.execute("SELECT COUNT(*) FROM arr_instances").fetchone()[0]
    if inst_count == 0:
        import os as _os
        legacy_keys = (
            "radarr_url", "radarr_api_key", "radarr_public_url",
            "sonarr_url", "sonarr_api_key", "sonarr_public_url",
        )
        legacy: dict[str, str] = {}
        for k in legacy_keys:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (k,)).fetchone()
            if row:
                try:
                    legacy[k] = json.loads(row["value"]) or ""
                except (TypeError, ValueError):
                    legacy[k] = ""
        # Env-var fallback for fresh deploys that never had the legacy keys
        # seeded (DEFAULTS no longer advertises them, so init_settings won't
        # pick them up -- but a one-time bootstrap from RADARR_URL etc. still
        # makes first-run from compose work without hand-adding an instance).
        env_map = {
            "radarr_url": "RADARR_URL",
            "radarr_api_key": "RADARR_API_KEY",
            "radarr_public_url": "RADARR_PUBLIC_URL",
            "sonarr_url": "SONARR_URL",
            "sonarr_api_key": "SONARR_API_KEY",
            "sonarr_public_url": "SONARR_PUBLIC_URL",
        }
        for k, env_name in env_map.items():
            if not legacy.get(k):
                env_val = _os.environ.get(env_name, "").strip()
                if env_val:
                    legacy[k] = env_val

        for kind in ("radarr", "sonarr"):
            url = (legacy.get(f"{kind}_url") or "").strip()
            api_key = (legacy.get(f"{kind}_api_key") or "").strip()
            pub = (legacy.get(f"{kind}_public_url") or "").strip()
            if url and api_key:
                conn.execute(
                    """INSERT INTO arr_instances
                       (kind, name, url, api_key, public_url, is_default)
                       VALUES (?, ?, ?, ?, ?, 1)""",
                    (kind, kind.capitalize(), url, api_key, pub),
                )
                log.info("Migration: seeded %s instance from legacy settings/env", kind)
        # Purge legacy keys regardless -- whether seeded or not, they're
        # no longer the source of truth.
        conn.execute(
            f"DELETE FROM settings WHERE key IN ({','.join('?' * len(legacy_keys))})",
            legacy_keys,
        )


def init_db() -> None:
    """Create tables if they don't exist and seed defaults."""
    conn = get_db()
    # WAL mode is set per-connection in get_db(), but run it at init so the
    # DB file is explicitly in WAL mode on first open. PRAGMA optimize tells
    # SQLite to gather stats / update query plans after schema changes.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    _migrate(conn)
    _seed_default_collections(conn)
    conn.execute("PRAGMA optimize")
    conn.commit()
    conn.close()
    log.info("Database initialized at %s", DB_PATH)


def prune_activity_log(conn, days: int = 90) -> int:
    """Delete activity_log rows older than ``days`` days.

    Returns the number of rows deleted. Caller is responsible for commit().
    """
    cur = conn.execute(
        "DELETE FROM activity_log WHERE timestamp < date('now', ?)",
        (f"-{int(days)} days",),
    )
    return cur.rowcount or 0


