package database

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

func openSQLite(dsn string) (*DB, error) {
	if dsn == "" {
		dsn = "/app/data/reclaimer.db"
	}
	dir := filepath.Dir(dsn)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	connStr := fmt.Sprintf("file:%s?_journal_mode=WAL&_foreign_keys=ON&_synchronous=NORMAL&_busy_timeout=30000", dsn)
	db, err := sqlx.Open("sqlite", connStr)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA temp_store=MEMORY",
		"PRAGMA cache_size=-20000",
		"PRAGMA mmap_size=268435456",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}

	return &DB{DB: db, Dialect: DialectSQLite}, nil
}

const sqliteSchema = `
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
    genre           TEXT,
    content_rating  TEXT,
    year            INTEGER,
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
    last_synced     TEXT NOT NULL DEFAULT (datetime('now')),
    source          TEXT DEFAULT 'plex'
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_arr_instances_default ON arr_instances(kind) WHERE is_default = 1;
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
CREATE INDEX IF NOT EXISTS idx_watch_history_grandparent_watched ON watch_history(grandparent_title COLLATE NOCASE, watched_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_log_type ON activity_log(event_type);
CREATE INDEX IF NOT EXISTS idx_activity_log_event_ts ON activity_log(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_activity_log_rk_ts ON activity_log(rating_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ratings_imdb ON ratings_cache(imdb_id);

CREATE TABLE IF NOT EXISTS viewer_users (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    username         TEXT NOT NULL,
    display_name     TEXT,
    email            TEXT,
    password_hash    TEXT,
    auth_provider    TEXT NOT NULL DEFAULT 'local',
    auth_provider_id TEXT,
    avatar_url       TEXT,
    is_active        BOOLEAN NOT NULL DEFAULT 1,
    is_admin         BOOLEAN NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(auth_provider, auth_provider_id)
);
CREATE INDEX IF NOT EXISTS idx_viewer_users_email ON viewer_users(email);

CREATE TABLE IF NOT EXISTS viewer_sessions (
    id           TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES viewer_users(id) ON DELETE CASCADE,
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    user_agent   TEXT,
    ip_address   TEXT
);
CREATE INDEX IF NOT EXISTS idx_viewer_sessions_user ON viewer_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_viewer_sessions_expires ON viewer_sessions(expires_at);

CREATE TABLE IF NOT EXISTS keep_tokens (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    token        TEXT NOT NULL UNIQUE,
    rating_key   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    used_at      TEXT,
    created_by   TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_keep_tokens_token ON keep_tokens(token);
`
