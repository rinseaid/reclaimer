package database

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func openPostgres(dsn string) (*DB, error) {
	if dsn == "" {
		dsn = "postgres://localhost:5432/reclaimer?sslmode=disable"
	}
	db, err := sqlx.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &DB{DB: db, Dialect: DialectPostgres}, nil
}

const postgresSchema = `
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS items (
    id              BIGSERIAL PRIMARY KEY,
    rating_key      TEXT NOT NULL,
    collection      TEXT NOT NULL,
    title           TEXT,
    media_type      TEXT NOT NULL DEFAULT 'movie',
    tmdb_id         BIGINT,
    tvdb_id         BIGINT,
    imdb_id         TEXT,
    arr_id          BIGINT,
    season_number   INTEGER,
    show_rating_key TEXT,
    size_bytes      BIGINT DEFAULT 0,
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
    id              BIGSERIAL PRIMARY KEY,
    rating_key      TEXT NOT NULL,
    collection      TEXT NOT NULL,
    rule_name       TEXT NOT NULL,
    passed          BOOLEAN NOT NULL,
    detail          TEXT,
    severity        TEXT DEFAULT 'info',
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id              BIGSERIAL PRIMARY KEY,
    plex_user_id    BIGINT UNIQUE,
    username        TEXT NOT NULL,
    email           TEXT,
    thumb           TEXT,
    is_protected    BOOLEAN DEFAULT FALSE,
    last_synced     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT DEFAULT 'plex'
);

CREATE TABLE IF NOT EXISTS watch_history (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    rating_key      TEXT NOT NULL,
    title           TEXT,
    grandparent_title TEXT,
    media_type      TEXT,
    season_number   INTEGER,
    episode_number  INTEGER,
    watched_at      TEXT NOT NULL,
    play_duration   BIGINT DEFAULT 0,
    media_duration  BIGINT DEFAULT 0,
    percent_complete INTEGER DEFAULT 0,
    UNIQUE(user_id, rating_key, watched_at)
);

CREATE TABLE IF NOT EXISTS activity_log (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type      TEXT NOT NULL,
    collection      TEXT,
    rating_key      TEXT,
    title           TEXT,
    detail          TEXT
);

CREATE TABLE IF NOT EXISTS debrid_cache (
    id              BIGSERIAL PRIMARY KEY,
    rating_key      TEXT NOT NULL,
    info_hash       TEXT,
    provider        TEXT NOT NULL,
    is_cached       BOOLEAN NOT NULL,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(rating_key, info_hash, provider)
);

CREATE TABLE IF NOT EXISTS collection_config (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT UNIQUE NOT NULL,
    media_type     TEXT NOT NULL DEFAULT 'movie',
    action         TEXT NOT NULL DEFAULT 'none',
    grace_days     INTEGER NOT NULL DEFAULT 30,
    criteria       TEXT,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    schedule_cron  TEXT,
    priority       INTEGER NOT NULL DEFAULT 100,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ratings_cache (
    imdb_id     TEXT PRIMARY KEY,
    imdb_rating DOUBLE PRECISION,
    rt_score    INTEGER,
    metacritic  INTEGER,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS arr_instances (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT NOT NULL CHECK (kind IN ('radarr', 'sonarr')),
    name        TEXT NOT NULL UNIQUE,
    url         TEXT NOT NULL,
    api_key     TEXT NOT NULL,
    public_url  TEXT NOT NULL DEFAULT '',
    is_default  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX IF NOT EXISTS idx_arr_instances_kind ON arr_instances(kind);
EXCEPTION WHEN others THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_items_collection ON items(collection);
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);
CREATE INDEX IF NOT EXISTS idx_items_rating_key ON items(rating_key);
CREATE INDEX IF NOT EXISTS idx_items_collection_rk ON items(collection, rating_key);
CREATE INDEX IF NOT EXISTS idx_items_status_grace ON items(status, grace_expires);
CREATE INDEX IF NOT EXISTS idx_rule_results_rk ON rule_results(rating_key, collection);
CREATE INDEX IF NOT EXISTS idx_watch_history_user ON watch_history(user_id);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk ON watch_history(rating_key);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk_grandparent ON watch_history(rating_key, grandparent_title);
CREATE INDEX IF NOT EXISTS idx_watch_history_rk_user ON watch_history(rating_key, user_id);
CREATE INDEX IF NOT EXISTS idx_watch_history_user_rk ON watch_history(user_id, rating_key);
CREATE INDEX IF NOT EXISTS idx_activity_log_type ON activity_log(event_type);
CREATE INDEX IF NOT EXISTS idx_activity_log_event_ts ON activity_log(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_ratings_imdb ON ratings_cache(imdb_id);

DO $$ BEGIN
    CREATE UNIQUE INDEX idx_arr_instances_default ON arr_instances(kind) WHERE is_default = TRUE;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;
`
