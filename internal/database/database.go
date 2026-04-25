package database

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jmoiron/sqlx"
)

type Dialect string

const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
)

type DB struct {
	*sqlx.DB
	Dialect Dialect
}

// Open creates a database connection, auto-detecting dialect from environment.
// DATABASE_URL or POSTGRES_DSN → Postgres; otherwise SQLite at DATABASE_PATH
// (default /app/data/reclaimer.db).
func Open() (*DB, error) {
	if dsn := coalesceEnv("DATABASE_URL", "POSTGRES_DSN"); dsn != "" {
		return openPostgres(dsn)
	}
	return openSQLite(coalesceEnv("DATABASE_PATH", ""))
}

func coalesceEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// Init creates all tables and runs migrations.
func (db *DB) Init() error {
	schema := sqliteSchema
	if db.Dialect == DialectPostgres {
		schema = postgresSchema
	}
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("schema init: %w", err)
	}
	if err := db.migrate(); err != nil {
		return fmt.Errorf("migration: %w", err)
	}
	if err := db.seedDefaults(); err != nil {
		return fmt.Errorf("seed defaults: %w", err)
	}
	slog.Info("database initialized", "dialect", db.Dialect)
	return nil
}

func (db *DB) migrate() error {
	renames := [][2]string{
		{"overseerr_url", "seerr_url"},
		{"overseerr_api_key", "seerr_api_key"},
		{"overseerr_public_url", "seerr_public_url"},
	}
	for _, r := range renames {
		db.Exec(db.Rebind("UPDATE settings SET key = ? WHERE key = ?"), r[1], r[0])
	}

	if db.Dialect == DialectPostgres {
		db.Exec("ALTER TABLE viewer_users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE")
	} else {
		db.Exec("ALTER TABLE viewer_users ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT 0")
	}

	return nil
}

func (db *DB) seedDefaults() error {
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM collection_config"); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	defaults := []struct {
		Name      string
		MediaType string
		Criteria  string
	}{
		{
			Name:      "Movies Leaving Soon",
			MediaType: "movie",
			Criteria:  defaultMovieCriteria,
		},
		{
			Name:      "TV Shows Leaving Soon",
			MediaType: "show",
			Criteria:  defaultShowCriteria,
		},
	}

	for _, d := range defaults {
		_, err := db.Exec(
			db.Rebind(`INSERT INTO collection_config (name, media_type, action, grace_days, criteria, enabled, priority) VALUES (?, ?, 'none', 30, ?, 1, 100)`),
			d.Name, d.MediaType, d.Criteria,
		)
		if err != nil {
			return fmt.Errorf("seed %s: %w", d.Name, err)
		}
	}
	slog.Info("seeded default collections")
	return nil
}

// Rebind converts ? placeholders to the dialect-appropriate form.
func (db *DB) Rebind(query string) string {
	if db.Dialect == DialectPostgres {
		return sqlx.Rebind(sqlx.DOLLAR, query)
	}
	return query
}

// PruneActivityLog deletes rows older than the given number of days.
func (db *DB) PruneActivityLog(days int) (int64, error) {
	var query string
	switch db.Dialect {
	case DialectPostgres:
		query = fmt.Sprintf("DELETE FROM activity_log WHERE timestamp < NOW() - INTERVAL '%d days'", days)
	default:
		query = fmt.Sprintf("DELETE FROM activity_log WHERE timestamp < datetime('now', '-%d days')", days)
	}
	res, err := db.Exec(query)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

const defaultMovieCriteria = `{
	"rules": {
		"never_watched": {"enabled": true, "min_days_unwatched": 0, "check_plex_views": true, "check_db_plays": true, "exclude_users": []},
		"no_keep_tag": {"enabled": true},
		"no_active_request": {"enabled": true},
		"no_protected_request": {"enabled": true},
		"not_in_keep_collection": {"enabled": true},
		"show_ended": {"enabled": false}
	},
	"action": "none",
	"grace_days": 30,
	"action_pipeline": []
}`

const defaultShowCriteria = `{
	"rules": {
		"never_watched": {"enabled": true, "min_days_unwatched": 0, "check_plex_views": true, "check_db_plays": true, "exclude_users": []},
		"no_keep_tag": {"enabled": true},
		"no_active_request": {"enabled": true},
		"no_protected_request": {"enabled": true},
		"not_in_keep_collection": {"enabled": true},
		"show_ended": {"enabled": false}
	},
	"action": "none",
	"grace_days": 30,
	"action_pipeline": []
}`
