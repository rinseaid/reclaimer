package config

import (
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/rinseaid/reclaimer/internal/database"
)

type settingDef struct {
	Default any
	EnvVar  string
}

var defaults = map[string]settingDef{
	"plex_url":                       {"", "PLEX_URL"},
	"plex_token":                     {"", "PLEX_TOKEN"},
	"plex_movies_section":            {1, "PLEX_MOVIES_SECTION"},
	"plex_tv_section":                {2, "PLEX_TV_SECTION"},
	"jellyfin_url":                   {"", "JELLYFIN_URL"},
	"jellyfin_api_key":               {"", "JELLYFIN_API_KEY"},
	"jellyfin_movies_section":        {"", "JELLYFIN_MOVIES_SECTION"},
	"jellyfin_tv_section":            {"", "JELLYFIN_TV_SECTION"},
	"jellyfin_movies_keep_collection": {"", "JELLYFIN_MOVIES_KEEP_COLLECTION"},
	"jellyfin_tv_keep_collection":    {"", "JELLYFIN_TV_KEEP_COLLECTION"},
	"seerr_url":                  {"", "SEERR_URL"},
	"seerr_api_key":              {"", "SEERR_API_KEY"},
	"plex_public_url":                {"", "PLEX_PUBLIC_URL"},
	"seerr_public_url":           {"", "SEERR_PUBLIC_URL"},
	"jellyfin_public_url":            {"", "JELLYFIN_PUBLIC_URL"},
	"protected_requesters":           {"", "PROTECTED_REQUESTERS"},
	"plex_movies_keep_collection":    {"", "PLEX_MOVIES_KEEP_COLLECTION"},
	"plex_tv_keep_collection":        {"", "PLEX_TV_KEEP_COLLECTION"},
	"movies_action":                  {"none", "MOVIES_ACTION"},
	"tv_action":                      {"none", "TV_ACTION"},
	"ended_action":                   {"none", "ENDED_ACTION"},
	"movies_grace_days":              {30, "MOVIES_GRACE_DAYS"},
	"tv_grace_days":                  {30, "TV_GRACE_DAYS"},
	"ended_grace_days":               {30, "ENDED_GRACE_DAYS"},
	"delete_files":                   {true, "DELETE_FILES"},
	"add_import_exclusion":           {true, "ADD_IMPORT_EXCLUSION"},
	"apprise_url":                    {"", "APPRISE_URL"},
	"schedule_hour":                  {2, ""},
	"schedule_minute":                {30, ""},
	"torbox_api_key":                 {"", "TORBOX_API_KEY"},
	"rd_api_key":                     {"", "RD_API_KEY"},
	"user_sync_interval_hours":       {6, "USER_SYNC_INTERVAL_HOURS"},
	"viewer_session_ttl_hours":       {168, "VIEWER_SESSION_TTL_HOURS"},
	"viewer_plex_enabled":            {false, "VIEWER_PLEX_ENABLED"},
	"viewer_jellyfin_enabled":        {false, "VIEWER_JELLYFIN_ENABLED"},
	"viewer_oidc_enabled":            {false, "VIEWER_OIDC_ENABLED"},
	"viewer_oidc_issuer_url":         {"", "VIEWER_OIDC_ISSUER_URL"},
	"viewer_oidc_client_id":          {"", "VIEWER_OIDC_CLIENT_ID"},
	"viewer_oidc_client_secret":      {"", "VIEWER_OIDC_CLIENT_SECRET"},
	"viewer_oidc_redirect_uri":       {"", "VIEWER_OIDC_REDIRECT_URI"},
	"viewer_oidc_scopes":             {"openid profile email", "VIEWER_OIDC_SCOPES"},
	"viewer_oidc_display_name":       {"SSO", "VIEWER_OIDC_DISPLAY_NAME"},
	"viewer_local_enabled":           {false, "VIEWER_LOCAL_ENABLED"},
	"viewer_keep_token_secret":       {"", "VIEWER_KEEP_TOKEN_SECRET"},
	"viewer_keep_token_ttl_hours":    {72, "VIEWER_KEEP_TOKEN_TTL_HOURS"},
	"leaving_base_url":               {"", "LEAVING_BASE_URL"},
}

var sensitiveKeys = map[string]bool{
	"plex_token":      true,
	"seerr_api_key": true,
	"torbox_api_key":  true,
	"rd_api_key":      true,
	"jellyfin_api_key":          true,
	"viewer_oidc_client_secret": true,
	"viewer_keep_token_secret":  true,
}

type Config struct {
	mu    sync.RWMutex
	cache map[string]any
	db    *database.DB
}

func New(db *database.DB) *Config {
	return &Config{
		cache: make(map[string]any),
		db:    db,
	}
}

func (c *Config) Init() error {
	var count int
	if err := c.db.Get(&count, "SELECT COUNT(*) FROM settings"); err != nil {
		return err
	}

	if count == 0 {
		slog.Info("seeding settings from environment variables")
		for key, def := range defaults {
			var val any
			if def.EnvVar != "" {
				envVal := os.Getenv(def.EnvVar)
				if envVal != "" {
					val = coerce(key, envVal)
				} else {
					val = def.Default
				}
			} else {
				val = def.Default
			}
			jsonVal, _ := json.Marshal(val)
			_, err := c.db.Exec(
				c.db.Rebind("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)"),
				key, string(jsonVal),
			)
			if err != nil {
				// Try Postgres upsert syntax
				_, err = c.db.Exec(
					c.db.Rebind("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
					key, string(jsonVal),
				)
				if err != nil {
					return err
				}
			}
		}
	} else {
		for key, def := range defaults {
			var existing *string
			err := c.db.Get(&existing, c.db.Rebind("SELECT value FROM settings WHERE key = ?"), key)
			if err != nil || existing == nil {
				var val any
				if def.EnvVar != "" {
					envVal := os.Getenv(def.EnvVar)
					if envVal != "" {
						val = coerce(key, envVal)
					} else {
						val = def.Default
					}
				} else {
					val = def.Default
				}
				jsonVal, _ := json.Marshal(val)
				c.db.Exec(
					c.db.Rebind("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING"),
					key, string(jsonVal),
				)
			}
		}
	}

	c.reloadCache()
	return nil
}

func (c *Config) reloadCache() {
	type row struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}
	var rows []row
	if err := c.db.Select(&rows, "SELECT key, value FROM settings"); err != nil {
		slog.Error("failed to reload config cache", "error", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]any, len(rows))
	for _, r := range rows {
		var val any
		if err := json.Unmarshal([]byte(r.Value), &val); err != nil {
			c.cache[r.Key] = r.Value
		} else {
			c.cache[r.Key] = val
		}
	}
}

func (c *Config) Get(key string) any {
	c.mu.RLock()
	v, ok := c.cache[key]
	c.mu.RUnlock()
	if ok {
		return v
	}
	if def, ok := defaults[key]; ok {
		return def.Default
	}
	return nil
}

func (c *Config) GetString(key string) string {
	v := c.Get(key)
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(s)
	default:
		return ""
	}
}

func (c *Config) GetInt(key string) int {
	v := c.Get(key)
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		i, _ := strconv.Atoi(n)
		return i
	default:
		return 0
	}
}

func (c *Config) GetBool(key string) bool {
	v := c.Get(key)
	if v == nil {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case float64:
		return b != 0
	case string:
		return strings.EqualFold(b, "true") || b == "1"
	default:
		return false
	}
}

func (c *Config) GetAll(redact bool) map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]any, len(c.cache))
	for k, v := range c.cache {
		if redact && sensitiveKeys[k] {
			if s, ok := v.(string); ok && s != "" {
				result[k] = "••••••••"
				continue
			}
		}
		result[k] = v
	}
	return result
}

func (c *Config) Update(updates map[string]any) (int, error) {
	count := 0
	for key, value := range updates {
		if _, ok := defaults[key]; !ok {
			continue
		}
		jsonVal, _ := json.Marshal(value)
		_, err := c.db.Exec(
			c.db.Rebind("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
			key, string(jsonVal),
		)
		if err != nil {
			return count, err
		}
		count++
	}
	c.reloadCache()
	return count, nil
}

func coerce(key string, value string) any {
	def, ok := defaults[key]
	if !ok {
		return value
	}
	switch def.Default.(type) {
	case bool:
		return strings.EqualFold(value, "true") || value == "1" || strings.EqualFold(value, "yes")
	case int:
		i, err := strconv.Atoi(value)
		if err != nil {
			return def.Default
		}
		return i
	default:
		return value
	}
}
