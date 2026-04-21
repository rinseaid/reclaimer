package models

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"
)

// NullString wraps sql.NullString with JSON-friendly marshaling.
type NullString struct{ sql.NullString }

func (n NullString) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.String)
}

func (n *NullString) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		n.Valid = false
		return nil
	}
	n.Valid = true
	return json.Unmarshal(b, &n.String)
}

// NullInt64 wraps sql.NullInt64 with JSON-friendly marshaling.
type NullInt64 struct{ sql.NullInt64 }

func (n NullInt64) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Int64)
}

func (n *NullInt64) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		n.Valid = false
		return nil
	}
	n.Valid = true
	return json.Unmarshal(b, &n.Int64)
}

// NullFloat64 wraps sql.NullFloat64 with JSON-friendly marshaling.
type NullFloat64 struct{ sql.NullFloat64 }

func (n NullFloat64) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Float64)
}

func (n *NullFloat64) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		n.Valid = false
		return nil
	}
	n.Valid = true
	return json.Unmarshal(b, &n.Float64)
}

// Ensure driver.Valuer is implemented for database scanning.
func (n NullString) Value() (driver.Value, error)  { return n.NullString.Value() }
func (n NullInt64) Value() (driver.Value, error)    { return n.NullInt64.Value() }
func (n NullFloat64) Value() (driver.Value, error)  { return n.NullFloat64.Value() }

type ItemStatus string

const (
	StatusStaged   ItemStatus = "staged"
	StatusActioned ItemStatus = "actioned"
	StatusMigrated ItemStatus = "migrated"
	StatusKept     ItemStatus = "kept"
)

type Item struct {
	ID             int64          `db:"id" json:"id"`
	RatingKey      string         `db:"rating_key" json:"rating_key"`
	Collection     string         `db:"collection" json:"collection"`
	Title          NullString `db:"title" json:"title"`
	MediaType      string         `db:"media_type" json:"media_type"`
	TmdbID         NullInt64  `db:"tmdb_id" json:"tmdb_id"`
	TvdbID         NullInt64  `db:"tvdb_id" json:"tvdb_id"`
	ImdbID         NullString `db:"imdb_id" json:"imdb_id"`
	ArrID          NullInt64  `db:"arr_id" json:"arr_id"`
	SeasonNumber   NullInt64  `db:"season_number" json:"season_number"`
	ShowRatingKey  NullString `db:"show_rating_key" json:"show_rating_key"`
	SizeBytes      int64          `db:"size_bytes" json:"size_bytes"`
	FirstSeen      string         `db:"first_seen" json:"first_seen"`
	LastSeen       string         `db:"last_seen" json:"last_seen"`
	GraceExpires   string         `db:"grace_expires" json:"grace_expires"`
	Status         string         `db:"status" json:"status"`
	ActionTaken    NullString `db:"action_taken" json:"action_taken"`
	ActionDate     NullString `db:"action_date" json:"action_date"`
	Override       NullString `db:"override" json:"override"`
	OverrideBy     NullString `db:"override_by" json:"override_by"`
}

type RuleResult struct {
	ID          int64  `db:"id" json:"id"`
	RatingKey   string `db:"rating_key" json:"rating_key"`
	Collection  string `db:"collection" json:"collection"`
	RuleName    string `db:"rule_name" json:"rule_name"`
	Passed      bool   `db:"passed" json:"passed"`
	Detail      string `db:"detail" json:"detail"`
	Severity    string `db:"severity" json:"severity"`
	EvaluatedAt string `db:"evaluated_at" json:"evaluated_at"`
}

type User struct {
	ID         int64          `db:"id" json:"id"`
	PlexUserID NullInt64  `db:"plex_user_id" json:"plex_user_id"`
	Username   string         `db:"username" json:"username"`
	Email      NullString `db:"email" json:"email"`
	Thumb      NullString `db:"thumb" json:"thumb"`
	IsProtected bool          `db:"is_protected" json:"is_protected"`
	LastSynced  string        `db:"last_synced" json:"last_synced"`
	Source      string        `db:"source" json:"source"`
}

type WatchHistory struct {
	ID               int64          `db:"id" json:"id"`
	UserID           int64          `db:"user_id" json:"user_id"`
	RatingKey        string         `db:"rating_key" json:"rating_key"`
	Title            NullString `db:"title" json:"title"`
	GrandparentTitle NullString `db:"grandparent_title" json:"grandparent_title"`
	MediaType        NullString `db:"media_type" json:"media_type"`
	SeasonNumber     NullInt64  `db:"season_number" json:"season_number"`
	EpisodeNumber    NullInt64  `db:"episode_number" json:"episode_number"`
	WatchedAt        string         `db:"watched_at" json:"watched_at"`
	PlayDuration     int64          `db:"play_duration" json:"play_duration"`
	MediaDuration    int64          `db:"media_duration" json:"media_duration"`
	PercentComplete  int            `db:"percent_complete" json:"percent_complete"`
}

type ActivityLog struct {
	ID        int64          `db:"id" json:"id"`
	Timestamp string         `db:"timestamp" json:"timestamp"`
	EventType string         `db:"event_type" json:"event_type"`
	Collection NullString `db:"collection" json:"collection"`
	RatingKey  NullString `db:"rating_key" json:"rating_key"`
	Title      NullString `db:"title" json:"title"`
	Detail     NullString `db:"detail" json:"detail"`
}

type DebridCache struct {
	ID        int64  `db:"id" json:"id"`
	RatingKey string `db:"rating_key" json:"rating_key"`
	InfoHash  string `db:"info_hash" json:"info_hash"`
	Provider  string `db:"provider" json:"provider"`
	IsCached  bool   `db:"is_cached" json:"is_cached"`
	CheckedAt string `db:"checked_at" json:"checked_at"`
}

type CollectionConfig struct {
	ID           int64          `db:"id" json:"id"`
	Name         string         `db:"name" json:"name"`
	MediaType    string         `db:"media_type" json:"media_type"`
	Action       string         `db:"action" json:"action"`
	GraceDays    int            `db:"grace_days" json:"grace_days"`
	Criteria     NullString `db:"criteria" json:"criteria"`
	Enabled      bool           `db:"enabled" json:"enabled"`
	ScheduleCron NullString `db:"schedule_cron" json:"schedule_cron"`
	Priority     int            `db:"priority" json:"priority"`
	CreatedAt    string         `db:"created_at" json:"created_at"`
	UpdatedAt    string         `db:"updated_at" json:"updated_at"`
}

type RatingsCache struct {
	ImdbID     string          `db:"imdb_id" json:"imdb_id"`
	ImdbRating NullFloat64 `db:"imdb_rating" json:"imdb_rating"`
	RtScore    NullInt64   `db:"rt_score" json:"rt_score"`
	Metacritic NullInt64   `db:"metacritic" json:"metacritic"`
	FetchedAt  string          `db:"fetched_at" json:"fetched_at"`
}

type ArrInstance struct {
	ID        int64  `db:"id" json:"id"`
	Kind      string `db:"kind" json:"kind"`
	Name      string `db:"name" json:"name"`
	URL       string `db:"url" json:"url"`
	APIKey    string `db:"api_key" json:"api_key"`
	PublicURL string `db:"public_url" json:"public_url"`
	IsDefault bool   `db:"is_default" json:"is_default"`
	CreatedAt string `db:"created_at" json:"created_at"`
	UpdatedAt string `db:"updated_at" json:"updated_at"`
}

type Setting struct {
	Key       string `db:"key" json:"key"`
	Value     string `db:"value" json:"value"`
	UpdatedAt string `db:"updated_at" json:"updated_at"`
}

// ActionStep represents a single step in an action pipeline.
type ActionStep struct {
	Type       string `json:"type"`
	Timing     string `json:"timing,omitempty"`
	Command    string `json:"command,omitempty"`
	InstanceID *int64 `json:"instance_id,omitempty"`
}

// CollectionCriteria is the parsed form of the criteria JSON stored in collection_config.
type CollectionCriteria struct {
	Rules                   map[string]json.RawMessage `json:"rules"`
	Action                  string                     `json:"action"`
	GraceDays               int                        `json:"grace_days"`
	DeleteFiles             bool                       `json:"delete_files"`
	AddImportExclusion      bool                       `json:"add_import_exclusion"`
	ActionPipeline          []ActionStep               `json:"action_pipeline"`
	ProtectedUsers          []string                   `json:"protected_users"`
	ProtectedTags           []string                   `json:"protected_tags"`
	ProtectedCollections    []string                   `json:"protected_collections"`
	LibrarySectionID        *StringOrInt               `json:"library_section_id"`
	LibrarySource           string                     `json:"library_source"`
	Granularity             string                     `json:"granularity"`

	// Parsed rule objects (populated after unmarshal)
	NeverWatched      *NeverWatchedRule      `json:"-"`
	NoKeepTag         *NoKeepTagRule         `json:"-"`
	NoActiveRequest   *NoActiveRequestRule   `json:"-"`
	NoProtectedReq    *NoProtectedRequestRule `json:"-"`
	NotInKeepColl     *NotInKeepCollectionRule `json:"-"`
	ShowEnded         *ShowEndedRule         `json:"-"`
	HighlyRated       *HighlyRatedRule       `json:"-"`
	LowRating         *LowRatingRule         `json:"-"`
	FileSizeMin       *FileSizeMinRule       `json:"-"`
	ReleaseYearBefore *ReleaseYearBeforeRule `json:"-"`
	WatchRatioLow     *WatchRatioLowRule     `json:"-"`
	OldSeason         *OldSeasonRule         `json:"-"`
	RecentlyAdded     *RecentlyAddedRule     `json:"-"`
	PartiallyWatched  *PartiallyWatchedRule  `json:"-"`
	OnWatchlist       *OnWatchlistRule       `json:"-"`
	PlexFavorited     *PlexFavoritedRule     `json:"-"`
	SeriesProtection  *SeriesProtectionRule  `json:"-"`
	NotWatchedRecently *NotWatchedRecentlyRule `json:"-"`
	RequestFulfilled  *RequestFulfilledRule  `json:"-"`
	AvailableOnDebrid *AvailableOnDebridRule `json:"-"`
	OldContent        *OldContentRule        `json:"-"`
}

// StringOrInt handles JSON fields that can be either a string or int.
type StringOrInt struct {
	Value string
}

func (s *StringOrInt) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		s.Value = str
		return nil
	}
	var num int64
	if err := json.Unmarshal(b, &num); err == nil {
		s.Value = json.Number(b).String()
		return nil
	}
	return nil
}

func (s StringOrInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Value)
}

func ParseCriteria(raw string) *CollectionCriteria {
	if raw == "" {
		return DefaultCriteria()
	}
	c := &CollectionCriteria{}
	if err := json.Unmarshal([]byte(raw), c); err != nil {
		return DefaultCriteria()
	}
	c.parseRules()
	return c
}

func DefaultCriteria() *CollectionCriteria {
	c := &CollectionCriteria{
		Rules:              make(map[string]json.RawMessage),
		Action:             "none",
		GraceDays:          30,
		AddImportExclusion: true,
		LibrarySource:      "plex",
		Granularity:        "show",
	}
	return c
}

func (c *CollectionCriteria) parseRules() {
	c.NeverWatched = parseRule[NeverWatchedRule](c.Rules, "never_watched")
	c.NoKeepTag = parseRule[NoKeepTagRule](c.Rules, "no_keep_tag")
	c.NoActiveRequest = parseRule[NoActiveRequestRule](c.Rules, "no_active_request")
	c.NoProtectedReq = parseRule[NoProtectedRequestRule](c.Rules, "no_protected_request")
	c.NotInKeepColl = parseRule[NotInKeepCollectionRule](c.Rules, "not_in_keep_collection")
	c.ShowEnded = parseRule[ShowEndedRule](c.Rules, "show_ended")
	c.HighlyRated = parseRule[HighlyRatedRule](c.Rules, "highly_rated")
	c.LowRating = parseRule[LowRatingRule](c.Rules, "low_rating")
	c.FileSizeMin = parseRule[FileSizeMinRule](c.Rules, "file_size_min")
	c.ReleaseYearBefore = parseRule[ReleaseYearBeforeRule](c.Rules, "release_year_before")
	c.WatchRatioLow = parseRule[WatchRatioLowRule](c.Rules, "watch_ratio_low")
	c.OldSeason = parseRule[OldSeasonRule](c.Rules, "old_season")
	c.RecentlyAdded = parseRule[RecentlyAddedRule](c.Rules, "recently_added")
	c.PartiallyWatched = parseRule[PartiallyWatchedRule](c.Rules, "partially_watched")
	c.OnWatchlist = parseRule[OnWatchlistRule](c.Rules, "on_watchlist")
	c.PlexFavorited = parseRule[PlexFavoritedRule](c.Rules, "plex_favorited")
	c.SeriesProtection = parseRule[SeriesProtectionRule](c.Rules, "series_protection")
	c.NotWatchedRecently = parseRule[NotWatchedRecentlyRule](c.Rules, "not_watched_recently")
	c.RequestFulfilled = parseRule[RequestFulfilledRule](c.Rules, "request_fulfilled")
	c.AvailableOnDebrid = parseRule[AvailableOnDebridRule](c.Rules, "available_on_debrid")
	c.OldContent = parseRule[OldContentRule](c.Rules, "old_content")
}

func parseRule[T any](rules map[string]json.RawMessage, name string) *T {
	raw, ok := rules[name]
	if !ok {
		return nil
	}
	var r T
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil
	}
	return &r
}

func (c *CollectionCriteria) IsRuleEnabled(name string) bool {
	switch name {
	case "never_watched":
		return c.NeverWatched != nil && c.NeverWatched.Enabled
	case "no_keep_tag":
		return c.NoKeepTag != nil && c.NoKeepTag.Enabled
	case "no_active_request":
		return c.NoActiveRequest != nil && c.NoActiveRequest.Enabled
	case "no_protected_request":
		return c.NoProtectedReq != nil && c.NoProtectedReq.Enabled
	case "not_in_keep_collection":
		return c.NotInKeepColl != nil && c.NotInKeepColl.Enabled
	case "show_ended":
		return c.ShowEnded != nil && c.ShowEnded.Enabled
	case "highly_rated":
		return c.HighlyRated != nil && c.HighlyRated.Enabled
	case "low_rating":
		return c.LowRating != nil && c.LowRating.Enabled
	case "file_size_min":
		return c.FileSizeMin != nil && c.FileSizeMin.Enabled
	case "release_year_before":
		return c.ReleaseYearBefore != nil && c.ReleaseYearBefore.Enabled
	case "watch_ratio_low":
		return c.WatchRatioLow != nil && c.WatchRatioLow.Enabled
	case "old_season":
		return c.OldSeason != nil && c.OldSeason.Enabled
	case "recently_added":
		return c.RecentlyAdded != nil && c.RecentlyAdded.Enabled
	case "partially_watched":
		return c.PartiallyWatched != nil && c.PartiallyWatched.Enabled
	case "on_watchlist":
		return c.OnWatchlist != nil && c.OnWatchlist.Enabled
	case "plex_favorited":
		return c.PlexFavorited != nil && c.PlexFavorited.Enabled
	case "series_protection":
		return c.SeriesProtection != nil && c.SeriesProtection.Enabled
	case "not_watched_recently":
		return c.NotWatchedRecently != nil && c.NotWatchedRecently.Enabled
	case "request_fulfilled":
		return c.RequestFulfilled != nil && c.RequestFulfilled.Enabled
	case "available_on_debrid":
		return c.AvailableOnDebrid != nil && c.AvailableOnDebrid.Enabled
	case "old_content":
		return c.OldContent != nil && c.OldContent.Enabled
	}
	return false
}

func (c *CollectionCriteria) NotWatchedRecentlyDays() int {
	if c.NotWatchedRecently != nil && c.NotWatchedRecently.Days > 0 {
		return c.NotWatchedRecently.Days
	}
	return 90
}

func (c *CollectionCriteria) OldContentDays() int {
	if c.OldContent != nil && c.OldContent.Days > 0 {
		return c.OldContent.Days
	}
	return 180
}

func (c *CollectionCriteria) ToJSON() string {
	b, _ := json.Marshal(c)
	return string(b)
}

// NowUTC returns the current time in UTC, used throughout for consistency.
func NowUTC() time.Time { return time.Now().UTC() }

// NowISO returns the current time as an ISO 8601 string.
func NowISO() string { return NowUTC().Format(time.RFC3339) }

// Rule type definitions

type NeverWatchedRule struct {
	Enabled          bool     `json:"enabled"`
	MinDaysUnwatched int      `json:"min_days_unwatched"`
	CheckPlexViews   bool     `json:"check_plex_views"`
	CheckDBPlays     bool     `json:"check_db_plays"`
	ExcludeUsers     []string `json:"exclude_users"`
}

type NoKeepTagRule struct {
	Enabled       bool     `json:"enabled"`
	TagName       string   `json:"tag_name"`
	ProtectedTags []string `json:"protected_tags"`
}

type NoActiveRequestRule struct {
	Enabled bool `json:"enabled"`
}

type NoProtectedRequestRule struct {
	Enabled bool `json:"enabled"`
}

type NotInKeepCollectionRule struct {
	Enabled              bool     `json:"enabled"`
	CollectionName       string   `json:"collection_name"`
	ProtectedCollections []string `json:"protected_collections"`
}

type HighlyRatedRule struct {
	Enabled      bool    `json:"enabled"`
	ImdbMin      float64 `json:"imdb_min"`
	RtMin        int     `json:"rt_min"`
	MetacriticMin int    `json:"metacritic_min"`
	RequireAll   bool    `json:"require_all"`
}

type ShowEndedRule struct {
	Enabled        bool `json:"enabled"`
	IncludeDeleted bool `json:"include_deleted"`
}

type LowRatingRule struct {
	Enabled    bool    `json:"enabled"`
	ImdbMax    float64 `json:"imdb_max"`
	CriticMax  int     `json:"critic_max"`
	RequireAll bool    `json:"require_all"`
}

type FileSizeMinRule struct {
	Enabled bool    `json:"enabled"`
	MinGB   float64 `json:"min_gb"`
}

type ReleaseYearBeforeRule struct {
	Enabled bool `json:"enabled"`
	Year    int  `json:"year"`
}

type WatchRatioLowRule struct {
	Enabled      bool `json:"enabled"`
	MaxPercent   int  `json:"max_percent"`
	RequirePlays bool `json:"require_plays"`
	Days         int  `json:"days"`
}

type OldSeasonRule struct {
	Enabled  bool `json:"enabled"`
	KeepLast int  `json:"keep_last"`
}

type RecentlyAddedRule struct {
	Enabled bool `json:"enabled"`
	Days    int  `json:"days"`
}

type PartiallyWatchedRule struct {
	Enabled bool `json:"enabled"`
	Days    int  `json:"days"`
}

type OnWatchlistRule struct {
	Enabled bool `json:"enabled"`
}

type PlexFavoritedRule struct {
	Enabled bool `json:"enabled"`
}

type SeriesProtectionRule struct {
	Enabled bool `json:"enabled"`
}

type NotWatchedRecentlyRule struct {
	Enabled bool `json:"enabled"`
	Days    int  `json:"days"`
}

type RequestFulfilledRule struct {
	Enabled bool `json:"enabled"`
}

type AvailableOnDebridRule struct {
	Enabled bool `json:"enabled"`
}

type OldContentRule struct {
	Enabled bool `json:"enabled"`
	Days    int  `json:"days"`
}

// Ratings holds extracted rating data from Plex/Jellyfin.
type Ratings struct {
	CriticRating   *int     `json:"critic_rating"`
	AudienceRating *float64 `json:"audience_rating"`
}

// SessionHistoryEntry is a normalized watch history entry from Plex or Jellyfin.
type SessionHistoryEntry struct {
	AccountID        int64  `json:"account_id"`
	RatingKey        string `json:"rating_key"`
	Title            string `json:"title"`
	GrandparentTitle string `json:"grandparent_title"`
	MediaType        string `json:"media_type"`
	SeasonNumber     *int   `json:"season_number"`
	EpisodeNumber    *int   `json:"episode_number"`
	WatchedAt        string `json:"watched_at"`
	ViewOffsetMS     int64  `json:"view_offset_ms"`
	MediaDurationMS  int64  `json:"media_duration_ms"`
}
