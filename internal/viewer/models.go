package viewer

import "database/sql"

type ViewerUser struct {
	ID             int64          `db:"id"              json:"id"`
	Username       string         `db:"username"        json:"username"`
	DisplayName    sql.NullString `db:"display_name"    json:"display_name"`
	Email          sql.NullString `db:"email"           json:"email"`
	PasswordHash   sql.NullString `db:"password_hash"   json:"-"`
	AuthProvider   string         `db:"auth_provider"   json:"auth_provider"`
	AuthProviderID sql.NullString `db:"auth_provider_id" json:"-"`
	AvatarURL      sql.NullString `db:"avatar_url"      json:"avatar_url"`
	IsActive       bool           `db:"is_active"       json:"is_active"`
	CreatedAt      string         `db:"created_at"      json:"created_at"`
	UpdatedAt      string         `db:"updated_at"      json:"updated_at"`
}

type ViewerSession struct {
	ID        string `db:"id"`
	UserID    int64  `db:"user_id"`
	ExpiresAt string `db:"expires_at"`
	CreatedAt string `db:"created_at"`
	UserAgent string `db:"user_agent"`
	IPAddress string `db:"ip_address"`
}

type KeepToken struct {
	ID        int64          `db:"id"`
	Token     string         `db:"token"`
	RatingKey string         `db:"rating_key"`
	ExpiresAt string         `db:"expires_at"`
	UsedAt    sql.NullString `db:"used_at"`
	CreatedBy sql.NullString `db:"created_by"`
	CreatedAt string         `db:"created_at"`
}

type StagedItem struct {
	RatingKey    string         `db:"rating_key"     json:"rating_key"`
	Collection   string         `db:"collection"     json:"collection"`
	Title        sql.NullString `db:"title"          json:"title"`
	MediaType    string         `db:"media_type"     json:"media_type"`
	SizeBytes    int64          `db:"size_bytes"     json:"size_bytes"`
	GraceExpires string         `db:"grace_expires"  json:"grace_expires"`
	FirstSeen    string         `db:"first_seen"     json:"first_seen"`
	Override     sql.NullString `db:"override"       json:"override"`
	TmdbID       sql.NullInt64  `db:"tmdb_id"        json:"tmdb_id"`
}
