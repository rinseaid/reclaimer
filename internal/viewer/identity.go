package viewer

import (
	"database/sql"
	"fmt"
	"log/slog"
)

type ExternalIdentity struct {
	Provider    string // "plex", "jellyfin", "oidc"
	ProviderID  string
	Username    string
	DisplayName string
	Email       string
	AvatarURL   string
}

func (s *Server) findOrCreateViewerUser(ident ExternalIdentity) (*ViewerUser, error) {
	// 1. Lookup by provider + provider_id (fast path)
	var user ViewerUser
	err := s.DB.Get(&user,
		s.DB.Rebind("SELECT * FROM viewer_users WHERE auth_provider = ? AND auth_provider_id = ?"),
		ident.Provider, ident.ProviderID)
	if err == nil {
		s.updateViewerProfile(&user, ident)
		return &user, nil
	}

	// 2. Lookup by email (cross-provider linking)
	if ident.Email != "" {
		err = s.DB.Get(&user,
			s.DB.Rebind("SELECT * FROM viewer_users WHERE email = ? AND email != ''"),
			ident.Email)
		if err == nil {
			// Link this provider to the existing user
			s.DB.Exec(s.DB.Rebind(
				"UPDATE viewer_users SET auth_provider = ?, auth_provider_id = ?, updated_at = ? WHERE id = ?"),
				ident.Provider, ident.ProviderID, nowISO(), user.ID)
			s.updateViewerProfile(&user, ident)
			slog.Info("linked viewer user to new provider",
				"user_id", user.ID, "provider", ident.Provider, "username", ident.Username)
			return &user, nil
		}
	}

	// 3. Create new user
	username := ident.Username
	if username == "" {
		username = ident.DisplayName
	}
	if username == "" {
		username = fmt.Sprintf("%s-%s", ident.Provider, ident.ProviderID)
	}

	displayName := ident.DisplayName
	if displayName == "" {
		displayName = username
	}

	result, err := s.DB.Exec(s.DB.Rebind(`
		INSERT INTO viewer_users (username, display_name, email, auth_provider, auth_provider_id, avatar_url)
		VALUES (?, ?, ?, ?, ?, ?)`),
		username, displayName, nullStr(ident.Email), ident.Provider, ident.ProviderID, nullStr(ident.AvatarURL))
	if err != nil {
		return nil, fmt.Errorf("create viewer user: %w", err)
	}

	id, _ := result.LastInsertId()
	slog.Info("created viewer user",
		"id", id, "provider", ident.Provider, "username", username)

	return s.getViewerUserByID(id)
}

func (s *Server) updateViewerProfile(user *ViewerUser, ident ExternalIdentity) {
	updates := map[string]any{}
	if ident.DisplayName != "" && (!user.DisplayName.Valid || user.DisplayName.String != ident.DisplayName) {
		updates["display_name"] = ident.DisplayName
	}
	if ident.Email != "" && (!user.Email.Valid || user.Email.String != ident.Email) {
		updates["email"] = ident.Email
	}
	if ident.AvatarURL != "" && (!user.AvatarURL.Valid || user.AvatarURL.String != ident.AvatarURL) {
		updates["avatar_url"] = ident.AvatarURL
	}
	if len(updates) == 0 {
		return
	}
	updates["updated_at"] = nowISO()

	query := "UPDATE viewer_users SET "
	args := []any{}
	i := 0
	for col, val := range updates {
		if i > 0 {
			query += ", "
		}
		query += col + " = ?"
		args = append(args, val)
		i++
	}
	query += " WHERE id = ?"
	args = append(args, user.ID)
	s.DB.Exec(s.DB.Rebind(query), args...)
}

func (s *Server) getViewerUserByID(id int64) (*ViewerUser, error) {
	var user ViewerUser
	err := s.DB.Get(&user,
		s.DB.Rebind("SELECT * FROM viewer_users WHERE id = ?"), id)
	return &user, err
}

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
