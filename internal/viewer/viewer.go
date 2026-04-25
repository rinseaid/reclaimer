package viewer

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/rinseaid/reclaimer/internal/config"
	"github.com/rinseaid/reclaimer/internal/database"
	"github.com/rinseaid/reclaimer/internal/store"
)

type Server struct {
	Store       *store.Store
	Config      *config.Config
	DB          *database.DB
	TemplateDir string
}

// AuthRoutes returns routes mounted at /auth (public, no session required).
func (s *Server) AuthRoutes() chi.Router {
	r := chi.NewRouter()
	r.Get("/status", s.handleAuthStatus)
	r.Post("/logout", s.handleLogout)
	r.Post("/login", s.handleLocalLogin)
	r.Post("/register", s.handleLocalRegister)
	r.Get("/plex/redirect", s.handlePlexRedirect)
	r.Get("/plex/callback", s.handlePlexCallback)
	r.Post("/jellyfin/login", s.handleJellyfinLogin)
	r.Get("/oidc/authorize", s.handleOIDCAuthorize)
	r.Get("/oidc/callback", s.handleOIDCCallback)
	r.Get("/me", s.handleMe)
	return r
}

// LeavingRoutes returns routes mounted at /leaving (requires auth).
func (s *Server) LeavingRoutes() chi.Router {
	r := chi.NewRouter()
	r.Use(s.requireAuth)
	r.Get("/", s.handleLeavingPage)
	r.Get("/items", s.handleLeavingItems)
	r.Post("/items/{ratingKey}/keep", s.handleKeepItem)
	return r
}

func (s *Server) hasLocalUsers() bool {
	var count int
	s.DB.Get(&count, "SELECT COUNT(*) FROM viewer_users WHERE auth_provider = 'local'")
	return count > 0
}

func (s *Server) hasAnyAdmins() bool {
	var count int
	s.DB.Get(&count, "SELECT COUNT(*) FROM viewer_users WHERE is_admin = 1")
	return count > 0
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	bootstrap := !s.hasAnyAdmins()

	plexURL := s.Config.GetString("plex_url")
	jellyfinURL := s.Config.GetString("jellyfin_url")
	oidcIssuer := s.Config.GetString("viewer_oidc_issuer_url")
	oidcClientID := s.Config.GetString("viewer_oidc_client_id")

	writeJSON(w, http.StatusOK, map[string]any{
		"bootstrap": bootstrap,
		"plex": map[string]any{
			"enabled":    s.Config.GetBool("viewer_plex_enabled"),
			"configured": plexURL != "",
		},
		"jellyfin": map[string]any{
			"enabled":    s.Config.GetBool("viewer_jellyfin_enabled"),
			"configured": jellyfinURL != "",
		},
		"oidc": map[string]any{
			"enabled":      s.Config.GetBool("viewer_oidc_enabled"),
			"configured":   oidcIssuer != "" && oidcClientID != "",
			"display_name": s.Config.GetString("viewer_oidc_display_name"),
		},
		"local": map[string]any{
			"enabled":   s.Config.GetBool("viewer_local_enabled"),
			"has_users": s.hasLocalUsers(),
		},
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.destroySession(w, r)
	if isJSONRequest(r) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	} else {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	}
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	user := s.validateSession(r)
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not authenticated"})
		return
	}
	displayName := user.Username
	if user.DisplayName.Valid && user.DisplayName.String != "" {
		displayName = user.DisplayName.String
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": map[string]any{
		"id":           user.ID,
		"username":     user.Username,
		"display_name": displayName,
		"is_admin":     user.IsAdmin,
	}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
