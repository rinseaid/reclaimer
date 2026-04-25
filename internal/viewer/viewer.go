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

func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()

	// Auth endpoints (no auth required)
	r.Get("/auth/status", s.handleAuthStatus)
	r.Post("/auth/logout", s.handleLogout)
	r.Post("/auth/login", s.handleLocalLogin)
	r.Post("/auth/register", s.handleLocalRegister)
	r.Post("/auth/plex/pin", s.handlePlexPin)
	r.Get("/auth/plex/pin/{pinId}", s.handlePlexPinCheck)
	r.Post("/auth/jellyfin/login", s.handleJellyfinLogin)
	r.Get("/auth/oidc/authorize", s.handleOIDCAuthorize)
	r.Get("/auth/oidc/callback", s.handleOIDCCallback)
	r.Get("/auth/me", s.handleMe)

	// Login page (no auth)
	r.Get("/login", s.handleLoginPage)

	// Protected routes
	r.Group(func(r chi.Router) {
		r.Use(s.requireViewer)
		r.Get("/", s.handleLeavingPage)
		r.Get("/items", s.handleLeavingItems)
		r.Post("/items/{ratingKey}/keep", s.handleKeepItem)
	})

	return r
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	plexURL := s.Config.GetString("plex_url")
	jellyfinURL := s.Config.GetString("jellyfin_url")
	oidcIssuer := s.Config.GetString("viewer_oidc_issuer_url")
	oidcClientID := s.Config.GetString("viewer_oidc_client_id")

	writeJSON(w, http.StatusOK, map[string]any{
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
			"enabled": s.Config.GetBool("viewer_local_enabled"),
		},
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.destroySession(w, r)
	if isJSONRequest(r) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	} else {
		http.Redirect(w, r, "/leaving/login", http.StatusSeeOther)
	}
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	user := s.validateSession(r)
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not authenticated"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
