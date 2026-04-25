package viewer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

const plexClientID = "reclaimer-viewer"

func (s *Server) baseURL(r *http.Request) string {
	if base := s.Config.GetString("leaving_base_url"); base != "" {
		return strings.TrimRight(base, "/")
	}
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (s *Server) handlePlexRedirect(w http.ResponseWriter, r *http.Request) {
	body := strings.NewReader("strong=true")
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "https://plex.tv/api/v2/pins", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Client-Identifier", plexClientID)
	req.Header.Set("X-Plex-Product", "Reclaimer")

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		http.Error(w, "failed to create Plex PIN", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var pin struct {
		ID   int    `json:"id"`
		Code string `json:"code"`
	}
	json.NewDecoder(resp.Body).Decode(&pin)

	callbackURL := fmt.Sprintf("/auth/plex/callback?pin_id=%d", pin.ID)

	authURL := fmt.Sprintf(
		"https://app.plex.tv/auth#?clientID=%s&code=%s&context%%5Bdevice%%5D%%5Bproduct%%5D=Reclaimer&forwardUrl=%s",
		plexClientID, pin.Code, url.QueryEscape(s.baseURL(r)+callbackURL))

	http.Redirect(w, r, authURL, http.StatusFound)
}

func (s *Server) handlePlexCallback(w http.ResponseWriter, r *http.Request) {
	pinID := r.URL.Query().Get("pin_id")
	if pinID == "" {
		http.Error(w, "missing pin_id", http.StatusBadRequest)
		return
	}

	req, _ := http.NewRequestWithContext(r.Context(), "GET",
		fmt.Sprintf("https://plex.tv/api/v2/pins/%s", pinID), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Client-Identifier", plexClientID)

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		http.Error(w, "failed to check Plex PIN", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var pin struct {
		AuthToken string `json:"authToken"`
	}
	json.NewDecoder(resp.Body).Decode(&pin)

	if pin.AuthToken == "" {
		http.Error(w, "Plex authentication was not completed", http.StatusUnauthorized)
		return
	}

	user, err := s.plexUserFromToken(r, pin.AuthToken)
	if err != nil {
		http.Error(w, "failed to get Plex user info", http.StatusUnauthorized)
		return
	}

	viewerUser, err := s.findOrCreateViewerUser(*user)
	if err != nil {
		http.Error(w, "failed to create user", http.StatusInternalServerError)
		return
	}

	if !s.hasAnyAdmins() && !viewerUser.IsAdmin {
		s.DB.Exec(s.DB.Rebind("UPDATE viewer_users SET is_admin = 1 WHERE id = ?"), viewerUser.ID)
		viewerUser.IsAdmin = true
	}

	if err := s.createSession(w, r, viewerUser.ID); err != nil {
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) plexUserFromToken(r *http.Request, authToken string) (*ExternalIdentity, error) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", "https://plex.tv/api/v2/user", nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Client-Identifier", plexClientID)
	req.Header.Set("X-Plex-Token", authToken)

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("plex user lookup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("plex returned %d", resp.StatusCode)
	}

	var plexUser struct {
		ID       int    `json:"id"`
		Username string `json:"username"`
		Title    string `json:"title"`
		Email    string `json:"email"`
		Thumb    string `json:"thumb"`
	}
	json.NewDecoder(resp.Body).Decode(&plexUser)

	displayName := plexUser.Title
	if displayName == "" {
		displayName = plexUser.Username
	}

	return &ExternalIdentity{
		Provider:    "plex",
		ProviderID:  fmt.Sprintf("%d", plexUser.ID),
		Username:    plexUser.Username,
		DisplayName: displayName,
		Email:       plexUser.Email,
		AvatarURL:   plexUser.Thumb,
	}, nil
}
