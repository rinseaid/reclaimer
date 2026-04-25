package viewer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

func (s *Server) handleJellyfinLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username and password required"})
		return
	}

	jellyfinURL := strings.TrimRight(s.Config.GetString("jellyfin_url"), "/")
	if jellyfinURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Jellyfin not configured"})
		return
	}

	authURL := jellyfinURL + "/Users/AuthenticateByName"
	authBody := map[string]string{"Username": body.Username, "Pw": body.Password}

	var authResp struct {
		User struct {
			ID   string `json:"Id"`
			Name string `json:"Name"`
		} `json:"User"`
		AccessToken string `json:"AccessToken"`
	}

	err := httpclient.PostJSON(authURL, map[string]string{
		"X-Emby-Authorization": fmt.Sprintf(
			`MediaBrowser Client="Reclaimer", Device="Server", DeviceId="reclaimer-viewer", Version="1.0"`),
	}, authBody, &authResp)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	// Best-effort: fetch email from user profile
	var email, avatarURL string
	if authResp.AccessToken != "" {
		profileURL := fmt.Sprintf("%s/Users/%s", jellyfinURL, authResp.User.ID)
		var profile struct {
			Policy struct {
				// no email here, but we try
			}
		}
		_ = httpclient.GetJSON(profileURL, map[string]string{
			"X-Emby-Token": authResp.AccessToken,
		}, nil, &profile)

		avatarURL = fmt.Sprintf("%s/Users/%s/Images/Primary", jellyfinURL, authResp.User.ID)
	}

	ident := ExternalIdentity{
		Provider:    "jellyfin",
		ProviderID:  authResp.User.ID,
		Username:    authResp.User.Name,
		DisplayName: authResp.User.Name,
		Email:       email,
		AvatarURL:   avatarURL,
	}

	viewerUser, err := s.findOrCreateViewerUser(ident)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create user"})
		return
	}

	if !s.hasAnyAdmins() && !viewerUser.IsAdmin {
		s.DB.Exec(s.DB.Rebind("UPDATE viewer_users SET is_admin = 1 WHERE id = ?"), viewerUser.ID)
		viewerUser.IsAdmin = true
	}

	if err := s.createSession(w, r, viewerUser.ID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"user": viewerUser})
}
