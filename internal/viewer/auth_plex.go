package viewer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

const plexClientID = "reclaimer-viewer"

func (s *Server) handlePlexPin(w http.ResponseWriter, r *http.Request) {
	body := strings.NewReader("strong=true")
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "https://plex.tv/api/v2/pins", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Client-Identifier", plexClientID)
	req.Header.Set("X-Plex-Product", "Reclaimer")

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var pin struct {
		ID   int    `json:"id"`
		Code string `json:"code"`
	}
	json.NewDecoder(resp.Body).Decode(&pin)

	authURL := fmt.Sprintf(
		"https://app.plex.tv/auth#?clientID=%s&code=%s&context%%5Bdevice%%5D%%5Bproduct%%5D=Reclaimer",
		plexClientID, pin.Code)

	writeJSON(w, http.StatusOK, map[string]any{
		"pin_id":   pin.ID,
		"code":     pin.Code,
		"auth_url": authURL,
	})
}

func (s *Server) handlePlexPinCheck(w http.ResponseWriter, r *http.Request) {
	pinID := chi.URLParam(r, "pinId")

	req, _ := http.NewRequestWithContext(r.Context(), "GET",
		fmt.Sprintf("https://plex.tv/api/v2/pins/%s", pinID), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Client-Identifier", plexClientID)

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var pin struct {
		AuthToken string `json:"authToken"`
	}
	json.NewDecoder(resp.Body).Decode(&pin)

	if pin.AuthToken == "" {
		writeJSON(w, http.StatusOK, map[string]any{"waiting": true})
		return
	}

	// Fetch user info from Plex
	user, err := s.plexUserFromToken(r, pin.AuthToken)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}

	viewerUser, err := s.findOrCreateViewerUser(*user)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create user"})
		return
	}

	if err := s.createSession(w, r, viewerUser.ID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"waiting": false,
		"user":    viewerUser,
	})
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
