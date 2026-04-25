package viewer

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

func (s *Server) handleKeepItem(w http.ResponseWriter, r *http.Request) {
	ratingKey := chi.URLParam(r, "ratingKey")
	user := UserFromContext(r.Context())

	reason := fmt.Sprintf("Kept by %s via /leaving", user.Username)
	override := "keep"
	if err := s.Store.SetItemOverride(ratingKey, &override, &reason); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to keep item"})
		return
	}

	slog.Info("item kept via viewer",
		"rating_key", ratingKey, "user", user.Username, "provider", user.AuthProvider)

	writeJSON(w, http.StatusOK, map[string]string{"status": "kept"})
}

// HandleMagicKeep handles unauthenticated /keep/{token} links from notification emails.
func (s *Server) HandleMagicKeep(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")

	ratingKey, err := s.validateKeepToken(token)
	if err != nil {
		s.renderKeepResult(w, r, false, "", err.Error())
		return
	}

	// Look up item title
	var title string
	s.DB.Get(&title, s.DB.Rebind(
		"SELECT COALESCE(title, rating_key) FROM items WHERE rating_key = ? LIMIT 1"), ratingKey)
	if title == "" {
		title = ratingKey
	}

	override := "keep"
	reason := "Kept via magic link"
	if err := s.Store.SetItemOverride(ratingKey, &override, &reason); err != nil {
		s.renderKeepResult(w, r, false, title, "failed to keep item")
		return
	}

	// Mark token as used
	s.DB.Exec(s.DB.Rebind(
		"UPDATE keep_tokens SET used_at = ? WHERE token = ?"), nowISO(), token)

	slog.Info("item kept via magic link", "rating_key", ratingKey)
	s.renderKeepResult(w, r, true, title, "")
}

func (s *Server) renderKeepResult(w http.ResponseWriter, r *http.Request, success bool, title, errMsg string) {
	tmplDir := s.TemplateDir
	if tmplDir == "" {
		tmplDir = "/app/templates"
	}

	t, err := template.ParseFiles(
		tmplDir+"/leaving_base.html",
		tmplDir+"/leaving_keep_result.html",
	)
	if err != nil {
		if success {
			fmt.Fprintf(w, "<h1>%s has been kept!</h1>", template.HTMLEscapeString(title))
		} else {
			http.Error(w, errMsg, http.StatusBadRequest)
		}
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t.ExecuteTemplate(w, "leaving_base.html", map[string]any{
		"Success": success,
		"Title":   title,
		"Error":   errMsg,
	})
}

// GenerateKeepToken creates a signed token for a given rating_key.
func (s *Server) GenerateKeepToken(ratingKey, createdBy string) (string, error) {
	secret := s.getKeepTokenSecret()

	ttlHours := s.Config.GetInt("viewer_keep_token_ttl_hours")
	if ttlHours <= 0 {
		ttlHours = 72
	}
	expiresAt := time.Now().UTC().Add(time.Duration(ttlHours) * time.Hour)

	payload := ratingKey + "|" + strconv.FormatInt(expiresAt.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	sig := mac.Sum(nil)

	token := base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(sig)

	_, err := s.DB.Exec(s.DB.Rebind(`
		INSERT INTO keep_tokens (token, rating_key, expires_at, created_by)
		VALUES (?, ?, ?, ?)`),
		token, ratingKey, expiresAt.Format(time.RFC3339), nullStr(createdBy))
	if err != nil {
		return "", fmt.Errorf("store keep token: %w", err)
	}

	return token, nil
}

func (s *Server) validateKeepToken(token string) (string, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid token format")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid token encoding")
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("invalid signature encoding")
	}

	secret := s.getKeepTokenSecret()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payloadBytes)
	expected := mac.Sum(nil)

	if !hmac.Equal(sigBytes, expected) {
		return "", fmt.Errorf("invalid token signature")
	}

	payload := string(payloadBytes)
	pipeIdx := strings.LastIndex(payload, "|")
	if pipeIdx < 0 {
		return "", fmt.Errorf("invalid token payload")
	}

	ratingKey := payload[:pipeIdx]
	expiryStr := payload[pipeIdx+1:]
	expiry, err := strconv.ParseInt(expiryStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid expiry")
	}

	if time.Now().UTC().Unix() > expiry {
		return "", fmt.Errorf("token expired")
	}

	// Check if already used
	var usedAt *string
	s.DB.Get(&usedAt, s.DB.Rebind(
		"SELECT used_at FROM keep_tokens WHERE token = ?"), token)
	if usedAt != nil && *usedAt != "" {
		return "", fmt.Errorf("token already used")
	}

	return ratingKey, nil
}

func (s *Server) getKeepTokenSecret() string {
	secret := s.Config.GetString("viewer_keep_token_secret")
	if secret == "" {
		b := make([]byte, 32)
		rand.Read(b)
		secret = hex.EncodeToString(b)
		s.Config.Update(map[string]any{"viewer_keep_token_secret": secret})
	}
	return secret
}
