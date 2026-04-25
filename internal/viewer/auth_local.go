package viewer

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"golang.org/x/crypto/bcrypt"
)

func (s *Server) handleLocalLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" || body.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username and password required"})
		return
	}

	var user ViewerUser
	err := s.DB.Get(&user,
		s.DB.Rebind("SELECT * FROM viewer_users WHERE username = ? AND auth_provider = 'local' AND is_active = 1"),
		body.Username)

	if err != nil || !user.PasswordHash.Valid {
		// Timing-safe: always run bcrypt even for invalid users
		bcrypt.CompareHashAndPassword([]byte("$2a$12$000000000000000000000000000000000000000000000000000000"), []byte(body.Password))
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash.String), []byte(body.Password)); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	if !s.hasAnyAdmins() && !user.IsAdmin {
		s.DB.Exec(s.DB.Rebind("UPDATE viewer_users SET is_admin = 1 WHERE id = ?"), user.ID)
		user.IsAdmin = true
		slog.Info("bootstrap: promoted user to admin on login", "username", user.Username)
	}

	if err := s.createSession(w, r, user.ID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

func (s *Server) handleLocalRegister(w http.ResponseWriter, r *http.Request) {
	bootstrap := !s.hasAnyAdmins()
	if !bootstrap && !s.Config.GetBool("viewer_local_enabled") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "local auth disabled"})
		return
	}

	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Email    string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" || body.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username and password required"})
		return
	}

	if len(body.Password) < 8 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "password must be at least 8 characters"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), 12)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to hash password"})
		return
	}

	result, err := s.DB.Exec(s.DB.Rebind(`
		INSERT INTO viewer_users (username, display_name, email, password_hash, auth_provider, auth_provider_id)
		VALUES (?, ?, ?, ?, 'local', ?)`),
		body.Username, body.Username, nullStr(body.Email), string(hash), body.Username)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "username already exists"})
		return
	}

	id, _ := result.LastInsertId()
	if bootstrap {
		s.DB.Exec(s.DB.Rebind("UPDATE viewer_users SET is_admin = 1 WHERE id = ?"), id)
		slog.Info("bootstrap: first user promoted to admin", "username", body.Username)
	}
	if err := s.createSession(w, r, id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
		return
	}

	user, _ := s.getViewerUserByID(id)
	writeJSON(w, http.StatusCreated, map[string]any{"user": user})
}
