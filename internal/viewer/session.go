package viewer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

)

type contextKey string

const viewerUserKey contextKey = "viewerUser"

func UserFromContext(ctx context.Context) *ViewerUser {
	u, _ := ctx.Value(viewerUserKey).(*ViewerUser)
	return u
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request, userID int64) error {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	sid := hex.EncodeToString(b)

	ttlHours := s.Config.GetInt("viewer_session_ttl_hours")
	if ttlHours <= 0 {
		ttlHours = 168
	}
	expires := time.Now().UTC().Add(time.Duration(ttlHours) * time.Hour)

	_, err := s.DB.Exec(
		s.DB.Rebind("INSERT INTO viewer_sessions (id, user_id, expires_at, user_agent, ip_address) VALUES (?, ?, ?, ?, ?)"),
		sid, userID, expires.Format(time.RFC3339), r.UserAgent(), r.RemoteAddr,
	)
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "reclaimer_viewer",
		Value:    sid,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
		MaxAge:   ttlHours * 3600,
	})
	return nil
}

func (s *Server) validateSession(r *http.Request) *ViewerUser {
	cookie, err := r.Cookie("reclaimer_viewer")
	if err != nil || cookie.Value == "" {
		return nil
	}

	var sess ViewerSession
	err = s.DB.Get(&sess,
		s.DB.Rebind("SELECT id, user_id, expires_at FROM viewer_sessions WHERE id = ?"),
		cookie.Value)
	if err != nil {
		return nil
	}

	expires, err := parseTime(sess.ExpiresAt)
	if err != nil || time.Now().UTC().After(expires) {
		s.DB.Exec(s.DB.Rebind("DELETE FROM viewer_sessions WHERE id = ?"), sess.ID)
		return nil
	}

	var user ViewerUser
	err = s.DB.Get(&user,
		s.DB.Rebind("SELECT * FROM viewer_users WHERE id = ? AND is_active = 1"),
		sess.UserID)
	if err != nil {
		return nil
	}
	return &user
}

func (s *Server) destroySession(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("reclaimer_viewer")
	if err == nil && cookie.Value != "" {
		s.DB.Exec(s.DB.Rebind("DELETE FROM viewer_sessions WHERE id = ?"), cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "reclaimer_viewer",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := s.validateSession(r)
		if user == nil {
			if isJSONRequest(r) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			} else {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
			}
			return
		}
		ctx := context.WithValue(r.Context(), viewerUserKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := s.validateSession(r)
		if user == nil {
			if isJSONRequest(r) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			} else {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
			}
			return
		}
		if !user.IsAdmin {
			if isJSONRequest(r) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin access required"})
			} else {
				http.Redirect(w, r, "/leaving", http.StatusSeeOther)
			}
			return
		}
		ctx := context.WithValue(r.Context(), viewerUserKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) CleanupSessions() {
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.DB.Exec(
		s.DB.Rebind("DELETE FROM viewer_sessions WHERE expires_at < ?"), now)
	if err != nil {
		slog.Error("session cleanup failed", "error", err)
		return
	}
	if n, _ := result.RowsAffected(); n > 0 {
		slog.Debug("cleaned up expired viewer sessions", "count", n)
	}
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, &time.ParseError{Value: s}
}

func isJSONRequest(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	return accept == "application/json" || r.Header.Get("Content-Type") == "application/json"
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

