package viewer

import (
	"html/template"
	"net/http"
)

func (s *Server) handleLeavingPage(w http.ResponseWriter, r *http.Request) {
	tmplDir := s.TemplateDir
	if tmplDir == "" {
		tmplDir = "/app/templates"
	}

	user := UserFromContext(r.Context())

	displayName := user.Username
	if user.DisplayName.Valid && user.DisplayName.String != "" {
		displayName = user.DisplayName.String
	}

	t, err := template.ParseFiles(
		tmplDir+"/leaving_base.html",
		tmplDir+"/leaving.html",
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := map[string]any{
		"Username":    user.Username,
		"DisplayName": displayName,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t.ExecuteTemplate(w, "leaving_base.html", data)
}

func (s *Server) HandleLoginPage() http.HandlerFunc {
	return s.handleLoginPage
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if user := s.validateSession(r); user != nil {
		if user.IsAdmin {
			http.Redirect(w, r, "/", http.StatusSeeOther)
		} else {
			http.Redirect(w, r, "/leaving", http.StatusSeeOther)
		}
		return
	}

	tmplDir := s.TemplateDir
	if tmplDir == "" {
		tmplDir = "/app/templates"
	}

	t, err := template.ParseFiles(
		tmplDir+"/leaving_base.html",
		tmplDir+"/leaving_login.html",
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t.ExecuteTemplate(w, "leaving_base.html", nil)
}

func (s *Server) handleLeavingItems(w http.ResponseWriter, r *http.Request) {
	var items []StagedItem
	err := s.DB.Select(&items, `
		SELECT rating_key, collection, title, media_type,
		       COALESCE(size_bytes, 0) as size_bytes, grace_expires,
		       first_seen, override, tmdb_id
		FROM items
		WHERE status = 'staged'
		ORDER BY grace_expires ASC`)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		title := it.RatingKey
		if it.Title.Valid && it.Title.String != "" {
			title = it.Title.String
		}
		m := map[string]any{
			"rating_key":    it.RatingKey,
			"collection":    it.Collection,
			"title":         title,
			"media_type":    it.MediaType,
			"size_bytes":    it.SizeBytes,
			"grace_expires": it.GraceExpires,
			"override":      it.Override.String,
		}
		if it.TmdbID.Valid {
			m["tmdb_id"] = it.TmdbID.Int64
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}
