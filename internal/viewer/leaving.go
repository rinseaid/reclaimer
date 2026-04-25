package viewer

import (
	"html/template"
	"net/http"
	"strings"
)

// HandleViewerPage renders the viewer UI using base.html + viewer.html.
func (s *Server) HandleViewerPage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tmplDir := s.TemplateDir
		if tmplDir == "" {
			tmplDir = "/app/templates"
		}

		user := UserFromContext(r.Context())
		displayName := user.Username
		if user.DisplayName.Valid && user.DisplayName.String != "" {
			displayName = user.DisplayName.String
		}

		t, err := template.New("").Funcs(template.FuncMap{
			"hasPrefix": strings.HasPrefix,
			"contains":  strings.Contains,
		}).ParseFiles(
			tmplDir+"/viewer_base.html",
			tmplDir+"/viewer.html",
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := map[string]any{
			"Title":       "Leaving Soon",
			"Path":        r.URL.Path,
			"Username":    user.Username,
			"DisplayName": displayName,
			"IsAdmin":     user.IsAdmin,
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		t.ExecuteTemplate(w, "viewer_base.html", data)
	}
}

// HandleLeavingItems returns the handler for the leaving items API.
func (s *Server) HandleLeavingItems() http.HandlerFunc {
	return s.handleLeavingItems
}

// HandleKeepItem returns the handler for keeping an item.
func (s *Server) HandleKeepItem() http.HandlerFunc {
	return s.handleKeepItem
}

func (s *Server) HandleLoginPage() http.HandlerFunc {
	return s.handleLoginPage
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if user := s.validateSession(r); user != nil {
		if user.IsAdmin {
			http.Redirect(w, r, "/admin", http.StatusSeeOther)
		} else {
			http.Redirect(w, r, "/", http.StatusSeeOther)
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
		       first_seen, override, tmdb_id, show_rating_key, season_number
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
		if it.ShowRatingKey.Valid {
			m["show_rating_key"] = it.ShowRatingKey.String
		}
		if it.SeasonNumber.Valid {
			m["season_number"] = it.SeasonNumber.Int64
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}
