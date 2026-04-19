package web

import (
	"html/template"
	"net/http"
	"strings"
	"unicode"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	TemplateDir string
}

type pageData struct {
	Title string
	Path  string
	Name  string
}

var funcMap = template.FuncMap{
	"hasPrefix": strings.HasPrefix,
	"contains":  strings.Contains,
	"titleize": func(s string) string {
		s = strings.ReplaceAll(s, "-", " ")
		prev := ' '
		return strings.Map(func(r rune) rune {
			if unicode.IsSpace(rune(prev)) {
				prev = r
				return unicode.ToUpper(r)
			}
			prev = r
			return r
		}, s)
	},
}

func (s *Server) render(tmplName, title string, dataFn func(r *http.Request) pageData) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t, err := template.New("").Funcs(funcMap).ParseFiles(
			s.TemplateDir+"/base.html",
			s.TemplateDir+"/"+tmplName,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data := pageData{Title: title, Path: r.URL.Path}
		if dataFn != nil {
			data = dataFn(r)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		t.ExecuteTemplate(w, "base.html", data)
	}
}

func (s *Server) Routes(r chi.Router) {
	if s.TemplateDir == "" {
		s.TemplateDir = "/app/templates"
	}

	r.Get("/", s.render("collections_list.html", "Rules", nil))
	r.Get("/rules", s.render("collections_list.html", "Rules", nil))
	r.Get("/rules/{name}", s.render("collections.html", "Collection", func(r *http.Request) pageData {
		return pageData{Title: "Collection", Path: r.URL.Path, Name: chi.URLParam(r, "name")}
	}))
	r.Get("/rules/{name}/settings", s.render("collection_settings.html", "Collection Settings", func(r *http.Request) pageData {
		return pageData{Title: "Collection Settings", Path: r.URL.Path, Name: chi.URLParam(r, "name")}
	}))
	r.Get("/search", s.render("item_detail.html", "Item Lookup", nil))
	r.Get("/items/{rating_key}", s.render("item_detail.html", "Item Detail", func(r *http.Request) pageData {
		return pageData{Title: "Item Detail", Path: r.URL.Path, Name: chi.URLParam(r, "rating_key")}
	}))
	r.Get("/logs", s.render("logs.html", "Logs", nil))
	r.Get("/activity", s.render("activity.html", "Watch History", nil))
	r.Get("/settings", s.render("settings_media.html", "Settings", nil))
	r.Get("/settings/media", s.render("settings_media.html", "Media Servers", nil))
	r.Get("/settings/statistics", s.render("settings_statistics.html", "Watch History", nil))
	r.Get("/settings/downloads", s.render("settings_downloads.html", "Downloaders", nil))
	r.Get("/settings/notifications", s.render("settings_notifications.html", "Notifications", nil))
	r.Get("/settings/schedule", s.render("settings_schedule.html", "Schedule", nil))
}
