package api

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rinseaid/reclaimer/internal/config"
	"github.com/rinseaid/reclaimer/internal/database"
	"github.com/rinseaid/reclaimer/internal/models"
	"github.com/rinseaid/reclaimer/internal/orchestrator"
	"github.com/rinseaid/reclaimer/internal/services/httpclient"
	"github.com/rinseaid/reclaimer/internal/store"
)

type Server struct {
	Store        *store.Store
	Config       *config.Config
	DB           *database.DB
	Orchestrator *orchestrator.Orchestrator

	runMu       sync.Mutex
	running     bool
	runProgress map[string]any
}

func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()

	// Dashboard
	r.Get("/dashboard", s.handleDashboard)

	// Collections
	r.Route("/collections", func(r chi.Router) {
		r.Get("/", s.handleListCollections)
		r.Post("/", s.handleCreateCollection)
		r.Get("/export", s.handleExportCollections)
		r.Post("/import", s.handleImportCollections)
		r.Get("/{name}", s.handleGetCollectionItems)
		r.Get("/{name}/config", s.handleGetCollectionConfig)
		r.Put("/{name}/config", s.handleUpdateCollectionConfig)
		r.Delete("/{name}", s.handleDeleteCollection)
		r.Get("/{name}/keep-members", s.handleKeepMembers)
	})

	// Items
	r.Route("/items", func(r chi.Router) {
		r.Get("/search", s.handleSearchItems)
		r.Get("/watchlist-members", s.handleWatchlistMembers)
		r.Post("/bulk-action", s.handleBulkAction)
		r.Get("/{ratingKey}", s.handleGetItem)
		r.Get("/{ratingKey}/evaluate", s.handleEvaluateItem)
		r.Get("/{ratingKey}/seerr-requests", s.handleSeerrRequests)
		r.Post("/{ratingKey}/action", s.handleItemAction)
		r.Post("/{ratingKey}/add-to-keep", s.handleAddToKeep)
		r.Delete("/{ratingKey}/keep-collection", s.handleRemoveFromKeep)
		r.Post("/{ratingKey}/watchlist", s.handleAddToWatchlist)
		r.Delete("/{ratingKey}/watchlist", s.handleRemoveFromWatchlist)
		r.Get("/{ratingKey}/poster", s.handlePoster)
	})

	// Settings
	r.Route("/settings", func(r chi.Router) {
		r.Get("/", s.handleGetSettings)
		r.Put("/", s.handleUpdateSettings)
		r.Get("/plex-libraries", s.handlePlexLibraries)
		r.Get("/plex-collections", s.handlePlexCollections)
		r.Get("/jellyfin-libraries", s.handleJellyfinLibraries)
		r.Get("/jellyfin-collections", s.handleJellyfinCollections)
		r.Get("/recycle-bin-status", s.handleRecycleBinStatus)
		r.Get("/arr-tags", s.handleArrTags)
		r.Post("/test-connection", s.handleTestConnection)
	})

	// Activity
	r.Route("/activity", func(r chi.Router) {
		r.Get("/", s.handleGetActivity)
		r.Delete("/", s.handleClearActivity)
		r.Get("/watch", s.handleWatchHistory)
		r.Get("/watch/segments", s.handleWatchSegments)
	})

	// Users
	r.Get("/users", s.handleListUsers)

	// Instances
	r.Route("/instances", func(r chi.Router) {
		r.Get("/", s.handleListInstances)
		r.Post("/", s.handleCreateInstance)
		r.Post("/test", s.handleTestInstance)
		r.Get("/{id}", s.handleGetInstance)
		r.Patch("/{id}", s.handleUpdateInstance)
		r.Delete("/{id}", s.handleDeleteInstance)
	})

	// Run
	r.Route("/run", func(r chi.Router) {
		r.Post("/", s.handleTriggerRun)
		r.Get("/status", s.handleRunStatus)
		r.Get("/progress", s.handleRunProgress)
		r.Post("/sync-users", s.handleSyncUsers)
	})

	return r
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("json encode", "error", err)
	}
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func queryStr(r *http.Request, key, def string) string {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	return v
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(s string) string {
	return strings.Trim(slugRe.ReplaceAllString(strings.ToLower(s), "-"), "-")
}

// resolveCollectionName finds the collection config whose slugified name matches the URL slug.
func (s *Server) resolveCollectionName(slug string) (*models.CollectionConfig, error) {
	configs, err := s.Store.ListCollectionConfigs()
	if err != nil {
		return nil, err
	}
	for i := range configs {
		if slugify(configs[i].Name) == slug {
			return &configs[i], nil
		}
	}
	return nil, sql.ErrNoRows
}

func redactAPIKey(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 8 {
		return "••••••••"
	}
	return key[:4] + "••••" + key[len(key)-4:]
}

func redactInstance(inst models.ArrInstance) models.ArrInstance {
	inst.APIKey = redactAPIKey(inst.APIKey)
	return inst
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	type collStat struct {
		Collection string `db:"collection"`
		Status     string `db:"status"`
		Count      int    `db:"cnt"`
		TotalSize  int64  `db:"total_size"`
	}
	var stats []collStat
	err := s.DB.Select(&stats, `
		SELECT collection, status, COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as total_size
		FROM items GROUP BY collection, status
	`)
	if err != nil {
		slog.Error("dashboard stats", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to load stats"})
		return
	}

	configs, _ := s.Store.ListCollectionConfigs()
	configMap := map[string]models.CollectionConfig{}
	for _, cfg := range configs {
		configMap[cfg.Name] = cfg
	}

	collections := map[string]map[string]any{}
	var totalTracked int
	var totalSizeBytes int64
	var pendingActions int

	for _, cfg := range configs {
		var pipeline any
		if cfg.Criteria.Valid {
			var parsed models.CollectionCriteria
			if json.Unmarshal([]byte(cfg.Criteria.String), &parsed) == nil {
				pipeline = parsed.ActionPipeline
			}
		}
		collections[cfg.Name] = map[string]any{
			"staged": 0, "actioned": 0, "migrated": 0, "kept": 0, "total": 0, "total_bytes": int64(0),
			"grace_days":      cfg.GraceDays,
			"action_pipeline": pipeline,
			"enabled":         cfg.Enabled,
			"priority":        cfg.Priority,
		}
	}

	for _, st := range stats {
		if _, ok := collections[st.Collection]; !ok {
			collections[st.Collection] = map[string]any{
				"staged": 0, "actioned": 0, "migrated": 0, "kept": 0, "total": 0, "total_bytes": int64(0),
			}
		}
		c := collections[st.Collection]
		c[st.Status] = st.Count
		c["total"] = c["total"].(int) + st.Count
		c["total_bytes"] = c["total_bytes"].(int64) + st.TotalSize
		totalTracked += st.Count
		totalSizeBytes += st.TotalSize
		if st.Status == "staged" {
			pendingActions += st.Count
		}
	}

	var lastRun sql.NullString
	_ = s.DB.Get(&lastRun, `SELECT MAX(timestamp) FROM activity_log WHERE event_type = 'run_completed'`)

	var lastRunObj any
	if lastRun.Valid && lastRun.String != "" {
		lastRunObj = map[string]string{"timestamp": lastRun.String}
	}

	settings := s.Config.GetAll(true)

	writeJSON(w, http.StatusOK, map[string]any{
		"collections":      collections,
		"total_tracked":    totalTracked,
		"total_size_bytes": totalSizeBytes,
		"pending_actions":  pendingActions,
		"last_run":         lastRunObj,
		"settings":         settings,
	})
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

func (s *Server) handleListCollections(w http.ResponseWriter, r *http.Request) {
	configs, err := s.Store.ListCollectionConfigs()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	type itemCount struct {
		Collection string `db:"collection"`
		Count      int    `db:"cnt"`
	}
	var counts []itemCount
	_ = s.DB.Select(&counts, "SELECT collection, COUNT(*) as cnt FROM items GROUP BY collection")
	countMap := map[string]int{}
	for _, c := range counts {
		countMap[c.Collection] = c.Count
	}

	result := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		m := map[string]any{
			"id":            cfg.ID,
			"name":          cfg.Name,
			"slug":          slugify(cfg.Name),
			"media_type":    cfg.MediaType,
			"action":        cfg.Action,
			"grace_days":    cfg.GraceDays,
			"enabled":       cfg.Enabled,
			"schedule_cron": cfg.ScheduleCron,
			"priority":      cfg.Priority,
			"created_at":    cfg.CreatedAt,
			"updated_at":    cfg.UpdatedAt,
			"item_count":    countMap[cfg.Name],
		}
		if cfg.Criteria.Valid {
			criteria := models.ParseCriteria(cfg.Criteria.String)
			m["criteria"] = criteria
		}
		result = append(result, m)
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleCreateCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string          `json:"name"`
		MediaType    string          `json:"media_type"`
		Action       string          `json:"action"`
		GraceDays    int             `json:"grace_days"`
		Criteria     json.RawMessage `json:"criteria"`
		Enabled      bool            `json:"enabled"`
		ScheduleCron *string         `json:"schedule_cron"`
		Priority     int             `json:"priority"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if body.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}

	criteriaStr := "{}"
	if body.Criteria != nil {
		criteriaStr = string(body.Criteria)
	}

	err := s.Store.CreateCollectionConfig(
		body.Name, body.MediaType, body.Action, body.GraceDays,
		criteriaStr, body.Enabled, body.ScheduleCron, body.Priority,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "name": body.Name, "slug": slugify(body.Name)})
}

func (s *Server) handleExportCollections(w http.ResponseWriter, r *http.Request) {
	configs, err := s.Store.ListCollectionConfigs()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	export := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		m := map[string]any{
			"name":          cfg.Name,
			"media_type":    cfg.MediaType,
			"action":        cfg.Action,
			"grace_days":    cfg.GraceDays,
			"enabled":       cfg.Enabled,
			"schedule_cron": cfg.ScheduleCron,
			"priority":      cfg.Priority,
		}
		if cfg.Criteria.Valid {
			var raw json.RawMessage
			_ = json.Unmarshal([]byte(cfg.Criteria.String), &raw)
			m["criteria"] = raw
		}
		export = append(export, m)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="reclaimer-rules.json"`)
	json.NewEncoder(w).Encode(export)
}

func (s *Server) handleImportCollections(w http.ResponseWriter, r *http.Request) {
	mode := queryStr(r, "mode", "merge")

	var rules []struct {
		Name         string          `json:"name"`
		MediaType    string          `json:"media_type"`
		Action       string          `json:"action"`
		GraceDays    int             `json:"grace_days"`
		Criteria     json.RawMessage `json:"criteria"`
		Enabled      bool            `json:"enabled"`
		ScheduleCron *string         `json:"schedule_cron"`
		Priority     int             `json:"priority"`
	}
	if err := readJSON(r, &rules); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if mode == "replace" {
		configs, _ := s.Store.ListCollectionConfigs()
		for _, cfg := range configs {
			_ = s.Store.DeleteCollectionConfig(cfg.Name)
		}
	}

	imported := 0
	for _, rule := range rules {
		criteriaStr := "{}"
		if rule.Criteria != nil {
			criteriaStr = string(rule.Criteria)
		}
		if mode == "merge" {
			_ = s.Store.DeleteCollectionConfig(rule.Name)
		}
		err := s.Store.CreateCollectionConfig(
			rule.Name, rule.MediaType, rule.Action, rule.GraceDays,
			criteriaStr, rule.Enabled, rule.ScheduleCron, rule.Priority,
		)
		if err != nil {
			slog.Error("import rule", "name", rule.Name, "error", err)
			continue
		}
		imported++
	}

	writeJSON(w, http.StatusOK, map[string]any{"imported": imported, "mode": mode})
}

func (s *Server) handleGetCollectionItems(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "name")
	cfg, err := s.resolveCollectionName(slug)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "collection not found"})
		return
	}

	page := queryInt(r, "page", 1)
	perPage := queryInt(r, "per_page", 50)
	status := queryStr(r, "status", "all")
	search := queryStr(r, "search", "")
	sort := queryStr(r, "sort", "first_seen")
	sortDir := queryStr(r, "sort_dir", "asc")

	items, total, err := s.Store.GetCollectionItems(cfg.Name, status, page, perPage, search, sort, sortDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	type itemWithRules struct {
		models.Item
		RuleResults []models.RuleResult `json:"rules"`
	}

	results := make([]itemWithRules, 0, len(items))
	for _, item := range items {
		rr, _ := s.Store.GetRuleResults(item.RatingKey, cfg.Name)
		if rr == nil {
			rr = []models.RuleResult{}
		}
		results = append(results, itemWithRules{Item: item, RuleResults: rr})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"collection":  cfg.Name,
		"slug":        slug,
		"items":       results,
		"total":       total,
		"page":        page,
		"per_page":    perPage,
		"total_pages": (total + perPage - 1) / perPage,
	})
}

func (s *Server) handleGetCollectionConfig(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "name")
	cfg, err := s.resolveCollectionName(slug)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "collection not found"})
		return
	}

	result := map[string]any{
		"id":            cfg.ID,
		"name":          cfg.Name,
		"slug":          slugify(cfg.Name),
		"media_type":    cfg.MediaType,
		"action":        cfg.Action,
		"grace_days":    cfg.GraceDays,
		"enabled":       cfg.Enabled,
		"schedule_cron": cfg.ScheduleCron,
		"priority":      cfg.Priority,
		"created_at":    cfg.CreatedAt,
		"updated_at":    cfg.UpdatedAt,
	}
	if cfg.Criteria.Valid {
		var raw json.RawMessage
		_ = json.Unmarshal([]byte(cfg.Criteria.String), &raw)
		result["criteria"] = raw
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleUpdateCollectionConfig(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "name")
	cfg, err := s.resolveCollectionName(slug)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "collection not found"})
		return
	}

	var body map[string]any
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	sets := []string{}
	args := []any{}
	allowed := map[string]bool{
		"name": true, "media_type": true, "action": true, "grace_days": true,
		"criteria": true, "enabled": true, "schedule_cron": true, "priority": true,
	}

	for k, v := range body {
		if !allowed[k] {
			continue
		}
		if k == "criteria" {
			b, _ := json.Marshal(v)
			sets = append(sets, "criteria = ?")
			args = append(args, string(b))
			continue
		}
		sets = append(sets, k+" = ?")
		args = append(args, v)
	}

	if len(sets) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no valid fields"})
		return
	}

	sets = append(sets, "updated_at = ?")
	args = append(args, models.NowISO())
	args = append(args, cfg.ID)

	q := fmt.Sprintf("UPDATE collection_config SET %s WHERE id = ?", strings.Join(sets, ", "))
	if _, err := s.DB.Exec(s.DB.Rebind(q), args...); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Handle rename: update items and rule_results
	if newName, ok := body["name"].(string); ok && newName != cfg.Name {
		s.DB.Exec(s.DB.Rebind("UPDATE items SET collection = ? WHERE collection = ?"), newName, cfg.Name)
		s.DB.Exec(s.DB.Rebind("UPDATE rule_results SET collection = ? WHERE collection = ?"), newName, cfg.Name)
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (s *Server) handleDeleteCollection(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "name")
	cfg, err := s.resolveCollectionName(slug)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "collection not found"})
		return
	}

	if err := s.Store.DeleteCollectionConfig(cfg.Name); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "name": cfg.Name})
}

func (s *Server) handleKeepMembers(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "name")
	cfg, err := s.resolveCollectionName(slug)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "collection not found"})
		return
	}

	criteria := models.ParseCriteria(cfg.Criteria.String)
	keepColl := ""
	if criteria.NotInKeepColl != nil {
		keepColl = criteria.NotInKeepColl.CollectionName
	}
	if keepColl == "" {
		if cfg.MediaType == "movie" {
			keepColl = s.Config.GetString("plex_movies_keep_collection")
		} else {
			keepColl = s.Config.GetString("plex_tv_keep_collection")
		}
	}

	var ratingKeys []string
	err = s.DB.Select(&ratingKeys,
		s.DB.Rebind("SELECT rating_key FROM items WHERE collection = ? AND override = 'keep'"),
		cfg.Name,
	)
	if err != nil {
		ratingKeys = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"collection":      cfg.Name,
		"keep_collection": keepColl,
		"members":         ratingKeys,
	})
}

// ---------------------------------------------------------------------------
// Items
// ---------------------------------------------------------------------------

func (s *Server) handleSearchItems(w http.ResponseWriter, r *http.Request) {
	q := queryStr(r, "q", "")
	if q == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "q param required"})
		return
	}

	var items []models.Item
	err := s.DB.Select(&items,
		s.DB.Rebind("SELECT * FROM items WHERE title LIKE ? ORDER BY title LIMIT 50"),
		"%"+q+"%",
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// TODO: also search Plex libraries via API

	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleWatchlistMembers(w http.ResponseWriter, r *http.Request) {
	// TODO: fetch from Seerr API
	writeJSON(w, http.StatusOK, map[string]any{"tmdb_ids": []int{}})
}

func (s *Server) handleGetItem(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")

	items, err := s.Store.GetItemsByRatingKey(rk)
	if err != nil || len(items) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "item not found"})
		return
	}

	item := items[0]

	var ruleResults []models.RuleResult
	for _, it := range items {
		rr, _ := s.Store.GetRuleResults(it.RatingKey, it.Collection)
		ruleResults = append(ruleResults, rr...)
	}

	var debridCache []models.DebridCache
	s.DB.Select(&debridCache,
		s.DB.Rebind("SELECT * FROM debrid_cache WHERE rating_key = ?"), rk)

	var activity []models.ActivityLog
	s.DB.Select(&activity,
		s.DB.Rebind("SELECT * FROM activity_log WHERE rating_key = ? ORDER BY timestamp DESC LIMIT 50"), rk)

	var watchHistory []models.WatchHistory
	s.DB.Select(&watchHistory,
		s.DB.Rebind("SELECT * FROM watch_history WHERE rating_key = ? ORDER BY watched_at DESC"), rk)

	type watcher struct {
		Username string `db:"username" json:"username"`
		Plays    int    `db:"plays" json:"plays"`
	}
	var watchers []watcher
	s.DB.Select(&watchers, s.DB.Rebind(`
		SELECT u.username, COUNT(wh.id) as plays
		FROM watch_history wh
		JOIN users u ON u.id = wh.user_id
		WHERE wh.rating_key = ?
		GROUP BY u.username
		ORDER BY plays DESC
	`), rk)

	var ratings *models.RatingsCache
	if item.ImdbID.Valid && item.ImdbID.String != "" {
		var rc models.RatingsCache
		err := s.DB.Get(&rc,
			s.DB.Rebind("SELECT * FROM ratings_cache WHERE imdb_id = ?"), item.ImdbID.String)
		if err == nil {
			ratings = &rc
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"item":          item,
		"entries":       items,
		"rules":         ruleResults,
		"debrid_cache":  debridCache,
		"activity":      activity,
		"watch_history": watchHistory,
		"watchers":      watchers,
		"ratings":       ratings,
	})
}

func (s *Server) handleEvaluateItem(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")

	items, err := s.Store.GetItemsByRatingKey(rk)
	if err != nil || len(items) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "item not found"})
		return
	}

	configs, err := s.Store.ListCollectionConfigs()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Return stored rule results per enabled collection
	results := map[string][]models.RuleResult{}
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		rr, _ := s.Store.GetRuleResults(rk, cfg.Name)
		if len(rr) > 0 {
			results[cfg.Name] = rr
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rating_key":   rk,
		"evaluations":  results,
		"evaluated_at": models.NowISO(),
	})
}

func (s *Server) handleSeerrRequests(w http.ResponseWriter, r *http.Request) {
	// TODO: fetch from Seerr/Overseerr API
	rk := chi.URLParam(r, "ratingKey")
	writeJSON(w, http.StatusOK, map[string]any{"rating_key": rk, "requests": []any{}})
}

func (s *Server) handleItemAction(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")

	var body struct {
		Action string  `json:"action"`
		Reason *string `json:"reason"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	switch body.Action {
	case "keep", "delete":
		override := body.Action
		if err := s.Store.SetItemOverride(rk, &override, body.Reason); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	case "clear":
		if err := s.Store.SetItemOverride(rk, nil, nil); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "action must be keep, delete, or clear"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "rating_key": rk, "action": body.Action})
}

func (s *Server) handleAddToKeep(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")
	override := "keep"
	reason := "added to keep via API"
	if err := s.Store.SetItemOverride(rk, &override, &reason); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "rating_key": rk})
}

func (s *Server) handleRemoveFromKeep(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")
	if err := s.Store.SetItemOverride(rk, nil, nil); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "rating_key": rk})
}

func (s *Server) handleAddToWatchlist(w http.ResponseWriter, r *http.Request) {
	// TODO: add to Seerr watchlist via API
	rk := chi.URLParam(r, "ratingKey")
	writeJSON(w, http.StatusOK, map[string]string{"status": "not_implemented", "rating_key": rk})
}

func (s *Server) handleRemoveFromWatchlist(w http.ResponseWriter, r *http.Request) {
	// TODO: remove from Seerr watchlist via API
	rk := chi.URLParam(r, "ratingKey")
	writeJSON(w, http.StatusOK, map[string]string{"status": "not_implemented", "rating_key": rk})
}

func (s *Server) handleBulkAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RatingKeys []string `json:"rating_keys"`
		Action     string   `json:"action"`
		Reason     *string  `json:"reason"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if len(body.RatingKeys) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rating_keys required"})
		return
	}
	if len(body.RatingKeys) > 500 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 500 items per bulk action"})
		return
	}

	switch body.Action {
	case "keep", "delete", "clear":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "action must be keep, delete, or clear"})
		return
	}

	updated := 0
	for _, rk := range body.RatingKeys {
		var err error
		if body.Action == "clear" {
			err = s.Store.SetItemOverride(rk, nil, nil)
		} else {
			a := body.Action
			err = s.Store.SetItemOverride(rk, &a, body.Reason)
		}
		if err == nil {
			updated++
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"updated": updated, "total": len(body.RatingKeys)})
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Config.GetAll(true))
}

func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var updates map[string]any
	if err := readJSON(r, &updates); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if err := s.Config.Update(updates); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (s *Server) handlePlexLibraries(w http.ResponseWriter, r *http.Request) {
	plexURL := s.Config.GetString("plex_url")
	plexToken := s.Config.GetString("plex_token")
	if plexURL == "" || plexToken == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "plex_url and plex_token required"})
		return
	}

	req, _ := http.NewRequestWithContext(r.Context(), "GET", plexURL+"/library/sections", nil)
	req.Header.Set("X-Plex-Token", plexToken)
	req.Header.Set("Accept", "application/json")

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var data map[string]any
	json.NewDecoder(resp.Body).Decode(&data)
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handlePlexCollections(w http.ResponseWriter, r *http.Request) {
	plexURL := s.Config.GetString("plex_url")
	plexToken := s.Config.GetString("plex_token")
	sectionID := queryStr(r, "section_id", "")
	if plexURL == "" || plexToken == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "plex_url and plex_token required"})
		return
	}
	if sectionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "section_id required"})
		return
	}

	endpoint := fmt.Sprintf("%s/library/sections/%s/collections", plexURL, sectionID)
	req, _ := http.NewRequestWithContext(r.Context(), "GET", endpoint, nil)
	req.Header.Set("X-Plex-Token", plexToken)
	req.Header.Set("Accept", "application/json")

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var data map[string]any
	json.NewDecoder(resp.Body).Decode(&data)
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleJellyfinLibraries(w http.ResponseWriter, r *http.Request) {
	jfURL := s.Config.GetString("jellyfin_url")
	jfKey := s.Config.GetString("jellyfin_api_key")
	if jfURL == "" || jfKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "jellyfin_url and jellyfin_api_key required"})
		return
	}

	req, _ := http.NewRequestWithContext(r.Context(), "GET", jfURL+"/Library/VirtualFolders", nil)
	req.Header.Set("X-Emby-Token", jfKey)

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var data any
	json.NewDecoder(resp.Body).Decode(&data)
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleJellyfinCollections(w http.ResponseWriter, r *http.Request) {
	jfURL := s.Config.GetString("jellyfin_url")
	jfKey := s.Config.GetString("jellyfin_api_key")
	if jfURL == "" || jfKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "jellyfin_url and jellyfin_api_key required"})
		return
	}

	endpoint := jfURL + "/Items?IncludeItemTypes=BoxSet&Recursive=true"
	req, _ := http.NewRequestWithContext(r.Context(), "GET", endpoint, nil)
	req.Header.Set("X-Emby-Token", jfKey)

	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var data any
	json.NewDecoder(resp.Body).Decode(&data)
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleRecycleBinStatus(w http.ResponseWriter, r *http.Request) {
	results := map[string]any{}

	for _, kind := range []string{"radarr", "sonarr"} {
		instances, err := s.Store.ListArrInstances(kind)
		if err != nil {
			continue
		}
		for _, inst := range instances {
			endpoint := inst.URL + "/api/v3/config/mediamanagement"
			req, _ := http.NewRequestWithContext(r.Context(), "GET", endpoint, nil)
			req.Header.Set("X-Api-Key", inst.APIKey)

			resp, err := httpclient.Client().Do(req)
			if err != nil {
				results[inst.Name] = map[string]any{"error": err.Error()}
				continue
			}

			var cfg map[string]any
			json.NewDecoder(resp.Body).Decode(&cfg)
			resp.Body.Close()

			recycleBin, _ := cfg["recycleBin"].(string)
			results[inst.Name] = map[string]any{
				"kind":        kind,
				"recycle_bin": recycleBin,
				"enabled":     recycleBin != "",
			}
		}
	}

	writeJSON(w, http.StatusOK, results)
}

func (s *Server) handleArrTags(w http.ResponseWriter, r *http.Request) {
	allTags := map[string]any{}

	for _, kind := range []string{"radarr", "sonarr"} {
		instances, err := s.Store.ListArrInstances(kind)
		if err != nil {
			continue
		}
		for _, inst := range instances {
			endpoint := inst.URL + "/api/v3/tag"
			req, _ := http.NewRequestWithContext(r.Context(), "GET", endpoint, nil)
			req.Header.Set("X-Api-Key", inst.APIKey)

			resp, err := httpclient.Client().Do(req)
			if err != nil {
				allTags[inst.Name] = map[string]any{"error": err.Error()}
				continue
			}

			var tags any
			json.NewDecoder(resp.Body).Decode(&tags)
			resp.Body.Close()

			allTags[inst.Name] = map[string]any{"kind": kind, "tags": tags}
		}
	}

	writeJSON(w, http.StatusOK, allTags)
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL     string `json:"url"`
		APIKey  string `json:"api_key"`
		Service string `json:"service"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Fall back to saved config when URL or key not provided in the request.
	if body.URL == "" || body.APIKey == "" {
		switch body.Service {
		case "plex":
			if body.URL == "" {
				body.URL = s.Config.GetString("plex_url")
			}
			if body.APIKey == "" {
				body.APIKey = s.Config.GetString("plex_token")
			}
		case "jellyfin":
			if body.URL == "" {
				body.URL = s.Config.GetString("jellyfin_url")
			}
			if body.APIKey == "" {
				body.APIKey = s.Config.GetString("jellyfin_api_key")
			}
		case "overseerr", "jellyseerr":
			if body.URL == "" {
				body.URL = s.Config.GetString(body.Service + "_url")
			}
			if body.APIKey == "" {
				body.APIKey = s.Config.GetString(body.Service + "_api_key")
			}
		case "apprise":
			if body.URL == "" {
				body.URL = s.Config.GetString("apprise_url")
			}
		}
	}

	parsed, err := url.Parse(body.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "invalid or missing url"})
		return
	}

	var testURL string
	switch body.Service {
	case "plex":
		testURL = body.URL + "/identity"
	case "jellyfin":
		testURL = body.URL + "/System/Info/Public"
	case "radarr", "sonarr", "lidarr", "readarr", "whisparr":
		testURL = body.URL + "/api/v3/system/status"
	case "overseerr", "jellyseerr":
		testURL = body.URL + "/api/v1/status"
	case "apprise":
		// Apprise notify endpoints only accept POST; test the base health endpoint instead.
		u, _ := url.Parse(body.URL)
		testURL = u.Scheme + "://" + u.Host + "/status"
	default:
		testURL = body.URL
	}

	req, _ := http.NewRequestWithContext(r.Context(), "GET", testURL, nil)
	if body.APIKey != "" {
		switch body.Service {
		case "plex":
			req.Header.Set("X-Plex-Token", body.APIKey)
		case "jellyfin":
			req.Header.Set("X-Emby-Token", body.APIKey)
		case "overseerr", "jellyseerr":
			req.Header.Set("X-Api-Key", body.APIKey)
		default:
			req.Header.Set("X-Api-Key", body.APIKey)
		}
	}
	req.Header.Set("Accept", "application/json")

	client := httpclient.Client()
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "detail": "Connected successfully"})
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("HTTP %d", resp.StatusCode)})
	}
}

// ---------------------------------------------------------------------------
// Activity
// ---------------------------------------------------------------------------

func (s *Server) handleGetActivity(w http.ResponseWriter, r *http.Request) {
	eventType := queryStr(r, "event_type", "")
	collection := queryStr(r, "collection", "")
	page := queryInt(r, "page", 1)
	perPage := queryInt(r, "per_page", 50)

	logs, total, err := s.Store.GetActivity(eventType, collection, page, perPage)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"activity":    logs,
		"total":       total,
		"page":        page,
		"per_page":    perPage,
		"total_pages": (total + perPage - 1) / perPage,
	})
}

func (s *Server) handleClearActivity(w http.ResponseWriter, r *http.Request) {
	count, err := s.Store.ClearActivity()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": count})
}

func (s *Server) handleWatchHistory(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	perPage := queryInt(r, "per_page", 50)
	userFilter := queryStr(r, "user", "")
	search := queryStr(r, "search", "")
	mediaType := queryStr(r, "media_type", "")
	sort := queryStr(r, "sort", "last_watched")
	sortDir := queryStr(r, "sort_dir", "desc")

	validSorts := map[string]string{
		"last_watched":  "last_watched",
		"total_plays":   "total_plays",
		"total_duration": "total_duration",
		"title":         "title",
		"username":      "username",
	}
	sortCol, ok := validSorts[sort]
	if !ok {
		sortCol = "last_watched"
	}
	if sortDir != "asc" {
		sortDir = "desc"
	}

	where := []string{}
	args := []any{}

	if userFilter != "" {
		where = append(where, "u.username = ?")
		args = append(args, userFilter)
	}
	if search != "" {
		where = append(where, "(wh.title LIKE ? OR wh.grandparent_title LIKE ?)")
		args = append(args, "%"+search+"%", "%"+search+"%")
	}
	if mediaType != "" {
		where = append(where, "wh.media_type = ?")
		args = append(args, mediaType)
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	// CTE groups plays by user+item+date
	cte := fmt.Sprintf(`
		WITH grouped AS (
			SELECT
				wh.user_id,
				u.username,
				u.thumb as thumb,
				wh.rating_key,
				COALESCE(wh.title, '') as title,
				COALESCE(wh.grandparent_title, '') as grandparent_title,
				wh.media_type,
				wh.season_number,
				wh.episode_number,
				COUNT(*) as total_plays,
				SUM(wh.play_duration) as total_duration,
				MAX(wh.watched_at) as last_watched,
				MAX(wh.percent_complete) as max_percent_complete
			FROM watch_history wh
			JOIN users u ON u.id = wh.user_id
			%s
			GROUP BY wh.user_id, wh.rating_key, SUBSTR(wh.watched_at, 1, 10)
		)
		SELECT * FROM grouped
		ORDER BY %s %s
	`, whereClause, sortCol, sortDir)

	countQ := fmt.Sprintf(`
		WITH grouped AS (
			SELECT wh.user_id, wh.rating_key, SUBSTR(wh.watched_at, 1, 10) as d
			FROM watch_history wh
			JOIN users u ON u.id = wh.user_id
			%s
			GROUP BY wh.user_id, wh.rating_key, d
		)
		SELECT COUNT(*) FROM grouped
	`, whereClause)

	var total int
	if err := s.DB.Get(&total, s.DB.Rebind(countQ), args...); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	offset := (page - 1) * perPage
	pagedQ := s.DB.Rebind(cte + " LIMIT ? OFFSET ?")
	pagedArgs := append(args, perPage, offset)

	rows, err := s.DB.Queryx(pagedQ, pagedArgs...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		m := map[string]any{}
		if err := rows.MapScan(m); err != nil {
			continue
		}
		// Convert []byte values to strings for JSON
		for k, v := range m {
			if b, ok := v.([]byte); ok {
				m[k] = string(b)
			}
		}
		results = append(results, m)
	}
	if results == nil {
		results = []map[string]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"activity":    results,
		"total":       total,
		"page":        page,
		"per_page":    perPage,
		"total_pages": (total + perPage - 1) / perPage,
	})
}

func (s *Server) handleWatchSegments(w http.ResponseWriter, r *http.Request) {
	ratingKey := queryStr(r, "rating_key", "")
	userID := queryStr(r, "user_id", "")
	date := queryStr(r, "date", "")

	if ratingKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rating_key required"})
		return
	}

	args := []any{ratingKey}
	where := "wh.rating_key = ?"

	if userID != "" {
		where += " AND wh.user_id = ?"
		args = append(args, userID)
	}
	if date != "" {
		where += " AND SUBSTR(wh.watched_at, 1, 10) = ?"
		args = append(args, date)
	}

	q := fmt.Sprintf(`
		SELECT wh.*, u.username
		FROM watch_history wh
		JOIN users u ON u.id = wh.user_id
		WHERE %s
		ORDER BY wh.watched_at ASC
	`, where)

	rows, err := s.DB.Queryx(s.DB.Rebind(q), args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var segments []map[string]any
	for rows.Next() {
		m := map[string]any{}
		if err := rows.MapScan(m); err != nil {
			continue
		}
		for k, v := range m {
			if b, ok := v.([]byte); ok {
				m[k] = string(b)
			}
		}
		segments = append(segments, m)
	}
	if segments == nil {
		segments = []map[string]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"segments": segments})
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.Store.ListUsers()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if users == nil {
		users = []map[string]any{}
	}
	// Convert []byte to string for JSON
	for _, u := range users {
		for k, v := range u {
			if b, ok := v.([]byte); ok {
				u[k] = string(b)
			}
		}
	}
	writeJSON(w, http.StatusOK, users)
}

// ---------------------------------------------------------------------------
// Instances (arr)
// ---------------------------------------------------------------------------

func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	kind := queryStr(r, "kind", "")
	instances, err := s.Store.ListArrInstances(kind)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	redacted := make([]models.ArrInstance, 0, len(instances))
	for _, inst := range instances {
		redacted = append(redacted, redactInstance(inst))
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": redacted})
}

func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	inst, err := s.Store.GetArrInstance(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "instance not found"})
		return
	}
	writeJSON(w, http.StatusOK, redactInstance(*inst))
}

func (s *Server) handleCreateInstance(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		URL       string `json:"url"`
		APIKey    string `json:"api_key"`
		PublicURL string `json:"public_url"`
		IsDefault bool   `json:"is_default"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if body.Kind == "" || body.Name == "" || body.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "kind, name, url required"})
		return
	}

	id, err := s.Store.CreateArrInstance(body.Kind, body.Name, body.URL, body.APIKey, body.PublicURL, body.IsDefault)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "status": "created"})
}

func (s *Server) handleUpdateInstance(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	var fields map[string]any
	if err := readJSON(r, &fields); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if err := s.Store.UpdateArrInstance(id, fields); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (s *Server) handleDeleteInstance(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	if err := s.Store.DeleteArrInstance(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "instance not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleTestInstance(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL        string `json:"url"`
		APIKey     string `json:"api_key"`
		Kind       string `json:"kind"`
		InstanceID *int64 `json:"instance_id"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Fall back to saved instance key when not provided.
	if body.APIKey == "" && body.InstanceID != nil {
		if inst, err := s.Store.GetArrInstance(*body.InstanceID); err == nil {
			if body.APIKey == "" {
				body.APIKey = inst.APIKey
			}
			if body.URL == "" {
				body.URL = inst.URL
			}
		}
	}

	endpoint := body.URL + "/api/v3/system/status"
	req, _ := http.NewRequestWithContext(r.Context(), "GET", endpoint, nil)
	req.Header.Set("X-Api-Key", body.APIKey)
	req.Header.Set("Accept", "application/json")

	client := httpclient.Client()
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "detail": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "detail": "Connected successfully"})
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "detail": fmt.Sprintf("HTTP %d", resp.StatusCode)})
	}
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func (s *Server) handleTriggerRun(w http.ResponseWriter, r *http.Request) {
	dryRun := queryStr(r, "dry_run", "false") == "true"
	rule := queryStr(r, "rule", "")

	s.runMu.Lock()
	if s.running {
		s.runMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "run already in progress"})
		return
	}
	s.running = true
	s.runProgress = map[string]any{
		"status":     "running",
		"started_at": models.NowISO(),
		"dry_run":    dryRun,
		"rule":       rule,
	}
	s.runMu.Unlock()

	go func() {
		defer func() {
			s.runMu.Lock()
			s.running = false
			s.runProgress["status"] = "complete"
			s.runProgress["completed_at"] = models.NowISO()
			s.runMu.Unlock()
		}()

		slog.Info("run triggered", "dry_run", dryRun, "rule", rule)
		if err := s.Orchestrator.Run(dryRun, rule); err != nil {
			slog.Error("orchestrator run failed", "error", err)
		}
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":  "started",
		"dry_run": dryRun,
		"rule":    rule,
	})
}

func (s *Server) handleRunStatus(w http.ResponseWriter, r *http.Request) {
	s.runMu.Lock()
	running := s.running
	s.runMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"running": running})
}

func (s *Server) handleRunProgress(w http.ResponseWriter, r *http.Request) {
	s.runMu.Lock()
	progress := s.runProgress
	s.runMu.Unlock()
	if progress == nil {
		progress = map[string]any{"status": "idle"}
	}
	writeJSON(w, http.StatusOK, progress)
}

func (s *Server) handleSyncUsers(w http.ResponseWriter, r *http.Request) {
	slog.Info("user sync triggered")
	go func() {
		if err := s.Orchestrator.SyncUsers(); err != nil {
			slog.Error("user sync failed", "error", err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started"})
}

// ---------------------------------------------------------------------------
// Poster proxy
// ---------------------------------------------------------------------------

const posterCacheDir = "/app/data/poster-cache"

func (s *Server) handlePoster(w http.ResponseWriter, r *http.Request) {
	rk := chi.URLParam(r, "ratingKey")

	cacheFile := filepath.Join(posterCacheDir, rk+".jpg")

	if info, err := os.Stat(cacheFile); err == nil {
		etag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(fmt.Sprintf("%s-%d", rk, info.ModTime().Unix()))))

		if match := r.Header.Get("If-None-Match"); match == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "public, max-age=86400")
		http.ServeFile(w, r, cacheFile)
		return
	}

	// Try Plex
	plexURL := s.Config.GetString("plex_url")
	plexToken := s.Config.GetString("plex_token")

	var posterURL string
	if plexURL != "" && plexToken != "" {
		posterURL = fmt.Sprintf("%s/library/metadata/%s/thumb?X-Plex-Token=%s", plexURL, rk, plexToken)
	}

	// Fallback to Jellyfin
	if posterURL == "" {
		jfURL := s.Config.GetString("jellyfin_url")
		jfKey := s.Config.GetString("jellyfin_api_key")
		if jfURL != "" && jfKey != "" {
			posterURL = fmt.Sprintf("%s/Items/%s/Images/Primary?api_key=%s", jfURL, rk, jfKey)
		}
	}

	if posterURL == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no media server configured"})
		return
	}

	req, _ := http.NewRequestWithContext(r.Context(), "GET", posterURL, nil)
	resp, err := httpclient.Client().Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		writeJSON(w, resp.StatusCode, map[string]string{"error": "upstream returned " + resp.Status})
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read poster"})
		return
	}

	os.MkdirAll(posterCacheDir, 0o755)
	_ = os.WriteFile(cacheFile, body, 0o644)

	etag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(fmt.Sprintf("%s-%d", rk, time.Now().Unix()))))
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "image/jpeg"
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Write(body)
}
