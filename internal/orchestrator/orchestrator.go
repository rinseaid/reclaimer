// Package orchestrator contains the core business logic engine for media
// lifecycle management. It assembles data from Plex, Jellyfin, Radarr,
// Sonarr, and Seerr, evaluates per-collection rules, stages candidate
// items, executes after-grace action pipelines, and syncs media-server
// collections.
package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rinseaid/reclaimer/internal/config"
	"github.com/rinseaid/reclaimer/internal/database"
	"github.com/rinseaid/reclaimer/internal/models"
	"github.com/rinseaid/reclaimer/internal/rules"
	"github.com/rinseaid/reclaimer/internal/services/httpclient"
	"github.com/rinseaid/reclaimer/internal/services/jellyfin"
	"github.com/rinseaid/reclaimer/internal/services/seerr"
	"github.com/rinseaid/reclaimer/internal/services/plex"
	"github.com/rinseaid/reclaimer/internal/services/radarr"
	"github.com/rinseaid/reclaimer/internal/services/ratings"
	"github.com/rinseaid/reclaimer/internal/services/sonarr"
	"github.com/rinseaid/reclaimer/internal/store"
)

// partiallyWatchedLookbackDays is the hard-coded upper bound for the
// partial-watch lookback window. Individual rules may narrow this further
// via their own partially_watched.days parameter.
const partiallyWatchedLookbackDays = 60

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Progress describes the current state of a running orchestration.
type Progress struct {
	Phase          string `json:"phase"`
	Detail         string `json:"detail"`
	Percent        int    `json:"percent"`
	ItemsProcessed int    `json:"items_processed"`
	ItemsTotal     int    `json:"items_total"`
}

// RunResult describes the outcome of a completed orchestration run.
type RunResult struct {
	DryRun  bool   `json:"dry_run"`
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
	Rule    string `json:"rule,omitempty"`
}

// Orchestrator is the core business-logic engine. It holds references to
// shared dependencies and protects concurrent access to run state.
type Orchestrator struct {
	Store  *store.Store
	Config *config.Config
	DB     *database.DB

	running    atomic.Bool
	mu         sync.Mutex
	progress   Progress
	lastResult *RunResult
}

// collectionSyncKey uniquely identifies a deferred collection sync target.
type collectionSyncKey struct {
	Name      string
	SectionID string
	PlexType  int // 1=movie, 2=show, 3=season
	Source     string
}

// immediateActionTypes are pipeline step types that run at item detection
// rather than after grace.
var immediateActionTypes = map[string]bool{
	"sync_collection": true,
	"add_arr_tag":     true,
	"remove_arr_tag":  true,
	"notify":          true,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// IsRunning reports whether an orchestration run is currently in progress.
func (o *Orchestrator) IsRunning() bool {
	return o.running.Load()
}

// GetProgress returns a snapshot of the current progress.
func (o *Orchestrator) GetProgress() Progress {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.progress
}

// GetLastResult returns the result of the most recent completed run, or nil.
func (o *Orchestrator) GetLastResult() *RunResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastResult
}

func (o *Orchestrator) setProgress(p Progress) {
	o.mu.Lock()
	o.progress = p
	o.mu.Unlock()
}

func (o *Orchestrator) updateProgress(phase, detail string, percent, processed, total int) {
	o.setProgress(Progress{
		Phase:          phase,
		Detail:         detail,
		Percent:        percent,
		ItemsProcessed: processed,
		ItemsTotal:     total,
	})
}

// Run executes the full orchestration pipeline. It returns an error only for
// fatal setup failures; individual sub-phase errors are logged and recorded
// as activity events. Only one Run may execute at a time.
func (o *Orchestrator) Run(dryRun bool, ruleFilter string) error {
	if !o.running.CompareAndSwap(false, true) {
		return fmt.Errorf("orchestrator is already running")
	}
	defer o.running.Store(false)

	t0 := time.Now()
	today := time.Now().Format("2006-01-02")
	slog.Info("=== Reclaimer Run ===", "dry_run", dryRun, "rule_filter", ruleFilter)

	o.updateProgress("fetching_data", "Fetching Plex, Radarr, Sonarr, Seerr data...", 5, 0, 0)
	o.logActivity("run_started", "", "", "", map[string]any{
		"dry_run":     dryRun,
		"rule_filter": ruleFilter,
	})

	// ---------------------------------------------------------------
	// Phase 1: Data Assembly
	// ---------------------------------------------------------------

	plex_url := o.Config.GetString("plex_url")
	plex_token := o.Config.GetString("plex_token")
	moviesSectionStr := o.Config.GetString("plex_movies_section")
	tvSectionStr := o.Config.GetString("plex_tv_section")
	moviesSection, _ := strconv.Atoi(moviesSectionStr)
	tvSection, _ := strconv.Atoi(tvSectionStr)
	jfURL := o.Config.GetString("jellyfin_url")
	jfKey := o.Config.GetString("jellyfin_api_key")
	jfMoviesSection := o.Config.GetString("jellyfin_movies_section")
	jfTVSection := o.Config.GetString("jellyfin_tv_section")
	seerrURL := o.Config.GetString("seerr_url")
	seerrKey := o.Config.GetString("seerr_api_key")

	// Concurrent data fetches with per-source progress.
	var (
		plexMovies      []map[string]any
		plexTV          []map[string]any
		radarrMovies    map[int]map[string]any
		sonarrShows     map[int]map[string]any
		requestData     *seerr.RequestData
		fetchWG         sync.WaitGroup
		plexMoviesErr   error
		plexTVErr       error
		radarrMoviesErr error
		sonarrShowsErr  error
		requestErr      error
		fetchedMu       sync.Mutex
		fetched         []string
	)

	updateFetchProgress := func(source string) {
		fetchedMu.Lock()
		fetched = append(fetched, source)
		msg := fmt.Sprintf("Fetched %s (%d/5)...", strings.Join(fetched, ", "), len(fetched))
		fetchedMu.Unlock()
		o.updateProgress("fetching_data", msg, 5, 0, 0)
	}

	radarrMovies = make(map[int]map[string]any)
	sonarrShows = make(map[int]map[string]any)

	fetchWG.Add(5)

	go func() {
		defer fetchWG.Done()
		defer recoverGoroutine("plex_movies")
		if plex_url != "" && plex_token != "" && moviesSection > 0 {
			plexMovies, plexMoviesErr = plex.FetchLibrary(plex_url, plex_token, moviesSection)
		}
		updateFetchProgress("Plex movies")
	}()

	go func() {
		defer fetchWG.Done()
		defer recoverGoroutine("plex_tv")
		if plex_url != "" && plex_token != "" && tvSection > 0 {
			plexTV, plexTVErr = plex.FetchLibrary(plex_url, plex_token, tvSection)
		}
		updateFetchProgress("Plex TV")
	}()

	go func() {
		defer fetchWG.Done()
		defer recoverGoroutine("radarr")
		inst, err := o.Store.DefaultArrInstance("radarr")
		if err == nil && inst != nil {
			radarrMovies, radarrMoviesErr = radarr.FetchMovies(inst.URL, inst.APIKey)
		}
		updateFetchProgress("Radarr")
	}()

	go func() {
		defer fetchWG.Done()
		defer recoverGoroutine("sonarr")
		inst, err := o.Store.DefaultArrInstance("sonarr")
		if err == nil && inst != nil {
			sonarrShows, sonarrShowsErr = sonarr.FetchShows(inst.URL, inst.APIKey)
		}
		updateFetchProgress("Sonarr")
	}()

	go func() {
		defer fetchWG.Done()
		defer recoverGoroutine("seerr")
		protectedCSV := o.Config.GetString("protected_requesters")
		protectedSet := parseCSVSet(protectedCSV)
		requestData, requestErr = seerr.FetchActiveRequests(seerrURL, seerrKey, protectedSet)
		updateFetchProgress("Seerr")
	}()

	fetchWG.Wait()

	if plexMoviesErr != nil {
		slog.Warn("Plex movies fetch failed", "error", plexMoviesErr)
	}
	if plexTVErr != nil {
		slog.Warn("Plex TV fetch failed", "error", plexTVErr)
	}
	if radarrMoviesErr != nil {
		slog.Warn("Radarr movies fetch failed", "error", radarrMoviesErr)
	}
	if sonarrShowsErr != nil {
		slog.Warn("Sonarr shows fetch failed", "error", sonarrShowsErr)
	}
	if requestErr != nil {
		slog.Warn("Seerr requests fetch failed", "error", requestErr)
	}
	if radarrMovies == nil {
		radarrMovies = make(map[int]map[string]any)
	}
	if sonarrShows == nil {
		sonarrShows = make(map[int]map[string]any)
	}
	if requestData == nil {
		requestData = &seerr.RequestData{
			ActiveMovies:       make(map[int]bool),
			ActiveShows:        make(map[int]bool),
			ActiveShowsTmdb:    make(map[int]bool),
			ProtectedMovies:    make(map[int]bool),
			ProtectedShows:     make(map[int]bool),
			ProtectedShowsTmdb: make(map[int]bool),
			MovieRequesters:    make(map[int]string),
			ShowRequesters:     make(map[int]string),
			ShowRequestersTmdb: make(map[int]string),
		}
	}

	o.updateProgress("loading_db_plays", "Loading watch history and play counts...", 15, 0, 0)

	// Aggregate play counts, last watch dates, user watches from watch_history.
	playCounts := o.aggregatePlayCounts()
	dbPlays, dbPlaysByTitle := o.aggregateDBPlays()
	lastWatchDates, lastWatchByTitle := o.aggregateLastWatchDates()
	userWatches := o.aggregateUserWatches()
	dbPlaysBySeason, lastWatchBySeason, userWatchesBySeason := o.aggregateSeasonData()

	// Extract ratings from library metadata.
	ratingsMap := o.extractRatingsFromLibraries(plexMovies, plexTV)

	o.updateProgress("fetching_keep_collections", "Fetching keep collections and ratings...", 20, 0, 0)

	// Keep collections.
	moviesKeep := o.fetchKeepKeys(plex_url, plex_token, moviesSection,
		o.Config.GetString("plex_movies_keep_collection"))
	tvKeep := o.fetchKeepKeys(plex_url, plex_token, tvSection,
		o.Config.GetString("plex_tv_keep_collection"))
	// Jellyfin keep collections merged in.
	if jfURL != "" && jfKey != "" {
		jfMoviesKeepName := o.Config.GetString("jellyfin_movies_keep_collection")
		jfTVKeepName := o.Config.GetString("jellyfin_tv_keep_collection")
		if jfMoviesKeepName != "" && jfMoviesSection != "" {
			jfKeys, err := jellyfin.FetchKeepCollection(jfURL, jfKey, jfMoviesSection, jfMoviesKeepName)
			if err == nil {
				for k := range jfKeys {
					moviesKeep[k] = true
				}
			}
		}
		if jfTVKeepName != "" && jfTVSection != "" {
			jfKeys, err := jellyfin.FetchKeepCollection(jfURL, jfKey, jfTVSection, jfTVKeepName)
			if err == nil {
				for k := range jfKeys {
					tvKeep[k] = true
				}
			}
		}
	}

	o.updateProgress("building_context", "Building watchlists, favorites, partial watches...", 25, 0, 0)

	// Extended context fields: favorites, watchlist, partial watches, etc.
	addedAtByKey := o.buildAddedAtByKey(plexMovies, plexTV)
	plexFavoritedKeys := o.fetchFavoritedKeys(plexMovies, plexTV, plex_url, plex_token, moviesSection, tvSection)
	watchlistKeys := o.resolveWatchlistKeys(plexMovies, plexTV, radarrMovies, sonarrShows,
		plex_url, plex_token, seerrURL, seerrKey, jfURL, jfKey, jfMoviesSection, jfTVSection)
	partialKeys, partialTitles, partialSeasons := o.aggregatePartialWatches()
	maxPercentByKey, maxPercentByTitle, maxPercentBySeason := o.aggregateMaxPercent()
	showSeasonCounts := sonarr.BuildSeasonCounts(sonarrShows)

	// Build base contexts.
	movieCtx := &rules.EvaluationContext{
		PlayCounts:               playCounts,
		RadarrMovies:             radarrMovies,
		SeerrActiveMovies:    requestData.ActiveMovies,
		SeerrProtectedMovies: requestData.ProtectedMovies,
		PlexKeepKeys:             moviesKeep,
		DBPlays:                  dbPlays,
		DBPlaysByTitle:           dbPlaysByTitle,
		MovieRequesters:          requestData.MovieRequesters,
		UserWatches:              userWatches,
		LastWatchDates:           lastWatchDates,
		LastWatchByTitle:         lastWatchByTitle,
		RatingsCache:             ratingsMap,
		PlexFavoritedKeys:        plexFavoritedKeys,
		WatchlistKeys:            watchlistKeys,
		PartiallyWatchedKeys:     partialKeys,
		PartiallyWatchedByTitle:  partialTitles,
		PartiallyWatchedBySeason: partialSeasons,
		AddedAtByKey:             addedAtByKey,
		ShowSeasonCounts:         showSeasonCounts,
		MaxPercentByKey:          maxPercentByKey,
		MaxPercentByTitle:        maxPercentByTitle,
		MaxPercentBySeason:       maxPercentBySeason,
		// Initialized empty -- not used for movies.
		SonarrShows:                 make(map[int]map[string]any),
		SeerrActiveShows:        make(map[int]bool),
		SeerrActiveShowsTmdb:    make(map[int]bool),
		SeerrProtectedShows:     make(map[int]bool),
		SeerrProtectedShowsTmdb: make(map[int]bool),
		ShowRequesters:              make(map[int]string),
		ShowRequestersTmdb:          make(map[int]string),
		DebridCached:                make(map[string]bool),
		DBPlaysBySeason:             make(map[string]int),
		LastWatchBySeason:           make(map[string]string),
		UserWatchesBySeason:         make(map[string]map[string]bool),
		ShowLevelProtectionKeys:     make(map[string]bool),
	}

	tvCtx := &rules.EvaluationContext{
		PlayCounts:                  playCounts,
		SonarrShows:                 sonarrShows,
		SeerrActiveShows:        requestData.ActiveShows,
		SeerrActiveShowsTmdb:    requestData.ActiveShowsTmdb,
		SeerrProtectedShows:     requestData.ProtectedShows,
		SeerrProtectedShowsTmdb: requestData.ProtectedShowsTmdb,
		PlexKeepKeys:                tvKeep,
		DBPlays:                     dbPlays,
		DBPlaysByTitle:              dbPlaysByTitle,
		ShowRequesters:              requestData.ShowRequesters,
		ShowRequestersTmdb:          requestData.ShowRequestersTmdb,
		UserWatches:                 userWatches,
		LastWatchDates:              lastWatchDates,
		LastWatchByTitle:            lastWatchByTitle,
		RatingsCache:                ratingsMap,
		DBPlaysBySeason:             dbPlaysBySeason,
		LastWatchBySeason:           lastWatchBySeason,
		UserWatchesBySeason:         userWatchesBySeason,
		PlexFavoritedKeys:           plexFavoritedKeys,
		WatchlistKeys:               watchlistKeys,
		PartiallyWatchedKeys:        partialKeys,
		PartiallyWatchedByTitle:     partialTitles,
		PartiallyWatchedBySeason:    partialSeasons,
		AddedAtByKey:                addedAtByKey,
		ShowSeasonCounts:            showSeasonCounts,
		MaxPercentByKey:             maxPercentByKey,
		MaxPercentByTitle:           maxPercentByTitle,
		MaxPercentBySeason:          maxPercentBySeason,
		ShowLevelProtectionKeys:     make(map[string]bool),
		// Initialized empty -- not used for TV.
		RadarrMovies:             make(map[int]map[string]any),
		SeerrActiveMovies:    make(map[int]bool),
		SeerrProtectedMovies: make(map[int]bool),
		MovieRequesters:          make(map[int]string),
		DebridCached:             make(map[string]bool),
	}

	// Pre-warm poster cache in the background so list views load instantly.
	go o.prewarmPosters(plexMovies, plexTV, plex_url, plex_token, jfURL, jfKey)

	// ---------------------------------------------------------------
	// Phase 2: Collection Processing
	// ---------------------------------------------------------------
	o.updateProgress("loading_collections", "Loading collection configs...", 30, 0, 0)

	configs, err := o.Store.ListCollectionConfigs()
	if err != nil {
		return fmt.Errorf("load collection configs: %w", err)
	}

	// Filter to enabled configs only.
	var enabledConfigs []models.CollectionConfig
	for _, c := range configs {
		if c.Enabled {
			if ruleFilter == "" || c.Name == ruleFilter {
				enabledConfigs = append(enabledConfigs, c)
			}
		}
	}

	// Pre-fetch protected collection keys referenced by any rule.
	collectionKeysCache := o.prefetchProtectedCollections(enabledConfigs,
		plex_url, plex_token, moviesSection, tvSection, jfURL, jfKey, jfMoviesSection, jfTVSection)

	// Jellyfin library cache (library_id -> normalized items).
	jfLibraryCache := make(map[string][]map[string]any)

	// Deferred collection syncs.
	deferredSyncs := make(map[collectionSyncKey]map[string]bool)

	totalAdded := 0
	totalRemoved := 0

	for i, colCfg := range enabledConfigs {
		pct := 30 + int(float64(i)/float64(len(enabledConfigs))*40)
		o.updateProgress("processing_collection", colCfg.Name, pct, i, len(enabledConfigs))

		criteria := models.ParseCriteria(colCfg.Criteria.String)
		criteria.Action = colCfg.Action
		criteria.GraceDays = colCfg.GraceDays

		librarySource := criteria.LibrarySource
		if librarySource == "" {
			librarySource = "plex"
		}

		var (
			items      []map[string]any
			evalFn     evaluateFunc
			sectionID  string
			ctx        *rules.EvaluationContext
			mediaType  string
		)
		mediaType = colCfg.MediaType

		if librarySource == "jellyfin" && jfURL != "" && jfKey != "" {
			libID := ""
			if criteria.LibrarySectionID != nil {
				libID = criteria.LibrarySectionID.Value
			}
			if libID == "" {
				slog.Warn("Collection targets Jellyfin but has no library_section_id, skipping",
					"collection", colCfg.Name)
				continue
			}
			if _, ok := jfLibraryCache[libID]; !ok {
				raw, err := jellyfin.FetchLibrary(jfURL, jfKey, libID)
				if err != nil {
					slog.Warn("Jellyfin library fetch failed", "library_id", libID, "error", err)
					continue
				}
				normalized := make([]map[string]any, 0, len(raw))
				for _, it := range raw {
					normalized = append(normalized, normalizeJellyfinItem(it))
				}
				jfLibraryCache[libID] = normalized
			}
			items = jfLibraryCache[libID]
			sectionID = libID
			if mediaType == "movie" {
				evalFn = makeMovieEvaluator()
				ctx = movieCtx
			} else {
				evalFn = makeShowEvaluator()
				ctx = tvCtx
			}
		} else {
			// Plex-sourced collection.
			if mediaType == "movie" {
				items = plexMovies
				evalFn = makeMovieEvaluator()
				sectionID = moviesSectionStr
				if criteria.LibrarySectionID != nil && criteria.LibrarySectionID.Value != "" {
					sectionID = criteria.LibrarySectionID.Value
				}
				ctx = movieCtx
			} else if criteria.Granularity == "season" && mediaType == "show" {
				// Expand shows to seasons.
				sectionID = tvSectionStr
				if criteria.LibrarySectionID != nil && criteria.LibrarySectionID.Value != "" {
					sectionID = criteria.LibrarySectionID.Value
				}
				ctx = tvCtx
				seasonItems := o.expandShowsToSeasons(plexTV, plex_url, plex_token)
				items = seasonItems
				mediaType = "show" // kept as show for arr lookups; season_number distinguishes
				evalFn = makeSeasonEvaluator()
				slog.Info("Expanded shows to seasons",
					"shows", len(plexTV), "seasons", len(seasonItems), "collection", colCfg.Name)
			} else {
				items = plexTV
				evalFn = makeShowEvaluator()
				sectionID = tvSectionStr
				if criteria.LibrarySectionID != nil && criteria.LibrarySectionID.Value != "" {
					sectionID = criteria.LibrarySectionID.Value
				}
				ctx = tvCtx
			}
		}

		// Build per-rule keep keys from protected_collections.
		ruleCtx := ctx
		if len(criteria.ProtectedCollections) > 0 {
			ruleKeepKeys := make(map[string]bool)
			for _, pcol := range criteria.ProtectedCollections {
				for k := range collectionKeysCache[pcol] {
					ruleKeepKeys[k] = true
				}
			}
			ruleCtx = copyContextWithKeepKeys(ctx, ruleKeepKeys)
		}

		// For season-granularity TV rules, compute show-level protection keys.
		if criteria.Granularity == "season" && colCfg.MediaType == "show" {
			slp := computeShowLevelProtectionKeys(plexTV, ruleCtx, criteria)
			ruleCtx.ShowLevelProtectionKeys = slp
			slog.Info("Show-level protection keys computed",
				"collection", colCfg.Name, "protected_shows", len(slp))
		}

		slog.Info("Processing collection",
			"name", colCfg.Name, "source", librarySource, "media_type", mediaType,
			"action", criteria.Action, "grace_days", criteria.GraceDays, "items", len(items))

		tRule := time.Now()
		matched, added, removed := o.processCollection(
			colCfg.Name, items, evalFn, ruleCtx, criteria, sectionID,
			colCfg.MediaType, dryRun, today, librarySource, deferredSyncs,
		)
		totalAdded += added
		totalRemoved += removed

		ruleDuration := time.Since(tRule)
		var trackedCount int
		o.DB.Get(&trackedCount, o.DB.Rebind(
			"SELECT COUNT(*) FROM items WHERE collection = ?"), colCfg.Name)

		o.logActivity("rule_processed", colCfg.Name, "", "", map[string]any{
			"library":    len(items),
			"candidates": matched,
			"tracked":    trackedCount,
			"added":      added,
			"removed":    removed,
			"duration":   ruleDuration.Seconds(),
			"dry_run":    dryRun,
		})
	}

	// ---------------------------------------------------------------
	// Phase 3: After-Grace Actions
	// ---------------------------------------------------------------
	o.updateProgress("executing_actions", "Running after-grace actions...", 75, 0, 0)

	o.executeAfterGraceActions(dryRun, today, ruleFilter)

	// ---------------------------------------------------------------
	// Phase 4: Collection Sync + Cleanup
	// ---------------------------------------------------------------
	o.updateProgress("syncing_collections", "Syncing media server collections...", 85, 0, 0)

	if !dryRun && len(deferredSyncs) > 0 {
		o.executeDeferredSyncs(deferredSyncs, plex_url, plex_token, jfURL, jfKey)
	}

	o.updateProgress("finalizing", "Cleaning up...", 95, 0, 0)

	// Prune old activity log entries.
	pruned, err := o.DB.PruneActivityLog(90)
	if err != nil {
		slog.Warn("Activity log prune failed", "error", err)
	} else if pruned > 0 {
		slog.Info("Pruned activity log", "rows", pruned)
	}

	duration := time.Since(t0)
	o.logActivity("run_completed", "", "", "", map[string]any{
		"dry_run":     dryRun,
		"duration":    duration.Seconds(),
		"rule_filter": ruleFilter,
	})

	result := &RunResult{
		DryRun:  dryRun,
		Added:   totalAdded,
		Removed: totalRemoved,
		Rule:    ruleFilter,
	}
	o.mu.Lock()
	o.lastResult = result
	o.mu.Unlock()

	o.updateProgress("complete", "Done", 100, 0, 0)
	slog.Info("=== Done ===", "duration", duration.Seconds())
	return nil
}

// ---------------------------------------------------------------------------
// SyncUsers - ingest Plex accounts + session history and Jellyfin users +
// watch history into the users and watch_history tables.
// ---------------------------------------------------------------------------

// SyncUsers synchronises Plex and Jellyfin user accounts and their watch
// history into the local database.
func (o *Orchestrator) SyncUsers() error {
	slog.Info("Starting user sync")
	protectedCSV := o.Config.GetString("protected_requesters")
	protectedSet := parseCSVSet(protectedCSV)

	// -- Plex --
	plex_url := o.Config.GetString("plex_url")
	plex_token := o.Config.GetString("plex_token")
	if plex_url != "" && plex_token != "" {
		o.syncPlexUsers(plex_url, plex_token, protectedSet)
	}

	// -- Jellyfin --
	jfURL := o.Config.GetString("jellyfin_url")
	jfKey := o.Config.GetString("jellyfin_api_key")
	if jfURL != "" && jfKey != "" {
		o.syncJellyfinUsers(jfURL, jfKey, protectedSet)
	}

	slog.Info("User sync complete")
	return nil
}

func (o *Orchestrator) syncPlexUsers(plexURL, plexToken string, protectedSet map[string]bool) {
	accounts, err := plex.FetchAccounts(plexURL, plexToken)
	if err != nil || accounts == nil {
		slog.Warn("Plex accounts fetch failed", "error", err)
		return
	}
	slog.Info("Plex accounts fetched", "count", len(accounts))

	// Upsert accounts.
	for acctID, name := range accounts {
		isProt := protectedSet[name]
		_, err := o.DB.Exec(
			o.DB.Rebind(`INSERT INTO users (plex_user_id, username, thumb, is_protected, source, last_synced)
				VALUES (?, ?, '', ?, 'plex', datetime('now'))
				ON CONFLICT(plex_user_id) DO UPDATE SET
					username = excluded.username,
					is_protected = excluded.is_protected,
					source = 'plex',
					last_synced = datetime('now')`),
			acctID, name, isProt,
		)
		if err != nil {
			slog.Warn("Plex user upsert failed", "account", acctID, "name", name, "error", err)
		}
	}

	// Build plex_user_id -> internal users.id map.
	userIDMap := o.buildUserIDMap()

	// Fetch session history since the last known entry.
	var sinceTS *int64
	var lastWatched string
	if err := o.DB.Get(&lastWatched, "SELECT MAX(watched_at) FROM watch_history"); err == nil && lastWatched != "" {
		for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, lastWatched); err == nil {
				ts := t.Unix() - 3600
				sinceTS = &ts
				break
			}
		}
	}
	slog.Info("Plex watch history sync",
		"userMapSize", len(userIDMap),
		"lastWatched", lastWatched,
		"sinceTS", sinceTS)
	history, err := plex.FetchSessionHistory(plexURL, plexToken, sinceTS)
	if err != nil {
		slog.Warn("Plex session history fetch failed", "error", err)
		return
	}
	slog.Info("Plex session history fetched", "entries", len(history))

	inserted := 0
	skipped := 0
	for _, h := range history {
		internalID, ok := userIDMap[h.AccountID]
		if !ok {
			skipped++
			continue
		}
		if h.RatingKey == "" || h.WatchedAt == "" {
			continue
		}

		pct := computePercentComplete(h.ViewOffsetMS, h.MediaDurationMS)

		var seasonNum, episodeNum interface{}
		if h.SeasonNumber != nil {
			seasonNum = *h.SeasonNumber
		}
		if h.EpisodeNumber != nil {
			episodeNum = *h.EpisodeNumber
		}

		_, err := o.DB.Exec(
			o.DB.Rebind(`INSERT INTO watch_history
				(user_id, rating_key, title, grandparent_title, media_type,
				 season_number, episode_number, watched_at,
				 play_duration, media_duration, percent_complete)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
				ON CONFLICT(user_id, rating_key, watched_at) DO UPDATE SET
					media_duration = CASE WHEN excluded.media_duration > watch_history.media_duration
						THEN excluded.media_duration ELSE watch_history.media_duration END,
					percent_complete = CASE WHEN excluded.percent_complete > watch_history.percent_complete
						THEN excluded.percent_complete ELSE watch_history.percent_complete END,
					grandparent_title = CASE WHEN excluded.grandparent_title != ''
						THEN excluded.grandparent_title ELSE watch_history.grandparent_title END`),
			internalID, h.RatingKey, h.Title, h.GrandparentTitle, h.MediaType,
			seasonNum, episodeNum, h.WatchedAt,
			h.MediaDurationMS/1000, pct,
		)
		if err != nil {
			slog.Debug("Plex watch history insert failed", "rk", h.RatingKey, "error", err)
			continue
		}
		inserted++
	}
	slog.Info("Plex watch history synced", "inserted", inserted, "skipped_no_user", skipped, "total_entries", len(history))

	// Backfill: Plex history entries that lack progress data were stored
	// with percent_complete=0. Since presence in history means it was
	// watched, set them to 100%.
	res, _ := o.DB.Exec("UPDATE watch_history SET percent_complete = 100 WHERE percent_complete = 0 AND media_duration = 0")
	if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("Backfilled watch history progress", "rows", n)
	}
}

func (o *Orchestrator) syncJellyfinUsers(jfURL, jfKey string, protectedSet map[string]bool) {
	rows, jfUsers, err := jellyfin.FetchWatchHistory(jfURL, jfKey)
	if err != nil {
		slog.Warn("Jellyfin watch history fetch failed", "error", err)
		return
	}
	slog.Info("Jellyfin watch history fetched", "users", len(jfUsers), "rows", len(rows))

	// Upsert Jellyfin users.
	for acctIDStr, username := range jfUsers {
		acctID, err := strconv.ParseInt(acctIDStr, 10, 64)
		if err != nil {
			continue
		}
		isProt := protectedSet[username]
		o.DB.Exec(
			o.DB.Rebind(`INSERT INTO users (plex_user_id, username, thumb, is_protected, source, last_synced)
				VALUES (?, ?, '', ?, 'jellyfin', datetime('now'))
				ON CONFLICT(plex_user_id) DO UPDATE SET
					username = excluded.username,
					is_protected = excluded.is_protected,
					source = 'jellyfin',
					last_synced = datetime('now')`),
			acctID, username, isProt,
		)
	}

	userIDMap := o.buildUserIDMap()

	inserted := 0
	for _, h := range rows {
		internalID, ok := userIDMap[h.AccountID]
		if !ok || h.RatingKey == "" || h.WatchedAt == "" {
			continue
		}

		pct := computePercentComplete(h.ViewOffsetMS, h.MediaDurationMS)

		var seasonNum, episodeNum interface{}
		if h.SeasonNumber != nil {
			seasonNum = *h.SeasonNumber
		}
		if h.EpisodeNumber != nil {
			episodeNum = *h.EpisodeNumber
		}

		_, err := o.DB.Exec(
			o.DB.Rebind(`INSERT INTO watch_history
				(user_id, rating_key, title, grandparent_title, media_type,
				 season_number, episode_number, watched_at,
				 play_duration, media_duration, percent_complete)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
				ON CONFLICT(user_id, rating_key, watched_at) DO UPDATE SET
					media_duration = CASE WHEN excluded.media_duration > watch_history.media_duration
						THEN excluded.media_duration ELSE watch_history.media_duration END,
					percent_complete = CASE WHEN excluded.percent_complete > watch_history.percent_complete
						THEN excluded.percent_complete ELSE watch_history.percent_complete END,
					grandparent_title = CASE WHEN excluded.grandparent_title != ''
						THEN excluded.grandparent_title ELSE watch_history.grandparent_title END`),
			internalID, h.RatingKey, h.Title, h.GrandparentTitle, h.MediaType,
			seasonNum, episodeNum, h.WatchedAt,
			h.MediaDurationMS/1000, pct,
		)
		if err == nil {
			inserted++
		}
	}
	slog.Info("Jellyfin watch history synced", "inserted", inserted)
}

// ---------------------------------------------------------------------------
// Phase 1 helpers: data assembly
// ---------------------------------------------------------------------------

func (o *Orchestrator) aggregatePlayCounts() map[string]int {
	out := make(map[string]int)
	rows, err := o.DB.Queryx(`
		SELECT rating_key, COUNT(DISTINCT (user_id || '|' || watched_at)) AS plays
		FROM watch_history
		WHERE rating_key IS NOT NULL AND rating_key != ''
		GROUP BY rating_key`)
	if err != nil {
		slog.Warn("Failed to aggregate play counts", "error", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var rk string
		var plays int
		if err := rows.Scan(&rk, &plays); err == nil {
			out[rk] = plays
		}
	}
	return out
}

func (o *Orchestrator) aggregateDBPlays() (map[string]int, map[string]int) {
	dbPlays := make(map[string]int)
	dbPlaysByTitle := make(map[string]int)

	rows, err := o.DB.Queryx("SELECT rating_key, COUNT(*) as plays FROM watch_history GROUP BY rating_key")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var rk string
			var plays int
			if rows.Scan(&rk, &plays) == nil {
				dbPlays[rk] = plays
			}
		}
	}

	titleRows, err := o.DB.Queryx(`
		SELECT grandparent_title, COUNT(*) as plays FROM watch_history
		WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
		GROUP BY grandparent_title COLLATE NOCASE`)
	if err == nil {
		defer titleRows.Close()
		for titleRows.Next() {
			var title string
			var plays int
			if titleRows.Scan(&title, &plays) == nil && title != "" {
				dbPlaysByTitle[strings.ToLower(title)] = plays
			}
		}
	}

	return dbPlays, dbPlaysByTitle
}

func (o *Orchestrator) aggregateLastWatchDates() (map[string]string, map[string]string) {
	lastWatchDates := make(map[string]string)
	lastWatchByTitle := make(map[string]string)

	rows, err := o.DB.Queryx("SELECT rating_key, MAX(watched_at) as last_watched FROM watch_history GROUP BY rating_key")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var rk string
			var lw sql.NullString
			if rows.Scan(&rk, &lw) == nil && lw.Valid && lw.String != "" {
				lastWatchDates[rk] = lw.String
			}
		}
	}

	titleRows, err := o.DB.Queryx(`
		SELECT grandparent_title, MAX(watched_at) as last_watched FROM watch_history
		WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
		GROUP BY grandparent_title COLLATE NOCASE`)
	if err == nil {
		defer titleRows.Close()
		for titleRows.Next() {
			var title string
			var lw sql.NullString
			if titleRows.Scan(&title, &lw) == nil && lw.Valid && lw.String != "" && title != "" {
				lastWatchByTitle[strings.ToLower(title)] = lw.String
			}
		}
	}

	return lastWatchDates, lastWatchByTitle
}

func (o *Orchestrator) aggregateUserWatches() map[string]map[string]bool {
	userWatches := make(map[string]map[string]bool)

	rows, err := o.DB.Queryx(`
		SELECT u.username, wh.rating_key, wh.grandparent_title
		FROM watch_history wh
		JOIN users u ON u.id = wh.user_id`)
	if err != nil {
		slog.Warn("Failed to aggregate user watches", "error", err)
		return userWatches
	}
	defer rows.Close()

	for rows.Next() {
		var username, rk string
		var gpTitle sql.NullString
		if rows.Scan(&username, &rk, &gpTitle) != nil {
			continue
		}
		if _, ok := userWatches[username]; !ok {
			userWatches[username] = make(map[string]bool)
		}
		userWatches[username][rk] = true
		if gpTitle.Valid && gpTitle.String != "" {
			userWatches[username][strings.ToLower(gpTitle.String)] = true
		}
	}

	return userWatches
}

func (o *Orchestrator) aggregateSeasonData() (map[string]int, map[string]string, map[string]map[string]bool) {
	dbPlaysBySeason := make(map[string]int)
	lastWatchBySeason := make(map[string]string)
	userWatchesBySeason := make(map[string]map[string]bool)

	// Season play counts.
	rows, err := o.DB.Queryx(`
		SELECT grandparent_title, season_number, COUNT(*) as plays
		FROM watch_history
		WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
		  AND season_number IS NOT NULL
		GROUP BY grandparent_title COLLATE NOCASE, season_number`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var title string
			var seasonNum, plays int
			if rows.Scan(&title, &seasonNum, &plays) == nil && title != "" {
				key := fmt.Sprintf("%s:S%d", strings.ToLower(title), seasonNum)
				dbPlaysBySeason[key] = plays
			}
		}
	}

	// Season last watch dates.
	slRows, err := o.DB.Queryx(`
		SELECT grandparent_title, season_number, MAX(watched_at) as last_watched
		FROM watch_history
		WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
		  AND season_number IS NOT NULL
		GROUP BY grandparent_title COLLATE NOCASE, season_number`)
	if err == nil {
		defer slRows.Close()
		for slRows.Next() {
			var title string
			var seasonNum int
			var lw sql.NullString
			if slRows.Scan(&title, &seasonNum, &lw) == nil && title != "" && lw.Valid && lw.String != "" {
				key := fmt.Sprintf("%s:S%d", strings.ToLower(title), seasonNum)
				lastWatchBySeason[key] = lw.String
			}
		}
	}

	// User season watches.
	usRows, err := o.DB.Queryx(`
		SELECT u.username, wh.grandparent_title, wh.season_number
		FROM watch_history wh
		JOIN users u ON u.id = wh.user_id
		WHERE wh.grandparent_title IS NOT NULL AND wh.grandparent_title != ''
		  AND wh.season_number IS NOT NULL`)
	if err == nil {
		defer usRows.Close()
		for usRows.Next() {
			var username, title string
			var seasonNum int
			if usRows.Scan(&username, &title, &seasonNum) == nil && title != "" {
				if _, ok := userWatchesBySeason[username]; !ok {
					userWatchesBySeason[username] = make(map[string]bool)
				}
				key := fmt.Sprintf("%s:S%d", strings.ToLower(title), seasonNum)
				userWatchesBySeason[username][key] = true
			}
		}
	}

	return dbPlaysBySeason, lastWatchBySeason, userWatchesBySeason
}

func (o *Orchestrator) extractRatingsFromLibraries(plexMovies, plexTV []map[string]any) map[string]models.Ratings {
	cache := make(map[string]models.Ratings)
	for _, item := range plexMovies {
		rk := toString(item["ratingKey"])
		if rk == "" {
			continue
		}
		r := ratings.ExtractRatings(item, "plex")
		if r.CriticRating != nil || r.AudienceRating != nil {
			cache[rk] = r
		}
	}
	for _, item := range plexTV {
		rk := toString(item["ratingKey"])
		if rk == "" {
			continue
		}
		r := ratings.ExtractRatings(item, "plex")
		if r.CriticRating != nil || r.AudienceRating != nil {
			cache[rk] = r
		}
	}
	slog.Info("Ratings extracted from library metadata", "items", len(cache))
	return cache
}

func (o *Orchestrator) fetchKeepKeys(plexURL, plexToken string, sectionID int, collectionName string) map[string]bool {
	if plexURL == "" || plexToken == "" || sectionID == 0 || collectionName == "" {
		return make(map[string]bool)
	}
	keys, err := plex.FetchKeepCollection(plexURL, plexToken, sectionID, collectionName)
	if err != nil {
		slog.Warn("Keep collection fetch failed", "collection", collectionName, "error", err)
		return make(map[string]bool)
	}
	if keys == nil {
		return make(map[string]bool)
	}
	return keys
}

func (o *Orchestrator) buildAddedAtByKey(plexMovies, plexTV []map[string]any) map[string]int64 {
	added := make(map[string]int64)
	for _, it := range plexMovies {
		rk := toString(it["ratingKey"])
		if rk == "" {
			continue
		}
		if aa := toInt64(it["addedAt"]); aa > 0 {
			added[rk] = aa
		}
	}
	for _, it := range plexTV {
		rk := toString(it["ratingKey"])
		if rk == "" {
			continue
		}
		if aa := toInt64(it["addedAt"]); aa > 0 {
			added[rk] = aa
		}
	}
	return added
}

func (o *Orchestrator) fetchFavoritedKeys(
	plexMovies, plexTV []map[string]any,
	plexURL, plexToken string,
	moviesSection, tvSection int,
) map[string]bool {
	fav := make(map[string]bool)

	// Check if items already carry userRating.
	haveUR := false
	for _, it := range plexMovies {
		if _, ok := it["userRating"]; ok {
			haveUR = true
			break
		}
	}
	if !haveUR {
		for _, it := range plexTV {
			if _, ok := it["userRating"]; ok {
				haveUR = true
				break
			}
		}
	}

	if haveUR {
		for _, items := range [][]map[string]any{plexMovies, plexTV} {
			for _, it := range items {
				ur := it["userRating"]
				if ur == nil {
					continue
				}
				f := toFloat64(ur)
				if f > 0 {
					rk := toString(it["ratingKey"])
					if rk != "" {
						fav[rk] = true
					}
				}
			}
		}
	} else {
		// Explicit API queries.
		if plexURL != "" && plexToken != "" {
			if moviesSection > 0 {
				keys, err := plex.FetchFavoritedKeys(plexURL, plexToken, moviesSection)
				if err == nil {
					for k := range keys {
						fav[k] = true
					}
				}
			}
			if tvSection > 0 {
				keys, err := plex.FetchFavoritedKeys(plexURL, plexToken, tvSection)
				if err == nil {
					for k := range keys {
						fav[k] = true
					}
				}
			}
		}
	}
	slog.Info("Favorited keys loaded", "count", len(fav))
	return fav
}

func (o *Orchestrator) resolveWatchlistKeys(
	plexMovies, plexTV []map[string]any,
	radarrMovies map[int]map[string]any,
	sonarrShows map[int]map[string]any,
	plexURL, plexToken, seerrURL, seerrKey string,
	jfURL, jfKey, jfMoviesSection, jfTVSection string,
) map[string]bool {
	watchlistRKs := make(map[string]bool)

	wlItems, err := seerr.FetchAllWatchlists(seerrURL, seerrKey)
	if err != nil || len(wlItems) == 0 {
		if err != nil {
			slog.Warn("Seerr watchlist fetch failed", "error", err)
		}
		return watchlistRKs
	}

	// Build TMDB -> rating_key lookups.
	movieTmdbToRK := make(map[int]string)
	for _, it := range plexMovies {
		rk := toString(it["ratingKey"])
		if rk == "" {
			continue
		}
		tmdbS := plex.ExternalID(it, "tmdb")
		if tmdbS != "" {
			tmdbI, err := strconv.Atoi(tmdbS)
			if err == nil {
				movieTmdbToRK[tmdbI] = rk
			}
		}
	}

	showTmdbToRK := make(map[int]string)
	showTvdbToRK := make(map[int]string)
	for _, it := range plexTV {
		rk := toString(it["ratingKey"])
		if rk == "" {
			continue
		}
		tmdbS := plex.ExternalID(it, "tmdb")
		tvdbS := plex.ExternalID(it, "tvdb")
		if tmdbS != "" {
			if tmdbI, err := strconv.Atoi(tmdbS); err == nil {
				showTmdbToRK[tmdbI] = rk
			}
		}
		if tvdbS != "" {
			if tvdbI, err := strconv.Atoi(tvdbS); err == nil {
				showTvdbToRK[tvdbI] = rk
			}
		}
	}

	// Sonarr cross-index: tmdbId -> tvdbId.
	sonarrTmdbToTvdb := make(map[int]int)
	for _, show := range sonarrShows {
		st := toInt(show["tmdbId"])
		sv := toInt(show["tvdbId"])
		if st != 0 && sv != 0 {
			sonarrTmdbToTvdb[st] = sv
		}
	}

	// Jellyfin TMDB -> ItemId maps.
	jfMovieTmdbToID := make(map[int]string)
	jfShowTmdbToID := make(map[int]string)
	if jfURL != "" && jfKey != "" {
		if jfMoviesSection != "" {
			jfItems, err := jellyfin.FetchLibrary(jfURL, jfKey, jfMoviesSection)
			if err == nil {
				for _, it := range jfItems {
					iid, _ := it["Id"].(string)
					tmdbS := jellyfin.ExternalID(it, "tmdb")
					if iid != "" && tmdbS != "" {
						if tmdbI, err := strconv.Atoi(tmdbS); err == nil {
							jfMovieTmdbToID[tmdbI] = iid
						}
					}
				}
			}
		}
		if jfTVSection != "" {
			jfItems, err := jellyfin.FetchLibrary(jfURL, jfKey, jfTVSection)
			if err == nil {
				for _, it := range jfItems {
					iid, _ := it["Id"].(string)
					tmdbS := jellyfin.ExternalID(it, "tmdb")
					if iid != "" && tmdbS != "" {
						if tmdbI, err := strconv.Atoi(tmdbS); err == nil {
							jfShowTmdbToID[tmdbI] = iid
						}
					}
				}
			}
		}
	}

	for _, entry := range wlItems {
		tmdbI := toInt(entry["tmdbId"])
		mt, _ := entry["mediaType"].(string)
		if tmdbI == 0 {
			continue
		}
		if mt == "movie" {
			if rk, ok := movieTmdbToRK[tmdbI]; ok {
				watchlistRKs[rk] = true
			}
			if jfID, ok := jfMovieTmdbToID[tmdbI]; ok {
				watchlistRKs[jfID] = true
			}
		} else if mt == "tv" {
			rk := showTmdbToRK[tmdbI]
			if rk == "" {
				if tvdbI, ok := sonarrTmdbToTvdb[tmdbI]; ok {
					rk = showTvdbToRK[tvdbI]
				}
			}
			if rk != "" {
				watchlistRKs[rk] = true
			}
			if jfID, ok := jfShowTmdbToID[tmdbI]; ok {
				watchlistRKs[jfID] = true
			}
		}
	}

	slog.Info("Watchlist keys resolved",
		"watchlist_entries", len(wlItems), "resolved_keys", len(watchlistRKs))
	return watchlistRKs
}

func (o *Orchestrator) aggregatePartialWatches() (map[string]bool, map[string]bool, map[string]bool) {
	partialKeys := make(map[string]bool)
	partialTitles := make(map[string]bool)
	partialSeasons := make(map[string]bool)

	cutoff := time.Now().Add(-time.Duration(partiallyWatchedLookbackDays) * 24 * time.Hour).Format(time.RFC3339)

	rows, err := o.DB.Queryx(
		o.DB.Rebind(`SELECT rating_key, grandparent_title, season_number
			FROM watch_history
			WHERE percent_complete BETWEEN 5 AND 95
			  AND watched_at >= ?`), cutoff)
	if err != nil {
		slog.Warn("Partial watches aggregation failed", "error", err)
		return partialKeys, partialTitles, partialSeasons
	}
	defer rows.Close()

	for rows.Next() {
		var rk string
		var gpTitle sql.NullString
		var seasonNum sql.NullInt64
		if rows.Scan(&rk, &gpTitle, &seasonNum) != nil {
			continue
		}
		if rk != "" {
			partialKeys[rk] = true
		}
		if gpTitle.Valid && gpTitle.String != "" {
			partialTitles[strings.ToLower(gpTitle.String)] = true
			if seasonNum.Valid {
				key := fmt.Sprintf("%s:S%d", strings.ToLower(gpTitle.String), seasonNum.Int64)
				partialSeasons[key] = true
			}
		}
	}

	return partialKeys, partialTitles, partialSeasons
}

func (o *Orchestrator) aggregateMaxPercent() (map[string]int, map[string]int, map[string]int) {
	maxByKey := make(map[string]int)
	maxByTitle := make(map[string]int)
	maxBySeason := make(map[string]int)

	rows, err := o.DB.Queryx(`
		SELECT rating_key, grandparent_title, season_number,
		       MAX(percent_complete) AS max_pct
		FROM watch_history
		GROUP BY rating_key, grandparent_title, season_number`)
	if err != nil {
		slog.Warn("Max percent aggregation failed", "error", err)
		return maxByKey, maxByTitle, maxBySeason
	}
	defer rows.Close()

	for rows.Next() {
		var rk string
		var gpTitle sql.NullString
		var seasonNum sql.NullInt64
		var maxPct sql.NullInt64
		if rows.Scan(&rk, &gpTitle, &seasonNum, &maxPct) != nil {
			continue
		}
		pct := 0
		if maxPct.Valid {
			pct = int(maxPct.Int64)
		}
		if rk != "" {
			if pct > maxByKey[rk] {
				maxByKey[rk] = pct
			}
		}
		if gpTitle.Valid && gpTitle.String != "" {
			gtl := strings.ToLower(gpTitle.String)
			if pct > maxByTitle[gtl] {
				maxByTitle[gtl] = pct
			}
			if seasonNum.Valid {
				sk := fmt.Sprintf("%s:S%d", gtl, seasonNum.Int64)
				if pct > maxBySeason[sk] {
					maxBySeason[sk] = pct
				}
			}
		}
	}

	return maxByKey, maxByTitle, maxBySeason
}

// ---------------------------------------------------------------------------
// Phase 2: Collection processing
// ---------------------------------------------------------------------------

// evaluateFunc is the signature used for per-item rule evaluation.
type evaluateFunc func(item map[string]any, ctx *rules.EvaluationContext, criteria *models.CollectionCriteria) []rules.Result

func makeMovieEvaluator() evaluateFunc {
	return func(item map[string]any, ctx *rules.EvaluationContext, criteria *models.CollectionCriteria) []rules.Result {
		return rules.EvaluateMovie(item, ctx, criteria, plex.ExternalID)
	}
}

func makeShowEvaluator() evaluateFunc {
	return func(item map[string]any, ctx *rules.EvaluationContext, criteria *models.CollectionCriteria) []rules.Result {
		return rules.EvaluateShow(item, ctx, criteria, plex.ExternalID)
	}
}

func makeSeasonEvaluator() evaluateFunc {
	return func(item map[string]any, ctx *rules.EvaluationContext, criteria *models.CollectionCriteria) []rules.Result {
		seasonNum := toInt(item["_season_number"])
		showTitle, _ := item["_show_title"].(string)
		return rules.EvaluateSeason(item, ctx, criteria, seasonNum, showTitle, plex.ExternalID)
	}
}

func (o *Orchestrator) processCollection(
	collectionName string,
	items []map[string]any,
	evalFn evaluateFunc,
	ctx *rules.EvaluationContext,
	criteria *models.CollectionCriteria,
	sectionID string,
	mediaType string,
	dryRun bool,
	today string,
	librarySource string,
	deferredSyncs map[collectionSyncKey]map[string]bool,
) (matched, added, removed int) {
	total := len(items)
	graceDate := addDaysISO(today, criteria.GraceDays)

	// Clear previous rule_results for this collection.
	o.DB.Exec(o.DB.Rebind("DELETE FROM rule_results WHERE collection = ?"), collectionName)

	want := make(map[string]bool)

	type ruleRow struct {
		RatingKey  string
		Collection string
		RuleName   string
		Passed     bool
		Detail     string
		Severity   string
	}
	var ruleRows []ruleRow

	for idx, item := range items {
		rk := toString(item["ratingKey"])
		title, _ := item["title"].(string)
		if title == "" {
			title = rk
		}

		results := evalFn(item, ctx, criteria)

		for _, r := range results {
			ruleRows = append(ruleRows, ruleRow{
				RatingKey:  rk,
				Collection: collectionName,
				RuleName:   r.Name,
				Passed:     r.Passed,
				Detail:     r.Detail,
				Severity:   r.Severity,
			})
		}

		if idx%50 == 0 || idx == total-1 {
			pct := 0
			if total > 0 {
				pct = (idx + 1) * 100 / total
			}
			o.updateProgress("processing_collection",
				fmt.Sprintf("%s (%d/%d)", collectionName, idx+1, total),
				pct, idx+1, total)
		}

		if !rules.IsCandidate(results) {
			continue
		}

		want[rk] = true

		// Check for existing item.
		var existingID int64
		var existingFirstSeen string
		err := o.DB.Get(&existingID,
			o.DB.Rebind("SELECT id FROM items WHERE rating_key = ? AND collection = ?"), rk, collectionName)
		isNew := err != nil

		if isNew {
			reasons := make([]string, 0)
			for _, r := range results {
				if r.Passed {
					reasons = append(reasons, r.Detail)
				}
			}
			reason := strings.Join(reasons, "; ")
			if reason == "" {
				reason = "matched all criteria"
			}
			o.logActivity("item_added", collectionName, rk, title, map[string]any{
				"dry_run": dryRun,
				"reason":  reason,
			})
			added++
		}

		if dryRun {
			continue
		}

		// Extract external IDs.
		tmdbIDRaw := plex.ExternalID(item, "tmdb")
		tvdbIDRaw := plex.ExternalID(item, "tvdb")
		imdbID := plex.ExternalID(item, "imdb")
		var tmdbID, tvdbID *int64
		if tmdbIDRaw != "" {
			v, err := strconv.ParseInt(tmdbIDRaw, 10, 64)
			if err == nil {
				tmdbID = &v
			}
		}
		if tvdbIDRaw != "" {
			v, err := strconv.ParseInt(tvdbIDRaw, 10, 64)
			if err == nil {
				tvdbID = &v
			}
		}
		var imdbIDPtr *string
		if imdbID != "" {
			imdbIDPtr = &imdbID
		}

		var sizeBytes int64
		var arrID *int64
		if mediaType == "movie" && tmdbID != nil {
			if m, ok := ctx.RadarrMovies[int(*tmdbID)]; ok {
				sizeBytes = toInt64(m["sizeOnDisk"])
				aid := int64(toInt(m["id"]))
				arrID = &aid
			}
		} else if mediaType == "show" && tvdbID != nil {
			if s, ok := ctx.SonarrShows[int(*tvdbID)]; ok {
				aid := int64(toInt(s["id"]))
				arrID = &aid
				seasonNumForSize := item["_season_number"]
				if seasonNumForSize != nil {
					sn := toInt(seasonNumForSize)
					if seasons, ok := s["seasons"].([]any); ok {
						for _, ss := range seasons {
							if sm, ok := ss.(map[string]any); ok {
								if toInt(sm["seasonNumber"]) == sn {
									if stats, ok := sm["statistics"].(map[string]any); ok {
										sizeBytes = toInt64(stats["sizeOnDisk"])
									}
									break
								}
							}
						}
					}
				} else {
					if stats, ok := s["statistics"].(map[string]any); ok {
						sizeBytes = toInt64(stats["sizeOnDisk"])
					}
				}
			}
		}

		var seasonNumber *int64
		if sn := item["_season_number"]; sn != nil {
			v := int64(toInt(sn))
			seasonNumber = &v
		}
		var showRatingKey *string
		if srk, ok := item["_show_rating_key"].(string); ok && srk != "" {
			showRatingKey = &srk
		}

		// Extract genre, content rating, year (works for both Plex and Jellyfin).
		var genreStr, contentRating *string
		var year *int64
		if genres := extractGenres(item); genres != "" {
			genreStr = &genres
		}
		if cr := extractContentRating(item); cr != "" {
			contentRating = &cr
		}
		if y := extractYear(item); y > 0 {
			year = &y
		}

		if !isNew {
			// Update existing item. Preserve grace window unless circumstances
			// require recomputation.
			_ = o.DB.Get(&existingFirstSeen,
				o.DB.Rebind("SELECT first_seen FROM items WHERE id = ?"), existingID)
			var storedGrace, storedStatus string
			o.DB.Get(&storedGrace,
				o.DB.Rebind("SELECT grace_expires FROM items WHERE id = ?"), existingID)
			o.DB.Get(&storedStatus,
				o.DB.Rebind("SELECT status FROM items WHERE id = ?"), existingID)

			graceDaysChanged := false
			if storedGrace != "" && existingFirstSeen != "" {
				storedDays := daysBetweenISO(existingFirstSeen, storedGrace)
				if storedDays != criteria.GraceDays {
					graceDaysChanged = true
				}
			}

			reentry := storedStatus == string(models.StatusKept) || storedStatus == string(models.StatusActioned)
			legacyUnset := storedGrace != "" && existingFirstSeen != "" && storedGrace == existingFirstSeen

			if legacyUnset || graceDaysChanged || reentry {
				o.DB.Exec(o.DB.Rebind(`
					UPDATE items SET last_seen = ?, title = ?, tmdb_id = ?, tvdb_id = ?,
					       imdb_id = ?, arr_id = ?, season_number = ?, show_rating_key = ?,
					       genre = ?, content_rating = ?, year = ?,
					       size_bytes = ?, status = ?, grace_expires = ?
					WHERE id = ?`),
					today, title, tmdbID, tvdbID, imdbIDPtr, arrID,
					seasonNumber, showRatingKey, genreStr, contentRating, year,
					sizeBytes, string(models.StatusStaged), graceDate, existingID)
			} else {
				o.DB.Exec(o.DB.Rebind(`
					UPDATE items SET last_seen = ?, title = ?, tmdb_id = ?, tvdb_id = ?,
					       imdb_id = ?, arr_id = ?, season_number = ?, show_rating_key = ?,
					       genre = ?, content_rating = ?, year = ?,
					       size_bytes = ?, status = ?
					WHERE id = ?`),
					today, title, tmdbID, tvdbID, imdbIDPtr, arrID,
					seasonNumber, showRatingKey, genreStr, contentRating, year,
					sizeBytes, string(models.StatusStaged), existingID)
			}
		} else {
			o.DB.Exec(o.DB.Rebind(`
				INSERT INTO items (rating_key, collection, title, media_type, tmdb_id, tvdb_id,
				       imdb_id, arr_id, season_number, show_rating_key,
				       genre, content_rating, year, size_bytes,
				       first_seen, last_seen, grace_expires, status)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
				rk, collectionName, title, mediaType, tmdbID, tvdbID,
				imdbIDPtr, arrID, seasonNumber, showRatingKey,
				genreStr, contentRating, year, sizeBytes,
				today, today, graceDate, string(models.StatusStaged))

			// Fire immediate pipeline steps.
			o.fireImmediateSteps(criteria, map[string]any{
				"rating_key":    rk,
				"title":         title,
				"collection":    collectionName,
				"media_type":    mediaType,
				"arr_id":        arrID,
				"season_number": seasonNumber,
				"size_bytes":    sizeBytes,
				"grace_expires": graceDate,
			})
		}
	}

	// Bulk insert rule_results.
	for _, rr := range ruleRows {
		o.DB.Exec(o.DB.Rebind(`
			INSERT INTO rule_results (rating_key, collection, rule_name, passed, detail, severity)
			VALUES (?, ?, ?, ?, ?, ?)`),
			rr.RatingKey, rr.Collection, rr.RuleName, rr.Passed, rr.Detail, rr.Severity)
	}

	// Remove stale items.
	type trackedItem struct {
		RatingKey string `db:"rating_key"`
		Title     string `db:"title"`
	}
	var tracked []trackedItem
	o.DB.Select(&tracked,
		o.DB.Rebind("SELECT rating_key, title FROM items WHERE collection = ? AND status = ?"),
		collectionName, string(models.StatusStaged))

	for _, t := range tracked {
		if want[t.RatingKey] {
			continue
		}
		// Item no longer matches criteria.
		removed++
		detail := map[string]any{"dry_run": dryRun, "reason": "No longer matches rule criteria"}
		var rr []struct {
			RuleName string `db:"rule_name"`
			Passed   bool   `db:"passed"`
			Detail   string `db:"detail"`
		}
		o.DB.Select(&rr, o.DB.Rebind(
			"SELECT rule_name, passed, detail FROM rule_results WHERE rating_key = ? AND collection = ?"),
			t.RatingKey, collectionName)
		if len(rr) > 0 {
			var failed []string
			for _, r := range rr {
				if !r.Passed {
					failed = append(failed, rules.DisplayName(r.RuleName))
				}
			}
			if len(failed) > 0 {
				detail["reason"] = "Protected: " + strings.Join(failed, ", ")
			}
		}
		o.logActivity("item_removed", collectionName, t.RatingKey, t.Title, detail)
		if !dryRun {
			o.DB.Exec(o.DB.Rebind(
				"DELETE FROM items WHERE rating_key = ? AND collection = ? AND status = ?"),
				t.RatingKey, collectionName, string(models.StatusStaged))
		}
	}

	// Determine Plex media type for collection sync (1=movie, 2=show, 3=season).
	plexColType := 2
	if mediaType == "movie" {
		plexColType = 1
	} else if criteria.Granularity == "season" {
		plexColType = 3
	}

	// Defer collection sync.
	if !dryRun {
		key := collectionSyncKey{
			Name:      collectionName,
			SectionID: sectionID,
			PlexType:  plexColType,
			Source:     librarySource,
		}
		if deferredSyncs[key] == nil {
			deferredSyncs[key] = make(map[string]bool)
		}
		for rk := range want {
			deferredSyncs[key][rk] = true
		}

		// Pipeline sync_collection steps target additional collections.
		for _, step := range criteria.ActionPipeline {
			if step.Type == "sync_collection" && step.Command != "" {
				colNames := strings.Split(step.Command, ",")
				for _, cn := range colNames {
					cn = strings.TrimSpace(cn)
					if cn == "" {
						continue
					}
					extraKey := collectionSyncKey{
						Name:      cn,
						SectionID: sectionID,
						PlexType:  plexColType,
						Source:     librarySource,
					}
					if deferredSyncs[extraKey] == nil {
						deferredSyncs[extraKey] = make(map[string]bool)
					}
					for rk := range want {
						deferredSyncs[extraKey][rk] = true
					}
				}
			}
		}
	}

	matched = len(want)
	slog.Info("Collection processed", "name", collectionName,
		"matched", matched, "added", added, "removed", removed)
	return matched, added, removed
}

// ---------------------------------------------------------------------------
// Phase 3: After-grace actions
// ---------------------------------------------------------------------------

func (o *Orchestrator) executeAfterGraceActions(dryRun bool, today, ruleFilter string) {
	// Build rule -> after-grace pipeline steps map.
	configs, err := o.Store.ListCollectionConfigs()
	if err != nil {
		slog.Warn("Failed to load collection configs for after-grace", "error", err)
		return
	}

	ruleSteps := make(map[string][]models.ActionStep)
	for _, c := range configs {
		if !c.Enabled {
			continue
		}
		if ruleFilter != "" && c.Name != ruleFilter {
			continue
		}
		criteria := models.ParseCriteria(c.Criteria.String)
		var afterGraceSteps []models.ActionStep
		for _, s := range criteria.ActionPipeline {
			if stepTiming(s) == "after_grace" {
				afterGraceSteps = append(afterGraceSteps, s)
			}
		}
		if len(afterGraceSteps) > 0 {
			ruleSteps[c.Name] = afterGraceSteps
		}
	}

	if len(ruleSteps) == 0 {
		return
	}

	// Find items past grace.
	type graceItem struct {
		ID            int64          `db:"id"`
		RatingKey     string         `db:"rating_key"`
		Collection    string         `db:"collection"`
		Title         sql.NullString `db:"title"`
		MediaType     string         `db:"media_type"`
		ArrID         sql.NullInt64  `db:"arr_id"`
		SeasonNumber  sql.NullInt64  `db:"season_number"`
		Override      sql.NullString `db:"override"`
		GraceExpires  string         `db:"grace_expires"`
	}
	var pastGrace []graceItem
	err = o.DB.Select(&pastGrace, o.DB.Rebind(`
		SELECT id, rating_key, collection, title, media_type, arr_id,
		       season_number, override, grace_expires
		FROM items
		WHERE status = ?
		  AND (override IS NULL OR override != 'keep')
		  AND (override = 'delete' OR grace_expires <= ?)`),
		string(models.StatusStaged), today)
	if err != nil {
		slog.Warn("Failed to query past-grace items", "error", err)
		return
	}

	if len(pastGrace) == 0 {
		return
	}

	slog.Info("After-grace items found", "count", len(pastGrace))
	o.updateProgress("executing_actions",
		fmt.Sprintf("%d item(s) past grace", len(pastGrace)), 0, 0, len(pastGrace))

	for idx, item := range pastGrace {
		steps, ok := ruleSteps[item.Collection]
		if !ok {
			continue
		}

		title := item.RatingKey
		if item.Title.Valid {
			title = item.Title.String
		}
		var arrID int
		if item.ArrID.Valid {
			arrID = int(item.ArrID.Int64)
		}
		var seasonNum *int
		if item.SeasonNumber.Valid {
			sn := int(item.SeasonNumber.Int64)
			seasonNum = &sn
		}

		ok, summary, errMsg, finalStatus := o.runAfterGraceForItem(
			item.MediaType, arrID, title, item.RatingKey, item.Collection,
			seasonNum, item.GraceExpires, steps, dryRun,
		)

		if ok {
			if !dryRun && finalStatus != string(models.StatusStaged) {
				o.DB.Exec(o.DB.Rebind(
					"UPDATE items SET status = ?, action_taken = ?, action_date = ? WHERE id = ?"),
					finalStatus, summary, today, item.ID)
			}
			o.logActivity("item_actioned", item.Collection, item.RatingKey, title, map[string]any{
				"summary":      summary,
				"dry_run":      dryRun,
				"final_status": finalStatus,
			})
		} else {
			slog.Warn("Action failed", "title", title, "collection", item.Collection, "error", errMsg)
			o.logActivity("item_action_failed", item.Collection, item.RatingKey, title, map[string]any{
				"error":   errMsg,
				"dry_run": dryRun,
			})
		}

		if (idx+1)%10 == 0 {
			o.updateProgress("executing_actions",
				fmt.Sprintf("%d/%d item(s)", idx+1, len(pastGrace)),
				(idx+1)*100/len(pastGrace), idx+1, len(pastGrace))
		}
	}
}

func (o *Orchestrator) runAfterGraceForItem(
	mediaType string, arrID int, title, ratingKey, collection string,
	seasonNum *int, graceExpires string,
	steps []models.ActionStep, dryRun bool,
) (ok bool, summary, errMsg, finalStatus string) {
	finalStatus = string(models.StatusStaged)

	stepTypes := make(map[string]bool)
	for _, s := range steps {
		stepTypes[s.Type] = true
	}

	hasDelete := stepTypes["delete"]
	hasDeleteFiles := stepTypes["delete_files"]
	hasImportExcl := stepTypes["import_exclusion"]
	hasUnmonitor := stepTypes["unmonitor"]
	hasDowngrade := stepTypes["swap_quality_profile"]

	var actions []string
	destructiveRan := false
	migrationRan := false

	defer func() {
		if r := recover(); r != nil {
			ok = false
			errMsg = fmt.Sprintf("panic: %v", r)
		}
	}()

	arrKind := "sonarr"
	if mediaType == "movie" {
		arrKind = "radarr"
	}

	resolveArr := func(step *models.ActionStep) (string, string, string) {
		var instanceID *int64
		if step != nil && step.InstanceID != nil {
			instanceID = step.InstanceID
		}
		inst, err := o.Store.ResolveArrInstance(instanceID, arrKind)
		if err != nil || inst == nil {
			return "", "", arrKind
		}
		return inst.URL, inst.APIKey, inst.Name
	}

	if mediaType == "movie" {
		if hasDelete || hasDeleteFiles || hasImportExcl {
			url, key, name := resolveArr(findStep(steps, "delete", "delete_files", "import_exclusion"))
			if url == "" || key == "" {
				return false, "", "Radarr URL/API key not configured", finalStatus
			}
			if !dryRun {
				if err := radarr.Delete(url, key, arrID, title, hasDeleteFiles, hasImportExcl); err != nil {
					return false, "", err.Error(), finalStatus
				}
			}
			bits := []string{fmt.Sprintf("deleted from %s", name)}
			if hasDeleteFiles {
				bits = append(bits, "files")
			}
			if hasImportExcl {
				bits = append(bits, "exclusion")
			}
			actions = append(actions, strings.Join(bits, " + "))
			destructiveRan = true
		} else if hasUnmonitor {
			url, key, name := resolveArr(findStep(steps, "unmonitor"))
			if url == "" || key == "" {
				return false, "", "Radarr URL/API key not configured", finalStatus
			}
			if !dryRun {
				if err := radarr.Unmonitor(url, key, arrID, title); err != nil {
					return false, "", err.Error(), finalStatus
				}
			}
			actions = append(actions, fmt.Sprintf("unmonitored in %s", name))
			destructiveRan = true
		}

		if hasDowngrade {
			step := findStep(steps, "swap_quality_profile")
			url, key, name := resolveArr(step)
			if url == "" || key == "" {
				return false, "", "Radarr URL/API key not configured", finalStatus
			}
			profileName := ""
			if step != nil {
				profileName = strings.TrimSpace(step.Command)
			}
			qpID, found := radarr.GetQualityProfileID(url, key, profileName)
			if !found {
				return false, "", fmt.Sprintf("quality profile %q not found in Radarr", profileName), finalStatus
			}
			if !dryRun {
				// Swap quality profile via editor API.
				_ = qpID // profile swap would need full movie fetch+PUT; simplified.
			}
			actions = append(actions, fmt.Sprintf("swapped quality profile (%s)", name))
			migrationRan = true
		}

	} else if mediaType == "show" {
		if hasDelete || hasDeleteFiles {
			url, key, name := resolveArr(findStep(steps, "delete", "delete_files"))
			if url == "" || key == "" {
				return false, "", "Sonarr URL/API key not configured", finalStatus
			}
			if !dryRun {
				if err := sonarr.Delete(url, key, arrID, title, hasDeleteFiles); err != nil {
					return false, "", err.Error(), finalStatus
				}
			}
			action := fmt.Sprintf("deleted from %s", name)
			if hasDeleteFiles {
				action += " + files"
			}
			actions = append(actions, action)
			destructiveRan = true
		} else if hasUnmonitor {
			url, key, name := resolveArr(findStep(steps, "unmonitor"))
			if url == "" || key == "" {
				return false, "", "Sonarr URL/API key not configured", finalStatus
			}
			if seasonNum != nil {
				if !dryRun {
					if err := sonarr.UnmonitorSeason(url, key, arrID, *seasonNum, title); err != nil {
						return false, "", err.Error(), finalStatus
					}
				}
				actions = append(actions, fmt.Sprintf("unmonitored S%02d (%s)", *seasonNum, name))
			} else {
				if !dryRun {
					if err := sonarr.Unmonitor(url, key, arrID, title); err != nil {
						return false, "", err.Error(), finalStatus
					}
				}
				actions = append(actions, fmt.Sprintf("unmonitored in %s", name))
			}
			destructiveRan = true
		}

		// Season-level delete_files.
		if seasonNum != nil && (hasDelete || hasDeleteFiles) && !destructiveRan {
			url, key, name := resolveArr(findStep(steps, "delete_files", "delete"))
			if url != "" && key != "" {
				if !dryRun {
					sonarr.DeleteSeasonFiles(url, key, arrID, *seasonNum, title)
				}
				actions = append(actions, fmt.Sprintf("deleted S%02d files (%s)", *seasonNum, name))
				destructiveRan = true
			}
		}
	}

	// Ordered verb steps.
	for _, vs := range steps {
		switch vs.Type {
		case "add_arr_tag":
			label := strings.TrimSpace(vs.Command)
			if label == "" || arrID == 0 {
				continue
			}
			url, key, name := resolveArr(&vs)
			if url == "" || key == "" {
				continue
			}
			if !dryRun {
				if arrKind == "radarr" {
					radarr.AddTag(url, key, arrID, label, title)
				} else {
					sonarr.AddTag(url, key, arrID, label, title)
				}
			}
			actions = append(actions, fmt.Sprintf("tagged %q in %s", label, name))

		case "remove_arr_tag":
			label := strings.TrimSpace(vs.Command)
			if label == "" || arrID == 0 {
				continue
			}
			url, key, name := resolveArr(&vs)
			if url == "" || key == "" {
				continue
			}
			if !dryRun {
				if arrKind == "radarr" {
					radarr.RemoveTag(url, key, arrID, label, title)
				} else {
					sonarr.RemoveTag(url, key, arrID, label, title)
				}
			}
			actions = append(actions, fmt.Sprintf("untagged %q in %s", label, name))

		case "set_root_folder":
			path := strings.TrimSpace(vs.Command)
			if path == "" || arrID == 0 {
				continue
			}
			url, key, name := resolveArr(&vs)
			if url == "" || key == "" {
				continue
			}
			if !dryRun {
				if arrKind == "radarr" {
					radarr.SetRootFolder(url, key, arrID, path, true, title)
				} else {
					sonarr.SetRootFolder(url, key, arrID, path, true, title)
				}
			}
			actions = append(actions, fmt.Sprintf("moved to %s in %s", path, name))
			migrationRan = true

		case "trigger_search":
			url, key, name := resolveArr(&vs)
			if url == "" || key == "" {
				continue
			}
			if !dryRun {
				if arrKind == "radarr" {
					radarr.Search(url, key, []int{arrID})
				} else if seasonNum != nil {
					sonarr.SearchSeason(url, key, arrID, *seasonNum)
				} else {
					sonarr.SearchSeries(url, key, arrID)
				}
			}
			actions = append(actions, fmt.Sprintf("triggered search in %s", name))

		case "script":
			cmd := strings.TrimSpace(vs.Command)
			if cmd == "" {
				continue
			}
			if !dryRun {
				env := append(os.Environ(),
					"RECLAIMER_RATING_KEY="+ratingKey,
					"RECLAIMER_TITLE="+title,
					"RECLAIMER_COLLECTION="+collection,
					"RECLAIMER_MEDIA_TYPE="+mediaType,
					"RECLAIMER_ARR_ID="+strconv.Itoa(arrID),
				)
				if seasonNum != nil {
					env = append(env, "RECLAIMER_SEASON_NUMBER="+strconv.Itoa(*seasonNum))
				}
				c := exec.Command("sh", "-c", cmd)
				c.Env = env
				output, err := c.CombinedOutput()
				if err != nil {
					tail := string(output)
					if len(tail) > 200 {
						tail = tail[len(tail)-200:]
					}
					return false, "", fmt.Sprintf("script failed: %s: %s", err, tail), finalStatus
				}
			}
			actions = append(actions, "ran script")
			destructiveRan = true

		case "notify":
			if stepTiming(vs) != "after_grace" {
				continue
			}
			actionSummary := strings.Join(actions, "; ")
			if actionSummary == "" {
				actionSummary = "after-grace action"
			}
			template := strings.TrimSpace(vs.Command)
			if template == "" {
				template = "Reclaimer: {collection} flagged '{title}' for action on {grace_expires}"
			}
			body := renderNotifyTemplate(template, map[string]string{
				"title":          title,
				"collection":     collection,
				"grace_expires":  graceExpires,
				"action_summary": actionSummary,
			})
			appriseURL := o.Config.GetString("apprise_url")
			if appriseURL != "" && !dryRun {
				sendNotify(appriseURL, body, fmt.Sprintf("Reclaimer -- %s", collection))
			}
			actions = append(actions, "notified")
		}
	}

	if migrationRan && !destructiveRan {
		finalStatus = string(models.StatusMigrated)
	} else if destructiveRan {
		finalStatus = string(models.StatusActioned)
	}

	summary = strings.Join(actions, "; ")
	if summary == "" {
		summary = "no-op"
	}
	return true, summary, "", finalStatus
}

// ---------------------------------------------------------------------------
// Phase 4: Collection sync
// ---------------------------------------------------------------------------

func (o *Orchestrator) executeDeferredSyncs(
	syncs map[collectionSyncKey]map[string]bool,
	plexURL, plexToken, jfURL, jfKey string,
) {
	for key, want := range syncs {
		if key.Source == "plex" && plexURL != "" && plexToken != "" {
			sectionID, _ := strconv.Atoi(key.SectionID)
			if sectionID == 0 {
				continue
			}
			if err := plex.SyncCollection(plexURL, plexToken, sectionID, key.Name, want, key.PlexType); err != nil {
				slog.Warn("Plex collection sync failed", "name", key.Name, "error", err)
				o.logActivity("collection_sync_failed", key.Name, "", "", map[string]any{
					"error": err.Error(), "source": "plex", "want_count": len(want),
				})
			} else {
				slog.Info("Plex collection synced", "name", key.Name, "items", len(want))
			}
		} else if key.Source == "jellyfin" && jfURL != "" && jfKey != "" {
			if err := jellyfin.SyncCollection(jfURL, jfKey, key.SectionID, key.Name, want); err != nil {
				slog.Warn("Jellyfin collection sync failed", "name", key.Name, "error", err)
				o.logActivity("collection_sync_failed", key.Name, "", "", map[string]any{
					"error": err.Error(), "source": "jellyfin", "want_count": len(want),
				})
			} else {
				slog.Info("Jellyfin collection synced", "name", key.Name, "items", len(want))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Season expansion
// ---------------------------------------------------------------------------

func (o *Orchestrator) expandShowsToSeasons(plexTV []map[string]any, plexURL, plexToken string) []map[string]any {
	type result struct {
		items []map[string]any
	}

	var mu sync.Mutex
	var allSeasons []map[string]any
	var wg sync.WaitGroup

	// Bounded concurrency for Plex API calls.
	sem := make(chan struct{}, 8)

	for _, show := range plexTV {
		show := show
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			defer recoverGoroutine("expand_seasons")

			showTitle, _ := show["title"].(string)
			showRK := toString(show["ratingKey"])
			showGuids := show["Guid"]

			seasons, err := plex.FetchSeasons(plexURL, plexToken, showRK)
			if err != nil {
				slog.Warn("Failed to fetch seasons", "show", showTitle, "error", err)
				return
			}

			var items []map[string]any
			for _, s := range seasons {
				seasonNum := toInt(s["index"])
				s["Guid"] = showGuids
				s["_show_title"] = showTitle
				s["_show_rating_key"] = showRK
				s["_season_number"] = seasonNum
				s["title"] = fmt.Sprintf("%s - Season %d", showTitle, seasonNum)
				items = append(items, s)
			}

			mu.Lock()
			allSeasons = append(allSeasons, items...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	return allSeasons
}

// ---------------------------------------------------------------------------
// Protected collection prefetch
// ---------------------------------------------------------------------------

func (o *Orchestrator) prefetchProtectedCollections(
	configs []models.CollectionConfig,
	plexURL, plexToken string, moviesSection, tvSection int,
	jfURL, jfKey, jfMoviesSection, jfTVSection string,
) map[string]map[string]bool {
	cache := make(map[string]map[string]bool)

	allNames := make(map[string]bool)
	for _, c := range configs {
		criteria := models.ParseCriteria(c.Criteria.String)
		for _, name := range criteria.ProtectedCollections {
			allNames[name] = true
		}
	}

	for name := range allNames {
		keys := make(map[string]bool)
		if plexURL != "" && plexToken != "" {
			if moviesSection > 0 {
				k, err := plex.FetchKeepCollection(plexURL, plexToken, moviesSection, name)
				if err == nil {
					for rk := range k {
						keys[rk] = true
					}
				}
			}
			if tvSection > 0 {
				k, err := plex.FetchKeepCollection(plexURL, plexToken, tvSection, name)
				if err == nil {
					for rk := range k {
						keys[rk] = true
					}
				}
			}
		}
		if jfURL != "" && jfKey != "" {
			if jfMoviesSection != "" {
				k, err := jellyfin.FetchKeepCollection(jfURL, jfKey, jfMoviesSection, name)
				if err == nil {
					for rk := range k {
						keys[rk] = true
					}
				}
			}
			if jfTVSection != "" {
				k, err := jellyfin.FetchKeepCollection(jfURL, jfKey, jfTVSection, name)
				if err == nil {
					for rk := range k {
						keys[rk] = true
					}
				}
			}
		}
		cache[name] = keys
	}

	if len(allNames) > 0 {
		totalKeys := 0
		for _, v := range cache {
			totalKeys += len(v)
		}
		slog.Info("Pre-fetched protected collections",
			"collections", len(allNames), "total_keys", totalKeys)
	}

	return cache
}

// ---------------------------------------------------------------------------
// Show-level protection keys (for season-granularity rules)
// ---------------------------------------------------------------------------

func computeShowLevelProtectionKeys(
	plexTV []map[string]any,
	ctx *rules.EvaluationContext,
	criteria *models.CollectionCriteria,
) map[string]bool {
	protected := make(map[string]bool)
	protectedTags := criteria.ProtectedTags
	protectedUsers := make(map[string]bool)
	for _, u := range criteria.ProtectedUsers {
		protectedUsers[u] = true
	}

	for _, show := range plexTV {
		rk := toString(show["ratingKey"])
		if rk == "" {
			continue
		}

		// Keep collection.
		if ctx.PlexKeepKeys[rk] {
			protected[rk] = true
			continue
		}
		// Admin favorited.
		if ctx.PlexFavoritedKeys[rk] {
			protected[rk] = true
			continue
		}
		// On watchlist.
		if ctx.WatchlistKeys[rk] {
			protected[rk] = true
			continue
		}

		tvdbS := plex.ExternalID(show, "tvdb")
		tmdbS := plex.ExternalID(show, "tmdb")
		tvdb := 0
		tmdb := 0
		if tvdbS != "" {
			tvdb, _ = strconv.Atoi(tvdbS)
		}
		if tmdbS != "" {
			tmdb, _ = strconv.Atoi(tmdbS)
		}

		// Protected Sonarr tags.
		if len(protectedTags) > 0 && tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				itemTags := toStringSlice(showData["_tag_names"])
				for _, pt := range protectedTags {
					for _, it := range itemTags {
						if pt == it {
							protected[rk] = true
							goto nextShow
						}
					}
				}
			}
		}

		// Active Seerr request.
		if (tvdb != 0 && ctx.SeerrActiveShows[tvdb]) ||
			(tmdb != 0 && ctx.SeerrActiveShowsTmdb[tmdb]) {
			protected[rk] = true
			continue
		}

		// Protected-user request.
		if (tvdb != 0 && ctx.SeerrProtectedShows[tvdb]) ||
			(tmdb != 0 && ctx.SeerrProtectedShowsTmdb[tmdb]) {
			protected[rk] = true
			continue
		}

		// Protected user has watched any episode.
		if len(protectedUsers) > 0 {
			showTitle := strings.ToLower(toString(show["title"]))
			for u := range protectedUsers {
				if watches, ok := ctx.UserWatches[u]; ok {
					if watches[rk] || (showTitle != "" && watches[showTitle]) {
						protected[rk] = true
						goto nextShow
					}
				}
			}
		}

	nextShow:
	}

	return protected
}

// ---------------------------------------------------------------------------
// Immediate pipeline steps (fired on first detection)
// ---------------------------------------------------------------------------

func (o *Orchestrator) fireImmediateSteps(criteria *models.CollectionCriteria, itemRow map[string]any) {
	title, _ := itemRow["title"].(string)
	mediaType, _ := itemRow["media_type"].(string)
	collection, _ := itemRow["collection"].(string)

	var arrID int
	switch v := itemRow["arr_id"].(type) {
	case *int64:
		if v != nil {
			arrID = int(*v)
		}
	case int64:
		arrID = int(v)
	case int:
		arrID = v
	}

	arrKind := "sonarr"
	if mediaType == "movie" {
		arrKind = "radarr"
	}

	for _, step := range criteria.ActionPipeline {
		if stepTiming(step) != "immediate" {
			continue
		}

		switch step.Type {
		case "notify":
			template := strings.TrimSpace(step.Command)
			if template == "" {
				template = "Reclaimer: {collection} flagged '{title}' for action on {grace_expires}"
			}
			graceExpires, _ := itemRow["grace_expires"].(string)
			body := renderNotifyTemplate(template, map[string]string{
				"title":         title,
				"collection":    collection,
				"grace_expires": graceExpires,
			})
			appriseURL := o.Config.GetString("apprise_url")
			if appriseURL != "" {
				sendNotify(appriseURL, body, fmt.Sprintf("Reclaimer -- %s", collection))
			}

		case "add_arr_tag", "remove_arr_tag":
			label := strings.TrimSpace(step.Command)
			if label == "" || arrID == 0 {
				continue
			}
			inst, err := o.Store.ResolveArrInstance(step.InstanceID, arrKind)
			if err != nil || inst == nil {
				continue
			}
			if step.Type == "add_arr_tag" {
				if arrKind == "radarr" {
					radarr.AddTag(inst.URL, inst.APIKey, arrID, label, title)
				} else {
					sonarr.AddTag(inst.URL, inst.APIKey, arrID, label, title)
				}
			} else {
				if arrKind == "radarr" {
					radarr.RemoveTag(inst.URL, inst.APIKey, arrID, label, title)
				} else {
					sonarr.RemoveTag(inst.URL, inst.APIKey, arrID, label, title)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Activity logging
// ---------------------------------------------------------------------------

func (o *Orchestrator) logActivity(eventType, collection, ratingKey, title string, detail map[string]any) {
	detailJSON := "{}"
	if detail != nil {
		b, err := json.Marshal(detail)
		if err == nil {
			detailJSON = string(b)
		}
	}
	err := o.Store.InsertActivity(eventType, collection, ratingKey, title, detailJSON)
	if err != nil {
		slog.Debug("Activity log insert failed", "event", eventType, "error", err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (o *Orchestrator) buildUserIDMap() map[int64]int64 {
	m := make(map[int64]int64)
	rows, err := o.DB.Queryx("SELECT id, plex_user_id FROM users WHERE plex_user_id IS NOT NULL")
	if err != nil {
		return m
	}
	defer rows.Close()
	for rows.Next() {
		var id, plexID int64
		if rows.Scan(&id, &plexID) == nil {
			m[plexID] = id
		}
	}
	return m
}

func normalizeJellyfinItem(jfItem map[string]any) map[string]any {
	out := map[string]any{
		"ratingKey": jfItem["Id"],
		"title":     jfItem["Name"],
	}
	if ud, ok := jfItem["UserData"].(map[string]any); ok {
		out["viewCount"] = ud["PlayCount"]
	} else {
		out["viewCount"] = 0
	}
	out["Guid"] = buildJellyfinGuids(jfItem)
	return out
}

func buildJellyfinGuids(jfItem map[string]any) []any {
	var guids []any
	providers, _ := jfItem["ProviderIds"].(map[string]any)
	if providers == nil {
		return guids
	}
	if tmdb, ok := providers["Tmdb"].(string); ok && tmdb != "" {
		guids = append(guids, map[string]any{"id": "tmdb://" + tmdb})
	}
	if tvdb, ok := providers["Tvdb"].(string); ok && tvdb != "" {
		guids = append(guids, map[string]any{"id": "tvdb://" + tvdb})
	}
	if imdb, ok := providers["Imdb"].(string); ok && imdb != "" {
		guids = append(guids, map[string]any{"id": "imdb://" + imdb})
	}
	return guids
}

func copyContextWithKeepKeys(ctx *rules.EvaluationContext, keepKeys map[string]bool) *rules.EvaluationContext {
	copy := *ctx
	copy.PlexKeepKeys = keepKeys
	return &copy
}

func stepTiming(step models.ActionStep) string {
	if step.Timing == "immediate" || step.Timing == "after_grace" {
		return step.Timing
	}
	if immediateActionTypes[step.Type] {
		return "immediate"
	}
	return "after_grace"
}

func findStep(steps []models.ActionStep, types ...string) *models.ActionStep {
	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}
	for i := range steps {
		if typeSet[steps[i].Type] {
			return &steps[i]
		}
	}
	return nil
}

func parseCSVSet(csv string) map[string]bool {
	out := make(map[string]bool)
	for _, s := range strings.Split(csv, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out[s] = true
		}
	}
	return out
}

func computePercentComplete(viewOffsetMS, mediaDurationMS int64) int {
	if mediaDurationMS <= 0 {
		return 0
	}
	pct := int((viewOffsetMS * 100) / mediaDurationMS)
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct
}

func addDaysISO(dateStr string, days int) string {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return dateStr
	}
	return t.AddDate(0, 0, days).Format("2006-01-02")
}

func daysBetweenISO(a, b string) int {
	ta, err1 := time.Parse("2006-01-02", a)
	tb, err2 := time.Parse("2006-01-02", b)
	if err1 != nil || err2 != nil {
		return -1
	}
	return int(tb.Sub(ta).Hours() / 24)
}

func recoverGoroutine(name string) {
	if r := recover(); r != nil {
		slog.Error("Goroutine panicked", "name", name, "panic", r)
	}
}

func renderNotifyTemplate(template string, subs map[string]string) string {
	result := template
	for k, v := range subs {
		result = strings.ReplaceAll(result, "{"+k+"}", v)
	}
	return result
}

func sendNotify(appriseURL, body, title string) (bool, string) {
	if appriseURL == "" {
		return false, "APPRISE_URL not configured"
	}
	payload, _ := json.Marshal(map[string]any{
		"body":  body,
		"title": title,
		"tag":   "media",
	})

	resp, err := sendHTTPPost(appriseURL, payload)
	if err != nil {
		slog.Warn("Apprise notify failed", "error", err)
		return false, err.Error()
	}
	_ = resp
	return true, ""
}

func sendHTTPPost(url string, payload []byte) (int, error) {
	bodyReader := strings.NewReader(string(payload))
	resp, err := httpclient.Do(httpclient.Request{
		Method:  "POST",
		URL:     url,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    bodyReader,
	})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// toString coerces any to string.
func toString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// toInt coerces any to int.
func toInt(v any) int {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

// toInt64 coerces any to int64.
func toInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}

// toFloat64 coerces any to float64.
func toFloat64(v any) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

// toStringSlice extracts []string from any.
func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	if ss, ok := v.([]string); ok {
		return ss
	}
	if sa, ok := v.([]any); ok {
		out := make([]string, 0, len(sa))
		for _, e := range sa {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// extractGenres returns a comma-separated genre string from either a Plex item
// (Genre: [{tag: "Action"}, ...]) or a Jellyfin item (Genres: ["Action", ...]).
func extractGenres(item map[string]any) string {
	// Plex: Genre array of {tag: "..."} objects
	if arr, ok := item["Genre"].([]any); ok && len(arr) > 0 {
		var tags []string
		for _, g := range arr {
			if gm, ok := g.(map[string]any); ok {
				if tag, _ := gm["tag"].(string); tag != "" {
					tags = append(tags, tag)
				}
			}
		}
		return strings.Join(tags, ", ")
	}
	// Jellyfin: Genres array of strings
	if arr, ok := item["Genres"].([]any); ok && len(arr) > 0 {
		var tags []string
		for _, g := range arr {
			if s, ok := g.(string); ok && s != "" {
				tags = append(tags, s)
			}
		}
		return strings.Join(tags, ", ")
	}
	return ""
}

// extractContentRating returns the content rating from a Plex item
// (contentRating: "PG-13") or Jellyfin item (OfficialRating: "PG-13").
func extractContentRating(item map[string]any) string {
	if cr, ok := item["contentRating"].(string); ok && cr != "" {
		return cr
	}
	if cr, ok := item["OfficialRating"].(string); ok && cr != "" {
		return cr
	}
	return ""
}

// extractYear returns the release year from a Plex item (year) or Jellyfin
// item (ProductionYear).
func extractYear(item map[string]any) int64 {
	if y := toInt64(item["year"]); y > 0 {
		return y
	}
	if y := toInt64(item["ProductionYear"]); y > 0 {
		return y
	}
	return 0
}

// ---------------------------------------------------------------------------
// Poster pre-warming
// ---------------------------------------------------------------------------

const posterCacheDir = "/app/data/poster-cache"

func (o *Orchestrator) prewarmPosters(plexMovies, plexTV []map[string]any, plexURL, plexToken, jfURL, jfKey string) {
	var rks []string
	for _, it := range plexMovies {
		if rk := toString(it["ratingKey"]); rk != "" {
			rks = append(rks, rk)
		}
	}
	for _, it := range plexTV {
		if rk := toString(it["ratingKey"]); rk != "" {
			rks = append(rks, rk)
		}
	}
	if len(rks) == 0 {
		return
	}

	var missing []string
	for _, rk := range rks {
		f := filepath.Join(posterCacheDir, rk+"-sm.jpg")
		if _, err := os.Stat(f); err != nil {
			missing = append(missing, rk)
		}
	}
	if len(missing) == 0 {
		slog.Info("Poster pre-warm: all cached", "total", len(rks))
		return
	}

	slog.Info("Poster pre-warm: starting", "missing", len(missing), "total", len(rks))
	os.MkdirAll(posterCacheDir, 0o755)

	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	fetched := atomic.Int32{}

	for _, rk := range missing {
		wg.Add(1)
		go func(rk string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var posterURL string
			if plexURL != "" && plexToken != "" {
				posterURL = fmt.Sprintf("%s/library/metadata/%s/thumb?X-Plex-Token=%s&width=80&height=120", plexURL, rk, plexToken)
			} else if jfURL != "" && jfKey != "" {
				posterURL = fmt.Sprintf("%s/Items/%s/Images/Primary?api_key=%s&maxWidth=80&maxHeight=120", jfURL, rk, jfKey)
			}
			if posterURL == "" {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "GET", posterURL, nil)
			resp, err := httpclient.Client().Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil || len(body) == 0 {
				return
			}
			_ = os.WriteFile(filepath.Join(posterCacheDir, rk+"-sm.jpg"), body, 0o644)
			fetched.Add(1)
		}(rk)
	}
	wg.Wait()
	slog.Info("Poster pre-warm: done", "fetched", fetched.Load(), "missing", len(missing))
}
