package rules

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rinseaid/reclaimer/internal/models"
	"github.com/rinseaid/reclaimer/internal/services/overseerr"
)

type EvaluationContext struct {
	PlayCounts                  map[string]int
	RadarrMovies                map[int]map[string]any
	SonarrShows                 map[int]map[string]any
	OverseerrActiveMovies       map[int]bool
	OverseerrActiveShows        map[int]bool
	OverseerrActiveShowsTmdb    map[int]bool
	OverseerrProtectedMovies    map[int]bool
	OverseerrProtectedShows     map[int]bool
	OverseerrProtectedShowsTmdb map[int]bool
	PlexKeepKeys                map[string]bool
	DebridCached                map[string]bool
	DBPlays                     map[string]int
	DBPlaysByTitle              map[string]int
	MovieRequesters             map[int]string
	ShowRequesters              map[int]string
	ShowRequestersTmdb          map[int]string
	UserWatches                 map[string]map[string]bool
	LastWatchDates              map[string]string
	LastWatchByTitle            map[string]string
	RatingsCache                map[string]models.Ratings
	DBPlaysBySeason             map[string]int
	LastWatchBySeason           map[string]string
	UserWatchesBySeason         map[string]map[string]bool
	PlexFavoritedKeys           map[string]bool
	WatchlistKeys               map[string]bool
	PartiallyWatchedKeys        map[string]bool
	PartiallyWatchedByTitle     map[string]bool
	PartiallyWatchedBySeason    map[string]bool
	AddedAtByKey                map[string]int64
	ShowSeasonCounts            map[int]int
	ShowLevelProtectionKeys     map[string]bool
	MaxPercentByKey             map[string]int
	MaxPercentByTitle           map[string]int
	MaxPercentBySeason          map[string]int
}

func NewEvaluationContext() *EvaluationContext {
	return &EvaluationContext{
		PlayCounts:                  make(map[string]int),
		RadarrMovies:                make(map[int]map[string]any),
		SonarrShows:                 make(map[int]map[string]any),
		OverseerrActiveMovies:       make(map[int]bool),
		OverseerrActiveShows:        make(map[int]bool),
		OverseerrActiveShowsTmdb:    make(map[int]bool),
		OverseerrProtectedMovies:    make(map[int]bool),
		OverseerrProtectedShows:     make(map[int]bool),
		OverseerrProtectedShowsTmdb: make(map[int]bool),
		PlexKeepKeys:                make(map[string]bool),
		DebridCached:                make(map[string]bool),
		DBPlays:                     make(map[string]int),
		DBPlaysByTitle:              make(map[string]int),
		MovieRequesters:             make(map[int]string),
		ShowRequesters:              make(map[int]string),
		ShowRequestersTmdb:          make(map[int]string),
		UserWatches:                 make(map[string]map[string]bool),
		LastWatchDates:              make(map[string]string),
		LastWatchByTitle:            make(map[string]string),
		RatingsCache:                make(map[string]models.Ratings),
		DBPlaysBySeason:             make(map[string]int),
		LastWatchBySeason:           make(map[string]string),
		UserWatchesBySeason:         make(map[string]map[string]bool),
		PlexFavoritedKeys:           make(map[string]bool),
		WatchlistKeys:               make(map[string]bool),
		PartiallyWatchedKeys:        make(map[string]bool),
		PartiallyWatchedByTitle:     make(map[string]bool),
		PartiallyWatchedBySeason:    make(map[string]bool),
		AddedAtByKey:                make(map[string]int64),
		ShowSeasonCounts:            make(map[int]int),
		ShowLevelProtectionKeys:     make(map[string]bool),
		MaxPercentByKey:             make(map[string]int),
		MaxPercentByTitle:           make(map[string]int),
		MaxPercentBySeason:          make(map[string]int),
	}
}

type Result struct {
	Name     string
	Passed   bool
	Detail   string
	Severity string
}

func EvaluateMovie(item map[string]any, ctx *EvaluationContext, criteria *models.CollectionCriteria, externalIDFn func(map[string]any, string) string) []Result {
	var results []Result
	rk := fmt.Sprintf("%v", item["ratingKey"])

	// 1. Never watched
	if criteria.IsRuleEnabled("never_watched") {
		nw := criteria.NeverWatched
		plexViews := toInt(item["viewCount"])
		playCounts := ctx.PlayCounts[rk]
		dbPlays := ctx.DBPlays[rk]

		watchedPlex := nw.CheckPlexViews && plexViews > 0
		watchedCounts := nw.CheckDBPlays && playCounts > 0
		watchedDB := dbPlays > 0

		if !watchedPlex && !watchedCounts && !watchedDB {
			results = append(results, Result{"never_watched", true,
				fmt.Sprintf("No play history (Plex views=%d, DB plays=%d)", plexViews, max(playCounts, dbPlays)), "info"})
		} else {
			total := max(plexViews, playCounts, dbPlays)
			s := "s"
			if total == 1 {
				s = ""
			}
			results = append(results, Result{"never_watched", false,
				fmt.Sprintf("Watched: %d play%s recorded", total, s), "blocking"})
		}
	}

	// 2. External IDs
	tmdbIDStr := externalIDFn(item, "tmdb")
	tmdb := 0
	if tmdbIDStr != "" {
		tmdb, _ = strconv.Atoi(tmdbIDStr)
	}

	// 3. No keep tag
	if criteria.IsRuleEnabled("no_keep_tag") {
		protectedTags := criteria.ProtectedTags
		if tmdb != 0 {
			if movieData, ok := ctx.RadarrMovies[tmdb]; ok {
				itemTags := toStringSlice(movieData["_tag_names"])
				matched := matchTags(protectedTags, itemTags)
				if len(matched) > 0 {
					results = append(results, Result{"no_keep_tag", false,
						fmt.Sprintf("Has protected tag: %s (item tags: %s)", strings.Join(matched, ", "), joinOrNone(itemTags)), "blocking"})
				} else {
					results = append(results, Result{"no_keep_tag", true,
						fmt.Sprintf("No protected tags found (item tags: %s)", joinOrNone(itemTags)), "info"})
				}
			} else {
				results = append(results, Result{"no_keep_tag", true, "Not in Radarr or no TMDB match", "info"})
			}
		} else {
			results = append(results, Result{"no_keep_tag", true, "Not in Radarr or no TMDB match", "info"})
		}
	}

	// 4. No active request
	if criteria.IsRuleEnabled("no_active_request") {
		if tmdb != 0 && ctx.OverseerrActiveMovies[tmdb] {
			requester := ctx.MovieRequesters[tmdb]
			if requester != "" && userHasWatched(ctx.UserWatches, requester, rk) {
				results = append(results, Result{"no_active_request", true,
					fmt.Sprintf("Requested by %s - they've watched it", requester), "info"})
			} else {
				label := requester
				if label == "" {
					label = "unknown"
				}
				results = append(results, Result{"no_active_request", false,
					fmt.Sprintf("Requested by %s - hasn't watched yet", label), "blocking"})
			}
		} else {
			var tagRequesters []string
			if tmdb != 0 {
				if movieData, ok := ctx.RadarrMovies[tmdb]; ok {
					tagRequesters = overseerr.ExtractRequestersFromTags(toStringSlice(movieData["_tag_names"]))
				}
			}
			if len(tagRequesters) > 0 {
				requester := tagRequesters[0]
				if userHasWatched(ctx.UserWatches, requester, rk) {
					results = append(results, Result{"no_active_request", true,
						fmt.Sprintf("Requested by %s (from Radarr tag) - they've watched it", requester), "info"})
				} else {
					results = append(results, Result{"no_active_request", false,
						fmt.Sprintf("Requested by %s (from Radarr tag) - hasn't watched yet", requester), "blocking"})
				}
			} else {
				results = append(results, Result{"no_active_request", true, "No Seerr request", "info"})
			}
		}
	}

	// 5. No protected request
	if criteria.IsRuleEnabled("no_protected_request") {
		if tmdb != 0 && ctx.OverseerrProtectedMovies[tmdb] {
			results = append(results, Result{"no_protected_request", false,
				"Requested by a protected user", "blocking"})
		} else {
			protectedNames := criteria.ProtectedUsers
			watchedBy := protectedUsersWhoWatched(protectedNames, ctx.UserWatches, rk)
			if len(watchedBy) > 0 {
				results = append(results, Result{"no_protected_request", false,
					fmt.Sprintf("Watched by protected user: %s", strings.Join(watchedBy, ", ")), "blocking"})
			} else {
				results = append(results, Result{"no_protected_request", true,
					"No protected-user request or watch", "info"})
			}
		}
	}

	// 6. Not in keep collection
	if criteria.IsRuleEnabled("not_in_keep_collection") {
		if ctx.PlexKeepKeys[rk] {
			results = append(results, Result{"not_in_keep_collection", false,
				"In Plex keep collection", "blocking"})
		} else {
			results = append(results, Result{"not_in_keep_collection", true,
				"Not in keep collection", "info"})
		}
	}

	// 7. Not watched recently
	if criteria.IsRuleEnabled("not_watched_recently") {
		thresholdDays := criteria.NotWatchedRecentlyDays()
		lastWatchedStr := ctx.LastWatchDates[rk]
		if lastWatchedStr != "" {
			if lastDate, err := parseISO(lastWatchedStr); err == nil {
				daysSince := daysBetween(lastDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("Last watched %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("Watched %d days ago (within %dd window)", daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					"Could not parse last watch date", "info"})
			}
		} else {
			addedAt := toInt(item["addedAt"])
			if addedAt > 0 {
				addedDate := time.Unix(int64(addedAt), 0)
				daysSince := daysBetween(addedDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("Never watched, added %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("Never watched but only added %d days ago - too new (threshold: %dd)", daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					fmt.Sprintf("Never watched, no add date available (threshold: %dd)", thresholdDays), "info"})
			}
		}
	}

	// 8. Request fulfilled
	if criteria.IsRuleEnabled("request_fulfilled") {
		requester := ""
		source := "Seerr"
		if tmdb != 0 {
			requester = ctx.MovieRequesters[tmdb]
		}
		if requester == "" && tmdb != 0 {
			if movieData, ok := ctx.RadarrMovies[tmdb]; ok {
				tagReqs := overseerr.ExtractRequestersFromTags(toStringSlice(movieData["_tag_names"]))
				if len(tagReqs) > 0 {
					requester = tagReqs[0]
					source = "Radarr tag"
				}
			}
		}
		if requester != "" {
			if userHasWatched(ctx.UserWatches, requester, rk) {
				results = append(results, Result{"request_fulfilled", true,
					fmt.Sprintf("Requested by %s (from %s) - they've watched it", requester, source), "info"})
			} else {
				results = append(results, Result{"request_fulfilled", false,
					fmt.Sprintf("Requested by %s (from %s) - hasn't watched yet", requester, source), "blocking"})
			}
		} else {
			results = append(results, Result{"request_fulfilled", true,
				"No Seerr request found", "info"})
		}
	}

	// 9. Available on debrid
	if criteria.IsRuleEnabled("available_on_debrid") {
		if cached, ok := ctx.DebridCached[rk]; ok && cached {
			results = append(results, Result{"available_on_debrid", true,
				"Available on debrid - can be re-streamed", "info"})
		} else {
			results = append(results, Result{"available_on_debrid", false,
				"Not confirmed on debrid", "blocking"})
		}
	}

	// 10. Old content
	if criteria.IsRuleEnabled("old_content") {
		addedAt := toInt(item["addedAt"])
		thresholdDays := criteria.OldContentDays()
		if addedAt > 0 {
			addedDate := time.Unix(int64(addedAt), 0)
			daysSince := daysBetween(addedDate, time.Now())
			if daysSince > thresholdDays {
				results = append(results, Result{"old_content", true,
					fmt.Sprintf("Added %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
			} else {
				results = append(results, Result{"old_content", false,
					fmt.Sprintf("Added %d days ago (within %dd)", daysSince, thresholdDays), "blocking"})
			}
		} else {
			results = append(results, Result{"old_content", true,
				"No add date available", "info"})
		}
	}

	// 11. Highly rated
	if criteria.IsRuleEnabled("highly_rated") {
		hr := criteria.HighlyRated
		ratings, ok := ctx.RatingsCache[rk]
		if !ok {
			results = append(results, Result{"highly_rated", true,
				"No ratings data available", "info"})
		} else {
			appendHighlyRatedResult(&results, hr, ratings)
		}
	}

	// 12. Low rating
	if criteria.IsRuleEnabled("low_rating") {
		lr := criteria.LowRating
		ratings, ok := ctx.RatingsCache[rk]
		if !ok {
			results = append(results, Result{"low_rating", true,
				"no ratings available", "info"})
		} else {
			appendLowRatingResult(&results, lr, ratings)
		}
	}

	// 13. File size minimum
	if criteria.IsRuleEnabled("file_size_min") {
		fsm := criteria.FileSizeMin
		minBytes := int64(fsm.MinGB * 1024 * 1024 * 1024)
		var sizeBytes int64
		if tmdb != 0 {
			if movieData, ok := ctx.RadarrMovies[tmdb]; ok {
				sizeBytes = toInt64(movieData["sizeOnDisk"])
			}
		}
		if minBytes <= 0 || sizeBytes <= 0 {
			detail := "unknown size"
			if sizeBytes > 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"file_size_min", true, detail, "info"})
		} else {
			sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)
			if sizeBytes >= minBytes {
				results = append(results, Result{"file_size_min", true,
					fmt.Sprintf("Size %.2f GB >= %.0f GB", sizeGB, fsm.MinGB), "info"})
			} else {
				results = append(results, Result{"file_size_min", false,
					fmt.Sprintf("Size %.2f GB < %.0f GB", sizeGB, fsm.MinGB), "blocking"})
			}
		}
	}

	// 14. Release year before
	if criteria.IsRuleEnabled("release_year_before") {
		ry := criteria.ReleaseYearBefore
		itemYear := toInt(item["year"])
		if ry.Year == 0 || itemYear == 0 {
			detail := "no year"
			if itemYear != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"release_year_before", true, detail, "info"})
		} else {
			if itemYear < ry.Year {
				results = append(results, Result{"release_year_before", true,
					fmt.Sprintf("Released %d < %d", itemYear, ry.Year), "info"})
			} else {
				results = append(results, Result{"release_year_before", false,
					fmt.Sprintf("Released %d >= %d", itemYear, ry.Year), "blocking"})
			}
		}
	}

	// 15. Watch ratio low
	if criteria.IsRuleEnabled("watch_ratio_low") {
		wr := criteria.WatchRatioLow
		_, hasMaxPct := ctx.MaxPercentByKey[rk]
		hasPlays := ctx.DBPlays[rk] > 0 ||
			ctx.PlayCounts[rk] > 0 ||
			toInt(item["viewCount"]) > 0 ||
			hasMaxPct
		maxPercent := ctx.MaxPercentByKey[rk]
		lastWatched := ctx.LastWatchDates[rk]
		evalWatchRatioLow(&results, wr, maxPercent, hasPlays, lastWatched, "")
	}

	// 16. Recently added
	if criteria.IsRuleEnabled("recently_added") {
		ra := criteria.RecentlyAdded
		addedAt := toInt(item["addedAt"])
		if addedAt == 0 {
			addedAt = int(ctx.AddedAtByKey[rk])
		}
		if ra.Days == 0 || addedAt == 0 {
			detail := "no addedAt"
			if addedAt != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"recently_added", true, detail, "info"})
		} else {
			addedDate := time.Unix(int64(addedAt), 0)
			daysSince := daysBetween(addedDate, time.Now())
			if daysSince <= ra.Days {
				results = append(results, Result{"recently_added", false,
					fmt.Sprintf("Added %d days ago (within %dd)", daysSince, ra.Days), "blocking"})
			} else {
				results = append(results, Result{"recently_added", true,
					fmt.Sprintf("Added %d days ago (> %dd)", daysSince, ra.Days), "info"})
			}
		}
	}

	// 17. Partially watched
	if criteria.IsRuleEnabled("partially_watched") {
		pw := criteria.PartiallyWatched
		if pw.Days == 0 {
			results = append(results, Result{"partially_watched", true, "threshold disabled", "info"})
		} else if ctx.PartiallyWatchedKeys[rk] {
			results = append(results, Result{"partially_watched", false,
				fmt.Sprintf("Partial play within last %d days", pw.Days), "blocking"})
		} else {
			results = append(results, Result{"partially_watched", true,
				fmt.Sprintf("No partial play within last %d days", pw.Days), "info"})
		}
	}

	// 18. On watchlist
	if criteria.IsRuleEnabled("on_watchlist") {
		if ctx.WatchlistKeys[rk] {
			results = append(results, Result{"on_watchlist", false,
				"On a Plex watchlist", "blocking"})
		} else {
			results = append(results, Result{"on_watchlist", true,
				"Not on any watchlist", "info"})
		}
	}

	// 19. Plex favorited
	if criteria.IsRuleEnabled("plex_favorited") {
		if ctx.PlexFavoritedKeys[rk] {
			results = append(results, Result{"plex_favorited", false,
				"Hearted by Plex admin", "blocking"})
		} else {
			results = append(results, Result{"plex_favorited", true,
				"Not hearted by Plex admin", "info"})
		}
	}

	// 20. Debrid cache status (informational)
	if cached, ok := ctx.DebridCached[rk]; ok {
		label := "Not cached"
		if cached {
			label = "Cached"
		}
		results = append(results, Result{"debrid_cached", cached,
			fmt.Sprintf("%s on debrid providers", label), "info"})
	}

	return results
}

func EvaluateShow(item map[string]any, ctx *EvaluationContext, criteria *models.CollectionCriteria, externalIDFn func(map[string]any, string) string) []Result {
	var results []Result
	rk := fmt.Sprintf("%v", item["ratingKey"])

	// 1. Never watched
	if criteria.IsRuleEnabled("never_watched") {
		nw := criteria.NeverWatched
		plexViews := toInt(item["viewCount"])
		playCounts := ctx.PlayCounts[rk]
		dbPlays := ctx.DBPlays[rk]
		showTitle, _ := item["title"].(string)
		if dbPlays == 0 && showTitle != "" {
			if titlePlays, ok := ctx.DBPlaysByTitle[strings.ToLower(showTitle)]; ok {
				dbPlays = titlePlays
			}
		}

		watchedPlex := nw.CheckPlexViews && plexViews > 0
		watchedCounts := nw.CheckDBPlays && playCounts > 0
		watchedDB := dbPlays > 0

		if !watchedPlex && !watchedCounts && !watchedDB {
			results = append(results, Result{"never_watched", true,
				fmt.Sprintf("No play history (Plex views=%d, DB plays=%d)", plexViews, max(playCounts, dbPlays)), "info"})
		} else {
			total := max(plexViews, playCounts, dbPlays)
			s := "s"
			if total == 1 {
				s = ""
			}
			results = append(results, Result{"never_watched", false,
				fmt.Sprintf("Watched: %d play%s recorded", total, s), "blocking"})
		}
	}

	tvdbIDStr := externalIDFn(item, "tvdb")
	tvdb := 0
	if tvdbIDStr != "" {
		tvdb, _ = strconv.Atoi(tvdbIDStr)
	}
	tmdbIDStr := externalIDFn(item, "tmdb")
	tmdb := 0
	if tmdbIDStr != "" {
		tmdb, _ = strconv.Atoi(tmdbIDStr)
	}

	// 2. No keep tag
	if criteria.IsRuleEnabled("no_keep_tag") {
		protectedTags := criteria.ProtectedTags
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				itemTags := toStringSlice(showData["_tag_names"])
				matched := matchTags(protectedTags, itemTags)
				if len(matched) > 0 {
					results = append(results, Result{"no_keep_tag", false,
						fmt.Sprintf("Has protected tag: %s (item tags: %s)", strings.Join(matched, ", "), joinOrNone(itemTags)), "blocking"})
				} else {
					results = append(results, Result{"no_keep_tag", true,
						fmt.Sprintf("No protected tags found (item tags: %s)", joinOrNone(itemTags)), "info"})
				}
			} else {
				results = append(results, Result{"no_keep_tag", true,
					"Not in Sonarr or no TVDB match", "info"})
			}
		} else {
			results = append(results, Result{"no_keep_tag", true,
				"Not in Sonarr or no TVDB match", "info"})
		}
	}

	// 3. No active request
	if criteria.IsRuleEnabled("no_active_request") {
		hasRequest := false
		requester := ""
		if tvdb != 0 && ctx.OverseerrActiveShows[tvdb] {
			hasRequest = true
			requester = ctx.ShowRequesters[tvdb]
		} else if tmdb != 0 && ctx.OverseerrActiveShowsTmdb[tmdb] {
			hasRequest = true
			requester = ctx.ShowRequestersTmdb[tmdb]
		}

		if hasRequest {
			showTitle, _ := item["title"].(string)
			if requester != "" && userHasWatchedShowLevel(ctx.UserWatches, requester, rk, showTitle) {
				results = append(results, Result{"no_active_request", true,
					fmt.Sprintf("Requested by %s - they've watched it", requester), "info"})
			} else {
				label := requester
				if label == "" {
					label = "unknown"
				}
				results = append(results, Result{"no_active_request", false,
					fmt.Sprintf("Requested by %s - hasn't watched yet", label), "blocking"})
			}
		} else {
			var tagRequesters []string
			if tvdb != 0 {
				if showData, ok := ctx.SonarrShows[tvdb]; ok {
					tagRequesters = overseerr.ExtractRequestersFromTags(toStringSlice(showData["_tag_names"]))
				}
			}
			if len(tagRequesters) > 0 {
				requester := tagRequesters[0]
				showTitle, _ := item["title"].(string)
				if userHasWatchedShowLevel(ctx.UserWatches, requester, rk, showTitle) {
					results = append(results, Result{"no_active_request", true,
						fmt.Sprintf("Requested by %s (from Sonarr tag) - they've watched it", requester), "info"})
				} else {
					results = append(results, Result{"no_active_request", false,
						fmt.Sprintf("Requested by %s (from Sonarr tag) - hasn't watched yet", requester), "blocking"})
				}
			} else {
				results = append(results, Result{"no_active_request", true,
					"No Seerr request", "info"})
			}
		}
	}

	// 4. No protected request
	if criteria.IsRuleEnabled("no_protected_request") {
		if (tvdb != 0 && ctx.OverseerrProtectedShows[tvdb]) ||
			(tmdb != 0 && ctx.OverseerrProtectedShowsTmdb[tmdb]) {
			results = append(results, Result{"no_protected_request", false,
				"Requested by a protected user", "blocking"})
		} else {
			protectedNames := criteria.ProtectedUsers
			showTitle, _ := item["title"].(string)
			var watchedBy []string
			for _, u := range protectedNames {
				if watches, ok := ctx.UserWatches[u]; ok {
					if watches[rk] || (showTitle != "" && watches[strings.ToLower(showTitle)]) {
						watchedBy = append(watchedBy, u)
					}
				}
			}
			if len(watchedBy) > 0 {
				results = append(results, Result{"no_protected_request", false,
					fmt.Sprintf("Watched by protected user: %s", strings.Join(watchedBy, ", ")), "blocking"})
			} else {
				results = append(results, Result{"no_protected_request", true,
					"No protected-user request or watch", "info"})
			}
		}
	}

	// 5. Not in keep collection
	if criteria.IsRuleEnabled("not_in_keep_collection") {
		if ctx.PlexKeepKeys[rk] {
			results = append(results, Result{"not_in_keep_collection", false,
				"In Plex keep collection", "blocking"})
		} else {
			results = append(results, Result{"not_in_keep_collection", true,
				"Not in keep collection", "info"})
		}
	}

	// 6. Show ended
	if criteria.IsRuleEnabled("show_ended") {
		includeDeleted := criteria.ShowEnded.IncludeDeleted
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				status := strings.ToLower(fmt.Sprintf("%v", showData["status"]))
				endedStatuses := []string{"ended"}
				if includeDeleted {
					endedStatuses = append(endedStatuses, "deleted")
				}
				if containsStr(endedStatuses, status) {
					results = append(results, Result{"show_ended", true,
						fmt.Sprintf("Sonarr status: %s", status), "info"})
				} else {
					results = append(results, Result{"show_ended", false,
						fmt.Sprintf("Sonarr status: %s (not ended)", status), "blocking"})
				}
			} else {
				results = append(results, Result{"show_ended", false,
					"Not in Sonarr", "blocking"})
			}
		} else {
			results = append(results, Result{"show_ended", false,
				"Not in Sonarr", "blocking"})
		}
	}

	// 7. Not watched recently
	if criteria.IsRuleEnabled("not_watched_recently") {
		thresholdDays := criteria.NotWatchedRecentlyDays()
		lastWatchedStr := ctx.LastWatchDates[rk]
		showTitle, _ := item["title"].(string)
		if lastWatchedStr == "" && showTitle != "" {
			lastWatchedStr = ctx.LastWatchByTitle[strings.ToLower(showTitle)]
		}
		if lastWatchedStr != "" {
			if lastDate, err := parseISO(lastWatchedStr); err == nil {
				daysSince := daysBetween(lastDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("Last watched %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("Watched %d days ago (within %dd window)", daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					"Could not parse last watch date", "info"})
			}
		} else {
			addedAt := toInt(item["addedAt"])
			if addedAt > 0 {
				addedDate := time.Unix(int64(addedAt), 0)
				daysSince := daysBetween(addedDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("Never watched, added %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("Never watched but only added %d days ago - too new (threshold: %dd)", daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					fmt.Sprintf("Never watched, no add date available (threshold: %dd)", thresholdDays), "info"})
			}
		}
	}

	// 8. Request fulfilled
	if criteria.IsRuleEnabled("request_fulfilled") {
		showTitle, _ := item["title"].(string)
		requester := ""
		source := "Seerr"
		if tvdb != 0 {
			requester = ctx.ShowRequesters[tvdb]
		}
		if requester == "" && tmdb != 0 {
			requester = ctx.ShowRequestersTmdb[tmdb]
		}
		if requester == "" && tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				tagReqs := overseerr.ExtractRequestersFromTags(toStringSlice(showData["_tag_names"]))
				if len(tagReqs) > 0 {
					requester = tagReqs[0]
					source = "Sonarr tag"
				}
			}
		}
		if requester != "" {
			if userHasWatchedShowLevel(ctx.UserWatches, requester, rk, showTitle) {
				results = append(results, Result{"request_fulfilled", true,
					fmt.Sprintf("Requested by %s (from %s) - they've watched it", requester, source), "info"})
			} else {
				results = append(results, Result{"request_fulfilled", false,
					fmt.Sprintf("Requested by %s (from %s) - hasn't watched yet", requester, source), "blocking"})
			}
		} else {
			results = append(results, Result{"request_fulfilled", true,
				"No Seerr request found", "info"})
		}
	}

	// 9. Available on debrid
	if criteria.IsRuleEnabled("available_on_debrid") {
		if cached, ok := ctx.DebridCached[rk]; ok && cached {
			results = append(results, Result{"available_on_debrid", true,
				"Available on debrid - can be re-streamed", "info"})
		} else {
			results = append(results, Result{"available_on_debrid", false,
				"Not confirmed on debrid", "blocking"})
		}
	}

	// 10. Old content
	if criteria.IsRuleEnabled("old_content") {
		addedAt := toInt(item["addedAt"])
		thresholdDays := criteria.OldContentDays()
		if addedAt > 0 {
			addedDate := time.Unix(int64(addedAt), 0)
			daysSince := daysBetween(addedDate, time.Now())
			if daysSince > thresholdDays {
				results = append(results, Result{"old_content", true,
					fmt.Sprintf("Added %d days ago (threshold: %dd)", daysSince, thresholdDays), "info"})
			} else {
				results = append(results, Result{"old_content", false,
					fmt.Sprintf("Added %d days ago (within %dd)", daysSince, thresholdDays), "blocking"})
			}
		} else {
			results = append(results, Result{"old_content", true,
				"No add date available", "info"})
		}
	}

	// 11. Highly rated
	if criteria.IsRuleEnabled("highly_rated") {
		hr := criteria.HighlyRated
		ratings, ok := ctx.RatingsCache[rk]
		if !ok {
			results = append(results, Result{"highly_rated", true,
				"No ratings data available", "info"})
		} else {
			appendHighlyRatedResult(&results, hr, ratings)
		}
	}

	// 12. Low rating
	if criteria.IsRuleEnabled("low_rating") {
		lr := criteria.LowRating
		ratings, ok := ctx.RatingsCache[rk]
		if !ok {
			results = append(results, Result{"low_rating", true,
				"no ratings available", "info"})
		} else {
			appendLowRatingResult(&results, lr, ratings)
		}
	}

	// 13. File size minimum
	if criteria.IsRuleEnabled("file_size_min") {
		fsm := criteria.FileSizeMin
		minBytes := int64(fsm.MinGB * 1024 * 1024 * 1024)
		var sizeBytes int64
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				if stats, ok := showData["statistics"].(map[string]any); ok {
					sizeBytes = toInt64(stats["sizeOnDisk"])
				}
				if sizeBytes == 0 {
					if seasons, ok := showData["seasons"].([]any); ok {
						for _, s := range seasons {
							if sm, ok := s.(map[string]any); ok {
								if sStats, ok := sm["statistics"].(map[string]any); ok {
									sizeBytes += toInt64(sStats["sizeOnDisk"])
								}
							}
						}
					}
				}
			}
		}
		if minBytes <= 0 || sizeBytes <= 0 {
			detail := "unknown size"
			if sizeBytes > 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"file_size_min", true, detail, "info"})
		} else {
			sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)
			if sizeBytes >= minBytes {
				results = append(results, Result{"file_size_min", true,
					fmt.Sprintf("Size %.2f GB >= %.0f GB", sizeGB, fsm.MinGB), "info"})
			} else {
				results = append(results, Result{"file_size_min", false,
					fmt.Sprintf("Size %.2f GB < %.0f GB", sizeGB, fsm.MinGB), "blocking"})
			}
		}
	}

	// 14. Release year before
	if criteria.IsRuleEnabled("release_year_before") {
		ry := criteria.ReleaseYearBefore
		itemYear := toInt(item["year"])
		if ry.Year == 0 || itemYear == 0 {
			detail := "no year"
			if itemYear != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"release_year_before", true, detail, "info"})
		} else {
			if itemYear < ry.Year {
				results = append(results, Result{"release_year_before", true,
					fmt.Sprintf("Released %d < %d", itemYear, ry.Year), "info"})
			} else {
				results = append(results, Result{"release_year_before", false,
					fmt.Sprintf("Released %d >= %d", itemYear, ry.Year), "blocking"})
			}
		}
	}

	// 15. Watch ratio low
	if criteria.IsRuleEnabled("watch_ratio_low") {
		wr := criteria.WatchRatioLow
		title := strings.ToLower(stringVal(item["title"]))
		_, hasMaxPctKey := ctx.MaxPercentByKey[rk]
		_, hasMaxPctTitle := ctx.MaxPercentByTitle[title]
		hasPlays := ctx.DBPlays[rk] > 0 ||
			(title != "" && ctx.DBPlaysByTitle[title] > 0) ||
			ctx.PlayCounts[rk] > 0 ||
			toInt(item["viewCount"]) > 0 ||
			hasMaxPctKey ||
			(title != "" && hasMaxPctTitle)
		maxPercent := max(ctx.MaxPercentByKey[rk], 0)
		if title != "" {
			maxPercent = max(maxPercent, ctx.MaxPercentByTitle[title])
		}
		lastByKey := ctx.LastWatchDates[rk]
		lastByTitle := ""
		if title != "" {
			lastByTitle = ctx.LastWatchByTitle[title]
		}
		lastWatched := latestISO(lastByKey, lastByTitle)
		evalWatchRatioLow(&results, wr, maxPercent, hasPlays, lastWatched, "")
	}

	// 16. Recently added
	if criteria.IsRuleEnabled("recently_added") {
		ra := criteria.RecentlyAdded
		addedAt := toInt(item["addedAt"])
		if addedAt == 0 {
			addedAt = int(ctx.AddedAtByKey[rk])
		}
		if ra.Days == 0 || addedAt == 0 {
			detail := "no addedAt"
			if addedAt != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"recently_added", true, detail, "info"})
		} else {
			addedDate := time.Unix(int64(addedAt), 0)
			daysSince := daysBetween(addedDate, time.Now())
			if daysSince <= ra.Days {
				results = append(results, Result{"recently_added", false,
					fmt.Sprintf("Added %d days ago (within %dd)", daysSince, ra.Days), "blocking"})
			} else {
				results = append(results, Result{"recently_added", true,
					fmt.Sprintf("Added %d days ago (> %dd)", daysSince, ra.Days), "info"})
			}
		}
	}

	// 17. Partially watched
	if criteria.IsRuleEnabled("partially_watched") {
		pw := criteria.PartiallyWatched
		title := strings.ToLower(stringVal(item["title"]))
		if pw.Days == 0 {
			results = append(results, Result{"partially_watched", true, "threshold disabled", "info"})
		} else if ctx.PartiallyWatchedKeys[rk] || (title != "" && ctx.PartiallyWatchedByTitle[title]) {
			results = append(results, Result{"partially_watched", false,
				fmt.Sprintf("Partial play within last %d days", pw.Days), "blocking"})
		} else {
			results = append(results, Result{"partially_watched", true,
				fmt.Sprintf("No partial play within last %d days", pw.Days), "info"})
		}
	}

	// 18. On watchlist
	if criteria.IsRuleEnabled("on_watchlist") {
		if ctx.WatchlistKeys[rk] {
			results = append(results, Result{"on_watchlist", false,
				"On a Plex watchlist", "blocking"})
		} else {
			results = append(results, Result{"on_watchlist", true,
				"Not on any watchlist", "info"})
		}
	}

	// 19. Plex favorited
	if criteria.IsRuleEnabled("plex_favorited") {
		if ctx.PlexFavoritedKeys[rk] {
			results = append(results, Result{"plex_favorited", false,
				"Hearted by Plex admin", "blocking"})
		} else {
			results = append(results, Result{"plex_favorited", true,
				"Not hearted by Plex admin", "info"})
		}
	}

	return results
}

func EvaluateSeason(item map[string]any, ctx *EvaluationContext, criteria *models.CollectionCriteria, seasonNumber int, showTitle string, externalIDFn func(map[string]any, string) string) []Result {
	var results []Result
	rk := fmt.Sprintf("%v", item["ratingKey"])
	showRK := stringVal(item["_show_rating_key"])
	title := showTitle
	if title == "" {
		title = stringVal(item["_show_title"])
	}
	seasonKey := fmt.Sprintf("%s:S%d", strings.ToLower(title), seasonNumber)

	// 1. Never watched (season-scoped)
	if criteria.IsRuleEnabled("never_watched") {
		nw := criteria.NeverWatched
		plexViews := toInt(item["viewCount"])
		playCounts := ctx.PlayCounts[rk]
		dbPlays := ctx.DBPlaysBySeason[seasonKey]

		watchedPlex := nw.CheckPlexViews && plexViews > 0
		watchedCounts := nw.CheckDBPlays && playCounts > 0
		watchedDB := dbPlays > 0

		if !watchedPlex && !watchedCounts && !watchedDB {
			results = append(results, Result{"never_watched", true,
				fmt.Sprintf("No play history for S%02d (Plex views=%d, DB plays=%d)", seasonNumber, plexViews, dbPlays), "info"})
		} else {
			total := max(plexViews, playCounts, dbPlays)
			s := "s"
			if total == 1 {
				s = ""
			}
			results = append(results, Result{"never_watched", false,
				fmt.Sprintf("S%02d watched: %d play%s recorded", seasonNumber, total, s), "blocking"})
		}
	}

	tvdbIDStr := externalIDFn(item, "tvdb")
	tvdb := 0
	if tvdbIDStr != "" {
		tvdb, _ = strconv.Atoi(tvdbIDStr)
	}
	tmdbIDStr := externalIDFn(item, "tmdb")
	tmdb := 0
	if tmdbIDStr != "" {
		tmdb, _ = strconv.Atoi(tmdbIDStr)
	}

	// 2. No keep tag (show-level, inherited)
	if criteria.IsRuleEnabled("no_keep_tag") {
		protectedTags := criteria.ProtectedTags
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				itemTags := toStringSlice(showData["_tag_names"])
				matched := matchTags(protectedTags, itemTags)
				if len(matched) > 0 {
					results = append(results, Result{"no_keep_tag", false,
						fmt.Sprintf("Has protected tag: %s", strings.Join(matched, ", ")), "blocking"})
				} else {
					results = append(results, Result{"no_keep_tag", true,
						fmt.Sprintf("No protected tags found (item tags: %s)", joinOrNone(itemTags)), "info"})
				}
			} else {
				results = append(results, Result{"no_keep_tag", true,
					"Not in Sonarr or no TVDB match", "info"})
			}
		} else {
			results = append(results, Result{"no_keep_tag", true,
				"Not in Sonarr or no TVDB match", "info"})
		}
	}

	// 3. No active request (show-level, season-scoped watch check)
	if criteria.IsRuleEnabled("no_active_request") {
		hasRequest := false
		requester := ""
		if tvdb != 0 && ctx.OverseerrActiveShows[tvdb] {
			hasRequest = true
			requester = ctx.ShowRequesters[tvdb]
		} else if tmdb != 0 && ctx.OverseerrActiveShowsTmdb[tmdb] {
			hasRequest = true
			requester = ctx.ShowRequestersTmdb[tmdb]
		}

		if hasRequest {
			requesterWatched := requester != "" && userHasWatchedSeason(ctx.UserWatchesBySeason, requester, seasonKey)
			if requesterWatched {
				results = append(results, Result{"no_active_request", true,
					fmt.Sprintf("Requested by %s - they've watched S%02d", requester, seasonNumber), "info"})
			} else {
				label := requester
				if label == "" {
					label = "unknown"
				}
				results = append(results, Result{"no_active_request", false,
					fmt.Sprintf("Requested by %s - hasn't watched S%02d yet", label, seasonNumber), "blocking"})
			}
		} else {
			var tagRequesters []string
			if tvdb != 0 {
				if showData, ok := ctx.SonarrShows[tvdb]; ok {
					tagRequesters = overseerr.ExtractRequestersFromTags(toStringSlice(showData["_tag_names"]))
				}
			}
			if len(tagRequesters) > 0 {
				requester := tagRequesters[0]
				requesterWatched := userHasWatchedSeason(ctx.UserWatchesBySeason, requester, seasonKey)
				if requesterWatched {
					results = append(results, Result{"no_active_request", true,
						fmt.Sprintf("Requested by %s (Sonarr tag) - they've watched S%02d", requester, seasonNumber), "info"})
				} else {
					results = append(results, Result{"no_active_request", false,
						fmt.Sprintf("Requested by %s (Sonarr tag) - hasn't watched S%02d yet", requester, seasonNumber), "blocking"})
				}
			} else {
				results = append(results, Result{"no_active_request", true,
					"No Seerr request", "info"})
			}
		}
	}

	// 4. No protected request (show-level request, season-level watch)
	if criteria.IsRuleEnabled("no_protected_request") {
		if (tvdb != 0 && ctx.OverseerrProtectedShows[tvdb]) ||
			(tmdb != 0 && ctx.OverseerrProtectedShowsTmdb[tmdb]) {
			results = append(results, Result{"no_protected_request", false,
				"Requested by a protected user", "blocking"})
		} else {
			protectedNames := criteria.ProtectedUsers
			seasonKeyForCheck := seasonKey
			if title == "" {
				seasonKeyForCheck = ""
			}
			var watchedBy []string
			for _, u := range protectedNames {
				if seasonKeyForCheck != "" {
					if watches, ok := ctx.UserWatchesBySeason[u]; ok && watches[seasonKeyForCheck] {
						watchedBy = append(watchedBy, u)
						continue
					}
				}
				if watches, ok := ctx.UserWatches[u]; ok {
					if watches[rk] || (title != "" && watches[strings.ToLower(title)]) {
						watchedBy = append(watchedBy, u)
					}
				}
			}
			if len(watchedBy) > 0 {
				results = append(results, Result{"no_protected_request", false,
					fmt.Sprintf("Watched by protected user: %s", strings.Join(watchedBy, ", ")), "blocking"})
			} else {
				results = append(results, Result{"no_protected_request", true,
					"No protected-user request or watch", "info"})
			}
		}
	}

	// 5. Not in keep collection (check both season and show ratingKey)
	if criteria.IsRuleEnabled("not_in_keep_collection") {
		if ctx.PlexKeepKeys[rk] || (showRK != "" && ctx.PlexKeepKeys[showRK]) {
			results = append(results, Result{"not_in_keep_collection", false,
				"In Plex keep collection", "blocking"})
		} else {
			results = append(results, Result{"not_in_keep_collection", true,
				"Not in keep collection", "info"})
		}
	}

	// 6. Show ended (show-level, inherited)
	if criteria.IsRuleEnabled("show_ended") {
		includeDeleted := criteria.ShowEnded.IncludeDeleted
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				status := strings.ToLower(fmt.Sprintf("%v", showData["status"]))
				endedStatuses := []string{"ended"}
				if includeDeleted {
					endedStatuses = append(endedStatuses, "deleted")
				}
				if containsStr(endedStatuses, status) {
					results = append(results, Result{"show_ended", true,
						fmt.Sprintf("Sonarr status: %s", status), "info"})
				} else {
					results = append(results, Result{"show_ended", false,
						fmt.Sprintf("Sonarr status: %s (not ended)", status), "blocking"})
				}
			} else {
				results = append(results, Result{"show_ended", false,
					"Not in Sonarr", "blocking"})
			}
		} else {
			results = append(results, Result{"show_ended", false,
				"Not in Sonarr", "blocking"})
		}
	}

	// 7. Not watched recently (season-scoped)
	if criteria.IsRuleEnabled("not_watched_recently") {
		thresholdDays := criteria.NotWatchedRecentlyDays()
		lastWatchedStr := ctx.LastWatchBySeason[seasonKey]
		if lastWatchedStr != "" {
			if lastDate, err := parseISO(lastWatchedStr); err == nil {
				daysSince := daysBetween(lastDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("S%02d last watched %d days ago (threshold: %dd)", seasonNumber, daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("S%02d watched %d days ago (within %dd window)", seasonNumber, daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					"Could not parse last watch date", "info"})
			}
		} else {
			addedAt := toInt(item["addedAt"])
			if addedAt > 0 {
				addedDate := time.Unix(int64(addedAt), 0)
				daysSince := daysBetween(addedDate, time.Now())
				if daysSince > thresholdDays {
					results = append(results, Result{"not_watched_recently", true,
						fmt.Sprintf("S%02d never watched, added %d days ago (threshold: %dd)", seasonNumber, daysSince, thresholdDays), "info"})
				} else {
					results = append(results, Result{"not_watched_recently", false,
						fmt.Sprintf("S%02d never watched but only added %d days ago (threshold: %dd)", seasonNumber, daysSince, thresholdDays), "blocking"})
				}
			} else {
				results = append(results, Result{"not_watched_recently", true,
					fmt.Sprintf("S%02d never watched, no add date available (threshold: %dd)", seasonNumber, thresholdDays), "info"})
			}
		}
	}

	// 8. Low rating (show-level ratings, fallback)
	if criteria.IsRuleEnabled("low_rating") {
		lr := criteria.LowRating
		ratings, ok := ctx.RatingsCache[rk]
		if !ok && showRK != "" {
			ratings, ok = ctx.RatingsCache[showRK]
		}
		if !ok {
			results = append(results, Result{"low_rating", true,
				"no ratings available", "info"})
		} else {
			appendLowRatingResult(&results, lr, ratings)
		}
	}

	// 9. File size minimum (season-specific Sonarr stats)
	if criteria.IsRuleEnabled("file_size_min") {
		fsm := criteria.FileSizeMin
		minBytes := int64(fsm.MinGB * 1024 * 1024 * 1024)
		var sizeBytes int64
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				if seasons, ok := showData["seasons"].([]any); ok {
					for _, s := range seasons {
						if sm, ok := s.(map[string]any); ok {
							if toInt(sm["seasonNumber"]) == seasonNumber {
								if sStats, ok := sm["statistics"].(map[string]any); ok {
									sizeBytes = toInt64(sStats["sizeOnDisk"])
								}
								break
							}
						}
					}
				}
			}
		}
		if minBytes <= 0 || sizeBytes <= 0 {
			detail := "unknown size"
			if sizeBytes > 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"file_size_min", true, detail, "info"})
		} else {
			sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)
			if sizeBytes >= minBytes {
				results = append(results, Result{"file_size_min", true,
					fmt.Sprintf("S%02d size %.2f GB >= %.0f GB", seasonNumber, sizeGB, fsm.MinGB), "info"})
			} else {
				results = append(results, Result{"file_size_min", false,
					fmt.Sprintf("S%02d size %.2f GB < %.0f GB", seasonNumber, sizeGB, fsm.MinGB), "blocking"})
			}
		}
	}

	// 10. Release year before (show-level year)
	if criteria.IsRuleEnabled("release_year_before") {
		ry := criteria.ReleaseYearBefore
		itemYear := toInt(item["year"])
		if itemYear == 0 {
			itemYear = toInt(item["parentYear"])
		}
		if ry.Year == 0 || itemYear == 0 {
			detail := "no year"
			if itemYear != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"release_year_before", true, detail, "info"})
		} else {
			if itemYear < ry.Year {
				results = append(results, Result{"release_year_before", true,
					fmt.Sprintf("Released %d < %d", itemYear, ry.Year), "info"})
			} else {
				results = append(results, Result{"release_year_before", false,
					fmt.Sprintf("Released %d >= %d", itemYear, ry.Year), "blocking"})
			}
		}
	}

	// 11. Watch ratio low (season-scoped)
	if criteria.IsRuleEnabled("watch_ratio_low") {
		wr := criteria.WatchRatioLow
		_, hasMaxPct := ctx.MaxPercentBySeason[seasonKey]
		hasPlays := ctx.DBPlaysBySeason[seasonKey] > 0 ||
			toInt(item["viewCount"]) > 0 ||
			ctx.PlayCounts[rk] > 0 ||
			hasMaxPct
		maxPercent := ctx.MaxPercentBySeason[seasonKey]
		lastWatched := ctx.LastWatchBySeason[seasonKey]
		evalWatchRatioLow(&results, wr, maxPercent, hasPlays, lastWatched, fmt.Sprintf("S%02d", seasonNumber))
	}

	// 12. Recently added (season addedAt, fallback to show)
	if criteria.IsRuleEnabled("recently_added") {
		ra := criteria.RecentlyAdded
		addedAt := toInt(item["addedAt"])
		if addedAt == 0 {
			addedAt = int(ctx.AddedAtByKey[rk])
		}
		if addedAt == 0 && showRK != "" {
			addedAt = int(ctx.AddedAtByKey[showRK])
		}
		if ra.Days == 0 || addedAt == 0 {
			detail := "no addedAt"
			if addedAt != 0 {
				detail = "threshold disabled"
			}
			results = append(results, Result{"recently_added", true, detail, "info"})
		} else {
			addedDate := time.Unix(int64(addedAt), 0)
			daysSince := daysBetween(addedDate, time.Now())
			if daysSince <= ra.Days {
				results = append(results, Result{"recently_added", false,
					fmt.Sprintf("S%02d added %d days ago (within %dd)", seasonNumber, daysSince, ra.Days), "blocking"})
			} else {
				results = append(results, Result{"recently_added", true,
					fmt.Sprintf("S%02d added %d days ago (> %dd)", seasonNumber, daysSince, ra.Days), "info"})
			}
		}
	}

	// 13. Partially watched (season key)
	if criteria.IsRuleEnabled("partially_watched") {
		pw := criteria.PartiallyWatched
		if pw.Days == 0 {
			results = append(results, Result{"partially_watched", true, "threshold disabled", "info"})
		} else if ctx.PartiallyWatchedBySeason[seasonKey] {
			results = append(results, Result{"partially_watched", false,
				fmt.Sprintf("S%02d partial play within last %d days", seasonNumber, pw.Days), "blocking"})
		} else {
			results = append(results, Result{"partially_watched", true,
				fmt.Sprintf("No partial play within last %d days", pw.Days), "info"})
		}
	}

	// 14. On watchlist (check parent show ratingKey)
	if criteria.IsRuleEnabled("on_watchlist") {
		if showRK != "" && ctx.WatchlistKeys[showRK] {
			results = append(results, Result{"on_watchlist", false,
				"Parent show on a Plex watchlist", "blocking"})
		} else if ctx.WatchlistKeys[rk] {
			results = append(results, Result{"on_watchlist", false,
				"On a Plex watchlist", "blocking"})
		} else {
			results = append(results, Result{"on_watchlist", true,
				"Not on any watchlist", "info"})
		}
	}

	// 15. Plex favorited (season or parent show)
	if criteria.IsRuleEnabled("plex_favorited") {
		if ctx.PlexFavoritedKeys[rk] || (showRK != "" && ctx.PlexFavoritedKeys[showRK]) {
			results = append(results, Result{"plex_favorited", false,
				"Hearted by Plex admin", "blocking"})
		} else {
			results = append(results, Result{"plex_favorited", true,
				"Not hearted by Plex admin", "info"})
		}
	}

	// 16. Old season (season-only)
	if criteria.IsRuleEnabled("old_season") {
		osRule := criteria.OldSeason
		var sonarrID int
		if tvdb != 0 {
			if showData, ok := ctx.SonarrShows[tvdb]; ok {
				sonarrID = toInt(showData["id"])
			}
		}
		maxSeason := 0
		if sonarrID != 0 {
			maxSeason = ctx.ShowSeasonCounts[sonarrID]
		}
		if osRule.KeepLast == 0 || maxSeason <= 0 {
			detail := "keep_last disabled"
			if osRule.KeepLast != 0 {
				detail = "unknown max season"
			}
			results = append(results, Result{"old_season", true, detail, "info"})
		} else {
			cutoff := maxSeason - osRule.KeepLast + 1
			if seasonNumber < cutoff {
				results = append(results, Result{"old_season", true,
					fmt.Sprintf("S%02d older than keep-last-%d cutoff S%02d (max S%02d)", seasonNumber, osRule.KeepLast, cutoff, maxSeason), "info"})
			} else {
				results = append(results, Result{"old_season", false,
					fmt.Sprintf("S%02d within last %d seasons (max S%02d)", seasonNumber, osRule.KeepLast, maxSeason), "blocking"})
			}
		}
	}

	// 17. Series protection (season-only)
	if criteria.IsRuleEnabled("series_protection") {
		if showRK != "" && ctx.ShowLevelProtectionKeys[showRK] {
			results = append(results, Result{"series_protection", false,
				"Parent show is protected at show-level", "blocking"})
		} else {
			results = append(results, Result{"series_protection", true,
				"Parent show is not protected", "info"})
		}
	}

	return results
}

func IsCandidate(results []Result) bool {
	for _, r := range results {
		if r.Severity == "blocking" && r.Name != "debrid_cached" && !r.Passed {
			return false
		}
	}
	return true
}

// --- helpers ---

func appendHighlyRatedResult(results *[]Result, hr *models.HighlyRatedRule, ratings models.Ratings) {
	audience := ratings.AudienceRating
	critic := ratings.CriticRating

	type check struct {
		name string
		met  bool
	}
	var checks []check
	var details []string

	if hr.ImdbMin > 0 && audience != nil {
		met := *audience >= hr.ImdbMin
		checks = append(checks, check{"audience", met})
		if met {
			details = append(details, fmt.Sprintf("Audience %.1f/10 meets %.1f threshold", *audience, hr.ImdbMin))
		}
	}
	if hr.RtMin > 0 && critic != nil {
		met := *critic >= hr.RtMin
		checks = append(checks, check{"critic", met})
		if met {
			details = append(details, fmt.Sprintf("Critic %d%% meets %d%% threshold", *critic, hr.RtMin))
		}
	}

	if len(checks) == 0 {
		*results = append(*results, Result{"highly_rated", true,
			"No applicable rating thresholds configured", "info"})
		return
	}

	var protected bool
	if hr.RequireAll {
		protected = true
		for _, c := range checks {
			if !c.met {
				protected = false
				break
			}
		}
	} else {
		for _, c := range checks {
			if c.met {
				protected = true
				break
			}
		}
	}

	if protected {
		detailText := "Meets rating thresholds"
		if len(details) > 0 {
			detailText = strings.Join(details, "; ")
		}
		*results = append(*results, Result{"highly_rated", false, detailText, "blocking"})
	} else {
		var below []string
		if hr.ImdbMin > 0 && audience != nil && *audience < hr.ImdbMin {
			below = append(below, fmt.Sprintf("Audience %.1f/10 < %.1f", *audience, hr.ImdbMin))
		}
		if hr.RtMin > 0 && critic != nil && *critic < hr.RtMin {
			below = append(below, fmt.Sprintf("Critic %d%% < %d%%", *critic, hr.RtMin))
		}
		detailText := "Below rating thresholds"
		if len(below) > 0 {
			detailText = strings.Join(below, "; ")
		}
		*results = append(*results, Result{"highly_rated", true, detailText, "info"})
	}
}

func appendLowRatingResult(results *[]Result, lr *models.LowRatingRule, ratings models.Ratings) {
	audience := ratings.AudienceRating
	critic := ratings.CriticRating

	type check struct {
		name string
		met  bool
	}
	var checks []check
	var details []string

	if lr.ImdbMax > 0 && audience != nil {
		met := *audience <= lr.ImdbMax
		checks = append(checks, check{"audience", met})
		details = append(details, fmt.Sprintf("Audience %.1f/10 vs <= %.1f", *audience, lr.ImdbMax))
	}
	if lr.CriticMax > 0 && critic != nil {
		met := *critic <= lr.CriticMax
		checks = append(checks, check{"critic", met})
		details = append(details, fmt.Sprintf("Critic %d%% vs <= %d%%", *critic, lr.CriticMax))
	}

	if len(checks) == 0 {
		*results = append(*results, Result{"low_rating", true,
			"No applicable rating thresholds configured", "info"})
		return
	}

	var isLow bool
	if lr.RequireAll {
		isLow = true
		for _, c := range checks {
			if !c.met {
				isLow = false
				break
			}
		}
	} else {
		for _, c := range checks {
			if c.met {
				isLow = true
				break
			}
		}
	}

	detailText := "low_rating evaluated"
	if len(details) > 0 {
		detailText = strings.Join(details, "; ")
	}

	if isLow {
		*results = append(*results, Result{"low_rating", true,
			fmt.Sprintf("Rated low: %s", detailText), "info"})
	} else {
		*results = append(*results, Result{"low_rating", false,
			fmt.Sprintf("Above rating ceiling (%s)", detailText), "blocking"})
	}
}

func evalWatchRatioLow(results *[]Result, wr *models.WatchRatioLowRule, maxPercent int, hasPlays bool, lastWatchedISO string, scopeLabel string) {
	scope := ""
	if scopeLabel != "" {
		scope = " for " + scopeLabel
	}

	if !hasPlays {
		if wr.RequirePlays {
			*results = append(*results, Result{"watch_ratio_low", true,
				fmt.Sprintf("No plays recorded%s (rule requires plays, skipping)", scope), "info"})
		} else {
			*results = append(*results, Result{"watch_ratio_low", true,
				fmt.Sprintf("Never attempted%s (treated as watch_ratio_low match)", scope), "info"})
		}
		return
	}

	if maxPercent > wr.MaxPercent {
		*results = append(*results, Result{"watch_ratio_low", false,
			fmt.Sprintf("Max play completion %d%%%s > %d%%", maxPercent, scope, wr.MaxPercent), "blocking"})
		return
	}

	daysThreshold := wr.Days
	if daysThreshold > 0 && lastWatchedISO != "" {
		if lastDT, err := parseISO(lastWatchedISO); err == nil {
			daysSince := daysBetween(lastDT, time.Now())
			if daysSince < daysThreshold {
				*results = append(*results, Result{"watch_ratio_low", false,
					fmt.Sprintf("Max play completion %d%%%s <= %d%% but last-watched %dd ago (< %dd threshold)",
						maxPercent, scope, wr.MaxPercent, daysSince, daysThreshold), "blocking"})
				return
			}
			*results = append(*results, Result{"watch_ratio_low", true,
				fmt.Sprintf("Max play completion %d%%%s <= %d%% and last-watched %dd ago (>= %dd)",
					maxPercent, scope, wr.MaxPercent, daysSince, daysThreshold), "info"})
			return
		}
	}

	*results = append(*results, Result{"watch_ratio_low", true,
		fmt.Sprintf("Max play completion %d%%%s <= %d%%", maxPercent, scope, wr.MaxPercent), "info"})
}

func toInt(v any) int {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func toInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func stringVal(v any) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

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

func matchTags(protected []string, itemTags []string) []string {
	tagSet := make(map[string]bool, len(itemTags))
	for _, t := range itemTags {
		tagSet[t] = true
	}
	var matched []string
	for _, t := range protected {
		if tagSet[t] {
			matched = append(matched, t)
		}
	}
	return matched
}

func joinOrNone(tags []string) string {
	if len(tags) == 0 {
		return "none"
	}
	return strings.Join(tags, ", ")
}

func userHasWatched(watches map[string]map[string]bool, username, ratingKey string) bool {
	if w, ok := watches[username]; ok {
		return w[ratingKey]
	}
	return false
}

func userHasWatchedShowLevel(watches map[string]map[string]bool, username, ratingKey, showTitle string) bool {
	w, ok := watches[username]
	if !ok {
		return false
	}
	return w[ratingKey] || (showTitle != "" && w[strings.ToLower(showTitle)])
}

func userHasWatchedSeason(watches map[string]map[string]bool, username, seasonKey string) bool {
	if w, ok := watches[username]; ok {
		return w[seasonKey]
	}
	return false
}

func protectedUsersWhoWatched(users []string, watches map[string]map[string]bool, ratingKey string) []string {
	var out []string
	for _, u := range users {
		if w, ok := watches[u]; ok && w[ratingKey] {
			out = append(out, u)
		}
	}
	return out
}

func containsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func parseISO(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t, nil
	}
	cleaned := strings.TrimSuffix(s, "Z")
	t, err = time.Parse("2006-01-02T15:04:05", cleaned)
	if err == nil {
		return t, nil
	}
	return time.Time{}, err
}

func daysBetween(from, to time.Time) int {
	return int(to.Sub(from).Hours() / 24)
}

func latestISO(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	if a >= b {
		return a
	}
	return b
}
