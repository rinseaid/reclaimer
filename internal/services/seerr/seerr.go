// Package seerr provides helpers for the Seerr (Seerr / Jellyseerr) API.
package seerr

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

// seerrTagPattern matches Seerr-style arr tags like "18 - angus.d0".
var seerrTagPattern = regexp.MustCompile(`^\d+\s*-\s*(.+)$`)

// statusLabels maps Seerr request status codes to labels.
var statusLabels = map[int]string{
	1: "Pending Approval",
	2: "Approved",
	3: "Declined",
	4: "Processing",
	5: "Available",
}

// RequestData holds the aggregated result of FetchActiveRequests.
type RequestData struct {
	ActiveMovies    map[int]bool
	ActiveShows     map[int]bool
	ActiveShowsTmdb map[int]bool

	ProtectedMovies    map[int]bool
	ProtectedShows     map[int]bool
	ProtectedShowsTmdb map[int]bool

	MovieRequesters    map[int]string
	ShowRequesters     map[int]string
	ShowRequestersTmdb map[int]string
}

// seerrHeaders returns the standard header map for Seerr requests.
func seerrHeaders(apiKey string) map[string]string {
	return map[string]string{"X-Api-Key": apiKey}
}

// doGet performs a GET against the Seerr API with the API key header.
func doGet(url, apiKey, path string, params map[string]string) (*http.Response, error) {
	return httpclient.Do(httpclient.Request{
		Method:  http.MethodGet,
		URL:     url + path,
		Headers: seerrHeaders(apiKey),
		Params:  params,
	})
}

// FetchActiveRequests paginates through all Seerr requests and returns
// sets of active/protected IDs plus requester maps.
func FetchActiveRequests(url, apiKey string, protectedRequesters map[string]bool) (*RequestData, error) {
	if url == "" || apiKey == "" {
		return &RequestData{
			ActiveMovies:       map[int]bool{},
			ActiveShows:        map[int]bool{},
			ActiveShowsTmdb:    map[int]bool{},
			ProtectedMovies:    map[int]bool{},
			ProtectedShows:     map[int]bool{},
			ProtectedShowsTmdb: map[int]bool{},
			MovieRequesters:    map[int]string{},
			ShowRequesters:     map[int]string{},
			ShowRequestersTmdb: map[int]string{},
		}, nil
	}

	rd := &RequestData{
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

	page, totalPages := 1, 1

	for page <= totalPages {
		skip := (page - 1) * 100
		resp, err := doGet(url, apiKey, "/api/v1/request", map[string]string{
			"take":   "100",
			"skip":   fmt.Sprintf("%d", skip),
			"filter": "all",
		})
		if err != nil {
			return nil, err
		}

		var body struct {
			PageInfo struct {
				Results int `json:"results"`
			} `json:"pageInfo"`
			Results []map[string]any `json:"results"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if decErr != nil {
			return nil, decErr
		}

		total := body.PageInfo.Results
		if total > 0 {
			totalPages = (total + 99) / 100
		}

		for _, req := range body.Results {
			status := int(toFloat64(req["status"]))
			media, _ := req["media"].(map[string]any)
			if media == nil {
				continue
			}
			mediaType, _ := media["mediaType"].(string)
			tmdbID := toInt(media["tmdbId"])
			tvdbID := toInt(media["tvdbId"])

			requestedBy, _ := req["requestedBy"].(map[string]any)
			var requester string
			if requestedBy != nil {
				requester, _ = requestedBy["plexUsername"].(string)
				if requester == "" {
					requester, _ = requestedBy["displayName"].(string)
				}
			}

			// Exclude declined (status 3)
			if status != 0 && status != 3 {
				if mediaType == "movie" && tmdbID != 0 {
					rd.ActiveMovies[tmdbID] = true
					if requester != "" {
						rd.MovieRequesters[tmdbID] = requester
					}
				} else if mediaType == "tv" {
					if tvdbID != 0 {
						rd.ActiveShows[tvdbID] = true
						if requester != "" {
							rd.ShowRequesters[tvdbID] = requester
						}
					}
					if tmdbID != 0 {
						rd.ActiveShowsTmdb[tmdbID] = true
						if requester != "" {
							rd.ShowRequestersTmdb[tmdbID] = requester
						}
					}
				}
			}

			if len(protectedRequesters) > 0 && protectedRequesters[requester] {
				if mediaType == "movie" && tmdbID != 0 {
					rd.ProtectedMovies[tmdbID] = true
				} else if mediaType == "tv" {
					if tvdbID != 0 {
						rd.ProtectedShows[tvdbID] = true
					}
					if tmdbID != 0 {
						rd.ProtectedShowsTmdb[tmdbID] = true
					}
				}
			}
		}

		page++
	}

	slog.Info("Seerr active requests",
		"movies", len(rd.ActiveMovies),
		"tv_tvdb", len(rd.ActiveShows),
		"tv_tmdb", len(rd.ActiveShowsTmdb))
	return rd, nil
}

// ExtractRequestersFromTags parses Seerr-style arr tags ("N - username") and
// returns the extracted usernames.
func ExtractRequestersFromTags(tagNames []string) []string {
	var requesters []string
	for _, t := range tagNames {
		m := seerrTagPattern.FindStringSubmatch(t)
		if m != nil {
			requesters = append(requesters, strings.TrimSpace(m[1]))
		}
	}
	return requesters
}

// AddToWatchlist adds a TMDB ID to a user's watchlist. mediaType must be
// "movie" or "tv". Returns true on success (201).
func AddToWatchlist(url, apiKey string, tmdbID int, mediaType string, onBehalfUserID *int) bool {
	if url == "" || apiKey == "" {
		return false
	}
	headers := seerrHeaders(apiKey)
	headers["Content-Type"] = "application/json"
	if onBehalfUserID != nil {
		headers["X-Api-User"] = fmt.Sprintf("%d", *onBehalfUserID)
	}

	payload, _ := json.Marshal(map[string]any{
		"tmdbId":    tmdbID,
		"mediaType": mediaType,
	})

	resp, err := httpclient.Do(httpclient.Request{
		Method:  http.MethodPost,
		URL:     url + "/api/v1/watchlist",
		Headers: headers,
		Body:    strings.NewReader(string(payload)),
	})
	if err != nil {
		slog.Warn("Seerr add-to-watchlist failed", "tmdb", tmdbID, "type", mediaType, "error", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 201
}

// RemoveFromWatchlist removes a TMDB ID from a user's watchlist. Idempotent.
func RemoveFromWatchlist(url, apiKey string, tmdbID int, onBehalfUserID *int) bool {
	if url == "" || apiKey == "" {
		return false
	}
	headers := seerrHeaders(apiKey)
	if onBehalfUserID != nil {
		headers["X-Api-User"] = fmt.Sprintf("%d", *onBehalfUserID)
	}

	resp, err := httpclient.Do(httpclient.Request{
		Method:  http.MethodDelete,
		URL:     fmt.Sprintf("%s/api/v1/watchlist/%d", url, tmdbID),
		Headers: headers,
	})
	if err != nil {
		slog.Warn("Seerr remove-from-watchlist failed", "tmdb", tmdbID, "error", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 204 || resp.StatusCode == 404
}

// UserWatchlistTmdbIDs returns the set of TMDB IDs on a user's watchlist.
func UserWatchlistTmdbIDs(url, apiKey string, userID int) map[int]bool {
	out := make(map[int]bool)
	if url == "" || apiKey == "" {
		return out
	}

	page, totalPages := 1, 1
	for page <= totalPages {
		resp, err := doGet(url, apiKey,
			fmt.Sprintf("/api/v1/user/%d/watchlist", userID),
			map[string]string{"page": fmt.Sprintf("%d", page)})
		if err != nil {
			slog.Warn("Seerr user watchlist fetch failed", "user_id", userID, "error", err)
			break
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			break
		}

		var body struct {
			TotalPages int              `json:"totalPages"`
			Results    []map[string]any `json:"results"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if decErr != nil {
			break
		}
		if body.TotalPages > 1 {
			totalPages = body.TotalPages
		}
		for _, e := range body.Results {
			tmdb := toInt(e["tmdbId"])
			if tmdb != 0 {
				out[tmdb] = true
			}
		}
		page++
	}
	return out
}

// GetAPIUserID returns the user ID that owns the admin API key.
func GetAPIUserID(url, apiKey string) (int, bool) {
	if url == "" || apiKey == "" {
		return 0, false
	}

	resp, err := doGet(url, apiKey, "/api/v1/auth/me", nil)
	if err != nil {
		slog.Warn("Seerr /auth/me lookup failed", "error", err)
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, false
	}
	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, false
	}
	uid := toInt(data["id"])
	if uid == 0 {
		return 0, false
	}
	return uid, true
}

// FetchAllWatchlists returns the union of every Seerr user's watchlist.
// Each entry has at least tmdbId and mediaType keys.
func FetchAllWatchlists(url, apiKey string) ([]map[string]any, error) {
	if url == "" || apiKey == "" {
		return nil, nil
	}

	// Fetch all users (paged).
	var users []map[string]any
	page, totalPages := 1, 1
	for page <= totalPages {
		skip := (page - 1) * 100
		resp, err := doGet(url, apiKey, "/api/v1/user", map[string]string{
			"take": "100",
			"skip": fmt.Sprintf("%d", skip),
		})
		if err != nil {
			slog.Warn("Seerr: failed to list users for watchlist fetch", "error", err)
			return nil, err
		}

		var body struct {
			PageInfo struct {
				Results int `json:"results"`
			} `json:"pageInfo"`
			Results []map[string]any `json:"results"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if decErr != nil {
			return nil, decErr
		}
		total := body.PageInfo.Results
		if total > 0 {
			totalPages = (total + 99) / 100
		}
		users = append(users, body.Results...)
		page++
	}

	// Per-user watchlist.
	var items []map[string]any
	for _, u := range users {
		uid := toInt(u["id"])
		if uid == 0 {
			continue
		}

		wlPage, wlTotalPages := 1, 1
		for wlPage <= wlTotalPages {
			resp, err := doGet(url, apiKey,
				fmt.Sprintf("/api/v1/user/%d/watchlist", uid),
				map[string]string{"page": fmt.Sprintf("%d", wlPage)})
			if err != nil {
				slog.Warn("Seerr: failed to fetch watchlist", "user_id", uid, "error", err)
				break
			}
			if resp.StatusCode == 403 || resp.StatusCode == 404 {
				resp.Body.Close()
				break
			}
			if resp.StatusCode >= 400 {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			}

			var body struct {
				TotalPages int              `json:"totalPages"`
				Results    []map[string]any `json:"results"`
			}
			decErr := json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if decErr != nil {
				break
			}
			if body.TotalPages > 1 {
				wlTotalPages = body.TotalPages
			}
			for _, entry := range body.Results {
				tmdb := toInt(entry["tmdbId"])
				mt, _ := entry["mediaType"].(string)
				if tmdb != 0 && mt != "" {
					items = append(items, map[string]any{
						"tmdbId":    tmdb,
						"mediaType": mt,
					})
				}
			}
			wlPage++
		}
	}

	slog.Info("Seerr watchlist", "entries", len(items), "users", len(users))
	return items, nil
}

// FetchItemRequests fetches all Seerr requests for a specific item by TMDB ID.
func FetchItemRequests(url, apiKey, mediaType string, tmdbID int) ([]map[string]any, error) {
	if url == "" || apiKey == "" || tmdbID == 0 {
		return nil, nil
	}

	seerrType := mediaType
	if mediaType == "show" {
		seerrType = "tv"
	}

	resp, err := doGet(url, apiKey, fmt.Sprintf("/api/v1/%s/%d", seerrType, tmdbID), nil)
	if err != nil {
		slog.Warn("Seerr: failed to fetch item", "type", seerrType, "tmdb", tmdbID, "error", err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	mediaInfo, _ := data["mediaInfo"].(map[string]any)
	if mediaInfo == nil {
		return nil, nil
	}

	tvdbID := toInt(mediaInfo["tvdbId"])
	if tvdbID == 0 {
		if extIDs, ok := data["externalIds"].(map[string]any); ok {
			tvdbID = toInt(extIDs["tvdbId"])
		}
	}

	requests, _ := mediaInfo["requests"].([]any)
	var results []map[string]any
	for _, r := range requests {
		reqMap, ok := r.(map[string]any)
		if !ok {
			continue
		}
		status := int(toFloat64(reqMap["status"]))
		requesterObj, _ := reqMap["requestedBy"].(map[string]any)
		var requester, avatar string
		if requesterObj != nil {
			requester, _ = requesterObj["plexUsername"].(string)
			if requester == "" {
				requester, _ = requesterObj["displayName"].(string)
			}
			if requester == "" {
				requester, _ = requesterObj["email"].(string)
			}
			avatar, _ = requesterObj["avatar"].(string)
		}

		label, ok := statusLabels[status]
		if !ok {
			label = fmt.Sprintf("Unknown (%d)", status)
		}

		createdAt, _ := reqMap["createdAt"].(string)
		updatedAt, _ := reqMap["updatedAt"].(string)

		results = append(results, map[string]any{
			"request_id":       toInt(reqMap["id"]),
			"requester":        requester,
			"requester_avatar": avatar,
			"status":           status,
			"status_label":     label,
			"requested_at":     createdAt,
			"updated_at":       updatedAt,
			"media_type":       mediaType,
			"tmdb_id":          tmdbID,
			"tvdb_id":          tvdbID,
		})
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

func toInt(v any) int {
	return int(toFloat64(v))
}
