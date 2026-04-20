// Package jellyfin provides helpers for the Jellyfin media-server API.
package jellyfin

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/rinseaid/reclaimer/internal/models"
	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

// _TICKS_PER_MS converts Jellyfin ticks (100-nanosecond intervals) to
// milliseconds: 10 000 ticks = 1 ms.
const ticksPerMS = 10_000

// providerMap translates lowercase provider names (used by Plex GUIDs) to
// the capitalised keys Jellyfin stores in ProviderIds.
var providerMap = map[string]string{
	"tmdb": "Tmdb",
	"tvdb": "Tvdb",
	"imdb": "Imdb",
}

// doReq is a thin wrapper that injects the API key as a query parameter and
// sets Accept: application/json.
func doReq(url, apiKey, method, path string, params map[string]string) (*http.Response, error) {
	if params == nil {
		params = make(map[string]string)
	}
	params["api_key"] = apiKey

	return httpclient.Do(httpclient.Request{
		Method: method,
		URL:    url + path,
		Headers: map[string]string{
			"Accept": "application/json",
		},
		Params: params,
	})
}

// doJSON is a convenience helper: perform a request and JSON-decode the body.
func doJSON(url, apiKey, method, path string, params map[string]string, dst any) error {
	resp, err := doReq(url, apiKey, method, path, params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(dst)
}

// FetchLibrary returns all Movie and Series items in the given library.
func FetchLibrary(url, apiKey, libraryID string) ([]map[string]any, error) {
	var envelope struct {
		Items []map[string]any `json:"Items"`
	}
	err := doJSON(url, apiKey, http.MethodGet, "/Items", map[string]string{
		"ParentId":         libraryID,
		"Recursive":        "true",
		"IncludeItemTypes": "Movie,Series",
		"Fields":           "ProviderIds,DateCreated,Overview",
	}, &envelope)
	if err != nil {
		return nil, err
	}
	slog.Info("Jellyfin library fetched", "library_id", libraryID, "items", len(envelope.Items))
	return envelope.Items, nil
}

// FetchLibraries returns all movie and TV show libraries.
func FetchLibraries(url, apiKey string) ([]map[string]any, error) {
	var folders []map[string]any
	err := doJSON(url, apiKey, http.MethodGet, "/Library/VirtualFolders", nil, &folders)
	if err != nil {
		return nil, err
	}

	var libraries []map[string]any
	for _, f := range folders {
		ct, _ := f["CollectionType"].(string)
		if ct != "movies" && ct != "tvshows" {
			continue
		}
		mediaType := "show"
		if ct == "movies" {
			mediaType = "movie"
		}
		id, _ := f["ItemId"].(string)
		name, _ := f["Name"].(string)
		libraries = append(libraries, map[string]any{
			"id":    id,
			"title": name,
			"type":  mediaType,
		})
	}
	return libraries, nil
}

// FetchUsers returns all Jellyfin users.
func FetchUsers(url, apiKey string) ([]map[string]any, error) {
	resp, err := doReq(url, apiKey, http.MethodGet, "/Users", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil
	}
	var users []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
		return nil, err
	}
	return users, nil
}

// FetchCollections returns all BoxSet collections in a library.
func FetchCollections(url, apiKey, libraryID string) ([]map[string]any, error) {
	resp, err := doReq(url, apiKey, http.MethodGet, "/Items", map[string]string{
		"IncludeItemTypes": "BoxSet",
		"ParentId":         libraryID,
		"Recursive":        "true",
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil
	}
	var envelope struct {
		Items []map[string]any `json:"Items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	return envelope.Items, nil
}

// collectionItemIDs returns the set of item IDs in a collection.
func collectionItemIDs(url, apiKey, collectionID string) (map[string]bool, error) {
	resp, err := doReq(url, apiKey, http.MethodGet, "/Items", map[string]string{
		"ParentId": collectionID,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return map[string]bool{}, nil
	}
	var envelope struct {
		Items []map[string]any `json:"Items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(envelope.Items))
	for _, item := range envelope.Items {
		if id, ok := item["Id"].(string); ok {
			ids[id] = true
		}
	}
	return ids, nil
}

// SyncCollection ensures a BoxSet collection matches the desired item set.
// Creates the collection if it does not exist, then adds/removes items.
func SyncCollection(url, apiKey, libraryID, name string, want map[string]bool) error {
	if len(want) == 0 {
		slog.Info("Collection: no candidates", "name", name)
		return nil
	}

	cols, err := FetchCollections(url, apiKey, libraryID)
	if err != nil {
		return err
	}

	var existingID string
	for _, c := range cols {
		if n, _ := c["Name"].(string); n == name {
			existingID, _ = c["Id"].(string)
			break
		}
	}

	if existingID == "" {
		ids := joinKeys(want)
		slog.Info("Collection: creating", "name", name, "items", len(want))
		resp, err := doReq(url, apiKey, http.MethodPost, "/Collections", map[string]string{
			"Name":     name,
			"Ids":      ids,
			"ParentId": libraryID,
		})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("create collection: HTTP %d: %s", resp.StatusCode, string(body))
		}
		return nil
	}

	current, err := collectionItemIDs(url, apiKey, existingID)
	if err != nil {
		return err
	}

	var toAdd, toRemove []string
	for id := range want {
		if !current[id] {
			toAdd = append(toAdd, id)
		}
	}
	for id := range current {
		if !want[id] {
			toRemove = append(toRemove, id)
		}
	}

	slog.Info("Collection: syncing", "name", name,
		"current", len(current), "target", len(want),
		"add", len(toAdd), "remove", len(toRemove))

	if len(toAdd) > 0 {
		resp, err := doReq(url, apiKey, http.MethodPost,
			fmt.Sprintf("/Collections/%s/Items", existingID),
			map[string]string{"Ids": strings.Join(toAdd, ",")})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("add to collection: HTTP %d: %s", resp.StatusCode, string(body))
		}
	}
	if len(toRemove) > 0 {
		resp, err := doReq(url, apiKey, http.MethodDelete,
			fmt.Sprintf("/Collections/%s/Items", existingID),
			map[string]string{"Ids": strings.Join(toRemove, ",")})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("remove from collection: HTTP %d: %s", resp.StatusCode, string(body))
		}
	}
	return nil
}

// AddToCollectionByName ensures itemID is a member of the named BoxSet.
// Creates the collection if it does not exist. Idempotent.
func AddToCollectionByName(url, apiKey, libraryID, name, itemID string) error {
	cols, err := FetchCollections(url, apiKey, libraryID)
	if err != nil {
		return err
	}

	var existingID string
	for _, c := range cols {
		if n, _ := c["Name"].(string); n == name {
			existingID, _ = c["Id"].(string)
			break
		}
	}

	if existingID == "" {
		resp, err := doReq(url, apiKey, http.MethodPost, "/Collections", map[string]string{
			"Name": name, "Ids": itemID, "ParentId": libraryID,
		})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("create collection: HTTP %d: %s", resp.StatusCode, string(body))
		}
		return nil
	}

	resp, err := doReq(url, apiKey, http.MethodPost,
		fmt.Sprintf("/Collections/%s/Items", existingID),
		map[string]string{"Ids": itemID})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add to collection: HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// RemoveFromCollectionByName removes itemID from the named BoxSet. Idempotent.
func RemoveFromCollectionByName(url, apiKey, libraryID, name, itemID string) error {
	cols, err := FetchCollections(url, apiKey, libraryID)
	if err != nil {
		return err
	}

	var existingID string
	for _, c := range cols {
		if n, _ := c["Name"].(string); n == name {
			existingID, _ = c["Id"].(string)
			break
		}
	}
	if existingID == "" {
		return nil
	}

	current, err := collectionItemIDs(url, apiKey, existingID)
	if err != nil {
		return err
	}
	if !current[itemID] {
		return nil
	}

	resp, err := doReq(url, apiKey, http.MethodDelete,
		fmt.Sprintf("/Collections/%s/Items", existingID),
		map[string]string{"Ids": itemID})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remove from collection: HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// FetchKeepCollection returns the set of item IDs in a named keep collection.
func FetchKeepCollection(url, apiKey, libraryID, name string) (map[string]bool, error) {
	if name == "" {
		return map[string]bool{}, nil
	}
	cols, err := FetchCollections(url, apiKey, libraryID)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		if n, _ := c["Name"].(string); n == name {
			id, _ := c["Id"].(string)
			keys, err := collectionItemIDs(url, apiKey, id)
			if err != nil {
				return nil, err
			}
			slog.Info("Jellyfin keep collection", "name", name, "excluded", len(keys))
			return keys, nil
		}
	}
	slog.Warn("Jellyfin keep collection not found", "name", name, "library_id", libraryID)
	return map[string]bool{}, nil
}

// ExternalID extracts a provider ID from a Jellyfin item's ProviderIds dict.
// The provider argument uses lowercase form (tmdb, tvdb, imdb).
func ExternalID(item map[string]any, provider string) string {
	jfKey, ok := providerMap[strings.ToLower(provider)]
	if !ok {
		// Fallback: capitalise first letter.
		jfKey = strings.ToUpper(provider[:1]) + provider[1:]
	}
	pids, _ := item["ProviderIds"].(map[string]any)
	if pids == nil {
		return ""
	}
	v, _ := pids[jfKey].(string)
	return v
}

// FetchSeasons returns season objects for a Jellyfin series.
func FetchSeasons(url, apiKey, seriesID string) ([]map[string]any, error) {
	resp, err := doReq(url, apiKey, http.MethodGet, fmt.Sprintf("/Shows/%s/Seasons", seriesID), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil
	}
	var envelope struct {
		Items []map[string]any `json:"Items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	return envelope.Items, nil
}

// FetchWatchHistory returns normalised watch history rows and a user map
// (accountIDStr -> username) across all Jellyfin users. It queries per-user
// IsPlayed and IsResumable items. Never returns an error that would halt the
// caller; failures are logged and result in empty data.
func FetchWatchHistory(url, apiKey string) ([]models.SessionHistoryEntry, map[string]string, error) {
	if url == "" || apiKey == "" {
		return nil, nil, nil
	}

	users, err := FetchUsers(url, apiKey)
	if err != nil {
		slog.Warn("Jellyfin: failed to fetch users for watch history", "error", err)
		return nil, nil, nil
	}

	var rows []models.SessionHistoryEntry
	userMap := make(map[string]string)

	for _, u := range users {
		uuid, _ := u["Id"].(string)
		username, _ := u["Name"].(string)
		if uuid == "" || username == "" {
			continue
		}
		acctInt := jfUserIDToInt(uuid)
		acctStr := fmt.Sprintf("%d", acctInt)
		userMap[acctStr] = username

		for _, flag := range []string{"IsPlayed", "IsResumable"} {
			resp, err := doReq(url, apiKey, http.MethodGet, fmt.Sprintf("/Users/%s/Items", uuid), map[string]string{
				"Recursive":        "true",
				"IncludeItemTypes": "Movie,Episode",
				"Filters":          flag,
				"Fields":           "UserData,SeriesName,ParentIndexNumber,IndexNumber,RunTimeTicks",
				"Limit":            "0", // Jellyfin treats 0 as no limit
			})
			if err != nil {
				slog.Warn("Jellyfin watch history fetch failed", "flag", flag, "user", username, "error", err)
				continue
			}
			if resp.StatusCode != 200 {
				resp.Body.Close()
				slog.Warn("Jellyfin watch history returned non-200", "flag", flag, "user", username, "status", resp.StatusCode)
				continue
			}

			var envelope struct {
				Items []map[string]any `json:"Items"`
			}
			decErr := json.NewDecoder(resp.Body).Decode(&envelope)
			resp.Body.Close()
			if decErr != nil {
				slog.Warn("Jellyfin watch history decode failed", "flag", flag, "user", username, "error", decErr)
				continue
			}

			for _, it := range envelope.Items {
				ud, _ := it["UserData"].(map[string]any)
				if ud == nil {
					ud = map[string]any{}
				}

				lastPlayed, _ := ud["LastPlayedDate"].(string)
				if lastPlayed == "" {
					continue
				}

				itemType, _ := it["Type"].(string)
				itemType = strings.ToLower(itemType)

				var mediaType, grandparentTitle string
				switch itemType {
				case "episode":
					mediaType = "episode"
					grandparentTitle, _ = it["SeriesName"].(string)
				case "movie":
					mediaType = "movie"
				default:
					continue
				}

				posTicks := toInt64(ud["PlaybackPositionTicks"])
				runTicks := toInt64(it["RunTimeTicks"])
				viewOffsetMS := posTicks / ticksPerMS
				mediaDurationMS := runTicks / ticksPerMS

				// Synthesise 100% completion for fully-watched items.
				played, _ := ud["Played"].(bool)
				if played && mediaDurationMS > 0 {
					viewOffsetMS = mediaDurationMS
				}

				// Normalise timestamp: strip fractional seconds and trailing Z.
				watchedAt := strings.TrimSuffix(lastPlayed, "Z")
				if idx := strings.Index(watchedAt, "."); idx >= 0 {
					watchedAt = watchedAt[:idx]
				}

				seasonNum := toIntPtr(it["ParentIndexNumber"])
				episodeNum := toIntPtr(it["IndexNumber"])

				itemID, _ := it["Id"].(string)
				title, _ := it["Name"].(string)

				rows = append(rows, models.SessionHistoryEntry{
					AccountID:        acctInt,
					RatingKey:        itemID,
					Title:            title,
					GrandparentTitle: grandparentTitle,
					MediaType:        mediaType,
					SeasonNumber:     seasonNum,
					EpisodeNumber:    episodeNum,
					WatchedAt:        watchedAt,
					ViewOffsetMS:     viewOffsetMS,
					MediaDurationMS:  mediaDurationMS,
				})
			}
		}
	}

	slog.Info("Jellyfin watch history", "rows", len(rows), "users", len(userMap))
	return rows, userMap, nil
}

// TestConnection tests the Jellyfin connection and returns server info.
func TestConnection(url, apiKey string) (map[string]any, error) {
	var data map[string]any
	err := doJSON(url, apiKey, http.MethodGet, "/System/Info", nil, &data)
	if err != nil {
		return nil, err
	}
	version, _ := data["Version"].(string)
	serverName, _ := data["ServerName"].(string)
	if version == "" {
		version = "?"
	}
	if serverName == "" {
		serverName = "?"
	}
	return map[string]any{
		"ok":     true,
		"detail": fmt.Sprintf("Jellyfin %s - %s", version, serverName),
	}, nil
}

// SearchLibrary searches across Jellyfin libraries for movies and series
// matching the query string.
func SearchLibrary(url, apiKey, query string) ([]map[string]any, error) {
	var envelope struct {
		Items []map[string]any `json:"Items"`
	}
	err := doJSON(url, apiKey, http.MethodGet, "/Items", map[string]string{
		"SearchTerm":       query,
		"Recursive":        "true",
		"IncludeItemTypes": "Movie,Series",
		"Limit":            "25",
	}, &envelope)
	if err != nil {
		return nil, fmt.Errorf("SearchLibrary: %w", err)
	}

	results := make([]map[string]any, 0, len(envelope.Items))
	for _, item := range envelope.Items {
		id, _ := item["Id"].(string)
		name, _ := item["Name"].(string)
		itemType, _ := item["Type"].(string)
		mediaType := "movie"
		if itemType == "Series" {
			mediaType = "show"
		}
		results = append(results, map[string]any{
			"rating_key": id,
			"title":      name,
			"media_type": mediaType,
			"source":     "jellyfin",
		})
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// jfUserIDToInt hashes a Jellyfin user UUID to a stable 63-bit positive int.
func jfUserIDToInt(userID string) int64 {
	h := sha1.Sum([]byte(userID))
	n := int64(binary.BigEndian.Uint64(h[:8]))
	return n & ((1 << 63) - 1) // keep positive
}

// toInt64 extracts a numeric value from an any that may be float64 or json.Number.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

// toIntPtr converts a numeric any to *int, returning nil for missing values.
func toIntPtr(v any) *int {
	if v == nil {
		return nil
	}
	switch n := v.(type) {
	case float64:
		i := int(n)
		return &i
	case json.Number:
		i64, err := n.Int64()
		if err != nil {
			return nil
		}
		i := int(i64)
		return &i
	case int:
		return &n
	case int64:
		i := int(n)
		return &i
	}
	return nil
}

// joinKeys joins map keys with commas.
func joinKeys(m map[string]bool) string {
	parts := make([]string, 0, len(m))
	for k := range m {
		parts = append(parts, k)
	}
	return strings.Join(parts, ",")
}
