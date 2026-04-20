// Package plex provides helpers for the Plex Media Server API.
package plex

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rinseaid/reclaimer/internal/models"
	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

// machineID is cached after the first successful lookup.
var (
	machineID   string
	machineOnce sync.Mutex
)

// plexParams holds normal and raw (un-encoded key) query parameters for a Plex request.
type plexParams struct {
	Normal map[string]string
	Raw    map[string]string // keys containing operators like > or >= that must not be percent-encoded
}

// doReq executes an HTTP request against the Plex server, adding the token as
// a query parameter and requesting JSON responses.
func doReq(method, plexURL, plexToken, path string, extraParams map[string]string) (*http.Response, error) {
	return doReqRaw(method, plexURL, plexToken, path, plexParams{Normal: extraParams})
}

func doReqRaw(method, plexURL, plexToken, path string, pp plexParams) (*http.Response, error) {
	params := map[string]string{
		"X-Plex-Token": plexToken,
	}
	for k, v := range pp.Normal {
		params[k] = v
	}
	headers := map[string]string{
		"Accept": "application/json",
	}
	return httpclient.Do(httpclient.Request{
		Method:    method,
		URL:       plexURL + path,
		Headers:   headers,
		Params:    params,
		RawParams: pp.Raw,
	})
}

// doReqJSON executes an HTTP request and decodes the response JSON into dst.
func doReqJSON(method, plexURL, plexToken, path string, extraParams map[string]string, dst any) error {
	return doReqJSONRaw(method, plexURL, plexToken, path, plexParams{Normal: extraParams}, dst)
}

func doReqJSONRaw(method, plexURL, plexToken, path string, pp plexParams, dst any) error {
	resp, err := doReqRaw(method, plexURL, plexToken, path, pp)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("plex HTTP %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// plexResponse is the generic envelope Plex wraps all responses in.
type plexResponse struct {
	MediaContainer map[string]any `json:"MediaContainer"`
}

// metadataSlice extracts the Metadata array from a MediaContainer as
// []map[string]any. Returns nil if the key is absent.
func metadataSlice(container map[string]any, key string) ([]map[string]any, error) {
	raw, ok := container[key]
	if !ok {
		return nil, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]map[string]any, 0, len(arr))
	for _, v := range arr {
		m, ok := v.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return out, nil
}

// FetchLibrary returns the Metadata array for a Plex library section.
func FetchLibrary(plexURL, plexToken string, sectionID int) ([]map[string]any, error) {
	var resp plexResponse
	path := fmt.Sprintf("/library/sections/%d/all", sectionID)
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, path, map[string]string{
		"includeGuids": "1",
	}, &resp); err != nil {
		return nil, fmt.Errorf("FetchLibrary section %d: %w", sectionID, err)
	}
	items, err := metadataSlice(resp.MediaContainer, "Metadata")
	if err != nil {
		return nil, err
	}
	slog.Info("Plex library fetched", "section", sectionID, "items", len(items))
	return items, nil
}

// FetchSeasons returns seasons for a show, filtering out Specials (index 0).
func FetchSeasons(plexURL, plexToken, showRatingKey string) ([]map[string]any, error) {
	var resp plexResponse
	path := fmt.Sprintf("/library/metadata/%s/children", showRatingKey)
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, path, nil, &resp); err != nil {
		return nil, fmt.Errorf("FetchSeasons %s: %w", showRatingKey, err)
	}
	all, err := metadataSlice(resp.MediaContainer, "Metadata")
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(all))
	for _, s := range all {
		idx := toInt(s["index"])
		if idx != 0 {
			out = append(out, s)
		}
	}
	return out, nil
}

// ExternalID extracts an external ID (tmdb, tvdb, imdb) from a Plex item.
// It checks the Guid array first (e.g. {"id":"tmdb://12345"}), then falls
// back to the legacy guid field.
func ExternalID(item map[string]any, idType string) string {
	prefix := idType + "://"
	if guids, ok := item["Guid"]; ok {
		if arr, ok := guids.([]any); ok {
			for _, g := range arr {
				if gm, ok := g.(map[string]any); ok {
					gid, _ := gm["id"].(string)
					if strings.HasPrefix(gid, prefix) {
						return strings.SplitN(gid, "://", 2)[1]
					}
				}
			}
		}
	}
	guid, _ := item["guid"].(string)
	re := regexp.MustCompile(idType + `://(\d+)`)
	m := re.FindStringSubmatch(guid)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

// collectionKey finds the ratingKey of a named collection in a section.
func collectionKey(plexURL, plexToken string, sectionID int, name string) (string, error) {
	var resp plexResponse
	path := fmt.Sprintf("/library/sections/%d/collections", sectionID)
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, path, nil, &resp); err != nil {
		return "", err
	}
	cols, _ := metadataSlice(resp.MediaContainer, "Metadata")
	for _, col := range cols {
		title, _ := col["title"].(string)
		if title == name {
			rk := toString(col["ratingKey"])
			return rk, nil
		}
	}
	return "", nil
}

// collectionItemKeys returns the set of ratingKeys in a collection.
func collectionItemKeys(plexURL, plexToken, colKey string) (map[string]bool, error) {
	var resp plexResponse
	path := fmt.Sprintf("/library/collections/%s/children", colKey)
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, path, nil, &resp); err != nil {
		return nil, err
	}
	items, _ := metadataSlice(resp.MediaContainer, "Metadata")
	out := make(map[string]bool, len(items))
	for _, m := range items {
		rk := toString(m["ratingKey"])
		if rk != "" {
			out[rk] = true
		}
	}
	return out, nil
}

// FetchKeepCollection returns the set of ratingKeys belonging to a named
// collection (used for keep-list exclusion).
func FetchKeepCollection(plexURL, plexToken string, sectionID int, name string) (map[string]bool, error) {
	if name == "" {
		return nil, nil
	}
	ck, err := collectionKey(plexURL, plexToken, sectionID, name)
	if err != nil {
		return nil, fmt.Errorf("FetchKeepCollection: %w", err)
	}
	if ck == "" {
		slog.Debug("Plex keep collection not found", "name", name, "section", sectionID)
		return nil, nil
	}
	keys, err := collectionItemKeys(plexURL, plexToken, ck)
	if err != nil {
		return nil, err
	}
	slog.Info("Plex keep collection loaded", "name", name, "items", len(keys))
	return keys, nil
}

// SyncCollection creates or updates a Plex collection so its membership
// matches the want set exactly.
func SyncCollection(plexURL, plexToken string, sectionID int, name string, want map[string]bool, mediaType int) error {
	if len(want) == 0 {
		slog.Info("Collection sync: no candidates", "name", name)
		return nil
	}

	ck, err := collectionKey(plexURL, plexToken, sectionID, name)
	if err != nil {
		return fmt.Errorf("SyncCollection: %w", err)
	}

	if ck == "" {
		// Create collection with the first item, then add the rest.
		keys := mapKeys(want)
		slog.Info("Collection creating", "name", name, "items", len(keys), "type", mediaType)

		uri, err := ItemURI(plexURL, plexToken, keys[0])
		if err != nil {
			return err
		}
		var createResp plexResponse
		if err := doReqJSON(http.MethodPost, plexURL, plexToken, "/library/collections", map[string]string{
			"type":      strconv.Itoa(mediaType),
			"title":     name,
			"smart":     "0",
			"sectionId": strconv.Itoa(sectionID),
			"uri":       uri,
		}, &createResp); err != nil {
			return fmt.Errorf("SyncCollection create: %w", err)
		}
		meta, _ := metadataSlice(createResp.MediaContainer, "Metadata")
		if len(meta) == 0 {
			return fmt.Errorf("SyncCollection: empty response after create")
		}
		ck = toString(meta[0]["ratingKey"])

		for _, k := range keys[1:] {
			uri, err := ItemURI(plexURL, plexToken, k)
			if err != nil {
				return err
			}
			resp, err := doReq(http.MethodPut, plexURL, plexToken,
				fmt.Sprintf("/library/collections/%s/items", ck),
				map[string]string{"uri": uri})
			if err != nil {
				return err
			}
			resp.Body.Close()
		}
		return nil
	}

	// Update existing collection.
	current, err := collectionItemKeys(plexURL, plexToken, ck)
	if err != nil {
		return err
	}

	toAdd := diffKeys(want, current)
	toRemove := diffKeys(current, want)

	slog.Info("Collection syncing",
		"name", name,
		"current", len(current),
		"target", len(want),
		"add", len(toAdd),
		"remove", len(toRemove),
	)

	var addErrors, removeErrors int
	for _, k := range toAdd {
		uri, err := ItemURI(plexURL, plexToken, k)
		if err != nil {
			addErrors++
			if addErrors <= 3 {
				slog.Warn("Collection add failed", "name", name, "key", k, "error", err)
			}
			continue
		}
		resp, err := doReq(http.MethodPut, plexURL, plexToken,
			fmt.Sprintf("/library/collections/%s/items", ck),
			map[string]string{"uri": uri})
		if err != nil {
			addErrors++
			if addErrors <= 3 {
				slog.Warn("Collection add failed", "name", name, "key", k, "error", err)
			}
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			addErrors++
			if addErrors <= 3 {
				slog.Warn("Collection add HTTP error", "name", name, "key", k, "status", resp.StatusCode)
			}
		}
	}

	for _, k := range toRemove {
		resp, err := doReq(http.MethodDelete, plexURL, plexToken,
			fmt.Sprintf("/library/collections/%s/items/%s", ck, k), nil)
		if err != nil {
			removeErrors++
			if removeErrors <= 3 {
				slog.Warn("Collection remove failed", "name", name, "key", k, "error", err)
			}
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			removeErrors++
			if removeErrors <= 3 {
				slog.Warn("Collection remove HTTP error", "name", name, "key", k, "status", resp.StatusCode)
			}
		}
	}

	if addErrors > 3 {
		slog.Warn("Collection add errors total", "name", name, "count", addErrors)
	}
	if removeErrors > 3 {
		slog.Warn("Collection remove errors total", "name", name, "count", removeErrors)
	}
	if addErrors > 0 || removeErrors > 0 {
		return fmt.Errorf("collection '%s' sync had %d add errors and %d remove errors",
			name, addErrors, removeErrors)
	}
	return nil
}

// AddToCollectionByName ensures an item is in the named collection, creating
// the collection if it does not exist. Idempotent.
func AddToCollectionByName(plexURL, plexToken string, sectionID int, name, ratingKey string, mediaType int) error {
	ck, err := collectionKey(plexURL, plexToken, sectionID, name)
	if err != nil {
		return fmt.Errorf("AddToCollectionByName: %w", err)
	}

	uri, err := ItemURI(plexURL, plexToken, ratingKey)
	if err != nil {
		return err
	}

	if ck == "" {
		resp, err := doReq(http.MethodPost, plexURL, plexToken, "/library/collections", map[string]string{
			"type":      strconv.Itoa(mediaType),
			"title":     name,
			"smart":     "0",
			"sectionId": strconv.Itoa(sectionID),
			"uri":       uri,
		})
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("AddToCollectionByName create: HTTP %d", resp.StatusCode)
		}
		return nil
	}

	resp, err := doReq(http.MethodPut, plexURL, plexToken,
		fmt.Sprintf("/library/collections/%s/items", ck),
		map[string]string{"uri": uri})
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("AddToCollectionByName: HTTP %d", resp.StatusCode)
	}
	return nil
}

// RemoveFromCollectionByName removes an item from the named collection.
// Idempotent: no error if the collection or item is absent.
func RemoveFromCollectionByName(plexURL, plexToken string, sectionID int, name, ratingKey string) error {
	ck, err := collectionKey(plexURL, plexToken, sectionID, name)
	if err != nil {
		return fmt.Errorf("RemoveFromCollectionByName: %w", err)
	}
	if ck == "" {
		return nil
	}
	current, err := collectionItemKeys(plexURL, plexToken, ck)
	if err != nil {
		return err
	}
	if !current[ratingKey] {
		return nil
	}
	resp, err := doReq(http.MethodDelete, plexURL, plexToken,
		fmt.Sprintf("/library/collections/%s/items/%s", ck, ratingKey), nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("RemoveFromCollectionByName: HTTP %d", resp.StatusCode)
	}
	return nil
}

// FetchAccounts returns a map of account ID to username for all accounts
// authorised on the Plex server.
func FetchAccounts(plexURL, plexToken string) (map[int64]string, error) {
	if plexURL == "" || plexToken == "" {
		return nil, nil
	}

	var resp plexResponse
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, "/accounts", nil, &resp); err != nil {
		slog.Warn("Plex /accounts fetch failed", "error", err)
		return nil, nil
	}

	accounts, _ := metadataSlice(resp.MediaContainer, "Account")
	out := make(map[int64]string, len(accounts))
	for _, a := range accounts {
		id := toInt64(a["id"])
		if id == 0 {
			continue
		}
		name, _ := a["name"].(string)
		if name == "" {
			if t, ok := a["title"].(string); ok && t != "" {
				name = t
			} else if d, ok := a["defaultName"].(string); ok && d != "" {
				name = d
			} else {
				name = fmt.Sprintf("user-%d", id)
			}
		}
		out[id] = name
	}
	return out, nil
}

// FetchSessionHistory performs a paginated walk of Plex session history and
// returns normalized entries. It never returns an error for transient failures;
// instead it logs the issue and returns whatever was gathered.
func FetchSessionHistory(plexURL, plexToken string, sinceTS *int64) ([]models.SessionHistoryEntry, error) {
	if plexURL == "" || plexToken == "" {
		return nil, nil
	}

	const pageSize = 500
	start := 0
	var out []models.SessionHistoryEntry

	for {
		pp := plexParams{
			Normal: map[string]string{
				"X-Plex-Container-Start": strconv.Itoa(start),
				"X-Plex-Container-Size":  strconv.Itoa(pageSize),
				"sort":                   "viewedAt:asc",
			},
		}
		if sinceTS != nil {
			pp.Raw = map[string]string{
				"viewedAt>": strconv.FormatInt(*sinceTS, 10),
			}
		}

		var resp plexResponse
		if err := doReqJSONRaw(http.MethodGet, plexURL, plexToken,
			"/status/sessions/history/all", pp, &resp); err != nil {
			slog.Warn("Plex session history fetch failed", "start", start, "error", err)
			break
		}

		entries, _ := metadataSlice(resp.MediaContainer, "Metadata")
		if len(entries) == 0 {
			break
		}

		for _, h := range entries {
			acctID := toInt64(h["accountID"])
			if acctID == 0 {
				continue
			}

			histType := strings.ToLower(toString(h["type"]))
			var mediaType string
			switch histType {
			case "episode":
				mediaType = "episode"
			case "movie":
				mediaType = "movie"
			default:
				continue
			}

			viewedAt := toInt64(h["viewedAt"])
			var watchedAtISO string
			if viewedAt > 0 {
				watchedAtISO = time.Unix(viewedAt, 0).UTC().Truncate(time.Second).Format(time.RFC3339)
			}

			var seasonNumber, episodeNumber *int
			if pi := h["parentIndex"]; pi != nil {
				v := toInt(pi)
				seasonNumber = &v
			}
			if idx := h["index"]; idx != nil {
				v := toInt(idx)
				episodeNumber = &v
			}

			mediaDurationMS := toInt64(h["duration"])
			viewOffsetMS := toInt64(h["viewOffset"])

			// Skip phantom entries: only when both fields are explicitly
			// present in the response AND zero. The history endpoint often
			// omits these fields entirely (they default to 0), so we check
			// for their presence before treating zeros as a phantom signal.
			_, hasDuration := h["duration"]
			_, hasOffset := h["viewOffset"]
			if hasDuration && hasOffset && viewOffsetMS == 0 && mediaDurationMS == 0 {
				continue
			}

			// The history endpoint often omits duration and/or viewOffset.
			// Presence in history implies the content was watched, so
			// treat missing progress data as a completed watch.
			if mediaDurationMS == 0 && viewOffsetMS == 0 {
				mediaDurationMS = 1
				viewOffsetMS = 1
			} else if viewOffsetMS == 0 && mediaDurationMS > 0 {
				// Completed playback: Plex clears viewOffset on completion.
				viewOffsetMS = mediaDurationMS
			}

			out = append(out, models.SessionHistoryEntry{
				AccountID:        acctID,
				RatingKey:        toString(h["ratingKey"]),
				Title:            toString(h["title"]),
				GrandparentTitle: toString(h["grandparentTitle"]),
				MediaType:        mediaType,
				SeasonNumber:     seasonNumber,
				EpisodeNumber:    episodeNumber,
				WatchedAt:        watchedAtISO,
				ViewOffsetMS:     viewOffsetMS,
				MediaDurationMS:  mediaDurationMS,
			})
		}

		if len(entries) < pageSize {
			break
		}
		start += pageSize
	}

	slog.Info("Plex session history fetched", "entries", len(out), "sinceTS", sinceTS)
	return out, nil
}

// FetchFavoritedKeys returns the ratingKeys of items with a positive user
// rating (hearted/favorited) in the given library section.
func FetchFavoritedKeys(plexURL, plexToken string, sectionID int) (map[string]bool, error) {
	if plexURL == "" || plexToken == "" || sectionID == 0 {
		return nil, nil
	}

	var resp plexResponse
	path := fmt.Sprintf("/library/sections/%d/all", sectionID)
	if err := doReqJSONRaw(http.MethodGet, plexURL, plexToken, path, plexParams{
		Raw: map[string]string{"userRating>": "0"},
	}, &resp); err != nil {
		slog.Warn("Plex favorites fetch failed", "section", sectionID, "error", err)
		return nil, nil
	}

	items, _ := metadataSlice(resp.MediaContainer, "Metadata")
	out := make(map[string]bool, len(items))
	for _, item := range items {
		rk := toString(item["ratingKey"])
		if rk != "" {
			out[rk] = true
		}
	}
	slog.Info("Plex favorites loaded", "section", sectionID, "hearted", len(out))
	return out, nil
}

// GetMachineID returns the Plex server's machineIdentifier, caching the result.
func GetMachineID(plexURL, plexToken string) (string, error) {
	machineOnce.Lock()
	defer machineOnce.Unlock()
	if machineID != "" {
		return machineID, nil
	}
	var resp plexResponse
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, "/", nil, &resp); err != nil {
		return "", fmt.Errorf("GetMachineID: %w", err)
	}
	mid, _ := resp.MediaContainer["machineIdentifier"].(string)
	if mid == "" {
		return "", fmt.Errorf("GetMachineID: machineIdentifier not found in response")
	}
	machineID = mid
	return machineID, nil
}

// ItemURI builds the canonical Plex server:// URI for an item, used when
// adding items to collections.
func ItemURI(plexURL, plexToken, ratingKey string) (string, error) {
	mid, err := GetMachineID(plexURL, plexToken)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("server://%s/com.plexapp.plugins.library/library/metadata/%s", mid, ratingKey), nil
}

// FetchMetadata fetches a single item's metadata by rating key and populates
// dst with the first Metadata entry (title, type, year, etc.).
func FetchMetadata(plexURL, plexToken, ratingKey string, dst *map[string]any) error {
	var resp plexResponse
	path := fmt.Sprintf("/library/metadata/%s", ratingKey)
	if err := doReqJSON(http.MethodGet, plexURL, plexToken, path, nil, &resp); err != nil {
		return err
	}
	items, _ := metadataSlice(resp.MediaContainer, "Metadata")
	if len(items) == 0 {
		return fmt.Errorf("no metadata for %s", ratingKey)
	}
	*dst = items[0]
	return nil
}

// SearchLibrary searches across Plex library sections for movies and shows
// matching the query string. Returns lightweight result maps with rating_key,
// title, media_type, and source fields.
func SearchLibrary(plexURL, plexToken, query string) ([]map[string]any, error) {
	var resp plexResponse
	err := doReqJSON(http.MethodGet, plexURL, plexToken, "/hubs/search", map[string]string{
		"query": query,
		"limit": "25",
	}, &resp)
	if err != nil {
		return nil, fmt.Errorf("SearchLibrary: %w", err)
	}

	hubs, _ := resp.MediaContainer["Hub"].([]any)
	var results []map[string]any
	for _, h := range hubs {
		hub, ok := h.(map[string]any)
		if !ok {
			continue
		}
		hubType := toString(hub["type"])
		if hubType != "movie" && hubType != "show" {
			continue
		}
		items, _ := metadataSlice(hub, "Metadata")
		for _, item := range items {
			results = append(results, map[string]any{
				"rating_key": toString(item["ratingKey"]),
				"title":      toString(item["title"]),
				"media_type": hubType,
				"source":     "plex",
			})
		}
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// toString coerces a JSON-decoded value to string.
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

// toInt coerces a JSON-decoded value to int.
func toInt(v any) int {
	return int(toInt64(v))
}

// toInt64 coerces a JSON-decoded value to int64.
func toInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int64(t)
	case json.Number:
		n, _ := t.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case int:
		return int64(t)
	case int64:
		return t
	default:
		return 0
	}
}

// mapKeys returns the keys of a map[string]bool as a slice.
func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// diffKeys returns keys present in a but not in b.
func diffKeys(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	return out
}
