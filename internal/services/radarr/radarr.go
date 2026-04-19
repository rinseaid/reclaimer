// Package radarr provides helpers for the Radarr v3 API.
package radarr

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/rinseaid/reclaimer/internal/services/httpclient"
)

// apiReq performs an HTTP request against the Radarr API.
func apiReq(radarrURL, radarrKey, method, path string, body any, params map[string]string) (*http.Response, error) {
	url := strings.TrimRight(radarrURL, "/") + "/api/v3" + path

	headers := map[string]string{
		"X-Api-Key": radarrKey,
	}

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = strings.NewReader(string(b))
		headers["Content-Type"] = "application/json"
	}

	return httpclient.Do(httpclient.Request{
		Method:  method,
		URL:     url,
		Headers: headers,
		Params:  params,
		Body:    bodyReader,
	})
}

// apiJSON performs a request and decodes the JSON response into dst.
func apiJSON(radarrURL, radarrKey, method, path string, body any, params map[string]string, dst any) error {
	resp, err := apiReq(radarrURL, radarrKey, method, path, body, params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("radarr %s %s: HTTP %d: %s", method, path, resp.StatusCode, string(b))
	}

	if dst != nil {
		return json.NewDecoder(resp.Body).Decode(dst)
	}
	return nil
}

// FetchMovies returns {tmdbId -> movie} with a "_tag_names" list attached to each movie.
func FetchMovies(radarrURL, radarrKey string) (map[int]map[string]any, error) {
	var movies []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/movie", nil, nil, &movies); err != nil {
		return nil, fmt.Errorf("fetch movies: %w", err)
	}

	var tags []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return nil, fmt.Errorf("fetch tags: %w", err)
	}

	tagMap := make(map[int]string, len(tags))
	for _, t := range tags {
		id := toInt(t["id"])
		label, _ := t["label"].(string)
		tagMap[id] = label
	}

	result := make(map[int]map[string]any, len(movies))
	for _, m := range movies {
		tmdbID := toInt(m["tmdbId"])
		tagIDs := toIntSlice(m["tags"])
		tagNames := make([]string, 0, len(tagIDs))
		for _, tid := range tagIDs {
			if name, ok := tagMap[tid]; ok {
				tagNames = append(tagNames, name)
			} else {
				tagNames = append(tagNames, strconv.Itoa(tid))
			}
		}
		m["_tag_names"] = tagNames
		result[tmdbID] = m
	}

	slog.Info("Radarr: fetched movies", "count", len(result))
	return result, nil
}

// FetchMovieByTmdb fetches a single movie by TMDB ID. Returns {tmdbId -> movie} with _tag_names.
func FetchMovieByTmdb(radarrURL, radarrKey string, tmdbID int) (map[int]map[string]any, error) {
	if radarrURL == "" || radarrKey == "" || tmdbID == 0 {
		return map[int]map[string]any{}, nil
	}

	var movies []map[string]any
	err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/movie", nil,
		map[string]string{"tmdbId": strconv.Itoa(tmdbID)}, &movies)
	if err != nil {
		return map[int]map[string]any{}, nil
	}
	if len(movies) == 0 {
		return map[int]map[string]any{}, nil
	}

	var tags []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return map[int]map[string]any{}, nil
	}

	tagMap := make(map[int]string, len(tags))
	for _, t := range tags {
		tagMap[toInt(t["id"])], _ = t["label"].(string)
	}

	result := make(map[int]map[string]any, len(movies))
	for _, m := range movies {
		tid := toInt(m["tmdbId"])
		tagIDs := toIntSlice(m["tags"])
		tagNames := make([]string, 0, len(tagIDs))
		for _, id := range tagIDs {
			if name, ok := tagMap[id]; ok {
				tagNames = append(tagNames, name)
			} else {
				tagNames = append(tagNames, strconv.Itoa(id))
			}
		}
		m["_tag_names"] = tagNames
		result[tid] = m
	}
	return result, nil
}

// Unmonitor sets monitored=false for a movie.
func Unmonitor(radarrURL, radarrKey string, movieID int, title string) error {
	var movie map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, fmt.Sprintf("/movie/%d", movieID), nil, nil, &movie); err != nil {
		return fmt.Errorf("unmonitor get movie %d: %w", movieID, err)
	}
	movie["monitored"] = false
	if err := apiJSON(radarrURL, radarrKey, http.MethodPut, fmt.Sprintf("/movie/%d", movieID), movie, nil, nil); err != nil {
		return fmt.Errorf("unmonitor put movie %d: %w", movieID, err)
	}
	slog.Info("Unmonitored in Radarr", "title", title)
	return nil
}

// Delete removes a movie from Radarr.
func Delete(radarrURL, radarrKey string, movieID int, title string, deleteFiles, addExclusion bool) error {
	params := map[string]string{
		"deleteFiles":        strconv.FormatBool(deleteFiles),
		"addImportExclusion": strconv.FormatBool(addExclusion),
	}
	resp, err := apiReq(radarrURL, radarrKey, http.MethodDelete, fmt.Sprintf("/movie/%d", movieID), nil, params)
	if err != nil {
		return fmt.Errorf("delete movie %d: %w", movieID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete movie %d: HTTP %d: %s", movieID, resp.StatusCode, string(b))
	}
	slog.Info("Deleted from Radarr", "title", title, "deleteFiles", deleteFiles)
	return nil
}

// Search triggers a MoviesSearch command for the given movie IDs.
func Search(radarrURL, radarrKey string, movieIDs []int) error {
	body := map[string]any{
		"name":     "MoviesSearch",
		"movieIds": movieIDs,
	}
	if err := apiJSON(radarrURL, radarrKey, http.MethodPost, "/command", body, nil, nil); err != nil {
		return fmt.Errorf("search movies: %w", err)
	}
	return nil
}

// EnsureTagID returns the ID of a tag with the given label, creating it if needed.
func EnsureTagID(radarrURL, radarrKey, label string) (int, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return 0, fmt.Errorf("empty tag label")
	}

	var tags []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return 0, fmt.Errorf("fetch tags: %w", err)
	}

	for _, t := range tags {
		tLabel, _ := t["label"].(string)
		if strings.EqualFold(strings.TrimSpace(tLabel), label) {
			return toInt(t["id"]), nil
		}
	}

	var created map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodPost, "/tag", map[string]string{"label": label}, nil, &created); err != nil {
		return 0, fmt.Errorf("create tag %q: %w", label, err)
	}
	return toInt(created["id"]), nil
}

// AddTag adds a tag to a movie by label, creating the tag if missing. Idempotent.
func AddTag(radarrURL, radarrKey string, movieID int, label, title string) error {
	tagID, err := EnsureTagID(radarrURL, radarrKey, label)
	if err != nil {
		return err
	}

	var movie map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, fmt.Sprintf("/movie/%d", movieID), nil, nil, &movie); err != nil {
		return fmt.Errorf("add tag get movie %d: %w", movieID, err)
	}

	tagIDs := toIntSlice(movie["tags"])
	for _, id := range tagIDs {
		if id == tagID {
			return nil // already present
		}
	}

	tagIDs = append(tagIDs, tagID)
	movie["tags"] = tagIDs
	if err := apiJSON(radarrURL, radarrKey, http.MethodPut, fmt.Sprintf("/movie/%d", movieID), movie, nil, nil); err != nil {
		return fmt.Errorf("add tag put movie %d: %w", movieID, err)
	}
	slog.Info("Added Radarr tag", "label", label, "title", title)
	return nil
}

// RemoveTag removes a tag from a movie by label. No-op if not present.
func RemoveTag(radarrURL, radarrKey string, movieID int, label, title string) error {
	label = strings.TrimSpace(label)
	if label == "" {
		return nil
	}

	var tags []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return fmt.Errorf("remove tag fetch tags: %w", err)
	}

	wantID := -1
	for _, t := range tags {
		tLabel, _ := t["label"].(string)
		if strings.EqualFold(strings.TrimSpace(tLabel), label) {
			wantID = toInt(t["id"])
			break
		}
	}
	if wantID < 0 {
		return nil
	}

	var movie map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, fmt.Sprintf("/movie/%d", movieID), nil, nil, &movie); err != nil {
		return fmt.Errorf("remove tag get movie %d: %w", movieID, err)
	}

	before := toIntSlice(movie["tags"])
	after := make([]int, 0, len(before))
	for _, id := range before {
		if id != wantID {
			after = append(after, id)
		}
	}
	if len(after) == len(before) {
		return nil
	}

	movie["tags"] = after
	if err := apiJSON(radarrURL, radarrKey, http.MethodPut, fmt.Sprintf("/movie/%d", movieID), movie, nil, nil); err != nil {
		return fmt.Errorf("remove tag put movie %d: %w", movieID, err)
	}
	slog.Info("Removed Radarr tag", "label", label, "title", title)
	return nil
}

// SetRootFolder moves a movie to a different root folder via /movie/editor.
func SetRootFolder(radarrURL, radarrKey string, movieID int, newRoot string, moveFiles bool, title string) error {
	newRoot = strings.TrimSpace(newRoot)
	if newRoot == "" {
		return fmt.Errorf("root folder path is required")
	}

	body := map[string]any{
		"movieIds":       []int{movieID},
		"rootFolderPath": newRoot,
		"moveFiles":      moveFiles,
	}
	if err := apiJSON(radarrURL, radarrKey, http.MethodPut, "/movie/editor", body, nil, nil); err != nil {
		return fmt.Errorf("set root folder movie %d: %w", movieID, err)
	}
	slog.Info("Moved Radarr movie", "title", title, "root", newRoot, "moveFiles", moveFiles)
	return nil
}

// AddMovie looks up a movie by TMDB ID and adds it to Radarr.
func AddMovie(radarrURL, radarrKey string, tmdbID int, title string, qualityProfileID int, rootFolder string, monitored, searchOnAdd bool, tags []int) (map[string]any, error) {
	var lookup any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/movie/lookup/tmdb", nil,
		map[string]string{"tmdbId": strconv.Itoa(tmdbID)}, &lookup); err != nil {
		return nil, fmt.Errorf("movie lookup tmdb %d: %w", tmdbID, err)
	}

	var payload map[string]any
	switch v := lookup.(type) {
	case []any:
		if len(v) == 0 {
			return nil, fmt.Errorf("TMDB id %d not found on Radarr", tmdbID)
		}
		var ok bool
		payload, ok = v[0].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected lookup result type")
		}
	case map[string]any:
		payload = v
	default:
		return nil, fmt.Errorf("unexpected lookup result type")
	}

	payload["qualityProfileId"] = qualityProfileID
	payload["rootFolderPath"] = rootFolder
	payload["monitored"] = monitored
	if _, ok := payload["minimumAvailability"]; !ok {
		payload["minimumAvailability"] = "released"
	}
	if tags == nil {
		tags = []int{}
	}
	payload["tags"] = tags
	payload["addOptions"] = map[string]any{
		"searchForMovie": searchOnAdd,
		"monitor":        "movieOnly",
	}

	var created map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodPost, "/movie", payload, nil, &created); err != nil {
		return nil, fmt.Errorf("add movie tmdb %d: %w", tmdbID, err)
	}
	slog.Info("Added movie to Radarr", "title", title, "tmdbId", tmdbID, "id", created["id"])
	return created, nil
}

// ListQualityProfiles returns [{id, name}, ...] from /qualityprofile.
func ListQualityProfiles(radarrURL, radarrKey string) ([]map[string]any, error) {
	if radarrURL == "" || radarrKey == "" {
		return nil, nil
	}

	var profiles []map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/qualityprofile", nil, nil, &profiles); err != nil {
		slog.Warn("Radarr list_quality_profiles failed", "error", err)
		return nil, nil
	}

	result := make([]map[string]any, 0, len(profiles))
	for _, p := range profiles {
		result = append(result, map[string]any{
			"id":   p["id"],
			"name": p["name"],
		})
	}
	return result, nil
}

// GetQualityProfileID finds a quality profile ID by name (case insensitive).
// Returns the ID and true if found, or 0 and false otherwise.
func GetQualityProfileID(radarrURL, radarrKey, name string) (int, bool) {
	if name == "" {
		return 0, false
	}
	target := strings.TrimSpace(strings.ToLower(name))
	profiles, _ := ListQualityProfiles(radarrURL, radarrKey)
	for _, p := range profiles {
		pName, _ := p["name"].(string)
		if strings.TrimSpace(strings.ToLower(pName)) == target {
			id := toInt(p["id"])
			if id != 0 {
				return id, true
			}
		}
	}
	return 0, false
}

// RecycleBinPath returns Radarr's configured recycle bin path, or empty string on failure.
func RecycleBinPath(radarrURL, radarrKey string) string {
	if radarrURL == "" || radarrKey == "" {
		return ""
	}

	var config map[string]any
	if err := apiJSON(radarrURL, radarrKey, http.MethodGet, "/config/mediamanagement", nil, nil, &config); err != nil {
		slog.Warn("Radarr recycle_bin_path lookup failed", "error", err)
		return ""
	}

	path, _ := config["recycleBin"].(string)
	return strings.TrimSpace(path)
}

// toInt converts a JSON number (float64) or other numeric value to int.
func toInt(v any) int {
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
	default:
		return 0
	}
}

// toIntSlice converts a JSON array of numbers to []int.
func toIntSlice(v any) []int {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]int, 0, len(arr))
	for _, item := range arr {
		result = append(result, toInt(item))
	}
	return result
}
