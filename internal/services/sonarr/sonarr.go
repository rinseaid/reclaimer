// Package sonarr provides helpers for the Sonarr v3 API.
package sonarr

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

// apiReq performs an HTTP request against the Sonarr API.
func apiReq(sonarrURL, sonarrKey, method, path string, body any, params map[string]string) (*http.Response, error) {
	url := strings.TrimRight(sonarrURL, "/") + "/api/v3" + path

	headers := map[string]string{
		"X-Api-Key": sonarrKey,
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
func apiJSON(sonarrURL, sonarrKey, method, path string, body any, params map[string]string, dst any) error {
	resp, err := apiReq(sonarrURL, sonarrKey, method, path, body, params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sonarr %s %s: HTTP %d: %s", method, path, resp.StatusCode, string(b))
	}

	if dst != nil {
		return json.NewDecoder(resp.Body).Decode(dst)
	}
	return nil
}

// FetchShows returns {tvdbId -> show} with a "_tag_names" list attached to each show.
func FetchShows(sonarrURL, sonarrKey string) (map[int]map[string]any, error) {
	var shows []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/series", nil, nil, &shows); err != nil {
		return nil, fmt.Errorf("fetch shows: %w", err)
	}

	var tags []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return nil, fmt.Errorf("fetch tags: %w", err)
	}

	tagMap := make(map[int]string, len(tags))
	for _, t := range tags {
		id := toInt(t["id"])
		label, _ := t["label"].(string)
		tagMap[id] = label
	}

	result := make(map[int]map[string]any, len(shows))
	for _, s := range shows {
		tvdbID := toInt(s["tvdbId"])
		tagIDs := toIntSlice(s["tags"])
		tagNames := make([]string, 0, len(tagIDs))
		for _, tid := range tagIDs {
			if name, ok := tagMap[tid]; ok {
				tagNames = append(tagNames, name)
			} else {
				tagNames = append(tagNames, strconv.Itoa(tid))
			}
		}
		s["_tag_names"] = tagNames
		result[tvdbID] = s
	}

	slog.Info("Sonarr: fetched shows", "count", len(result))
	return result, nil
}

// FetchShowByTvdb fetches a single show by TVDB ID. Returns {tvdbId -> show} with _tag_names.
func FetchShowByTvdb(sonarrURL, sonarrKey string, tvdbID int) (map[int]map[string]any, error) {
	if sonarrURL == "" || sonarrKey == "" || tvdbID == 0 {
		return map[int]map[string]any{}, nil
	}

	var shows []map[string]any
	err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/series", nil,
		map[string]string{"tvdbId": strconv.Itoa(tvdbID)}, &shows)
	if err != nil {
		return map[int]map[string]any{}, nil
	}
	if len(shows) == 0 {
		return map[int]map[string]any{}, nil
	}

	var tags []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return map[int]map[string]any{}, nil
	}

	tagMap := make(map[int]string, len(tags))
	for _, t := range tags {
		tagMap[toInt(t["id"])], _ = t["label"].(string)
	}

	result := make(map[int]map[string]any, len(shows))
	for _, s := range shows {
		tid := toInt(s["tvdbId"])
		tagIDs := toIntSlice(s["tags"])
		tagNames := make([]string, 0, len(tagIDs))
		for _, id := range tagIDs {
			if name, ok := tagMap[id]; ok {
				tagNames = append(tagNames, name)
			} else {
				tagNames = append(tagNames, strconv.Itoa(id))
			}
		}
		s["_tag_names"] = tagNames
		result[tid] = s
	}
	return result, nil
}

// Unmonitor sets monitored=false for a series.
func Unmonitor(sonarrURL, sonarrKey string, seriesID int, title string) error {
	var show map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, fmt.Sprintf("/series/%d", seriesID), nil, nil, &show); err != nil {
		return fmt.Errorf("unmonitor get series %d: %w", seriesID, err)
	}
	show["monitored"] = false
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPut, fmt.Sprintf("/series/%d", seriesID), show, nil, nil); err != nil {
		return fmt.Errorf("unmonitor put series %d: %w", seriesID, err)
	}
	slog.Info("Unmonitored in Sonarr", "title", title)
	return nil
}

// Delete removes a series from Sonarr.
func Delete(sonarrURL, sonarrKey string, seriesID int, title string, deleteFiles bool) error {
	params := map[string]string{
		"deleteFiles": strconv.FormatBool(deleteFiles),
	}
	resp, err := apiReq(sonarrURL, sonarrKey, http.MethodDelete, fmt.Sprintf("/series/%d", seriesID), nil, params)
	if err != nil {
		return fmt.Errorf("delete series %d: %w", seriesID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete series %d: HTTP %d: %s", seriesID, resp.StatusCode, string(b))
	}
	slog.Info("Deleted from Sonarr", "title", title, "deleteFiles", deleteFiles)
	return nil
}

// UnmonitorSeason sets monitored=false for a single season without affecting others.
func UnmonitorSeason(sonarrURL, sonarrKey string, seriesID, seasonNumber int, title string) error {
	var show map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, fmt.Sprintf("/series/%d", seriesID), nil, nil, &show); err != nil {
		return fmt.Errorf("unmonitor season get series %d: %w", seriesID, err)
	}

	seasons, _ := show["seasons"].([]any)
	for _, s := range seasons {
		season, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if toInt(season["seasonNumber"]) == seasonNumber {
			season["monitored"] = false
			break
		}
	}

	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPut, fmt.Sprintf("/series/%d", seriesID), show, nil, nil); err != nil {
		return fmt.Errorf("unmonitor season put series %d: %w", seriesID, err)
	}
	slog.Info("Unmonitored season in Sonarr", "season", seasonNumber, "title", title)
	return nil
}

// DeleteSeasonFiles deletes all episode files for a specific season.
func DeleteSeasonFiles(sonarrURL, sonarrKey string, seriesID, seasonNumber int, title string) error {
	var files []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/episodefile", nil,
		map[string]string{"seriesId": strconv.Itoa(seriesID)}, &files); err != nil {
		return fmt.Errorf("delete season files fetch %d: %w", seriesID, err)
	}

	var seasonFiles []map[string]any
	for _, f := range files {
		if toInt(f["seasonNumber"]) == seasonNumber {
			seasonFiles = append(seasonFiles, f)
		}
	}

	deleted := 0
	for _, ef := range seasonFiles {
		fileID := toInt(ef["id"])
		resp, err := apiReq(sonarrURL, sonarrKey, http.MethodDelete, fmt.Sprintf("/episodefile/%d", fileID), nil, nil)
		if err != nil {
			slog.Warn("Failed to delete episode file", "fileId", fileID, "title", title, "season", seasonNumber, "error", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			slog.Warn("Failed to delete episode file", "fileId", fileID, "title", title, "season", seasonNumber, "status", resp.StatusCode)
			continue
		}
		deleted++
	}

	slog.Info("Deleted season files", "deleted", deleted, "total", len(seasonFiles), "title", title, "season", seasonNumber)
	return nil
}

// SearchSeason triggers a SeasonSearch command.
func SearchSeason(sonarrURL, sonarrKey string, seriesID, seasonNumber int) error {
	body := map[string]any{
		"name":         "SeasonSearch",
		"seriesId":     seriesID,
		"seasonNumber": seasonNumber,
	}
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPost, "/command", body, nil, nil); err != nil {
		return fmt.Errorf("search season: %w", err)
	}
	return nil
}

// SearchSeries triggers a SeriesSearch command.
func SearchSeries(sonarrURL, sonarrKey string, seriesID int) error {
	body := map[string]any{
		"name":     "SeriesSearch",
		"seriesId": seriesID,
	}
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPost, "/command", body, nil, nil); err != nil {
		return fmt.Errorf("search series: %w", err)
	}
	return nil
}

// EnsureTagID returns the ID of a tag with the given label, creating it if needed.
func EnsureTagID(sonarrURL, sonarrKey, label string) (int, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return 0, fmt.Errorf("empty tag label")
	}

	var tags []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
		return 0, fmt.Errorf("fetch tags: %w", err)
	}

	for _, t := range tags {
		tLabel, _ := t["label"].(string)
		if strings.EqualFold(strings.TrimSpace(tLabel), label) {
			return toInt(t["id"]), nil
		}
	}

	var created map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPost, "/tag", map[string]string{"label": label}, nil, &created); err != nil {
		return 0, fmt.Errorf("create tag %q: %w", label, err)
	}
	return toInt(created["id"]), nil
}

// AddTag adds a tag to a series by label, creating the tag if missing. Idempotent.
func AddTag(sonarrURL, sonarrKey string, seriesID int, label, title string) error {
	tagID, err := EnsureTagID(sonarrURL, sonarrKey, label)
	if err != nil {
		return err
	}

	var show map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, fmt.Sprintf("/series/%d", seriesID), nil, nil, &show); err != nil {
		return fmt.Errorf("add tag get series %d: %w", seriesID, err)
	}

	tagIDs := toIntSlice(show["tags"])
	for _, id := range tagIDs {
		if id == tagID {
			return nil // already present
		}
	}

	tagIDs = append(tagIDs, tagID)
	show["tags"] = tagIDs
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPut, fmt.Sprintf("/series/%d", seriesID), show, nil, nil); err != nil {
		return fmt.Errorf("add tag put series %d: %w", seriesID, err)
	}
	slog.Info("Added Sonarr tag", "label", label, "title", title)
	return nil
}

// RemoveTag removes a tag from a series by label. No-op if not present.
func RemoveTag(sonarrURL, sonarrKey string, seriesID int, label, title string) error {
	label = strings.TrimSpace(label)
	if label == "" {
		return nil
	}

	var tags []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/tag", nil, nil, &tags); err != nil {
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

	var show map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, fmt.Sprintf("/series/%d", seriesID), nil, nil, &show); err != nil {
		return fmt.Errorf("remove tag get series %d: %w", seriesID, err)
	}

	before := toIntSlice(show["tags"])
	after := make([]int, 0, len(before))
	for _, id := range before {
		if id != wantID {
			after = append(after, id)
		}
	}
	if len(after) == len(before) {
		return nil
	}

	show["tags"] = after
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPut, fmt.Sprintf("/series/%d", seriesID), show, nil, nil); err != nil {
		return fmt.Errorf("remove tag put series %d: %w", seriesID, err)
	}
	slog.Info("Removed Sonarr tag", "label", label, "title", title)
	return nil
}

// SetRootFolder moves a series to a different root folder via /series/editor.
func SetRootFolder(sonarrURL, sonarrKey string, seriesID int, newRoot string, moveFiles bool, title string) error {
	newRoot = strings.TrimSpace(newRoot)
	if newRoot == "" {
		return fmt.Errorf("root folder path is required")
	}

	body := map[string]any{
		"seriesIds":      []int{seriesID},
		"rootFolderPath": newRoot,
		"moveFiles":      moveFiles,
	}
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPut, "/series/editor", body, nil, nil); err != nil {
		return fmt.Errorf("set root folder series %d: %w", seriesID, err)
	}
	slog.Info("Moved Sonarr series", "title", title, "root", newRoot, "moveFiles", moveFiles)
	return nil
}

// AddSeries looks up a series by TVDB ID and adds it to Sonarr.
func AddSeries(sonarrURL, sonarrKey string, tvdbID int, title string, qualityProfileID int, rootFolder string, monitored, searchOnAdd bool, tags []int) (map[string]any, error) {
	var lookup []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/series/lookup", nil,
		map[string]string{"term": fmt.Sprintf("tvdb:%d", tvdbID)}, &lookup); err != nil {
		return nil, fmt.Errorf("series lookup tvdb %d: %w", tvdbID, err)
	}
	if len(lookup) == 0 {
		return nil, fmt.Errorf("TVDB id %d not found on Sonarr", tvdbID)
	}

	payload := lookup[0]
	payload["qualityProfileId"] = qualityProfileID
	payload["rootFolderPath"] = rootFolder
	payload["monitored"] = monitored
	if _, ok := payload["seasonFolder"]; !ok {
		payload["seasonFolder"] = true
	}
	if tags == nil {
		tags = []int{}
	}
	payload["tags"] = tags
	payload["addOptions"] = map[string]any{
		"searchForMissingEpisodes":       searchOnAdd,
		"searchForCutoffUnmetEpisodes":   false,
		"monitor":                        "all",
	}

	var created map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodPost, "/series", payload, nil, &created); err != nil {
		return nil, fmt.Errorf("add series tvdb %d: %w", tvdbID, err)
	}
	slog.Info("Added series to Sonarr", "title", title, "tvdbId", tvdbID, "id", created["id"])
	return created, nil
}

// ListQualityProfiles returns [{id, name}, ...] from /qualityprofile.
func ListQualityProfiles(sonarrURL, sonarrKey string) ([]map[string]any, error) {
	if sonarrURL == "" || sonarrKey == "" {
		return nil, nil
	}

	var profiles []map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/qualityprofile", nil, nil, &profiles); err != nil {
		slog.Warn("Sonarr list_quality_profiles failed", "error", err)
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
func GetQualityProfileID(sonarrURL, sonarrKey, name string) (int, bool) {
	if name == "" {
		return 0, false
	}
	target := strings.TrimSpace(strings.ToLower(name))
	profiles, _ := ListQualityProfiles(sonarrURL, sonarrKey)
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

// RecycleBinPath returns Sonarr's configured recycle bin path, or empty string on failure.
func RecycleBinPath(sonarrURL, sonarrKey string) string {
	if sonarrURL == "" || sonarrKey == "" {
		return ""
	}

	var config map[string]any
	if err := apiJSON(sonarrURL, sonarrKey, http.MethodGet, "/config/mediamanagement", nil, nil, &config); err != nil {
		slog.Warn("Sonarr recycle_bin_path lookup failed", "error", err)
		return ""
	}

	path, _ := config["recycleBin"].(string)
	return strings.TrimSpace(path)
}

// BuildSeasonCounts computes {series_id -> highest seasonNumber with files}. Skips season 0.
func BuildSeasonCounts(shows map[int]map[string]any) map[int]int {
	counts := make(map[int]int)
	for _, show := range shows {
		seriesID := toInt(show["id"])
		if seriesID == 0 {
			continue
		}

		highest := -1
		seasons, _ := show["seasons"].([]any)
		for _, s := range seasons {
			season, ok := s.(map[string]any)
			if !ok {
				continue
			}
			seasonNum := toInt(season["seasonNumber"])
			if seasonNum <= 0 {
				continue
			}

			stats, _ := season["statistics"].(map[string]any)
			if stats == nil {
				continue
			}
			fileCount := toInt(stats["episodeFileCount"])
			if fileCount > 0 && seasonNum > highest {
				highest = seasonNum
			}
		}

		if highest > 0 {
			counts[seriesID] = highest
		}
	}
	return counts
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
