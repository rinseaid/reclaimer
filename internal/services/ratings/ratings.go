// Package ratings extracts critic and audience ratings from Plex and Jellyfin
// item metadata.
package ratings

import (
	"log/slog"
	"math"

	"github.com/rinseaid/reclaimer/internal/models"
)

// safeFloat parses v to a float64, returning nil for missing or invalid values.
func safeFloat(v any) *float64 {
	if v == nil {
		return nil
	}
	switch n := v.(type) {
	case float64:
		return &n
	case float32:
		f := float64(n)
		return &f
	case int:
		f := float64(n)
		return &f
	case int64:
		f := float64(n)
		return &f
	case string:
		// Ratings should be numeric; log and skip strings.
		return nil
	}
	return nil
}

// ExtractPlexRatings extracts ratings from a Plex metadata item.
//
// Plex stores:
//   - rating: Rotten Tomatoes critic score on a 0-10 scale (multiply by 10 for %)
//   - audienceRating: community / IMDb-style rating on a 0-10 scale
func ExtractPlexRatings(item map[string]any) models.Ratings {
	var r models.Ratings

	if pf := safeFloat(item["rating"]); pf != nil {
		v := int(math.Round(*pf * 10)) // convert 0-10 -> 0-100%
		r.CriticRating = &v
	}

	if af := safeFloat(item["audienceRating"]); af != nil {
		v := math.Round(*af*10) / 10 // one decimal place
		r.AudienceRating = &v
	}

	return r
}

// ExtractJellyfinRatings extracts ratings from a Jellyfin metadata item.
//
// Jellyfin stores:
//   - CriticRating: Rotten Tomatoes critic score already on a 0-100% scale
//   - CommunityRating: community / IMDb-style rating on a 0-10 scale
func ExtractJellyfinRatings(item map[string]any) models.Ratings {
	var r models.Ratings

	if cf := safeFloat(item["CriticRating"]); cf != nil {
		v := int(*cf)
		r.CriticRating = &v
	}

	if af := safeFloat(item["CommunityRating"]); af != nil {
		v := math.Round(*af*10) / 10 // one decimal place
		r.AudienceRating = &v
	}

	return r
}

// ExtractRatings dispatches to the appropriate extractor based on source
// ("plex" or "jellyfin").
func ExtractRatings(item map[string]any, source string) models.Ratings {
	switch source {
	case "jellyfin":
		return ExtractJellyfinRatings(item)
	case "plex":
		return ExtractPlexRatings(item)
	default:
		slog.Warn("Unknown rating source, defaulting to plex", "source", source)
		return ExtractPlexRatings(item)
	}
}
