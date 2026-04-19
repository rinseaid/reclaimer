"""Ratings extraction from Plex/Jellyfin metadata."""
from __future__ import annotations

import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


def _safe_float(x: Any) -> Optional[float]:
    """Parse ``x`` to a float, returning None for missing/unparseable input.

    Rating fields in Plex/Jellyfin are optional and sometimes arrive as
    strings, None, or obviously-invalid values. Silent failure is intentional
    here -- these are informational; a missing rating should not raise.
    """
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def extract_plex_ratings(item: dict) -> dict:
    """Extract ratings from a Plex metadata item.

    Plex stores:
    - `rating` → Rotten Tomatoes critic score (0-10 scale, multiply by 10 for %)
    - `audienceRating` → Community/IMDb-style rating (0-10 scale)
    - `contentRating` → Age rating (PG-13, R, etc.) - not a quality score

    Returns {"critic_rating": int|None, "audience_rating": float|None}.
    critic_rating is RT percentage (0-100), audience_rating is 0-10 scale.
    """
    result: dict = {"critic_rating": None, "audience_rating": None}

    # Plex's `rating` is the critic score (RT) on a 0-10 scale
    plex_rating = _safe_float(item.get("rating"))
    if plex_rating is not None:
        result["critic_rating"] = round(plex_rating * 10)  # Convert to 0-100%

    # Plex's `audienceRating` is the audience/community score on a 0-10 scale
    audience = _safe_float(item.get("audienceRating"))
    if audience is not None:
        result["audience_rating"] = round(audience, 1)

    return result


def extract_jellyfin_ratings(item: dict) -> dict:
    """Extract ratings from a Jellyfin metadata item.

    Jellyfin stores:
    - `CriticRating` → Rotten Tomatoes critic score (0-100%)
    - `CommunityRating` → Community/IMDb-style rating (0-10 scale)

    Returns {"critic_rating": int|None, "audience_rating": float|None}.
    """
    result: dict = {"critic_rating": None, "audience_rating": None}

    critic = _safe_float(item.get("CriticRating"))
    if critic is not None:
        result["critic_rating"] = int(critic)

    community = _safe_float(item.get("CommunityRating"))
    if community is not None:
        result["audience_rating"] = round(community, 1)

    return result


def extract_ratings(item: dict, source: str = "plex") -> dict:
    """Extract ratings from a media server item.

    Args:
        item: Plex or Jellyfin item dict
        source: "plex" or "jellyfin"

    Returns {"critic_rating": int|None, "audience_rating": float|None}
    """
    if source == "jellyfin":
        return extract_jellyfin_ratings(item)
    return extract_plex_ratings(item)
