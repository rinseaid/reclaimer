"""Criteria schema - defines the per-collection rule configuration stored in collection_config.criteria."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional


# --- Individual rule configs ---

@dataclass
class NeverWatchedRule:
    enabled: bool = True
    min_days_unwatched: int = 0
    check_plex_views: bool = True
    # When True, also consult watch_history (native Plex/Jellyfin session
    # history ingested into our DB) in addition to Plex viewCount.
    check_db_plays: bool = True
    exclude_users: list[str] = field(default_factory=list)


@dataclass
class NoKeepTagRule:
    enabled: bool = True
    tag_name: str = "keep"
    protected_tags: list[str] = field(default_factory=list)  # multi-tag support


@dataclass
class NoActiveRequestRule:
    enabled: bool = True


@dataclass
class NoProtectedRequestRule:
    enabled: bool = True


@dataclass
class NotInKeepCollectionRule:
    enabled: bool = True
    collection_name: str = ""  # populated from settings at runtime if empty
    protected_collections: list[str] = field(default_factory=list)  # multi-collection support


@dataclass
class HighlyRatedRule:
    enabled: bool = False
    imdb_min: float = 0.0
    rt_min: int = 0
    metacritic_min: int = 0
    require_all: bool = False


@dataclass
class ShowEndedRule:
    enabled: bool = False
    include_deleted: bool = True  # count "deleted" status as ended


# --- New criteria rules (inclusion/exclusion) ---

@dataclass
class LowRatingRule:
    """Flag as candidate when ratings fall below thresholds.

    Mirrors HighlyRatedRule but inverted: we want LOW-rated content to be
    swept up as candidates. audience/critic use the same scales as
    HighlyRatedRule (audience 0-10, critic 0-100).
    """
    enabled: bool = False
    imdb_max: float = 0.0  # audience 0-10; 0 = disabled
    critic_max: int = 0    # critic 0-100; 0 = disabled
    require_all: bool = False  # all configured thresholds must be below


@dataclass
class FileSizeMinRule:
    """Flag as candidate when the on-disk size is at least min_gb."""
    enabled: bool = False
    min_gb: float = 0.0  # 0 = disabled


@dataclass
class ReleaseYearBeforeRule:
    """Flag as candidate when release year is strictly before year."""
    enabled: bool = False
    year: int = 0  # 0 = disabled


@dataclass
class WatchRatioLowRule:
    """Flag as candidate when historical plays all left the item unfinished.

    If the MAX ``percent_complete`` across all plays is ``<= max_percent``
    AND the last-played timestamp is older than ``days`` days, the item is a
    candidate. ``days = 0`` disables the recency gate (any unfinished play,
    however recent, counts).

    ``percent_complete`` is derived directly from the media server's
    last-known playback offset (Plex viewOffset / Jellyfin
    PlaybackPositionTicks) -- the media server is authoritative for
    "did this finish?".

    When no plays exist, ``require_plays`` controls behaviour:
        True  -> skip the rule (no data to judge, pass non-blocking),
        False -> treat as "never attempted" (pass, candidate).
    """
    enabled: bool = False
    max_percent: int = 30
    require_plays: bool = True
    days: int = 0  # minimum days since last play; 0 = disabled (no recency check)


# --- New protection rules (firing them blocks candidacy) ---

@dataclass
class RecentlyAddedRule:
    """Protect items added to the library within the last N days."""
    enabled: bool = False
    days: int = 14  # 0 = disabled


@dataclass
class PartiallyWatchedRule:
    """Protect items with an incomplete play within last N days."""
    enabled: bool = False
    days: int = 30  # 0 = disabled


@dataclass
class OnWatchlistRule:
    """Protect items on any Overseerr user's watchlist."""
    enabled: bool = False


@dataclass
class PlexFavoritedRule:
    """Protect items the Plex admin has hearted (set userRating)."""
    enabled: bool = False


@dataclass
class OldSeasonRule:
    """Season-only: flag seasons older than (max_season - keep_last + 1)."""
    enabled: bool = False
    keep_last: int = 2  # 0 = disabled


@dataclass
class SeriesProtectionRule:
    """Season-only: protect seasons whose parent show is itself protected."""
    enabled: bool = False


@dataclass
class NotWatchedRecentlyRule:
    """Flag as candidate when last-watched date is older than ``days``."""
    enabled: bool = False
    days: int = 90


@dataclass
class RequestFulfilledRule:
    """Flag as candidate once the requesting user has watched the item."""
    enabled: bool = False


@dataclass
class AvailableOnDebridRule:
    """Flag as candidate only when the item is confirmed cached on debrid."""
    enabled: bool = False


@dataclass
class OldContentRule:
    """Flag as candidate when ``addedAt`` is older than ``days`` days."""
    enabled: bool = False
    days: int = 180


# --- Top-level criteria ---

@dataclass
class CollectionCriteria:
    """Full criteria configuration for a single collection."""
    rules: dict[str, Any] = field(default_factory=dict)
    action: str = "none"
    grace_days: int = 30
    delete_files: bool = False
    add_import_exclusion: bool = True
    action_pipeline: list = field(default_factory=list)  # [{type: "script"|"unmonitor"|"delete", command: "..."}]
    protected_users: list[str] = field(default_factory=list)
    protected_tags: list[str] = field(default_factory=list)
    protected_collections: list[str] = field(default_factory=list)
    watchlist_protected_users: list[str] = field(default_factory=list)
    library_section_id: Optional[int | str] = None
    library_source: str = "plex"  # "plex" or "jellyfin"
    granularity: str = "show"  # "show" or "season" - season evaluates individual seasons

    # Parsed rule objects (not serialised)
    _never_watched: Optional[NeverWatchedRule] = field(default=None, repr=False)
    _no_keep_tag: Optional[NoKeepTagRule] = field(default=None, repr=False)
    _no_active_request: Optional[NoActiveRequestRule] = field(default=None, repr=False)
    _no_protected_request: Optional[NoProtectedRequestRule] = field(default=None, repr=False)
    _not_in_keep_collection: Optional[NotInKeepCollectionRule] = field(default=None, repr=False)
    _show_ended: Optional[ShowEndedRule] = field(default=None, repr=False)
    _highly_rated: Optional[HighlyRatedRule] = field(default=None, repr=False)
    _low_rating: Optional[LowRatingRule] = field(default=None, repr=False)
    _file_size_min: Optional[FileSizeMinRule] = field(default=None, repr=False)
    _release_year_before: Optional[ReleaseYearBeforeRule] = field(default=None, repr=False)
    _watch_ratio_low: Optional[WatchRatioLowRule] = field(default=None, repr=False)
    _old_season: Optional[OldSeasonRule] = field(default=None, repr=False)
    _recently_added: Optional[RecentlyAddedRule] = field(default=None, repr=False)
    _partially_watched: Optional[PartiallyWatchedRule] = field(default=None, repr=False)
    _on_watchlist: Optional[OnWatchlistRule] = field(default=None, repr=False)
    _plex_favorited: Optional[PlexFavoritedRule] = field(default=None, repr=False)
    _series_protection: Optional[SeriesProtectionRule] = field(default=None, repr=False)
    _not_watched_recently: Optional[NotWatchedRecentlyRule] = field(default=None, repr=False)
    _request_fulfilled: Optional[RequestFulfilledRule] = field(default=None, repr=False)
    _available_on_debrid: Optional[AvailableOnDebridRule] = field(default=None, repr=False)
    _old_content: Optional[OldContentRule] = field(default=None, repr=False)

    def __post_init__(self):
        self._parse_rules()

    def _parse_rules(self):
        """Hydrate typed rule objects from the raw rules dict."""
        def _get(name, cls):
            raw = self.rules.get(name, {})
            if isinstance(raw, dict):
                return cls(**{k: v for k, v in raw.items() if k in cls.__dataclass_fields__})
            return cls()

        self._never_watched = _get("never_watched", NeverWatchedRule)
        self._no_keep_tag = _get("no_keep_tag", NoKeepTagRule)
        self._no_active_request = _get("no_active_request", NoActiveRequestRule)
        self._no_protected_request = _get("no_protected_request", NoProtectedRequestRule)
        self._not_in_keep_collection = _get("not_in_keep_collection", NotInKeepCollectionRule)
        self._show_ended = _get("show_ended", ShowEndedRule)
        self._highly_rated = _get("highly_rated", HighlyRatedRule)
        self._low_rating = _get("low_rating", LowRatingRule)
        self._file_size_min = _get("file_size_min", FileSizeMinRule)
        self._release_year_before = _get("release_year_before", ReleaseYearBeforeRule)
        self._watch_ratio_low = _get("watch_ratio_low", WatchRatioLowRule)
        self._old_season = _get("old_season", OldSeasonRule)
        self._recently_added = _get("recently_added", RecentlyAddedRule)
        self._partially_watched = _get("partially_watched", PartiallyWatchedRule)
        self._on_watchlist = _get("on_watchlist", OnWatchlistRule)
        self._plex_favorited = _get("plex_favorited", PlexFavoritedRule)
        self._series_protection = _get("series_protection", SeriesProtectionRule)
        self._not_watched_recently = _get("not_watched_recently", NotWatchedRecentlyRule)
        self._request_fulfilled = _get("request_fulfilled", RequestFulfilledRule)
        self._available_on_debrid = _get("available_on_debrid", AvailableOnDebridRule)
        self._old_content = _get("old_content", OldContentRule)

    # Convenience accessors for the two rules whose day thresholds the
    # engine reads on every evaluation. Default values apply when the rule
    # object is missing or the configured value is zero.
    @property
    def not_watched_recently_days(self) -> int:
        return int((self._not_watched_recently.days
                    if self._not_watched_recently else 90) or 90)

    @property
    def old_content_days(self) -> int:
        return int((self._old_content.days if self._old_content else 180) or 180)

    def is_rule_enabled(self, name: str) -> bool:
        """Check whether a named rule is enabled in this criteria."""
        rule_obj = getattr(self, f"_{name}", None)
        if rule_obj is None:
            return False
        return getattr(rule_obj, "enabled", False)

    def to_dict(self) -> dict:
        """Serialise to a dict suitable for JSON storage."""
        return {
            "rules": dict(self.rules),
            "action": self.action,
            "grace_days": self.grace_days,
            "delete_files": self.delete_files,
            "add_import_exclusion": self.add_import_exclusion,
            "action_pipeline": self.action_pipeline,
            "protected_users": self.protected_users,
            "protected_tags": self.protected_tags,
            "protected_collections": self.protected_collections,
            "watchlist_protected_users": self.watchlist_protected_users,
            "library_section_id": self.library_section_id,
            "library_source": self.library_source,
            "granularity": self.granularity,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, raw: str | None) -> "CollectionCriteria":
        """Parse criteria from the JSON string stored in collection_config.criteria."""
        if not raw:
            return cls()
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return cls()
        if not isinstance(data, dict):
            return cls()

        return cls(
            rules=dict(data.get("rules") or {}),
            action=data.get("action", "none"),
            grace_days=data.get("grace_days", 30),
            delete_files=data.get("delete_files", False),
            add_import_exclusion=data.get("add_import_exclusion", True),
            action_pipeline=data.get("action_pipeline", []),
            protected_users=data.get("protected_users", []),
            protected_tags=data.get("protected_tags", []),
            protected_collections=data.get("protected_collections", []),
            watchlist_protected_users=data.get("watchlist_protected_users", []),
            library_section_id=data.get("library_section_id"),
            library_source=data.get("library_source", "plex"),
            granularity=data.get("granularity", "show"),
        )


# --- Default criteria for the 3 seed collections ---

DEFAULT_MOVIES_LEAVING_CRITERIA: dict = {
    "rules": {
        "never_watched": {
            "enabled": True,
            "min_days_unwatched": 0,
            "check_plex_views": True,
            "check_db_plays": True,
            "exclude_users": [],
        },
        "no_keep_tag": {
            "enabled": True,
        },
        "no_active_request": {
            "enabled": True,
        },
        "no_protected_request": {
            "enabled": True,
        },
        "not_in_keep_collection": {
            "enabled": True,
        },
        "show_ended": {
            "enabled": False,
        },
    },
    "action": "none",
    "grace_days": 30,
    "action_pipeline": [],
}

DEFAULT_TV_LEAVING_CRITERIA: dict = {
    "rules": {
        "never_watched": {
            "enabled": True,
            "min_days_unwatched": 0,
            "check_plex_views": True,
            "check_db_plays": True,
            "exclude_users": [],
        },
        "no_keep_tag": {
            "enabled": True,
        },
        "no_active_request": {
            "enabled": True,
        },
        "no_protected_request": {
            "enabled": True,
        },
        "not_in_keep_collection": {
            "enabled": True,
        },
        "show_ended": {
            "enabled": False,
        },
    },
    "action": "none",
    "grace_days": 30,
    "action_pipeline": [],
}

DEFAULT_ENDED_DORMANT_CRITERIA: dict = {
    "rules": {
        "never_watched": {
            "enabled": True,
            "min_days_unwatched": 0,
            "check_plex_views": True,
            "check_db_plays": True,
            "exclude_users": [],
        },
        "no_keep_tag": {
            "enabled": True,
        },
        "no_active_request": {
            "enabled": True,
        },
        "no_protected_request": {
            "enabled": True,
        },
        "not_in_keep_collection": {
            "enabled": True,
        },
        "show_ended": {
            "enabled": True,
            "include_deleted": True,
        },
    },
    "action": "none",
    "grace_days": 30,
    "action_pipeline": [],
}
