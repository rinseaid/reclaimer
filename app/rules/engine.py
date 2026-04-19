"""Rule engine for evaluating why content is a candidate for collection/action."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .criteria import CollectionCriteria


@dataclass
class RuleResult:
    name: str
    passed: bool  # True = this rule says "include in collection"
    detail: str
    severity: str = "info"  # info, warning, blocking


@dataclass
class EvaluationContext:
    """Bundle of pre-fetched data for rule evaluation."""
    # Rating_key -> play count, aggregated from watch_history (Plex session
    # history + Jellyfin per-user queries ingested by the orchestrator).
    # Exposed also as ``tau_plays`` via an init kwarg alias + property for
    # backwards compatibility with older call sites.
    play_counts: dict[str, int] = field(default_factory=dict)
    radarr_movies: dict[int, dict] = field(default_factory=dict)
    sonarr_shows: dict[int, dict] = field(default_factory=dict)
    overseerr_active_movies: set[int] = field(default_factory=set)
    overseerr_active_shows: set[int] = field(default_factory=set)
    overseerr_active_shows_tmdb: set[int] = field(default_factory=set)
    overseerr_protected_movies: set[int] = field(default_factory=set)
    overseerr_protected_shows: set[int] = field(default_factory=set)
    overseerr_protected_shows_tmdb: set[int] = field(default_factory=set)
    plex_keep_keys: set[str] = field(default_factory=set)
    debrid_cached: dict[str, bool] = field(default_factory=dict)  # rating_key -> cached
    db_plays: dict[str, int] = field(default_factory=dict)  # rating_key -> play count from watch_history
    db_plays_by_title: dict[str, int] = field(default_factory=dict)  # show title -> play count
    movie_requesters: dict[int, str] = field(default_factory=dict)  # tmdb_id -> requester username
    show_requesters: dict[int, str] = field(default_factory=dict)  # tvdb_id -> requester username
    show_requesters_tmdb: dict[int, str] = field(default_factory=dict)  # tmdb_id -> requester username (fallback)
    user_watches: dict[str, set] = field(default_factory=dict)  # username -> set of rating_keys watched
    last_watch_dates: dict[str, str] = field(default_factory=dict)  # rating_key -> ISO date string
    last_watch_by_title: dict[str, str] = field(default_factory=dict)  # show title -> ISO date string
    ratings_cache: dict[str, dict] = field(default_factory=dict)  # rating_key -> {critic_rating, audience_rating}
    # Season-scoped data (for granularity=season)
    db_plays_by_season: dict[str, int] = field(default_factory=dict)  # "show_title:S{num}" -> play count
    last_watch_by_season: dict[str, str] = field(default_factory=dict)  # "show_title:S{num}" -> ISO date
    user_watches_by_season: dict[str, set] = field(default_factory=dict)  # username -> set of season keys

    # --- Populated by orchestrator for the newer protection/criteria rules ---
    # Plex admin "hearted" / favourited items (items with userRating present).
    plex_favorited_keys: set[str] = field(default_factory=set)
    # Rating keys on any Overseerr user's watchlist (show-level rating keys).
    watchlist_keys: set[str] = field(default_factory=set)
    # Rating keys with at least one partial play (play_duration > 0,
    # percent_complete < 100) within the configured lookback window.
    partially_watched_keys: set[str] = field(default_factory=set)
    # Lowercase show titles partially watched recently (show-level fallback).
    partially_watched_by_title: set[str] = field(default_factory=set)
    # Season keys ("{show_title_lower}:S{num}") partially watched recently.
    partially_watched_by_season: set[str] = field(default_factory=set)
    # rating_key -> Unix timestamp of item.addedAt. Useful for seasons whose
    # own addedAt may be missing (falls back to parent show's addedAt).
    added_at_by_key: dict[str, int] = field(default_factory=dict)
    # Sonarr series_id -> highest season number that has files on disk (for
    # the season-level `old_season` rule).
    show_season_counts: dict[int, int] = field(default_factory=dict)
    # Show rating_keys whose show-level evaluation marked them as protected;
    # consumed by the season-level `series_protection` rule.
    show_level_protection_keys: set[str] = field(default_factory=set)
    # MAX percent_complete across all plays for a given scope. Used by
    # `watch_ratio_low`; 0/absent means "no plays recorded" for that scope.
    max_percent_by_key: dict[str, int] = field(default_factory=dict)
    max_percent_by_title: dict[str, int] = field(default_factory=dict)
    max_percent_by_season: dict[str, int] = field(default_factory=dict)

    @property
    def tau_plays(self) -> dict[str, int]:
        """Read-only alias for ``play_counts``.

        Retained so any external consumer that still reads ``ctx.tau_plays``
        keeps working -- the field was renamed when the watch ingestion
        moved from Tautulli to native Plex/Jellyfin session history.
        """
        return self.play_counts


# Construction-time backwards compatibility: the field was renamed from
# ``tau_plays`` to ``play_counts``. Some callers still pass ``tau_plays=...``
# as a keyword argument to ``EvaluationContext(...)``. Wrap the generated
# ``__init__`` so it transparently maps the legacy kwarg onto the new field.
_orig_evalctx_init = EvaluationContext.__init__


def _evalctx_init_with_compat(self, *args, tau_plays=None, **kwargs):
    if tau_plays is not None and "play_counts" not in kwargs:
        kwargs["play_counts"] = tau_plays
    _orig_evalctx_init(self, *args, **kwargs)


EvaluationContext.__init__ = _evalctx_init_with_compat  # type: ignore[method-assign]


def evaluate_movie(item: dict, ctx: EvaluationContext,
                   criteria: Optional[CollectionCriteria] = None) -> list[RuleResult]:
    """Evaluate rules for a movie based on criteria config.

    If criteria is None, all rules are evaluated (backwards-compatible).
    """
    if criteria is None:
        criteria = CollectionCriteria()
        # Default: enable all rules except show_ended for movies
        criteria.rules = {
            "never_watched": {"enabled": True, "check_plex_views": True, "check_db_plays": True, "exclude_users": []},
            "no_keep_tag": {"enabled": True},
            "no_active_request": {"enabled": True},
            "no_protected_request": {"enabled": True},
            "not_in_keep_collection": {"enabled": True},
            "show_ended": {"enabled": False},
        }
        criteria._parse_rules()

    results = []
    rk = str(item["ratingKey"])

    # 1. Never watched
    if criteria.is_rule_enabled("never_watched"):
        nw = criteria._never_watched
        plex_views = item.get("viewCount", 0) or 0
        play_counts = ctx.play_counts.get(rk, 0)
        db_plays = ctx.db_plays.get(rk, 0)

        watched_plex = plex_views > 0 if nw.check_plex_views else False
        watched_counts = play_counts > 0 if nw.check_db_plays else False
        watched_db = db_plays > 0

        if not watched_plex and not watched_counts and not watched_db:
            results.append(RuleResult("never_watched", True,
                                      f"No play history (Plex views={plex_views}, DB plays={max(play_counts, db_plays)})"))
        else:
            total = max(plex_views, play_counts, db_plays)
            results.append(RuleResult("never_watched", False,
                                      f"Watched: {total} play{'s' if total != 1 else ''} recorded",
                                      severity="blocking"))

    # 2. Get external IDs for arr/overseerr checks
    from ..core.plex import external_id
    tmdb_id_str = external_id(item, "tmdb")
    tmdb = int(tmdb_id_str) if tmdb_id_str else None

    # 3. No keep tag
    if criteria.is_rule_enabled("no_keep_tag"):
        protected_tags = criteria.protected_tags or []
        if tmdb and tmdb in ctx.radarr_movies:
            item_tags = ctx.radarr_movies[tmdb].get("_tag_names", [])
            matched = [t for t in protected_tags if t in item_tags]
            if matched:
                results.append(RuleResult("no_keep_tag", False,
                                          f"Has protected tag: {', '.join(matched)} (item tags: {', '.join(item_tags) or 'none'})", severity="blocking"))
            else:
                results.append(RuleResult("no_keep_tag", True,
                                          f"No protected tags found (item tags: {', '.join(item_tags) or 'none'})"))
        else:
            results.append(RuleResult("no_keep_tag", True,
                                      "Not in Radarr or no TMDB match"))

    # 4. Unwatched Seerr request - protect if requester hasn't watched
    if criteria.is_rule_enabled("no_active_request"):
        if tmdb and tmdb in ctx.overseerr_active_movies:
            requester = ctx.movie_requesters.get(tmdb, "")
            requester_watched = (requester and requester in ctx.user_watches
                                 and rk in ctx.user_watches[requester])
            if requester_watched:
                results.append(RuleResult("no_active_request", True,
                                          f"Requested by {requester} - they've watched it"))
            else:
                results.append(RuleResult("no_active_request", False,
                                          f"Requested by {requester or 'unknown'} - hasn't watched yet",
                                          severity="blocking"))
        else:
            # Fallback: check Radarr tags for Seerr-pattern requesters
            from ..core.overseerr import extract_requesters_from_tags
            tag_requesters = []
            if tmdb and tmdb in ctx.radarr_movies:
                tag_requesters = extract_requesters_from_tags(
                    ctx.radarr_movies[tmdb].get("_tag_names", []))
            if tag_requesters:
                requester = tag_requesters[0]
                requester_watched = (requester in ctx.user_watches
                                     and rk in ctx.user_watches[requester])
                if requester_watched:
                    results.append(RuleResult("no_active_request", True,
                        f"Requested by {requester} (from Radarr tag) - they've watched it"))
                else:
                    results.append(RuleResult("no_active_request", False,
                        f"Requested by {requester} (from Radarr tag) - hasn't watched yet",
                        severity="blocking"))
            else:
                results.append(RuleResult("no_active_request", True,
                                          "No Seerr request"))

    # 5. No protected user (requested OR watched)
    if criteria.is_rule_enabled("no_protected_request"):
        protected_names = set(criteria.protected_users or [])
        # Check Seerr requests by protected users
        if tmdb and tmdb in ctx.overseerr_protected_movies:
            results.append(RuleResult("no_protected_request", False,
                                      "Requested by a protected user", severity="blocking"))
        else:
            # Check if any protected user has watched this item
            watched_by = [u for u in protected_names if u in ctx.user_watches and rk in ctx.user_watches[u]]
            if watched_by:
                results.append(RuleResult("no_protected_request", False,
                    f"Watched by protected user: {', '.join(watched_by)}", severity="blocking"))
            else:
                results.append(RuleResult("no_protected_request", True,
                                          "No protected-user request or watch"))

    # 6. Not in keep collection
    if criteria.is_rule_enabled("not_in_keep_collection"):
        if rk in ctx.plex_keep_keys:
            results.append(RuleResult("not_in_keep_collection", False,
                                      "In Plex keep collection", severity="blocking"))
        else:
            results.append(RuleResult("not_in_keep_collection", True,
                                      "Not in keep collection"))

    # 7. Not watched recently
    if criteria.is_rule_enabled("not_watched_recently"):
        threshold_days = criteria.not_watched_recently_days or 90
        last_watched_str = ctx.last_watch_dates.get(rk)
        if last_watched_str:
            try:
                last_date = datetime.fromisoformat(last_watched_str.replace("Z", ""))
                days_since = (datetime.now() - last_date).days
                if days_since > threshold_days:
                    results.append(RuleResult("not_watched_recently", True,
                        f"Last watched {days_since} days ago (threshold: {threshold_days}d)"))
                else:
                    results.append(RuleResult("not_watched_recently", False,
                        f"Watched {days_since} days ago (within {threshold_days}d window)",
                        severity="blocking"))
            except Exception:
                results.append(RuleResult("not_watched_recently", True,
                    "Could not parse last watch date"))
        else:
            # Never watched - check if content is old enough to be considered stale
            added_at = item.get("addedAt", 0)
            if added_at:
                try:
                    added_date = datetime.fromtimestamp(int(added_at))
                    days_since_added = (datetime.now() - added_date).days
                    if days_since_added > threshold_days:
                        results.append(RuleResult("not_watched_recently", True,
                            f"Never watched, added {days_since_added} days ago (threshold: {threshold_days}d)"))
                    else:
                        results.append(RuleResult("not_watched_recently", False,
                            f"Never watched but only added {days_since_added} days ago - too new (threshold: {threshold_days}d)",
                            severity="blocking"))
                except Exception:
                    results.append(RuleResult("not_watched_recently", True,
                        "Never watched, could not determine add date"))
            else:
                results.append(RuleResult("not_watched_recently", True,
                    f"Never watched, no add date available (threshold: {threshold_days}d)"))

    # 8. Request fulfilled (requester has watched it)
    if criteria.is_rule_enabled("request_fulfilled"):
        requester = ctx.movie_requesters.get(tmdb, "") if tmdb else ""
        source = "Seerr"
        if not requester and tmdb and tmdb in ctx.radarr_movies:
            from ..core.overseerr import extract_requesters_from_tags
            tag_reqs = extract_requesters_from_tags(
                ctx.radarr_movies[tmdb].get("_tag_names", []))
            if tag_reqs:
                requester = tag_reqs[0]
                source = "Radarr tag"
        if requester:
            requester_watched = (requester in ctx.user_watches
                                 and rk in ctx.user_watches[requester])
            if requester_watched:
                results.append(RuleResult("request_fulfilled", True,
                    f"Requested by {requester} (from {source}) - they've watched it"))
            else:
                results.append(RuleResult("request_fulfilled", False,
                    f"Requested by {requester} (from {source}) - hasn't watched yet",
                    severity="blocking"))
        else:
            results.append(RuleResult("request_fulfilled", True,
                "No Seerr request found"))

    # 9. Available on debrid (blocking if enabled and not cached)
    if criteria.is_rule_enabled("available_on_debrid"):
        if rk in ctx.debrid_cached and ctx.debrid_cached[rk]:
            results.append(RuleResult("available_on_debrid", True,
                "Available on debrid - can be re-streamed"))
        else:
            results.append(RuleResult("available_on_debrid", False,
                "Not confirmed on debrid",
                severity="blocking"))

    # 10. Old content (added to Plex more than N days ago)
    if criteria.is_rule_enabled("old_content"):
        added_at = item.get("addedAt", 0)
        threshold_days = criteria.old_content_days or 180
        if added_at:
            try:
                added_date = datetime.fromtimestamp(int(added_at))
                days_since = (datetime.now() - added_date).days
                if days_since > threshold_days:
                    results.append(RuleResult("old_content", True,
                        f"Added {days_since} days ago (threshold: {threshold_days}d)"))
                else:
                    results.append(RuleResult("old_content", False,
                        f"Added {days_since} days ago (within {threshold_days}d)",
                        severity="blocking"))
            except Exception:
                results.append(RuleResult("old_content", True,
                    "Could not parse add date"))
        else:
            results.append(RuleResult("old_content", True,
                "No add date available"))

    # 11. Highly rated protection
    if criteria.is_rule_enabled("highly_rated"):
        hr = criteria._highly_rated
        ratings = ctx.ratings_cache.get(rk, {})
        if not ratings:
            results.append(RuleResult("highly_rated", True,
                                      "No ratings data available"))
        else:
            _append_highly_rated_result(results, hr, ratings)

    # 12. Low rating (criterion - below threshold = candidate)
    if criteria.is_rule_enabled("low_rating"):
        lr = criteria._low_rating
        ratings = ctx.ratings_cache.get(rk, {})
        if not ratings:
            results.append(RuleResult("low_rating", True,
                                      "no ratings available"))
        else:
            _append_low_rating_result(results, lr, ratings)

    # 13. File size minimum (criterion)
    if criteria.is_rule_enabled("file_size_min"):
        fsm = criteria._file_size_min
        min_bytes = int((fsm.min_gb or 0) * (1024 ** 3))
        size_bytes = 0
        if tmdb and tmdb in ctx.radarr_movies:
            size_bytes = int(ctx.radarr_movies[tmdb].get("sizeOnDisk", 0) or 0)
        if min_bytes <= 0 or size_bytes <= 0:
            results.append(RuleResult("file_size_min", True,
                                      "unknown size" if size_bytes <= 0 else "threshold disabled"))
        else:
            size_gb = size_bytes / (1024 ** 3)
            if size_bytes >= min_bytes:
                results.append(RuleResult("file_size_min", True,
                    f"Size {size_gb:.2f} GB >= {fsm.min_gb} GB"))
            else:
                results.append(RuleResult("file_size_min", False,
                    f"Size {size_gb:.2f} GB < {fsm.min_gb} GB",
                    severity="blocking"))

    # 14. Release year before (criterion)
    if criteria.is_rule_enabled("release_year_before"):
        ry = criteria._release_year_before
        item_year = item.get("year")
        if not ry.year or not item_year:
            results.append(RuleResult("release_year_before", True,
                                      "no year" if not item_year else "threshold disabled"))
        else:
            try:
                iy = int(item_year)
                if iy < ry.year:
                    results.append(RuleResult("release_year_before", True,
                        f"Released {iy} < {ry.year}"))
                else:
                    results.append(RuleResult("release_year_before", False,
                        f"Released {iy} >= {ry.year}",
                        severity="blocking"))
            except (TypeError, ValueError):
                results.append(RuleResult("release_year_before", True,
                                          "no year"))

    # 15. Watch ratio low (criterion)
    if criteria.is_rule_enabled("watch_ratio_low"):
        wr = criteria._watch_ratio_low
        has_plays = (
            ctx.db_plays.get(rk, 0) > 0
            or ctx.play_counts.get(rk, 0) > 0
            or (item.get("viewCount", 0) or 0) > 0
            or rk in ctx.max_percent_by_key
        )
        max_percent = int(ctx.max_percent_by_key.get(rk, 0) or 0)
        last_watched = ctx.last_watch_dates.get(rk)
        _eval_watch_ratio_low(results, wr, max_percent, has_plays,
                              last_watched_iso=last_watched)

    # 16. Recently added (protection - within N days of addedAt = protected)
    if criteria.is_rule_enabled("recently_added"):
        ra = criteria._recently_added
        added_at = item.get("addedAt", 0) or ctx.added_at_by_key.get(rk, 0)
        if not ra.days or not added_at:
            results.append(RuleResult("recently_added", True,
                "no addedAt" if not added_at else "threshold disabled"))
        else:
            try:
                added_date = datetime.fromtimestamp(int(added_at))
                days_since = (datetime.now() - added_date).days
                if days_since <= ra.days:
                    results.append(RuleResult("recently_added", False,
                        f"Added {days_since} days ago (within {ra.days}d)",
                        severity="blocking"))
                else:
                    results.append(RuleResult("recently_added", True,
                        f"Added {days_since} days ago (> {ra.days}d)"))
            except Exception:
                results.append(RuleResult("recently_added", True,
                                          "could not parse addedAt"))

    # 17. Partially watched (protection)
    if criteria.is_rule_enabled("partially_watched"):
        pw = criteria._partially_watched
        if not pw.days:
            results.append(RuleResult("partially_watched", True,
                                      "threshold disabled"))
        elif rk in ctx.partially_watched_keys:
            results.append(RuleResult("partially_watched", False,
                f"Partial play within last {pw.days} days",
                severity="blocking"))
        else:
            results.append(RuleResult("partially_watched", True,
                f"No partial play within last {pw.days} days"))

    # 18. On watchlist (protection)
    if criteria.is_rule_enabled("on_watchlist"):
        if rk in ctx.watchlist_keys:
            results.append(RuleResult("on_watchlist", False,
                "On a Seerr watchlist", severity="blocking"))
        else:
            results.append(RuleResult("on_watchlist", True,
                "Not on any watchlist"))

    # 19. Plex favorited (protection)
    if criteria.is_rule_enabled("plex_favorited"):
        if rk in ctx.plex_favorited_keys:
            results.append(RuleResult("plex_favorited", False,
                "Hearted by Plex admin", severity="blocking"))
        else:
            results.append(RuleResult("plex_favorited", True,
                "Not hearted by Plex admin"))

    # 20. Debrid cache status (informational, not blocking - always checked if data present)
    if rk in ctx.debrid_cached:
        cached = ctx.debrid_cached[rk]
        results.append(RuleResult("debrid_cached", cached,
                                  f"{'Cached' if cached else 'Not cached'} on debrid providers",
                                  severity="info"))

    return results


def evaluate_show(item: dict, ctx: EvaluationContext,
                  criteria: Optional[CollectionCriteria] = None,
                  check_ended: bool = False) -> list[RuleResult]:
    """Evaluate rules for a TV show based on criteria config.

    The check_ended parameter is kept for backwards compatibility but is
    superseded by criteria.rules.show_ended.enabled when criteria is provided.
    """
    if criteria is None:
        criteria = CollectionCriteria()
        criteria.rules = {
            "never_watched": {"enabled": True, "check_plex_views": True, "check_db_plays": True, "exclude_users": []},
            "no_keep_tag": {"enabled": True},
            "no_active_request": {"enabled": True},
            "no_protected_request": {"enabled": True},
            "not_in_keep_collection": {"enabled": True},
            "show_ended": {"enabled": check_ended, "include_deleted": True},
        }
        criteria._parse_rules()

    results = []
    rk = str(item["ratingKey"])

    # 1. Never watched
    if criteria.is_rule_enabled("never_watched"):
        nw = criteria._never_watched
        plex_views = item.get("viewCount", 0) or 0
        play_counts = ctx.play_counts.get(rk, 0)
        db_plays = ctx.db_plays.get(rk, 0)
        # For TV shows, also check by title (episodes have different rating_keys)
        show_title = item.get("title", "")
        if not db_plays and show_title and show_title.lower() in ctx.db_plays_by_title:
            db_plays = ctx.db_plays_by_title[show_title.lower()]

        watched_plex = plex_views > 0 if nw.check_plex_views else False
        watched_counts = play_counts > 0 if nw.check_db_plays else False
        watched_db = db_plays > 0

        if not watched_plex and not watched_counts and not watched_db:
            results.append(RuleResult("never_watched", True,
                                      f"No play history (Plex views={plex_views}, DB plays={max(play_counts, db_plays)})"))
        else:
            total = max(plex_views, play_counts, db_plays)
            results.append(RuleResult("never_watched", False,
                                      f"Watched: {total} play{'s' if total != 1 else ''} recorded",
                                      severity="blocking"))

    from ..core.plex import external_id
    tvdb_id_str = external_id(item, "tvdb")
    tvdb = int(tvdb_id_str) if tvdb_id_str else None
    tmdb_id_str = external_id(item, "tmdb")
    tmdb = int(tmdb_id_str) if tmdb_id_str else None

    # 2. No keep tag
    if criteria.is_rule_enabled("no_keep_tag"):
        protected_tags = criteria.protected_tags or []
        if tvdb and tvdb in ctx.sonarr_shows:
            item_tags = ctx.sonarr_shows[tvdb].get("_tag_names", [])
            matched = [t for t in protected_tags if t in item_tags]
            if matched:
                results.append(RuleResult("no_keep_tag", False,
                                          f"Has protected tag: {', '.join(matched)} (item tags: {', '.join(item_tags) or 'none'})", severity="blocking"))
            else:
                results.append(RuleResult("no_keep_tag", True,
                                          f"No protected tags found (item tags: {', '.join(item_tags) or 'none'})"))
        else:
            results.append(RuleResult("no_keep_tag", True,
                                      "Not in Sonarr or no TVDB match"))

    # 3. Unwatched Seerr request - check TVDB first, then TMDB as fallback
    if criteria.is_rule_enabled("no_active_request"):
        has_request = False
        requester = ""
        if tvdb and tvdb in ctx.overseerr_active_shows:
            has_request = True
            requester = ctx.show_requesters.get(tvdb, "")
        elif tmdb and tmdb in ctx.overseerr_active_shows_tmdb:
            has_request = True
            requester = ctx.show_requesters_tmdb.get(tmdb, "")

        if has_request:
            show_title = item.get("title", "")
            requester_watched = (requester and requester in ctx.user_watches
                                 and (rk in ctx.user_watches[requester]
                                      or show_title.lower() in ctx.user_watches[requester]))
            if requester_watched:
                results.append(RuleResult("no_active_request", True,
                                          f"Requested by {requester} - they've watched it"))
            else:
                results.append(RuleResult("no_active_request", False,
                                          f"Requested by {requester or 'unknown'} - hasn't watched yet",
                                          severity="blocking"))
        else:
            # Fallback: check Sonarr tags for Seerr-pattern requesters
            from ..core.overseerr import extract_requesters_from_tags
            tag_requesters = []
            if tvdb and tvdb in ctx.sonarr_shows:
                tag_requesters = extract_requesters_from_tags(
                    ctx.sonarr_shows[tvdb].get("_tag_names", []))
            if tag_requesters:
                requester = tag_requesters[0]
                show_title = item.get("title", "")
                requester_watched = (requester in ctx.user_watches
                                     and (rk in ctx.user_watches[requester]
                                          or show_title.lower() in ctx.user_watches[requester]))
                if requester_watched:
                    results.append(RuleResult("no_active_request", True,
                        f"Requested by {requester} (from Sonarr tag) - they've watched it"))
                else:
                    results.append(RuleResult("no_active_request", False,
                        f"Requested by {requester} (from Sonarr tag) - hasn't watched yet",
                        severity="blocking"))
            else:
                results.append(RuleResult("no_active_request", True,
                                          "No Seerr request"))

    # 4. No protected user (requested OR watched) - check TVDB first, then TMDB as fallback
    if criteria.is_rule_enabled("no_protected_request"):
        if (tvdb and tvdb in ctx.overseerr_protected_shows) or \
           (tmdb and tmdb in ctx.overseerr_protected_shows_tmdb):
            results.append(RuleResult("no_protected_request", False,
                                      "Requested by a protected user", severity="blocking"))
        else:
            # Check if any protected user has watched any episode of this show
            protected_names = set(criteria.protected_users or [])
            show_title = item.get("title", "")
            watched_by = [u for u in protected_names if u in ctx.user_watches
                          and (rk in ctx.user_watches[u] or show_title.lower() in ctx.user_watches[u])]
            if watched_by:
                results.append(RuleResult("no_protected_request", False,
                    f"Watched by protected user: {', '.join(watched_by)}", severity="blocking"))
            else:
                results.append(RuleResult("no_protected_request", True,
                                          "No protected-user request or watch"))

    # 5. Not in keep collection
    if criteria.is_rule_enabled("not_in_keep_collection"):
        if rk in ctx.plex_keep_keys:
            results.append(RuleResult("not_in_keep_collection", False,
                                      "In Plex keep collection", severity="blocking"))
        else:
            results.append(RuleResult("not_in_keep_collection", True,
                                      "Not in keep collection"))

    # 6. Show ended
    if criteria.is_rule_enabled("show_ended"):
        include_deleted = criteria._show_ended.include_deleted
        if tvdb and tvdb in ctx.sonarr_shows:
            status = ctx.sonarr_shows[tvdb].get("status", "").lower()
            ended_statuses = ["ended"]
            if include_deleted:
                ended_statuses.append("deleted")
            if status in ended_statuses:
                results.append(RuleResult("show_ended", True,
                                          f"Sonarr status: {status}"))
            else:
                results.append(RuleResult("show_ended", False,
                                          f"Sonarr status: {status} (not ended)", severity="blocking"))
        else:
            results.append(RuleResult("show_ended", False,
                                      "Not in Sonarr", severity="blocking"))

    # 7. Not watched recently
    if criteria.is_rule_enabled("not_watched_recently"):
        threshold_days = criteria.not_watched_recently_days or 90
        last_watched_str = ctx.last_watch_dates.get(rk)
        # For TV shows, also check by title
        show_title = item.get("title", "")
        if not last_watched_str and show_title:
            last_watched_str = ctx.last_watch_by_title.get(show_title.lower() if show_title else "")
        if last_watched_str:
            try:
                last_date = datetime.fromisoformat(last_watched_str.replace("Z", ""))
                days_since = (datetime.now() - last_date).days
                if days_since > threshold_days:
                    results.append(RuleResult("not_watched_recently", True,
                        f"Last watched {days_since} days ago (threshold: {threshold_days}d)"))
                else:
                    results.append(RuleResult("not_watched_recently", False,
                        f"Watched {days_since} days ago (within {threshold_days}d window)",
                        severity="blocking"))
            except Exception:
                results.append(RuleResult("not_watched_recently", True,
                    "Could not parse last watch date"))
        else:
            # Never watched - check if content is old enough to be considered stale
            added_at = item.get("addedAt", 0)
            if added_at:
                try:
                    added_date = datetime.fromtimestamp(int(added_at))
                    days_since_added = (datetime.now() - added_date).days
                    if days_since_added > threshold_days:
                        results.append(RuleResult("not_watched_recently", True,
                            f"Never watched, added {days_since_added} days ago (threshold: {threshold_days}d)"))
                    else:
                        results.append(RuleResult("not_watched_recently", False,
                            f"Never watched but only added {days_since_added} days ago - too new (threshold: {threshold_days}d)",
                            severity="blocking"))
                except Exception:
                    results.append(RuleResult("not_watched_recently", True,
                        "Never watched, could not determine add date"))
            else:
                results.append(RuleResult("not_watched_recently", True,
                    f"Never watched, no add date available (threshold: {threshold_days}d)"))

    # 8. Request fulfilled (requester has watched it)
    if criteria.is_rule_enabled("request_fulfilled"):
        show_title = item.get("title", "")
        requester = (ctx.show_requesters.get(tvdb, "") if tvdb else "") or \
                    (ctx.show_requesters_tmdb.get(tmdb, "") if tmdb else "")
        source = "Seerr"
        if not requester and tvdb and tvdb in ctx.sonarr_shows:
            from ..core.overseerr import extract_requesters_from_tags
            tag_reqs = extract_requesters_from_tags(
                ctx.sonarr_shows[tvdb].get("_tag_names", []))
            if tag_reqs:
                requester = tag_reqs[0]
                source = "Sonarr tag"
        if requester:
            requester_watched = (requester in ctx.user_watches
                                 and (rk in ctx.user_watches[requester]
                                      or show_title.lower() in ctx.user_watches[requester]))
            if requester_watched:
                results.append(RuleResult("request_fulfilled", True,
                    f"Requested by {requester} (from {source}) - they've watched it"))
            else:
                results.append(RuleResult("request_fulfilled", False,
                    f"Requested by {requester} (from {source}) - hasn't watched yet",
                    severity="blocking"))
        else:
            results.append(RuleResult("request_fulfilled", True,
                "No Seerr request found"))

    # 9. Available on debrid (blocking if enabled and not cached)
    if criteria.is_rule_enabled("available_on_debrid"):
        if rk in ctx.debrid_cached and ctx.debrid_cached[rk]:
            results.append(RuleResult("available_on_debrid", True,
                "Available on debrid - can be re-streamed"))
        else:
            results.append(RuleResult("available_on_debrid", False,
                "Not confirmed on debrid",
                severity="blocking"))

    # 10. Old content (added to Plex more than N days ago)
    if criteria.is_rule_enabled("old_content"):
        added_at = item.get("addedAt", 0)
        threshold_days = criteria.old_content_days or 180
        if added_at:
            try:
                added_date = datetime.fromtimestamp(int(added_at))
                days_since = (datetime.now() - added_date).days
                if days_since > threshold_days:
                    results.append(RuleResult("old_content", True,
                        f"Added {days_since} days ago (threshold: {threshold_days}d)"))
                else:
                    results.append(RuleResult("old_content", False,
                        f"Added {days_since} days ago (within {threshold_days}d)",
                        severity="blocking"))
            except Exception:
                results.append(RuleResult("old_content", True,
                    "Could not parse add date"))
        else:
            results.append(RuleResult("old_content", True,
                "No add date available"))

    # 11. Highly rated protection
    if criteria.is_rule_enabled("highly_rated"):
        hr = criteria._highly_rated
        ratings = ctx.ratings_cache.get(rk, {})
        if not ratings:
            results.append(RuleResult("highly_rated", True,
                                      "No ratings data available"))
        else:
            _append_highly_rated_result(results, hr, ratings)

    # 12. Low rating (criterion)
    if criteria.is_rule_enabled("low_rating"):
        lr = criteria._low_rating
        ratings = ctx.ratings_cache.get(rk, {})
        if not ratings:
            results.append(RuleResult("low_rating", True,
                                      "no ratings available"))
        else:
            _append_low_rating_result(results, lr, ratings)

    # 13. File size minimum (criterion) - sum across all seasons from Sonarr
    if criteria.is_rule_enabled("file_size_min"):
        fsm = criteria._file_size_min
        min_bytes = int((fsm.min_gb or 0) * (1024 ** 3))
        size_bytes = 0
        if tvdb and tvdb in ctx.sonarr_shows:
            show_data = ctx.sonarr_shows[tvdb]
            # Prefer top-level statistics.sizeOnDisk if present (Sonarr v3/v4).
            stats = show_data.get("statistics") or {}
            size_bytes = int(stats.get("sizeOnDisk", 0) or 0)
            if not size_bytes:
                # Fall back to summing per-season stats.
                for season in (show_data.get("seasons") or []):
                    s_stats = season.get("statistics") or {}
                    size_bytes += int(s_stats.get("sizeOnDisk", 0) or 0)
        if min_bytes <= 0 or size_bytes <= 0:
            results.append(RuleResult("file_size_min", True,
                                      "unknown size" if size_bytes <= 0 else "threshold disabled"))
        else:
            size_gb = size_bytes / (1024 ** 3)
            if size_bytes >= min_bytes:
                results.append(RuleResult("file_size_min", True,
                    f"Size {size_gb:.2f} GB >= {fsm.min_gb} GB"))
            else:
                results.append(RuleResult("file_size_min", False,
                    f"Size {size_gb:.2f} GB < {fsm.min_gb} GB",
                    severity="blocking"))

    # 14. Release year before (criterion)
    if criteria.is_rule_enabled("release_year_before"):
        ry = criteria._release_year_before
        item_year = item.get("year")
        if not ry.year or not item_year:
            results.append(RuleResult("release_year_before", True,
                                      "no year" if not item_year else "threshold disabled"))
        else:
            try:
                iy = int(item_year)
                if iy < ry.year:
                    results.append(RuleResult("release_year_before", True,
                        f"Released {iy} < {ry.year}"))
                else:
                    results.append(RuleResult("release_year_before", False,
                        f"Released {iy} >= {ry.year}",
                        severity="blocking"))
            except (TypeError, ValueError):
                results.append(RuleResult("release_year_before", True,
                                          "no year"))

    # 15. Watch ratio low (criterion) - show-level: by rating_key or by title
    if criteria.is_rule_enabled("watch_ratio_low"):
        wr = criteria._watch_ratio_low
        title = (item.get("title") or "").lower()
        has_plays = (
            ctx.db_plays.get(rk, 0) > 0
            or (title and ctx.db_plays_by_title.get(title, 0) > 0)
            or ctx.play_counts.get(rk, 0) > 0
            or (item.get("viewCount", 0) or 0) > 0
            or rk in ctx.max_percent_by_key
            or (title and title in ctx.max_percent_by_title)
        )
        max_percent = max(
            int(ctx.max_percent_by_key.get(rk, 0) or 0),
            int(ctx.max_percent_by_title.get(title, 0) or 0) if title else 0,
        )
        # Take the latest timestamp across rating_key and title so the
        # recency gate picks up "any episode watched recently = not stale".
        last_candidates = [
            ctx.last_watch_dates.get(rk),
            ctx.last_watch_by_title.get(title) if title else None,
        ]
        last_watched = max((t for t in last_candidates if t), default=None)
        _eval_watch_ratio_low(results, wr, max_percent, has_plays,
                              last_watched_iso=last_watched)

    # 16. Recently added (protection)
    if criteria.is_rule_enabled("recently_added"):
        ra = criteria._recently_added
        added_at = item.get("addedAt", 0) or ctx.added_at_by_key.get(rk, 0)
        if not ra.days or not added_at:
            results.append(RuleResult("recently_added", True,
                "no addedAt" if not added_at else "threshold disabled"))
        else:
            try:
                added_date = datetime.fromtimestamp(int(added_at))
                days_since = (datetime.now() - added_date).days
                if days_since <= ra.days:
                    results.append(RuleResult("recently_added", False,
                        f"Added {days_since} days ago (within {ra.days}d)",
                        severity="blocking"))
                else:
                    results.append(RuleResult("recently_added", True,
                        f"Added {days_since} days ago (> {ra.days}d)"))
            except Exception:
                results.append(RuleResult("recently_added", True,
                                          "could not parse addedAt"))

    # 17. Partially watched (protection)
    if criteria.is_rule_enabled("partially_watched"):
        pw = criteria._partially_watched
        title = (item.get("title") or "").lower()
        if not pw.days:
            results.append(RuleResult("partially_watched", True,
                                      "threshold disabled"))
        elif rk in ctx.partially_watched_keys \
                or (title and title in ctx.partially_watched_by_title):
            results.append(RuleResult("partially_watched", False,
                f"Partial play within last {pw.days} days",
                severity="blocking"))
        else:
            results.append(RuleResult("partially_watched", True,
                f"No partial play within last {pw.days} days"))

    # 18. On watchlist (protection)
    if criteria.is_rule_enabled("on_watchlist"):
        if rk in ctx.watchlist_keys:
            results.append(RuleResult("on_watchlist", False,
                "On a Seerr watchlist", severity="blocking"))
        else:
            results.append(RuleResult("on_watchlist", True,
                "Not on any watchlist"))

    # 19. Plex favorited (protection)
    if criteria.is_rule_enabled("plex_favorited"):
        if rk in ctx.plex_favorited_keys:
            results.append(RuleResult("plex_favorited", False,
                "Hearted by Plex admin", severity="blocking"))
        else:
            results.append(RuleResult("plex_favorited", True,
                "Not hearted by Plex admin"))

    return results


def evaluate_season(item: dict, ctx: EvaluationContext,
                    criteria: Optional[CollectionCriteria] = None,
                    season_number: int = 0,
                    show_title: str = "") -> list[RuleResult]:
    """Evaluate rules for an individual TV season.

    ``item`` is the Plex season dict, augmented with the parent show's Guid
    list (``Guid``), ``_show_title``, and ``_show_rating_key``.
    Watch data is looked up by season key (``"{show_title_lower}:S{num}"``),
    while show-level properties (tags, requests, ended status) are inherited
    from the parent show.
    """
    if criteria is None:
        criteria = CollectionCriteria()
        criteria.rules = {
            "never_watched": {"enabled": True, "check_plex_views": True, "check_db_plays": True, "exclude_users": []},
            "no_keep_tag": {"enabled": True},
            "no_active_request": {"enabled": True},
            "no_protected_request": {"enabled": True},
            "not_in_keep_collection": {"enabled": True},
            "show_ended": {"enabled": False},
        }
        criteria._parse_rules()

    results = []
    rk = str(item["ratingKey"])
    show_rk = str(item.get("_show_rating_key", ""))
    title = show_title or item.get("_show_title", "")
    season_key = f"{title.lower()}:S{season_number}"

    # 1. Never watched (season-scoped)
    if criteria.is_rule_enabled("never_watched"):
        nw = criteria._never_watched
        plex_views = item.get("viewCount", 0) or 0
        play_counts = ctx.play_counts.get(rk, 0)
        db_plays = ctx.db_plays_by_season.get(season_key, 0)

        watched_plex = plex_views > 0 if nw.check_plex_views else False
        watched_counts = play_counts > 0 if nw.check_db_plays else False
        watched_db = db_plays > 0

        if not watched_plex and not watched_counts and not watched_db:
            results.append(RuleResult("never_watched", True,
                f"No play history for S{season_number:02d} (Plex views={plex_views}, DB plays={db_plays})"))
        else:
            total = max(plex_views, play_counts, db_plays)
            results.append(RuleResult("never_watched", False,
                f"S{season_number:02d} watched: {total} play{'s' if total != 1 else ''} recorded",
                severity="blocking"))

    from ..core.plex import external_id
    tvdb_id_str = external_id(item, "tvdb")
    tvdb = int(tvdb_id_str) if tvdb_id_str else None
    tmdb_id_str = external_id(item, "tmdb")
    tmdb = int(tmdb_id_str) if tmdb_id_str else None

    # 2. No keep tag (show-level, inherited)
    if criteria.is_rule_enabled("no_keep_tag"):
        protected_tags = criteria.protected_tags or []
        if tvdb and tvdb in ctx.sonarr_shows:
            item_tags = ctx.sonarr_shows[tvdb].get("_tag_names", [])
            matched = [t for t in protected_tags if t in item_tags]
            if matched:
                results.append(RuleResult("no_keep_tag", False,
                    f"Has protected tag: {', '.join(matched)}", severity="blocking"))
            else:
                results.append(RuleResult("no_keep_tag", True,
                    f"No protected tags found (item tags: {', '.join(item_tags) or 'none'})"))
        else:
            results.append(RuleResult("no_keep_tag", True,
                "Not in Sonarr or no TVDB match"))

    # 3. Unwatched Seerr request (show-level, inherited)
    if criteria.is_rule_enabled("no_active_request"):
        has_request = False
        requester = ""
        if tvdb and tvdb in ctx.overseerr_active_shows:
            has_request = True
            requester = ctx.show_requesters.get(tvdb, "")
        elif tmdb and tmdb in ctx.overseerr_active_shows_tmdb:
            has_request = True
            requester = ctx.show_requesters_tmdb.get(tmdb, "")

        if has_request:
            # Check if requester has watched THIS season
            requester_watched = (requester and requester in ctx.user_watches_by_season
                                 and season_key in ctx.user_watches_by_season[requester])
            if requester_watched:
                results.append(RuleResult("no_active_request", True,
                    f"Requested by {requester} - they've watched S{season_number:02d}"))
            else:
                results.append(RuleResult("no_active_request", False,
                    f"Requested by {requester or 'unknown'} - hasn't watched S{season_number:02d} yet",
                    severity="blocking"))
        else:
            from ..core.overseerr import extract_requesters_from_tags
            tag_requesters = []
            if tvdb and tvdb in ctx.sonarr_shows:
                tag_requesters = extract_requesters_from_tags(
                    ctx.sonarr_shows[tvdb].get("_tag_names", []))
            if tag_requesters:
                requester = tag_requesters[0]
                requester_watched = (requester in ctx.user_watches_by_season
                                     and season_key in ctx.user_watches_by_season[requester])
                if requester_watched:
                    results.append(RuleResult("no_active_request", True,
                        f"Requested by {requester} (Sonarr tag) - they've watched S{season_number:02d}"))
                else:
                    results.append(RuleResult("no_active_request", False,
                        f"Requested by {requester} (Sonarr tag) - hasn't watched S{season_number:02d} yet",
                        severity="blocking"))
            else:
                results.append(RuleResult("no_active_request", True,
                    "No Seerr request"))

    # 4. No protected user (requested OR watched) - show-level request, season-level watch
    if criteria.is_rule_enabled("no_protected_request"):
        if (tvdb and tvdb in ctx.overseerr_protected_shows) or \
           (tmdb and tmdb in ctx.overseerr_protected_shows_tmdb):
            results.append(RuleResult("no_protected_request", False,
                "Requested by a protected user", severity="blocking"))
        else:
            # Check if any protected user has watched any episode in this season
            protected_names = set(criteria.protected_users or [])
            season_key = f"{show_title.lower()}:S{season_number}" if show_title else ""
            watched_by = [u for u in protected_names
                          if u in ctx.user_watches_by_season and season_key in ctx.user_watches_by_season.get(u, set())]
            # Also check show-level watches
            if not watched_by:
                watched_by = [u for u in protected_names if u in ctx.user_watches
                              and (rk in ctx.user_watches[u] or show_title.lower() in ctx.user_watches[u])]
            if watched_by:
                results.append(RuleResult("no_protected_request", False,
                    f"Watched by protected user: {', '.join(watched_by)}", severity="blocking"))
            else:
                results.append(RuleResult("no_protected_request", True,
                    "No protected-user request or watch"))

    # 5. Not in keep collection (check BOTH season ratingKey AND show ratingKey)
    if criteria.is_rule_enabled("not_in_keep_collection"):
        if rk in ctx.plex_keep_keys or show_rk in ctx.plex_keep_keys:
            results.append(RuleResult("not_in_keep_collection", False,
                "In Plex keep collection", severity="blocking"))
        else:
            results.append(RuleResult("not_in_keep_collection", True,
                "Not in keep collection"))

    # 6. Show ended (show-level, inherited)
    if criteria.is_rule_enabled("show_ended"):
        include_deleted = criteria._show_ended.include_deleted
        if tvdb and tvdb in ctx.sonarr_shows:
            status = ctx.sonarr_shows[tvdb].get("status", "").lower()
            ended_statuses = ["ended"]
            if include_deleted:
                ended_statuses.append("deleted")
            if status in ended_statuses:
                results.append(RuleResult("show_ended", True,
                    f"Sonarr status: {status}"))
            else:
                results.append(RuleResult("show_ended", False,
                    f"Sonarr status: {status} (not ended)", severity="blocking"))
        else:
            results.append(RuleResult("show_ended", False,
                "Not in Sonarr", severity="blocking"))

    # 7. Not watched recently (season-scoped)
    if criteria.is_rule_enabled("not_watched_recently"):
        threshold_days = criteria.not_watched_recently_days or 90
        last_watched_str = ctx.last_watch_by_season.get(season_key)
        if last_watched_str:
            try:
                last_date = datetime.fromisoformat(last_watched_str.replace("Z", ""))
                days_since = (datetime.now() - last_date).days
                if days_since > threshold_days:
                    results.append(RuleResult("not_watched_recently", True,
                        f"S{season_number:02d} last watched {days_since} days ago (threshold: {threshold_days}d)"))
                else:
                    results.append(RuleResult("not_watched_recently", False,
                        f"S{season_number:02d} watched {days_since} days ago (within {threshold_days}d window)",
                        severity="blocking"))
            except Exception:
                results.append(RuleResult("not_watched_recently", True,
                    "Could not parse last watch date"))
        else:
            # Use the season's own addedAt
            added_at = item.get("addedAt", 0)
            if added_at:
                try:
                    added_date = datetime.fromtimestamp(int(added_at))
                    days_since_added = (datetime.now() - added_date).days
                    if days_since_added > threshold_days:
                        results.append(RuleResult("not_watched_recently", True,
                            f"S{season_number:02d} never watched, added {days_since_added} days ago (threshold: {threshold_days}d)"))
                    else:
                        results.append(RuleResult("not_watched_recently", False,
                            f"S{season_number:02d} never watched but only added {days_since_added} days ago (threshold: {threshold_days}d)",
                            severity="blocking"))
                except Exception:
                    results.append(RuleResult("not_watched_recently", True,
                        "Never watched, could not determine add date"))
            else:
                results.append(RuleResult("not_watched_recently", True,
                    f"S{season_number:02d} never watched, no add date available (threshold: {threshold_days}d)"))

    # 8. Low rating (criterion) - reuse show-level ratings
    if criteria.is_rule_enabled("low_rating"):
        lr = criteria._low_rating
        # Seasons rarely have their own ratings cache entry; fall back to show.
        ratings = ctx.ratings_cache.get(rk) or ctx.ratings_cache.get(show_rk) or {}
        if not ratings:
            results.append(RuleResult("low_rating", True,
                                      "no ratings available"))
        else:
            _append_low_rating_result(results, lr, ratings)

    # 9. File size minimum (criterion) - season-specific Sonarr stats
    if criteria.is_rule_enabled("file_size_min"):
        fsm = criteria._file_size_min
        min_bytes = int((fsm.min_gb or 0) * (1024 ** 3))
        size_bytes = 0
        if tvdb and tvdb in ctx.sonarr_shows:
            for season in (ctx.sonarr_shows[tvdb].get("seasons") or []):
                if int(season.get("seasonNumber", -1)) == int(season_number):
                    s_stats = season.get("statistics") or {}
                    size_bytes = int(s_stats.get("sizeOnDisk", 0) or 0)
                    break
        if min_bytes <= 0 or size_bytes <= 0:
            results.append(RuleResult("file_size_min", True,
                                      "unknown size" if size_bytes <= 0 else "threshold disabled"))
        else:
            size_gb = size_bytes / (1024 ** 3)
            if size_bytes >= min_bytes:
                results.append(RuleResult("file_size_min", True,
                    f"S{season_number:02d} size {size_gb:.2f} GB >= {fsm.min_gb} GB"))
            else:
                results.append(RuleResult("file_size_min", False,
                    f"S{season_number:02d} size {size_gb:.2f} GB < {fsm.min_gb} GB",
                    severity="blocking"))

    # 10. Release year before (criterion) - use the show-level year
    if criteria.is_rule_enabled("release_year_before"):
        ry = criteria._release_year_before
        item_year = item.get("year") or item.get("parentYear")
        if not ry.year or not item_year:
            results.append(RuleResult("release_year_before", True,
                                      "no year" if not item_year else "threshold disabled"))
        else:
            try:
                iy = int(item_year)
                if iy < ry.year:
                    results.append(RuleResult("release_year_before", True,
                        f"Released {iy} < {ry.year}"))
                else:
                    results.append(RuleResult("release_year_before", False,
                        f"Released {iy} >= {ry.year}",
                        severity="blocking"))
            except (TypeError, ValueError):
                results.append(RuleResult("release_year_before", True,
                                          "no year"))

    # 11. Watch ratio low (criterion) - season-scoped
    if criteria.is_rule_enabled("watch_ratio_low"):
        wr = criteria._watch_ratio_low
        has_plays = (
            ctx.db_plays_by_season.get(season_key, 0) > 0
            or (item.get("viewCount", 0) or 0) > 0
            or ctx.play_counts.get(rk, 0) > 0
            or season_key in ctx.max_percent_by_season
        )
        max_percent = int(ctx.max_percent_by_season.get(season_key, 0) or 0)
        last_watched = ctx.last_watch_by_season.get(season_key)
        _eval_watch_ratio_low(results, wr, max_percent, has_plays,
                              last_watched_iso=last_watched,
                              scope_label=f"S{season_number:02d}")

    # 12. Recently added (protection) - season addedAt may be missing, fall
    # back to parent show's addedAt (via added_at_by_key[show_rk]).
    if criteria.is_rule_enabled("recently_added"):
        ra = criteria._recently_added
        added_at = (
            item.get("addedAt", 0)
            or ctx.added_at_by_key.get(rk, 0)
            or ctx.added_at_by_key.get(show_rk, 0)
        )
        if not ra.days or not added_at:
            results.append(RuleResult("recently_added", True,
                "no addedAt" if not added_at else "threshold disabled"))
        else:
            try:
                added_date = datetime.fromtimestamp(int(added_at))
                days_since = (datetime.now() - added_date).days
                if days_since <= ra.days:
                    results.append(RuleResult("recently_added", False,
                        f"S{season_number:02d} added {days_since} days ago (within {ra.days}d)",
                        severity="blocking"))
                else:
                    results.append(RuleResult("recently_added", True,
                        f"S{season_number:02d} added {days_since} days ago (> {ra.days}d)"))
            except Exception:
                results.append(RuleResult("recently_added", True,
                                          "could not parse addedAt"))

    # 13. Partially watched (protection) - season key
    if criteria.is_rule_enabled("partially_watched"):
        pw = criteria._partially_watched
        if not pw.days:
            results.append(RuleResult("partially_watched", True,
                                      "threshold disabled"))
        elif season_key in ctx.partially_watched_by_season:
            results.append(RuleResult("partially_watched", False,
                f"S{season_number:02d} partial play within last {pw.days} days",
                severity="blocking"))
        else:
            results.append(RuleResult("partially_watched", True,
                f"No partial play within last {pw.days} days"))

    # 14. On watchlist (protection) - check the parent show rating_key
    if criteria.is_rule_enabled("on_watchlist"):
        if show_rk and show_rk in ctx.watchlist_keys:
            results.append(RuleResult("on_watchlist", False,
                "Parent show on a Seerr watchlist",
                severity="blocking"))
        elif rk in ctx.watchlist_keys:
            results.append(RuleResult("on_watchlist", False,
                "On a Seerr watchlist", severity="blocking"))
        else:
            results.append(RuleResult("on_watchlist", True,
                "Not on any watchlist"))

    # 15. Plex favorited (protection) - hearted season or parent show
    if criteria.is_rule_enabled("plex_favorited"):
        if rk in ctx.plex_favorited_keys or (show_rk and show_rk in ctx.plex_favorited_keys):
            results.append(RuleResult("plex_favorited", False,
                "Hearted by Plex admin", severity="blocking"))
        else:
            results.append(RuleResult("plex_favorited", True,
                "Not hearted by Plex admin"))

    # 16. Old season (season-only criterion)
    if criteria.is_rule_enabled("old_season"):
        os_rule = criteria._old_season
        # Look up parent show's Sonarr id via the tvdb -> sonarr_shows map.
        sonarr_id: Optional[int] = None
        if tvdb and tvdb in ctx.sonarr_shows:
            sonarr_id = ctx.sonarr_shows[tvdb].get("id")
        max_season = ctx.show_season_counts.get(sonarr_id, 0) if sonarr_id else 0
        if not os_rule.keep_last or max_season <= 0:
            results.append(RuleResult("old_season", True,
                "keep_last disabled" if not os_rule.keep_last else "unknown max season"))
        else:
            cutoff = max_season - os_rule.keep_last + 1
            if season_number < cutoff:
                results.append(RuleResult("old_season", True,
                    f"S{season_number:02d} older than keep-last-{os_rule.keep_last} cutoff S{cutoff:02d} (max S{max_season:02d})"))
            else:
                results.append(RuleResult("old_season", False,
                    f"S{season_number:02d} within last {os_rule.keep_last} seasons (max S{max_season:02d})",
                    severity="blocking"))

    # 17. Series protection (season-only) - if parent show would be
    # protected at show-level, protect all its seasons.
    if criteria.is_rule_enabled("series_protection"):
        if show_rk and show_rk in ctx.show_level_protection_keys:
            results.append(RuleResult("series_protection", False,
                "Parent show is protected at show-level", severity="blocking"))
        else:
            results.append(RuleResult("series_protection", True,
                "Parent show is not protected"))

    return results


def _append_highly_rated_result(results: list[RuleResult], hr, ratings: dict) -> None:
    """Evaluate highly_rated rule and append the result.

    Ratings dict has:
    - critic_rating: RT-style percentage (0-100), maps to hr.rt_min
    - audience_rating: community/IMDb-style score (0-10), maps to hr.imdb_min
    """
    audience = ratings.get("audience_rating")  # 0-10 scale
    critic = ratings.get("critic_rating")      # 0-100% scale

    checks: list[tuple[str, bool]] = []
    details: list[str] = []

    if hr.imdb_min > 0 and audience is not None:
        met = audience >= hr.imdb_min
        checks.append(("audience", met))
        if met:
            details.append(f"Audience {audience}/10 meets {hr.imdb_min} threshold")
    if hr.rt_min > 0 and critic is not None:
        met = critic >= hr.rt_min
        checks.append(("critic", met))
        if met:
            details.append(f"Critic {critic}% meets {hr.rt_min}% threshold")

    if not checks:
        results.append(RuleResult("highly_rated", True,
                                  "No applicable rating thresholds configured"))
        return

    if hr.require_all:
        protected = all(met for _, met in checks)
    else:
        protected = any(met for _, met in checks)

    if protected:
        detail_text = "; ".join(details) if details else "Meets rating thresholds"
        results.append(RuleResult("highly_rated", False,
                                  detail_text, severity="blocking"))
    else:
        below = []
        if hr.imdb_min > 0 and audience is not None and audience < hr.imdb_min:
            below.append(f"Audience {audience}/10 < {hr.imdb_min}")
        if hr.rt_min > 0 and critic is not None and critic < hr.rt_min:
            below.append(f"Critic {critic}% < {hr.rt_min}%")
        detail_text = "; ".join(below) if below else "Below rating thresholds"
        results.append(RuleResult("highly_rated", True, detail_text))


def _append_low_rating_result(results: list[RuleResult], lr, ratings: dict) -> None:
    """Evaluate low_rating rule and append the result.

    Inverse of HighlyRatedRule: `imdb_max`/`critic_max` are upper bounds.
    Passing (candidate) means the item is *below* the configured ceiling(s).
    """
    audience = ratings.get("audience_rating")  # 0-10 scale
    critic = ratings.get("critic_rating")      # 0-100 scale

    checks: list[tuple[str, bool]] = []
    details: list[str] = []

    if lr.imdb_max > 0 and audience is not None:
        met = audience <= lr.imdb_max
        checks.append(("audience", met))
        details.append(f"Audience {audience}/10 vs <= {lr.imdb_max}")
    if lr.critic_max > 0 and critic is not None:
        met = critic <= lr.critic_max
        checks.append(("critic", met))
        details.append(f"Critic {critic}% vs <= {lr.critic_max}%")

    if not checks:
        results.append(RuleResult("low_rating", True,
                                  "No applicable rating thresholds configured"))
        return

    if lr.require_all:
        is_low = all(met for _, met in checks)
    else:
        is_low = any(met for _, met in checks)

    detail_text = "; ".join(details) if details else "low_rating evaluated"
    if is_low:
        results.append(RuleResult("low_rating", True,
                                  f"Rated low: {detail_text}"))
    else:
        results.append(RuleResult("low_rating", False,
                                  f"Above rating ceiling ({detail_text})",
                                  severity="blocking"))


def _eval_watch_ratio_low(results: list[RuleResult], wr, max_percent: int,
                          has_plays: bool, last_watched_iso: str | None = None,
                          scope_label: str = "") -> None:
    """Shared evaluator for the ``watch_ratio_low`` criteria rule.

    Two gates:
      (1) MAX ``percent_complete`` across all plays in scope is <=
          ``wr.max_percent``;
      (2) ``last_watched_iso`` is older than ``wr.days`` days ago (if
          ``wr.days`` > 0; disabled otherwise).

    When no plays exist at all, ``require_plays`` decides whether the rule
    passes (candidate) or is skipped as "no data". ``last_watched_iso`` may
    be ``None`` when we have per-item play counts but no timestamp; the
    recency check is then treated as "unknown" and the rule still passes on
    the percent gate alone.
    """
    scope = f" for {scope_label}" if scope_label else ""
    if not has_plays:
        if wr.require_plays:
            # No play data to judge - don't block candidacy either way.
            results.append(RuleResult("watch_ratio_low", True,
                f"No plays recorded{scope} (rule requires plays, skipping)"))
        else:
            results.append(RuleResult("watch_ratio_low", True,
                f"Never attempted{scope} (treated as watch_ratio_low match)"))
        return

    # Percent gate first (cheap to compute + explain).
    if max_percent > wr.max_percent:
        results.append(RuleResult("watch_ratio_low", False,
            f"Max play completion {max_percent}%{scope} > {wr.max_percent}%",
            severity="blocking"))
        return

    # Optional recency gate. ``days`` <= 0 means "don't care when they watched".
    days_threshold = int(getattr(wr, "days", 0) or 0)
    if days_threshold > 0 and last_watched_iso:
        try:
            last_dt = datetime.fromisoformat(last_watched_iso.replace("Z", ""))
            days_since = (datetime.now() - last_dt).days
            if days_since < days_threshold:
                results.append(RuleResult("watch_ratio_low", False,
                    f"Max play completion {max_percent}%{scope} <= {wr.max_percent}% "
                    f"but last-watched {days_since}d ago (< {days_threshold}d threshold)",
                    severity="blocking"))
                return
            results.append(RuleResult("watch_ratio_low", True,
                f"Max play completion {max_percent}%{scope} <= {wr.max_percent}% "
                f"and last-watched {days_since}d ago (>= {days_threshold}d)"))
            return
        except (ValueError, TypeError):
            # Unparseable timestamp -- fall through to percent-only path.
            pass

    results.append(RuleResult("watch_ratio_low", True,
        f"Max play completion {max_percent}%{scope} <= {wr.max_percent}%"))


def is_candidate(results: list[RuleResult]) -> bool:
    """An item is a candidate if every blocking rule passed.

    We only gate on rules with severity "blocking"; informational rules such
    as ``debrid_cached`` never veto candidacy even when they "fail" (i.e. the
    item isn't cached). The previous form used ``or`` which flipped the
    semantics so that *any* blocking rule OR any non-debrid rule would need
    to pass, incorrectly excluding items based on informational checks.
    """
    return all(
        r.passed for r in results
        if r.severity == "blocking" and r.name != "debrid_cached"
    )
