"""Orchestrator - main collection-building logic, refactored from run()."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from datetime import date, datetime, timedelta

from . import config
from .constants import ItemStatus
from .database import get_db
from .core import plex, jellyfin, radarr, sonarr, overseerr
from .rules.engine import EvaluationContext, evaluate_movie, evaluate_show, evaluate_season, is_candidate
from .rules.criteria import CollectionCriteria
from .api.run import update_progress

log = logging.getLogger(__name__)


def _normalize_jellyfin_item(jf_item: dict) -> dict:
    """Convert a Jellyfin item to Plex-like format for the rule engine."""
    return {
        "ratingKey": jf_item.get("Id", ""),
        "title": jf_item.get("Name", ""),
        "viewCount": jf_item.get("UserData", {}).get("PlayCount", 0),
        "Guid": _build_jellyfin_guids(jf_item),
    }


def _build_jellyfin_guids(jf_item: dict) -> list[dict]:
    """Build Plex-style Guid list from Jellyfin ProviderIds."""
    guids = []
    providers = jf_item.get("ProviderIds", {})
    if "Tmdb" in providers:
        guids.append({"id": f"tmdb://{providers['Tmdb']}"})
    if "Tvdb" in providers:
        guids.append({"id": f"tvdb://{providers['Tvdb']}"})
    if "Imdb" in providers:
        guids.append({"id": f"imdb://{providers['Imdb']}"})
    return guids


def _log_activity(conn, event_type: str, **kwargs):
    conn.execute(
        "INSERT INTO activity_log (event_type, collection, rating_key, title, detail) VALUES (?, ?, ?, ?, ?)",
        (event_type, kwargs.get("collection"), kwargs.get("rating_key"),
         kwargs.get("title"), json.dumps(kwargs.get("detail", {}))),
    )


# Hard-coded upper bound for the partial-watch lookback. Individual rules may
# narrow this further via their own ``partially_watched.days`` parameter --
# populating a wider window here is cheap (a single aggregate query) and lets
# the rule engine compare against whichever threshold it wants.
PARTIALLY_WATCHED_LOOKBACK_DAYS = 60


def _set_ctx_fields(ctx: EvaluationContext, **fields) -> EvaluationContext:
    """Assign attrs on an ``EvaluationContext`` in-place, tolerating fields
    that don't exist on the dataclass yet.

    The rule-engine dataclass and this orchestrator may land in separate
    commits; guarding with setattr lets either ship first without the other
    crashing. Unknown fields are silently ignored -- rule code that reads them
    will see the dataclass default (empty set/dict), which is the safe fallback.
    """
    for name, value in fields.items():
        try:
            setattr(ctx, name, value)
        except Exception as e:
            log.warning("EvaluationContext missing field %s (%s); skipping", name, e)
    return ctx


def _build_extended_context(
    plex_movies: list[dict],
    plex_tv: list[dict],
    radarr_movies: dict[int, dict],
    sonarr_shows: dict[int, dict],
    plex_url: str | None, plex_token: str | None,
    movies_section: int | str | None, tv_section: int | str | None,
    overseerr_url: str | None, overseerr_key: str | None,
    jf_url: str | None = None, jf_key: str | None = None,
    jf_movies_section: str | None = None, jf_tv_section: str | None = None,
) -> dict:
    """Fetch + derive the data fields shared across every rule evaluation.

    Returns a dict whose keys mirror the new ``EvaluationContext`` fields so
    callers can drop it straight onto ``movie_ctx`` / ``tv_ctx`` via
    :func:`_set_ctx_fields`. Every sub-fetch is wrapped in try/except so a
    single upstream outage doesn't kill the whole run.
    """
    result: dict = {
        "plex_favorited_keys": set(),
        "watchlist_keys": set(),
        "partially_watched_keys": set(),
        "partially_watched_by_title": set(),
        "partially_watched_by_season": set(),
        "added_at_by_key": {},
        "show_season_counts": {},
        "max_percent_by_key": {},
        "max_percent_by_title": {},
        "max_percent_by_season": {},
    }

    # ------------------------------------------------------------------
    # added_at_by_key -- trivially derived from the cached library items.
    # We prefer the season's own addedAt where present; seasons without one
    # fall back to the show's addedAt during orchestration, but this map
    # stores only what's explicitly on each item.
    # ------------------------------------------------------------------
    added: dict[str, int] = {}
    for it in plex_movies:
        rk = str(it.get("ratingKey", ""))
        if rk and it.get("addedAt"):
            try:
                added[rk] = int(it["addedAt"])
            except (TypeError, ValueError):
                continue
    for it in plex_tv:
        rk = str(it.get("ratingKey", ""))
        if rk and it.get("addedAt"):
            try:
                added[rk] = int(it["addedAt"])
            except (TypeError, ValueError):
                continue
    result["added_at_by_key"] = added

    # ------------------------------------------------------------------
    # plex_favorited_keys -- union of hearted keys across movie + TV libs.
    # Plex surfaces this as ``userRating`` on library items; if present on the
    # items we already have, read it directly. Otherwise, fall back to an
    # explicit filtered query to the /all endpoint.
    # ------------------------------------------------------------------
    fav: set[str] = set()
    have_user_rating = any("userRating" in it for it in plex_movies) or \
                       any("userRating" in it for it in plex_tv)
    if have_user_rating:
        for it in plex_movies + plex_tv:
            ur = it.get("userRating")
            if ur is not None:
                try:
                    if float(ur) > 0:
                        fav.add(str(it["ratingKey"]))
                except (TypeError, ValueError):
                    continue
    else:
        # Explicit fallback query (one per library section).
        try:
            if plex_url and plex_token and movies_section:
                fav.update(plex.fetch_favorited_keys(plex_url, plex_token, int(movies_section)))
        except Exception as e:
            log.warning("Favorites (movies) fetch failed: %s", e)
        try:
            if plex_url and plex_token and tv_section:
                fav.update(plex.fetch_favorited_keys(plex_url, plex_token, int(tv_section)))
        except Exception as e:
            log.warning("Favorites (TV) fetch failed: %s", e)
    result["plex_favorited_keys"] = fav
    log.info("Extended ctx: %d hearted items", len(fav))

    # ------------------------------------------------------------------
    # watchlist_keys -- resolve Overseerr TMDB ids to Plex rating_keys via the
    # Radarr movies table (tmdb -> movie) and the Plex TV library's Guid list
    # (tmdb -> show). Fall back to sonarr_shows.tmdbId when Plex's TV items
    # don't expose the TMDB guid.
    # ------------------------------------------------------------------
    watchlist_rks: set[str] = set()
    try:
        wl_items = overseerr.fetch_all_watchlists(overseerr_url, overseerr_key)
    except Exception as e:
        log.warning("Seerr watchlist fetch failed: %s", e)
        wl_items = []
    if wl_items:
        # Build tmdb -> rating_key lookups once.
        movie_tmdb_to_rk: dict[int, str] = {}
        for it in plex_movies:
            rk = str(it.get("ratingKey", ""))
            if not rk:
                continue
            try:
                tmdb_s = plex.external_id(it, "tmdb")
            except Exception:
                tmdb_s = None
            if tmdb_s:
                try:
                    movie_tmdb_to_rk[int(tmdb_s)] = rk
                except (TypeError, ValueError):
                    continue
        show_tmdb_to_rk: dict[int, str] = {}
        show_tvdb_to_rk: dict[int, str] = {}
        for it in plex_tv:
            rk = str(it.get("ratingKey", ""))
            if not rk:
                continue
            try:
                tmdb_s = plex.external_id(it, "tmdb")
                tvdb_s = plex.external_id(it, "tvdb")
            except Exception:
                tmdb_s, tvdb_s = None, None
            if tmdb_s:
                try:
                    show_tmdb_to_rk[int(tmdb_s)] = rk
                except (TypeError, ValueError):
                    pass
            if tvdb_s:
                try:
                    show_tvdb_to_rk[int(tvdb_s)] = rk
                except (TypeError, ValueError):
                    pass

        # Sonarr sometimes carries a tmdbId; use that as a cross-index to fill
        # gaps when Plex doesn't expose a TV show's TMDB guid.
        sonarr_tmdb_to_tvdb: dict[int, int] = {}
        for show in sonarr_shows.values():
            st = show.get("tmdbId")
            sv = show.get("tvdbId")
            if st and sv:
                try:
                    sonarr_tmdb_to_tvdb[int(st)] = int(sv)
                except (TypeError, ValueError):
                    continue

        # Jellyfin TMDB -> ItemId maps. Built lazily so we only pay the
        # library fetch when Jellyfin is configured AND there are watchlist
        # entries to resolve.
        jf_movie_tmdb_to_id: dict[int, str] = {}
        jf_show_tmdb_to_id: dict[int, str] = {}
        if jf_url and jf_key:
            try:
                if jf_movies_section:
                    for it in jellyfin.fetch_library(jf_url, jf_key, str(jf_movies_section)):
                        iid = it.get("Id")
                        tmdb_s = jellyfin.external_id(it, "tmdb")
                        if iid and tmdb_s:
                            try:
                                jf_movie_tmdb_to_id[int(tmdb_s)] = str(iid)
                            except (TypeError, ValueError):
                                pass
                if jf_tv_section:
                    for it in jellyfin.fetch_library(jf_url, jf_key, str(jf_tv_section)):
                        iid = it.get("Id")
                        tmdb_s = jellyfin.external_id(it, "tmdb")
                        if iid and tmdb_s:
                            try:
                                jf_show_tmdb_to_id[int(tmdb_s)] = str(iid)
                            except (TypeError, ValueError):
                                pass
            except Exception as e:
                log.warning("Jellyfin TMDB map build failed: %s", e)

        for entry in wl_items:
            tmdb = entry.get("tmdbId")
            mt = entry.get("mediaType")
            if not tmdb:
                continue
            try:
                tmdb_i = int(tmdb)
            except (TypeError, ValueError):
                continue
            if mt == "movie":
                rk = movie_tmdb_to_rk.get(tmdb_i)
                if rk:
                    watchlist_rks.add(rk)
                jf_id = jf_movie_tmdb_to_id.get(tmdb_i)
                if jf_id:
                    watchlist_rks.add(jf_id)
            elif mt == "tv":
                rk = show_tmdb_to_rk.get(tmdb_i)
                if not rk:
                    tvdb_i = sonarr_tmdb_to_tvdb.get(tmdb_i)
                    if tvdb_i:
                        rk = show_tvdb_to_rk.get(tvdb_i)
                if rk:
                    watchlist_rks.add(rk)
                jf_id = jf_show_tmdb_to_id.get(tmdb_i)
                if jf_id:
                    watchlist_rks.add(jf_id)
    result["watchlist_keys"] = watchlist_rks
    log.info("Extended ctx: %d watchlist keys (from %d watchlist entries; plex movies=%d, plex shows=%d, jellyfin movies=%d, jellyfin shows=%d)",
             len(watchlist_rks), len(wl_items) if wl_items else 0,
             len(movie_tmdb_to_rk) if wl_items else 0,
             len(show_tmdb_to_rk) if wl_items else 0,
             len(jf_movie_tmdb_to_id) if wl_items else 0,
             len(jf_show_tmdb_to_id) if wl_items else 0)

    # ------------------------------------------------------------------
    # Partial watches + max percent aggregates -- all derived from
    # watch_history in a single connection.
    # ------------------------------------------------------------------
    try:
        conn = get_db()
        cutoff = (datetime.now() - timedelta(days=PARTIALLY_WATCHED_LOOKBACK_DAYS)).isoformat()

        partial_keys: set[str] = set()
        partial_titles: set[str] = set()
        partial_seasons: set[str] = set()
        # Partial = started but didn't finish. We now source ``percent_complete``
        # straight from the media server's last-playback offset (see
        # plex.fetch_session_history / jellyfin.fetch_watch_history). Anything
        # between 5% and 95% inclusive is treated as "meaningfully started
        # but not completed"; the lower bound avoids trivial skims and the
        # upper bound covers credits/outro trims that Plex leaves behind.
        partial_rows = conn.execute("""
            SELECT rating_key, grandparent_title, season_number
            FROM watch_history
            WHERE percent_complete BETWEEN 5 AND 95
              AND watched_at >= ?
        """, (cutoff,)).fetchall()
        for r in partial_rows:
            rk = r["rating_key"]
            if rk:
                partial_keys.add(str(rk))
            gt = r["grandparent_title"]
            if gt:
                partial_titles.add(gt.lower())
                if r["season_number"] is not None:
                    partial_seasons.add(f"{gt.lower()}:S{r['season_number']}")
        result["partially_watched_keys"] = partial_keys
        result["partially_watched_by_title"] = partial_titles
        result["partially_watched_by_season"] = partial_seasons
        log.info(
            "Extended ctx: %d partial rating_keys, %d partial show titles, %d partial seasons (last %dd)",
            len(partial_keys), len(partial_titles), len(partial_seasons),
            PARTIALLY_WATCHED_LOOKBACK_DAYS,
        )

        max_by_key: dict[str, int] = {}
        max_by_title: dict[str, int] = {}
        max_by_season: dict[str, int] = {}
        max_rows = conn.execute("""
            SELECT rating_key, grandparent_title, season_number,
                   MAX(percent_complete) AS max_pct
            FROM watch_history
            GROUP BY rating_key, grandparent_title, season_number
        """).fetchall()
        for r in max_rows:
            pct = r["max_pct"] or 0
            rk = r["rating_key"]
            if rk:
                rk_s = str(rk)
                if pct > max_by_key.get(rk_s, -1):
                    max_by_key[rk_s] = int(pct)
            gt = r["grandparent_title"]
            if gt:
                gtl = gt.lower()
                if pct > max_by_title.get(gtl, -1):
                    max_by_title[gtl] = int(pct)
                if r["season_number"] is not None:
                    sk = f"{gtl}:S{r['season_number']}"
                    if pct > max_by_season.get(sk, -1):
                        max_by_season[sk] = int(pct)
        result["max_percent_by_key"] = max_by_key
        result["max_percent_by_title"] = max_by_title
        result["max_percent_by_season"] = max_by_season
        log.info("Extended ctx: max_percent for %d keys / %d titles / %d seasons",
                 len(max_by_key), len(max_by_title), len(max_by_season))

        conn.close()
    except Exception as e:
        log.warning("Extended ctx: watch_history aggregation failed: %s", e)

    # ------------------------------------------------------------------
    # show_season_counts -- pure derivation from sonarr_shows, no network.
    # ------------------------------------------------------------------
    try:
        result["show_season_counts"] = sonarr.build_season_counts(sonarr_shows)
        log.info("Extended ctx: season counts for %d shows",
                 len(result["show_season_counts"]))
    except Exception as e:
        log.warning("Extended ctx: season count derivation failed: %s", e)

    return result


# Protection signals used when computing show-level protection keys for
# season-granularity rules. Each key names a field on EvaluationContext
# or an inline check; the function below applies the relevant signals to
# every show in the TV library and returns the protected set.
def _compute_show_level_protection_keys(
    plex_tv: list[dict],
    ctx: EvaluationContext,
    criteria: CollectionCriteria,
) -> set[str]:
    """Return the set of show rating_keys that would be protected under
    show-level rules given the supplied ``criteria``.

    This mirrors the ``no_keep_tag``, Seerr-request, and keep-collection
    protections but intentionally does NOT include anything watch-based --
    show-level watch state isn't what ``series_protection`` wants to capture
    (the rule author's intent is "the SHOW is protected from blanket deletion,
    so don't nuke its seasons either"). Admin-favorited and watchlist
    protections apply per the task description.
    """
    from .core.plex import external_id as _ext

    protected: set[str] = set()
    protected_tags = list(criteria.protected_tags or [])
    protected_users = set(criteria.protected_users or [])
    plex_keep_keys = ctx.plex_keep_keys or set()
    favorited = getattr(ctx, "plex_favorited_keys", set()) or set()
    watchlist = getattr(ctx, "watchlist_keys", set()) or set()

    for show in plex_tv:
        rk = str(show.get("ratingKey", ""))
        if not rk:
            continue

        # (1) Plex keep collection
        if rk in plex_keep_keys:
            protected.add(rk)
            continue

        # (2) Admin-favorited
        if rk in favorited:
            protected.add(rk)
            continue

        # (3) On any Overseerr watchlist
        if rk in watchlist:
            protected.add(rk)
            continue

        # External IDs for remaining checks
        try:
            tvdb_s = _ext(show, "tvdb")
            tmdb_s = _ext(show, "tmdb")
        except Exception:
            tvdb_s, tmdb_s = None, None
        tvdb = int(tvdb_s) if tvdb_s else None
        tmdb = int(tmdb_s) if tmdb_s else None

        # (4) Has any protected Sonarr tag
        if protected_tags and tvdb and tvdb in ctx.sonarr_shows:
            item_tags = ctx.sonarr_shows[tvdb].get("_tag_names", []) or []
            if any(t in item_tags for t in protected_tags):
                protected.add(rk)
                continue

        # (5) Active Seerr request (not yet fulfilled by requester). We
        # consider the show protected if it has an active request at all --
        # the per-rule evaluator will re-check whether the requester has
        # watched it. Here we just want "would the show-level rule keep it?".
        if ((tvdb and tvdb in ctx.overseerr_active_shows)
                or (tmdb and tmdb in ctx.overseerr_active_shows_tmdb)):
            protected.add(rk)
            continue

        # (6) Protected-user request
        if ((tvdb and tvdb in ctx.overseerr_protected_shows)
                or (tmdb and tmdb in ctx.overseerr_protected_shows_tmdb)):
            protected.add(rk)
            continue

        # (7) Protected user has watched any episode of this show
        show_title = (show.get("title") or "").lower()
        if protected_users:
            for u in protected_users:
                watched = ctx.user_watches.get(u, set())
                if watched and (rk in watched or (show_title and show_title in watched)):
                    protected.add(rk)
                    break

    return protected


def _process_collection(
    conn,
    collection_name: str,
    plex_items: list[dict],
    evaluate_fn,
    ctx: EvaluationContext,
    criteria: CollectionCriteria,
    section_id: int | str,
    media_type: str,
    dry_run: bool,
    today: date,
    library_source: str = "plex",
    deferred_syncs: dict | None = None,
):
    """Process a single collection: evaluate rules per criteria, update state, fire actions."""
    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    today_str = today.isoformat()
    grace_date = (today + timedelta(days=criteria.grace_days)).isoformat()

    total = len(plex_items)
    update_progress(
        phase="processing_collection",
        detail=f"{collection_name} -- {total} items",
        percent=0,
        items_processed=0,
        items_total=total,
    )

    want: set[str] = set()

    # Accumulate rule_results rows to bulk-insert after the loop. One DELETE
    # + one executemany INSERT is dramatically cheaper than per-item
    # DELETE+INSERT pairs; the rule-result rows are only read back at the
    # end of the run, so there's no need to materialize them eagerly.
    rule_rows: list[tuple] = []
    conn.execute(
        "DELETE FROM rule_results WHERE collection = ?", (collection_name,),
    )

    from .core.plex import external_id

    for idx, item in enumerate(plex_items, 1):
        rk = str(item["ratingKey"])
        title = item.get("title", rk)

        results = evaluate_fn(item, ctx, criteria=criteria)

        for r in results:
            rule_rows.append(
                (rk, collection_name, r.name, r.passed, r.detail, r.severity)
            )

        # Update progress every 50 items or on the last item
        if idx % 50 == 0 or idx == total:
            update_progress(
                phase="processing_collection",
                detail=f"{collection_name} ({idx}/{total})",
                percent=int(idx * 100 / total) if total else 100,
                items_processed=idx,
                items_total=total,
            )

        if not is_candidate(results):
            continue

        want.add(rk)

        existing = conn.execute(
            "SELECT id, first_seen FROM items WHERE rating_key = ? AND collection = ?",
            (rk, collection_name),
        ).fetchone()

        if not existing:
            reasons = [r.detail for r in results if r.passed]
            reason = "; ".join(reasons) if reasons else "matched all criteria"
            _log_activity(conn, "item_added", collection=collection_name,
                         rating_key=rk, title=title,
                         detail={"dry_run": dry_run, "reason": reason})

        if dry_run:
            continue

        tmdb_id_raw = external_id(item, "tmdb")
        tvdb_id_raw = external_id(item, "tvdb")
        tmdb_id = int(tmdb_id_raw) if tmdb_id_raw else None
        tvdb_id = int(tvdb_id_raw) if tvdb_id_raw else None
        imdb_id = external_id(item, "imdb") or None
        size = 0
        arr_id = None

        if media_type == "movie" and tmdb_id and tmdb_id in ctx.radarr_movies:
            m = ctx.radarr_movies[tmdb_id]
            size = m.get("sizeOnDisk", 0)
            arr_id = m.get("id")
        elif media_type == "show" and tvdb_id and tvdb_id in ctx.sonarr_shows:
            s = ctx.sonarr_shows[tvdb_id]
            arr_id = s.get("id")
            # For season-level items, get per-season size from Sonarr's seasons array
            item_season_number_for_size = item.get("_season_number")
            if item_season_number_for_size is not None:
                for sonarr_season in s.get("seasons", []):
                    if sonarr_season.get("seasonNumber") == item_season_number_for_size:
                        size = sonarr_season.get("statistics", {}).get("sizeOnDisk", 0)
                        break
            else:
                size = s.get("statistics", {}).get("sizeOnDisk", 0)

        item_season_number = item.get("_season_number")
        item_show_rating_key = item.get("_show_rating_key")

        if existing:
            # Decide whether to recompute grace_expires. We want to preserve
            # the existing grace window across runs for continuously-matching
            # items (so the clock actually runs out). We only reset it when:
            #   (a) grace_expires was legacy-null-equivalent (equal to
            #       first_seen -- our old migration bug),
            #   (b) the collection's grace_days has changed since the item
            #       was staged (user edited the rule), OR
            #   (c) the item is re-entering after being ``kept`` or
            #       ``actioned`` -- a fresh grace window is appropriate.
            existing_row = conn.execute(
                "SELECT grace_expires, first_seen, status FROM items WHERE id = ?",
                (existing["id"],),
            ).fetchone()
            stored_grace = existing_row["grace_expires"] if existing_row else None
            stored_first_seen = existing_row["first_seen"] if existing_row else None
            stored_status = existing_row["status"] if existing_row else None

            # Detect grace_days change by comparing stored grace window length
            # against the current criteria.grace_days.
            grace_days_changed = False
            if stored_grace and stored_first_seen:
                from datetime import date as _date
                try:
                    stored_grace_d = _date.fromisoformat(stored_grace)
                    stored_first_seen_d = _date.fromisoformat(stored_first_seen)
                    stored_days = (stored_grace_d - stored_first_seen_d).days
                    grace_days_changed = stored_days != criteria.grace_days
                except ValueError:
                    grace_days_changed = True  # unparseable, recompute to heal

            reentry = stored_status in (str(ItemStatus.KEPT), str(ItemStatus.ACTIONED))
            legacy_unset = stored_grace and stored_first_seen and stored_grace == stored_first_seen

            fix_grace = ""
            fix_params = [today_str, title, tmdb_id, tvdb_id, imdb_id, arr_id,
                          item_season_number, item_show_rating_key, size,
                          str(ItemStatus.STAGED)]
            if legacy_unset or grace_days_changed or reentry:
                fix_grace = ", grace_expires = ?"
                fix_params.append(grace_date)
            fix_params.append(existing["id"])
            conn.execute(f"""
                UPDATE items SET last_seen = ?, title = ?, tmdb_id = ?, tvdb_id = ?,
                       imdb_id = ?, arr_id = ?, season_number = ?, show_rating_key = ?,
                       size_bytes = ?, status = ?{fix_grace}
                WHERE id = ?
            """, fix_params)
        else:
            conn.execute("""
                INSERT INTO items (rating_key, collection, title, media_type, tmdb_id, tvdb_id,
                       imdb_id, arr_id, season_number, show_rating_key, size_bytes,
                       first_seen, last_seen, grace_expires, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (rk, collection_name, title, media_type, tmdb_id, tvdb_id,
                  imdb_id, arr_id, item_season_number, item_show_rating_key, size,
                  today_str, today_str, grace_date, str(ItemStatus.STAGED)))

            # Fire ``immediate`` notify steps on first detection. After-grace
            # notifies are handled by ``_run_after_grace_for_item`` later.
            # We fire here (not in the dry-run branch) so dry-runs don't
            # spam Apprise; the item_added activity_log line is enough.
            try:
                _fire_immediate_notifies(
                    pipeline=(criteria.action_pipeline or []),
                    item_row={
                        "rating_key": rk,
                        "title": title,
                        "collection": collection_name,
                        "media_type": media_type,
                        "arr_id": arr_id,
                        "season_number": item_season_number,
                        "episode_number": item.get("_episode_number"),
                        "show_title": item.get("_show_title") or item.get("grandparentTitle"),
                        "size_bytes": size,
                        "grace_expires": grace_date,
                    },
                )
            except Exception as e:
                log.warning("Immediate notify failed for %s: %s", title, e)

    # Bulk-insert accumulated rule_results rows. This must run before the
    # stale-items detection below because that code reads rule_results to
    # explain why an item was dropped.
    if rule_rows:
        conn.executemany(
            "INSERT INTO rule_results (rating_key, collection, rule_name, passed, detail, severity)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            rule_rows,
        )

    # Check for items that would be / are removed
    tracked_rks = {row["rating_key"] for row in conn.execute(
        "SELECT rating_key FROM items WHERE collection = ? AND status = ?",
        (collection_name, str(ItemStatus.STAGED)),
    ).fetchall()}
    stale = tracked_rks - want
    if stale:
        for rk in stale:
            row = conn.execute(
                "SELECT title FROM items WHERE rating_key = ? AND collection = ?",
                (rk, collection_name)).fetchone()
            title = row["title"] if row else rk
            # Log why: check rule_results for blocking rules
            blockers = conn.execute("""
                SELECT rule_name, detail FROM rule_results
                WHERE rating_key = ? AND collection = ? AND passed = 0
            """, (rk, collection_name)).fetchall()
            reason = "; ".join(b['detail'] for b in blockers) if blockers else "no longer matches criteria"
            _log_activity(conn, "item_removed", collection=collection_name,
                         rating_key=rk, title=title,
                         detail={"dry_run": dry_run, "reason": reason})
            if not dry_run:
                conn.execute(
                    "DELETE FROM items WHERE rating_key = ? AND collection = ? AND status = ?",
                    (rk, collection_name, str(ItemStatus.STAGED)),
                )
        log.info("%s: %s %d stale items", collection_name,
                 "would remove" if dry_run else "removed", len(stale))

    # Determine Plex media type for collection sync
    # 1=movie, 2=show, 3=season, 4=episode
    plex_col_type = None
    if media_type == "movie":
        plex_col_type = 1
    elif criteria.granularity == "season":
        plex_col_type = 3
    else:
        plex_col_type = 2

    # Defer collection syncs - accumulate want sets per collection name.
    # Multiple rules can contribute to the same Plex/Jellyfin collection.
    # The orchestrator will union all want sets and sync once after all rules.
    if not dry_run and deferred_syncs is not None:
        # Always sync to the rule's own collection name
        deferred_syncs.setdefault(
            (collection_name, section_id, plex_col_type, library_source), set()
        ).update(want)

        # Pipeline sync_collection steps add to additional collections
        for step in (criteria.action_pipeline or []):
            if step.get("type") == "sync_collection" and step.get("command"):
                col_names = [c.strip() for c in step["command"].split(",") if c.strip()]
                for col_name in col_names:
                    deferred_syncs.setdefault(
                        (col_name, section_id, plex_col_type, library_source), set()
                    ).update(want)

    log.info("%s: %d candidates", collection_name, len(want))
    return want


def _load_collection_configs(conn) -> list[dict]:
    """Load all enabled collection configs from the database."""
    rows = conn.execute(
        "SELECT * FROM collection_config WHERE enabled = 1 ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


IMMEDIATE_ACTION_TYPES = frozenset({
    "sync_collection", "add_arr_tag", "remove_arr_tag", "notify",
})

# Action types that leave the item's state unchanged -- ``notify`` emits an
# external signal but doesn't count as "the item was dealt with", so an
# after-grace pipeline composed of only notify steps keeps the item at
# ``staged`` rather than promoting it to ``actioned``/``migrated``.
NONDESTRUCTIVE_ACTION_TYPES = frozenset({"notify"})


def _format_size(bytes_: int | None) -> str:
    """Pretty byte size for notification templates -- matches UI conventions."""
    if not bytes_ or bytes_ <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    n = float(bytes_)
    idx = 0
    while n >= 1024 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.1f} {units[idx]}"


def _render_notify_template(template: str, substitutions: dict) -> str:
    """Safe ``str.format_map``-style substitution.

    Missing/unknown placeholders are left literally intact rather than raising
    KeyError -- rules shouldn't crash if a template mentions a field this
    item doesn't have (e.g. ``{show}`` on a movie).
    """
    class _Safe(dict):
        def __missing__(self, key):
            return "{" + key + "}"
    try:
        return template.format_map(_Safe(substitutions))
    except Exception as e:
        log.warning("notify template render failed (%s); using raw template", e)
        return template


def _send_notify(apprise_url: str, body: str, title: str) -> tuple[bool, str]:
    """POST a notification to an Apprise endpoint. Returns (ok, detail).

    Best-effort: any failure is returned as a string for the summary rather
    than raising, since notifications are informational and shouldn't abort
    the action pipeline.
    """
    if not apprise_url:
        return False, "APPRISE_URL not configured"
    try:
        from .core.clients import get_client
        r = get_client().post(
            apprise_url,
            json={"body": body, "title": title, "tag": "media"},
            timeout=10,
        )
        if r.status_code >= 400:
            return False, f"apprise HTTP {r.status_code}"
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _fire_immediate_notifies(pipeline: list[dict], item_row: dict) -> None:
    """Fire all ``immediate``-timed steps on a newly staged item.

    Handles notify (Apprise), add_arr_tag, and remove_arr_tag. Other step
    types are ignored here -- they only run after grace. Name is kept for
    backward compat even though the function now fans out by type; the
    sole caller (line ~642) treats this as an opaque "do immediate stuff".
    """
    from .core import arr_instances
    apprise_url = config.get("apprise_url") or ""
    media_type = item_row.get("media_type") or ""
    arr_id = item_row.get("arr_id")
    title = item_row.get("title") or item_row.get("rating_key") or ""

    for step in pipeline:
        stype = step.get("type")
        if _step_timing(step) != "immediate":
            continue
        if stype == "notify":
            template = (step.get("command") or "").strip()
            if not template:
                template = "Reclaimer: {collection} flagged '{title}' for action on {grace_expires}"
            subs = _build_notify_substitutions(
                item_row, item_row.get("collection") or "",
                item_row.get("grace_expires"),
                "detected",
            )
            body = _render_notify_template(template, subs)
            ntitle = f"Reclaimer -- {item_row.get('collection') or 'rule'}"
            ok_n, detail_n = _send_notify(apprise_url, body, ntitle)
            if not ok_n:
                log.warning("immediate notify failed for %s: %s", title, detail_n)
        elif stype in ("add_arr_tag", "remove_arr_tag"):
            label = (step.get("command") or "").strip()
            if not label or not arr_id:
                continue
            kind = "radarr" if media_type == "movie" else "sonarr"
            iid_raw = step.get("instance_id")
            try:
                iid = int(iid_raw) if iid_raw not in (None, "", 0, "0") else None
            except (TypeError, ValueError):
                iid = None
            inst = arr_instances.resolve(iid, kind)
            if not inst:
                log.warning("immediate %s: no %s instance configured for %s",
                            stype, kind, title)
                continue
            mod = radarr if kind == "radarr" else sonarr
            try:
                if stype == "add_arr_tag":
                    mod.add_tag(inst["url"], inst["api_key"], arr_id, label, title)
                else:
                    mod.remove_tag(inst["url"], inst["api_key"], arr_id, label, title)
            except Exception as e:
                log.warning("immediate %s failed for %s: %s", stype, title, e)


def _build_notify_substitutions(item: dict, collection: str,
                                 grace_expires: str | None,
                                 action_summary: str) -> dict:
    """Assemble the template-variable dict used by notify steps."""
    season = item.get("season_number")
    return {
        "title": item.get("title") or item.get("rating_key") or "",
        "show": item.get("show_title") or item.get("grandparent_title") or "",
        "season": f"S{int(season):02d}" if season is not None else "",
        "episode": f"E{int(item['episode_number']):02d}" if item.get("episode_number") is not None else "",
        "collection": collection or "",
        "size": _format_size(item.get("size_bytes") or 0),
        "grace_expires": grace_expires or "",
        "action_summary": action_summary or "",
    }


def _step_timing(step: dict) -> str:
    """Return 'immediate' or 'after_grace' for a pipeline step, honoring the
    user's explicit override (step['timing']) and falling back to the type's
    default (sync_collection/add_arr_tag default to immediate; everything
    else defaults to after_grace)."""
    t = step.get("timing")
    if t in ("immediate", "after_grace"):
        return t
    return "immediate" if step.get("type") in IMMEDIATE_ACTION_TYPES else "after_grace"


def _run_after_grace_for_item(
    item: dict,
    steps: list[dict],
    dry_run: bool,
) -> tuple[bool, str, str | None, str]:
    """Execute after-grace steps for a single item.

    Returns ``(ok, summary, error, final_status)``. ``final_status`` tells the
    caller which ``items.status`` to write:
      * ``"migrated"`` when the pipeline downgrades quality or migrates the
        item to another instance (non-deletion, non-retention).
      * ``"staged"`` when the pipeline ran only informational steps (e.g.
        notify) -- the item hasn't been dealt with, keep watching it.
      * ``"actioned"`` for every destructive / terminal pipeline.

    Each step may carry an ``instance_id`` pointing at a specific
    ``arr_instances`` row; absent that, the default instance of the
    relevant kind is used.
    """
    from .core import arr_instances

    def _arr_for(step: dict | None, kind: str) -> tuple[str | None, str | None, str]:
        """Resolve (url, key, display_name) for an arr step. Falls back to
        the default instance when the step has no ``instance_id`` or the
        referenced instance doesn't exist / doesn't match the kind."""
        iid = None
        if step is not None:
            raw = step.get("instance_id")
            if raw not in (None, "", 0, "0"):
                try:
                    iid = int(raw)
                except (TypeError, ValueError):
                    iid = None
        inst = arr_instances.resolve(iid, kind)
        if not inst:
            return None, None, "radarr" if kind == "radarr" else "sonarr"
        return inst["url"], inst["api_key"], inst["name"]

    types = {s.get("type") for s in steps}
    # Destructive arr ops are coalesced into a single DELETE call with
    # appropriate flags. We grab instance_id from any of the contributing
    # steps (they must target the same arr by convention).
    destructive_step = next(
        (s for s in steps if s.get("type") in ("delete", "delete_files", "import_exclusion")),
        None,
    )
    unmonitor_step = next((s for s in steps if s.get("type") == "unmonitor"), None)
    has_delete = "delete" in types
    has_delete_files = "delete_files" in types
    has_import_excl = "import_exclusion" in types
    has_unmonitor = "unmonitor" in types
    has_downgrade = "downgrade_quality" in types or "swap_quality_profile" in types
    scripts = [s for s in steps if s.get("type") == "script" and s.get("command")]
    notify_steps = [s for s in steps if s.get("type") == "notify"]
    downgrade_steps = [s for s in steps
                       if s.get("type") in ("downgrade_quality", "swap_quality_profile")]

    # Ordered "verb" steps -- run in pipeline order rather than coalesced,
    # since their sequencing matters (e.g. set_root_folder before trigger_search).
    verb_types = ("add_arr_tag", "remove_arr_tag", "set_root_folder",
                  "trigger_search", "migrate_to_instance")
    verb_steps = [s for s in steps if s.get("type") in verb_types]

    media_type = item["media_type"]
    arr_id = item["arr_id"]
    title = item["title"] or item["rating_key"]
    season_num = item["season_number"]

    needs_arr = (has_delete or has_delete_files or has_import_excl
                 or has_unmonitor or has_downgrade or bool(verb_steps))
    if needs_arr and not arr_id:
        return False, "", f"no arr_id recorded for {media_type} {title!r}", str(ItemStatus.STAGED)

    actions: list[str] = []
    destructive_ran = False
    migration_ran = False

    try:
        if media_type == "movie":
            if has_delete or has_delete_files or has_import_excl:
                url, key, name = _arr_for(destructive_step, "radarr")
                if not (url and key):
                    return False, "", "Radarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    radarr.delete(
                        url, key, arr_id, title,
                        delete_files=has_delete_files,
                        add_exclusion=has_import_excl,
                    )
                bits = [f"deleted from {name}"]
                if has_delete_files: bits.append("files")
                if has_import_excl: bits.append("exclusion")
                actions.append(" + ".join(bits))
                destructive_ran = True
            elif has_unmonitor:
                url, key, name = _arr_for(unmonitor_step, "radarr")
                if not (url and key):
                    return False, "", "Radarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    radarr.unmonitor(url, key, arr_id, title)
                actions.append(f"unmonitored in {name}")
                destructive_ran = True

            if has_downgrade:
                url, key, name = _arr_for(downgrade_steps[0], "radarr")
                if not (url and key):
                    return False, "", "Radarr URL/API key not configured", str(ItemStatus.STAGED)
                qp_name = (downgrade_steps[0].get("command") or "").strip()
                ok_dq, detail_dq = _apply_downgrade_movie(
                    url, key, arr_id, qp_name, title, dry_run,
                )
                if not ok_dq:
                    return False, "", detail_dq, str(ItemStatus.STAGED)
                actions.append(f"{detail_dq} ({name})")
                migration_ran = True

        elif media_type == "show":
            if has_delete or has_delete_files:
                url, key, name = _arr_for(destructive_step, "sonarr")
                if not (url and key):
                    return False, "", "Sonarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    sonarr.delete(url, key, arr_id, title,
                                  delete_files=has_delete_files)
                actions.append(f"deleted from {name}" + (" + files" if has_delete_files else ""))
                destructive_ran = True
            elif has_unmonitor:
                url, key, name = _arr_for(unmonitor_step, "sonarr")
                if not (url and key):
                    return False, "", "Sonarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    sonarr.unmonitor(url, key, arr_id, title)
                actions.append(f"unmonitored in {name}")
                destructive_ran = True

            if has_downgrade:
                url, key, name = _arr_for(downgrade_steps[0], "sonarr")
                if not (url and key):
                    return False, "", "Sonarr URL/API key not configured", str(ItemStatus.STAGED)
                qp_name = (downgrade_steps[0].get("command") or "").strip()
                ok_dq, detail_dq = _apply_downgrade_show(
                    url, key, arr_id, qp_name, title, dry_run,
                )
                if not ok_dq:
                    return False, "", detail_dq, str(ItemStatus.STAGED)
                actions.append(f"{detail_dq} ({name})")
                migration_ran = True

        elif media_type == "season":
            if season_num is None:
                return False, "", "season item missing season_number", str(ItemStatus.STAGED)
            if has_delete_files or has_delete:
                url, key, name = _arr_for(destructive_step, "sonarr")
                if not (url and key):
                    return False, "", "Sonarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    sonarr.delete_season_files(url, key, arr_id, season_num, title)
                actions.append(f"deleted S{season_num:02d} files ({name})")
                destructive_ran = True
            if has_unmonitor:
                url, key, name = _arr_for(unmonitor_step, "sonarr")
                if not (url and key):
                    return False, "", "Sonarr URL/API key not configured", str(ItemStatus.STAGED)
                if not dry_run:
                    sonarr.unmonitor_season(url, key, arr_id, season_num, title)
                actions.append(f"unmonitored S{season_num:02d} ({name})")
                destructive_ran = True
            if has_downgrade:
                # Quality profile is series-level in Sonarr, so a season-
                # scoped rule can't downgrade without affecting other seasons.
                return False, "", "swap_quality_profile is not supported for season-granularity rules", str(ItemStatus.STAGED)

        else:
            return False, "", f"unsupported media_type: {media_type!r}", str(ItemStatus.STAGED)

        # --- Ordered verbs (tag add/remove, root-folder move, search,
        # cross-instance migration). Run in the order the user wrote them.
        for vs in verb_steps:
            vtype = vs.get("type")
            kind = "radarr" if media_type == "movie" else "sonarr"
            # season-granularity supports only a narrow slice
            if media_type == "season" and vtype in ("set_root_folder", "migrate_to_instance"):
                return False, "", f"{vtype} is not supported for season-granularity rules", str(ItemStatus.STAGED)

            mod = radarr if kind == "radarr" else sonarr
            url, key, name = _arr_for(vs, kind)
            if not (url and key):
                return False, "", f"{name} URL/API key not configured", str(ItemStatus.STAGED)

            if vtype == "add_arr_tag":
                label = (vs.get("command") or "").strip()
                if not label:
                    continue
                if not dry_run:
                    mod.add_tag(url, key, arr_id, label, title)
                actions.append(f"tagged {label!r} in {name}")
            elif vtype == "remove_arr_tag":
                label = (vs.get("command") or "").strip()
                if not label:
                    continue
                if not dry_run:
                    mod.remove_tag(url, key, arr_id, label, title)
                actions.append(f"untagged {label!r} in {name}")
            elif vtype == "set_root_folder":
                path = (vs.get("command") or "").strip()
                if not path:
                    return False, "", f"set_root_folder step missing path", str(ItemStatus.STAGED)
                if not dry_run:
                    mod.set_root_folder(url, key, arr_id, path, move_files=True, title=title)
                actions.append(f"moved to {path} in {name}")
                migration_ran = True
            elif vtype == "trigger_search":
                if dry_run:
                    actions.append(f"would trigger search in {name}")
                else:
                    if kind == "radarr":
                        mod.search(url, key, [arr_id])
                    elif media_type == "season":
                        mod.search_season(url, key, arr_id, season_num)
                    else:
                        mod.search_series(url, key, arr_id)
                    actions.append(f"triggered search in {name}")
            elif vtype == "migrate_to_instance":
                try:
                    target_iid = int(vs.get("target_instance_id") or 0)
                except (TypeError, ValueError):
                    target_iid = 0
                if not target_iid:
                    return False, "", "migrate_to_instance step missing target_instance_id", str(ItemStatus.STAGED)
                target = arr_instances.get_instance(target_iid)
                if not target or target["kind"] != kind:
                    return False, "", f"migrate target instance {target_iid} not found or wrong kind", str(ItemStatus.STAGED)
                target_root = (vs.get("target_root_folder") or "").strip()
                if not target_root:
                    return False, "", "migrate_to_instance step missing target_root_folder", str(ItemStatus.STAGED)
                target_profile = (vs.get("target_profile") or "").strip()
                keep_source = bool(vs.get("keep_source", True))
                ok_m, detail_m = _apply_migrate(
                    kind, url, key, target["url"], target["api_key"],
                    target["name"], arr_id, item, target_root, target_profile,
                    keep_source, dry_run,
                )
                if not ok_m:
                    return False, "", detail_m, str(ItemStatus.STAGED)
                actions.append(detail_m)
                migration_ran = True
                if not keep_source:
                    destructive_ran = True

        for s in scripts:
            cmd = s["command"].strip()
            if not dry_run:
                import os as _os
                import subprocess as _sp
                env = {
                    **_os.environ,
                    "RECLAIMER_RATING_KEY": item["rating_key"] or "",
                    "RECLAIMER_TITLE": title or "",
                    "RECLAIMER_COLLECTION": item["collection"] or "",
                    "RECLAIMER_MEDIA_TYPE": media_type or "",
                    "RECLAIMER_ARR_ID": str(arr_id or ""),
                    "RECLAIMER_SEASON_NUMBER": str(season_num if season_num is not None else ""),
                }
                result = _sp.run(cmd, shell=True, env=env, capture_output=True, timeout=60)
                if result.returncode != 0:
                    stderr_tail = (result.stderr or b"").decode(errors="replace")[-200:]
                    return False, "", f"script exit {result.returncode}: {stderr_tail}", str(ItemStatus.STAGED)
            actions.append("ran script")
            destructive_ran = True  # scripts are treated as terminal actions

        # Notify steps run AFTER the primary action so their ``action_summary``
        # placeholder can reflect what was actually done this pipeline.
        if notify_steps:
            action_summary = "; ".join(actions) if actions else "after-grace action"
            apprise_url = config.get("apprise_url")
            for s in notify_steps:
                template = (s.get("command") or "").strip()
                if not template:
                    template = "Reclaimer: {collection} flagged '{title}' for action on {grace_expires}"
                subs = _build_notify_substitutions(
                    item, item.get("collection") or "",
                    item.get("grace_expires"),
                    action_summary,
                )
                body = _render_notify_template(template, subs)
                ntitle = f"Reclaimer -- {item.get('collection') or 'rule'}"
                if dry_run:
                    actions.append(f"would notify ({body!r})")
                else:
                    ok_n, detail_n = _send_notify(apprise_url, body, ntitle)
                    if ok_n:
                        actions.append("notified")
                    else:
                        # Best-effort: log but do not fail the pipeline.
                        log.warning("notify failed for %s: %s", title, detail_n)
                        actions.append(f"notify failed ({detail_n})")

        final_status = str(ItemStatus.STAGED)
        if migration_ran and not destructive_ran:
            final_status = str(ItemStatus.MIGRATED)
        elif destructive_ran:
            final_status = str(ItemStatus.ACTIONED)
        # else: notify-only pipeline -- stay staged.

        return True, "; ".join(actions) if actions else "no-op", None, final_status
    except Exception as e:
        return False, "", f"{type(e).__name__}: {e}", str(ItemStatus.STAGED)


def _apply_downgrade_movie(
    radarr_url: str, radarr_key: str, radarr_id: int, profile_name: str,
    title: str, dry_run: bool,
) -> tuple[bool, str]:
    """Switch a Radarr movie's qualityProfileId. Returns (ok, detail)."""
    if not profile_name:
        return False, f"quality profile '' not found in Radarr"
    qp_id = radarr.get_quality_profile_id(radarr_url, radarr_key, profile_name)
    if qp_id is None:
        return False, f"quality profile {profile_name!r} not found in Radarr"
    if dry_run:
        return True, f"would downgrade to {profile_name} in Radarr"
    # Fetch + PUT the full movie -- partial updates aren't supported by
    # Radarr v3 and would nuke unmanaged fields.
    r = radarr._api(radarr_url, radarr_key, "get", f"/movie/{radarr_id}")
    r.raise_for_status()
    movie = r.json()
    movie["qualityProfileId"] = qp_id
    put = radarr._api(radarr_url, radarr_key, "put", f"/movie/{radarr_id}", json=movie)
    put.raise_for_status()
    log.info("Downgraded Radarr movie %s to profile %s (id=%d)", title, profile_name, qp_id)
    return True, f"downgraded to {profile_name} in Radarr"


def _apply_downgrade_show(
    sonarr_url: str, sonarr_key: str, series_id: int, profile_name: str,
    title: str, dry_run: bool,
) -> tuple[bool, str]:
    """Switch a Sonarr series' qualityProfileId. Returns (ok, detail)."""
    if not profile_name:
        return False, f"quality profile '' not found in Sonarr"
    qp_id = sonarr.get_quality_profile_id(sonarr_url, sonarr_key, profile_name)
    if qp_id is None:
        return False, f"quality profile {profile_name!r} not found in Sonarr"
    if dry_run:
        return True, f"would downgrade to {profile_name} in Sonarr"
    r = sonarr._api(sonarr_url, sonarr_key, "get", f"/series/{series_id}")
    r.raise_for_status()
    show = r.json()
    show["qualityProfileId"] = qp_id
    put = sonarr._api(sonarr_url, sonarr_key, "put", f"/series/{series_id}", json=show)
    put.raise_for_status()
    log.info("Downgraded Sonarr series %s to profile %s (id=%d)", title, profile_name, qp_id)
    return True, f"downgraded to {profile_name} in Sonarr"


def _apply_migrate(
    kind: str, source_url: str, source_key: str,
    target_url: str, target_key: str, target_name: str,
    source_arr_id: int, item: dict, target_root: str, target_profile_name: str,
    keep_source: bool, dry_run: bool,
) -> tuple[bool, str]:
    """Copy an item from one arr instance to another. ``kind`` is
    ``"radarr"`` or ``"sonarr"``. The source is looked up by id on the
    source instance; the target is created by tmdb/tvdb lookup so we
    don't have to reconstruct Radarr's titleSlug/images by hand. When
    ``keep_source`` is false the source item is deleted *without* files
    (Radarr/Sonarr would double-delete the files we just copied)."""
    mod = radarr if kind == "radarr" else sonarr
    external_field = "tmdbId" if kind == "radarr" else "tvdbId"

    # Fetch source to read title, external id, tags-by-name.
    r = mod._api(source_url, source_key, "get",
                 f"/movie/{source_arr_id}" if kind == "radarr"
                 else f"/series/{source_arr_id}")
    r.raise_for_status()
    src = r.json()
    ext_id = src.get(external_field)
    if not ext_id:
        return False, f"source item has no {external_field}"
    src_title = src.get("title") or item.get("title") or ""

    # Resolve quality profile on the TARGET. Fall back to the source's
    # profile name if the caller didn't supply one.
    if not target_profile_name:
        # Source profile name is looked up on the source by id.
        src_qp_id = src.get("qualityProfileId")
        for p in mod.list_quality_profiles(source_url, source_key):
            if p.get("id") == src_qp_id:
                target_profile_name = p.get("name") or ""
                break
    qp_id = mod.get_quality_profile_id(target_url, target_key, target_profile_name)
    if qp_id is None:
        return False, (f"quality profile {target_profile_name!r} not found on {target_name} "
                       "(supply target_profile or match names across instances)")

    # Carry tags by label (ids differ across instances).
    src_tag_ids = src.get("tags") or []
    src_tag_map = {t["id"]: t["label"]
                   for t in mod._api(source_url, source_key, "get", "/tag").json()}
    tag_labels = [src_tag_map.get(t) for t in src_tag_ids if src_tag_map.get(t)]
    target_tag_ids = []
    if not dry_run:
        for label in tag_labels:
            tid = mod.ensure_tag_id(target_url, target_key, label)
            if tid is not None:
                target_tag_ids.append(tid)

    if dry_run:
        would_delete = "" if keep_source else f" (and delete from source)"
        return True, (f"would migrate {src_title!r} to {target_name} "
                      f"at {target_root} with profile {target_profile_name}{would_delete}")

    try:
        if kind == "radarr":
            created = mod.add_movie(
                target_url, target_key, int(ext_id), src_title,
                qp_id, target_root, monitored=bool(src.get("monitored", True)),
                search_on_add=False, tags=target_tag_ids,
            )
        else:
            created = mod.add_series(
                target_url, target_key, int(ext_id), src_title,
                qp_id, target_root, monitored=bool(src.get("monitored", True)),
                search_on_add=False, tags=target_tag_ids,
            )
    except Exception as e:
        return False, f"failed to add to {target_name}: {e}"

    msg = f"migrated to {target_name} ({src_title!r})"
    if not keep_source:
        try:
            # delete_files=False: the target instance now manages the files
            # at target_root; source and target are often on the same
            # physical storage, so deleting files from source would break
            # the target's library.
            if kind == "radarr":
                mod.delete(source_url, source_key, source_arr_id, src_title,
                           delete_files=False, add_exclusion=False)
            else:
                mod.delete(source_url, source_key, source_arr_id, src_title,
                           delete_files=False)
            msg += " + removed from source"
        except Exception as e:
            msg += f" (WARNING: source removal failed: {e})"
    return True, msg


def _execute_after_grace_actions(conn, dry_run: bool, today: date,
                                  rule_filter: str | None = None) -> None:
    """Run after-grace pipeline steps for staged items whose grace has expired
    (or whose override forces immediate deletion). Skips items with
    override='keep'. Marks successful items as 'actioned' (or 'migrated' for
    quality-downgrade pipelines).

    ``rule_filter`` restricts action execution to a single rule so a
    per-rule dry-run (or targeted re-run) doesn't fire actions for items
    in other collections.
    """
    rows = conn.execute(
        "SELECT name, criteria FROM collection_config WHERE enabled = 1"
    ).fetchall()
    rule_steps: dict[str, list[dict]] = {}
    for r in rows:
        if rule_filter and r["name"] != rule_filter:
            continue
        try:
            crit = json.loads(r["criteria"]) if r["criteria"] else {}
        except (TypeError, ValueError):
            continue
        steps = [s for s in (crit.get("action_pipeline") or [])
                 if _step_timing(s) == "after_grace"]
        if steps:
            rule_steps[r["name"]] = steps

    if not rule_steps:
        return

    today_str = today.isoformat()
    # Pick items where grace has expired OR user forced immediate via override.
    # Skip items protected by override='keep'.
    items = conn.execute("""
        SELECT id, rating_key, collection, title, media_type, arr_id,
               season_number, override, grace_expires
        FROM items
        WHERE status = ?
          AND (override IS NULL OR override != 'keep')
          AND (override = 'delete' OR grace_expires <= ?)
    """, (ItemStatus.STAGED, today_str)).fetchall()

    if not items:
        return

    update_progress(
        phase="executing_actions",
        detail=f"{len(items)} item(s) past grace",
        items_processed=0, items_total=len(items), percent=0,
    )

    attempted = succeeded = failed = 0
    for idx, item in enumerate(items, 1):
        steps = rule_steps.get(item["collection"])
        if not steps:
            continue
        attempted += 1
        ok, summary, error, final_status = _run_after_grace_for_item(
            dict(item), steps, dry_run,
        )
        if ok:
            succeeded += 1
            # Only persist a status transition when the executor signalled one
            # (anything other than STAGED). A notify-only pipeline keeps the
            # item staged so it can be re-evaluated next run.
            if not dry_run and final_status != str(ItemStatus.STAGED):
                conn.execute(
                    "UPDATE items SET status = ?, action_taken = ?, action_date = ? WHERE id = ?",
                    (final_status, summary, today_str, item["id"]),
                )
            _log_activity(
                conn, "item_actioned",
                collection=item["collection"], rating_key=item["rating_key"],
                title=item["title"],
                detail={"summary": summary, "dry_run": dry_run,
                        "final_status": final_status},
            )
        else:
            failed += 1
            log.warning("Action failed for %s (%s): %s",
                        item["title"], item["collection"], error)
            _log_activity(
                conn, "item_action_failed",
                collection=item["collection"], rating_key=item["rating_key"],
                title=item["title"],
                detail={"error": error, "dry_run": dry_run},
            )
        if idx % 10 == 0:
            conn.commit()
            update_progress(
                phase="executing_actions",
                detail=f"{idx}/{len(items)} item(s)",
                items_processed=idx, items_total=len(items),
                percent=int(idx * 100 / len(items)),
            )

    conn.commit()
    log.info("After-grace actions%s: %d attempted, %d succeeded, %d failed",
             " (DRY RUN)" if dry_run else "", attempted, succeeded, failed)


def run_orchestrator(dry_run: bool = False, rule_filter: str | None = None) -> None:
    """Run the full orchestration pipeline.

    ``rule_filter`` optionally restricts processing to a single rule by its
    exact ``collection_config.name``. Other rules are skipped (their staged
    items are NOT re-evaluated in this run). After-grace actions still fire
    for the filtered rule's items only when ``rule_filter`` is set.
    """
    t0 = time.time()
    today = date.today()
    log.info("=== Reclaimer Run %s%s===",
             "(DRY RUN) " if dry_run else "",
             f"[rule={rule_filter}] " if rule_filter else "")

    conn = get_db()
    _run_detail = {"dry_run": dry_run}
    if rule_filter:
        _run_detail["rule_filter"] = rule_filter
    _log_activity(conn, "run_started", detail=_run_detail)
    conn.commit()

    # Load settings
    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    movies_section = config.get("plex_movies_section")
    tv_section = config.get("plex_tv_section")

    # Jellyfin settings
    jf_url = config.get("jellyfin_url")
    jf_key = config.get("jellyfin_api_key")

    # Fetch all data
    log.info("Fetching data from services...")
    update_progress(phase="fetching_plex", detail="movies")
    plex_movies = plex.fetch_library(plex_url, plex_token, movies_section)
    update_progress(phase="fetching_plex", detail=f"TV shows ({len(plex_movies)} movies fetched)")
    plex_tv = plex.fetch_library(plex_url, plex_token, tv_section)
    update_progress(phase="fetching_radarr", detail=f"{len(plex_tv)} TV shows fetched, Radarr next")
    from .core import arr_instances as _arr
    _rinst = _arr.default_instance("radarr")
    _sinst = _arr.default_instance("sonarr")
    radarr_movies = (radarr.fetch_movies(_rinst["url"], _rinst["api_key"])
                     if _rinst else {})
    update_progress(phase="fetching_sonarr", detail="shows")
    sonarr_shows = (sonarr.fetch_shows(_sinst["url"], _sinst["api_key"])
                    if _sinst else {})

    # Derive per-item play counts straight from watch_history -- the media
    # servers' session history (ingested by ``_sync_users``) is the single
    # source of truth. ``tau_movies`` / ``tau_tv`` keep their names purely
    # as the legacy kwarg handed to ``EvaluationContext``; the engine reads
    # them off ``ctx.play_counts``.
    update_progress(phase="loading_db_plays",
                    detail="aggregating play counts from watch_history")
    tau_movies: dict[str, int] = {}
    tau_tv: dict[str, int] = {}
    try:
        _count_conn = get_db()
        # Count distinct sessions per item (a single (user, rating_key,
        # watched_at) tuple = one play).
        _play_rows = _count_conn.execute("""
            SELECT rating_key,
                   COUNT(DISTINCT (user_id || '|' || watched_at)) AS plays
            FROM watch_history
            WHERE rating_key IS NOT NULL AND rating_key != ''
            GROUP BY rating_key
        """).fetchall()
        _play_counts = {str(r["rating_key"]): int(r["plays"] or 0) for r in _play_rows}
        _count_conn.close()
        # Both movies and TV rules consult the same dict -- the rating_keys
        # don't overlap across libraries, so one shared map is fine.
        tau_movies = dict(_play_counts)
        tau_tv = dict(_play_counts)
    except Exception as e:
        log.warning("Failed to load play counts from watch_history: %s", e)

    update_progress(phase="fetching_overseerr", detail="active requests")
    protected = {u.strip() for u in config.get("protected_requesters").split(",") if u.strip()}
    (ov_am, ov_as, ov_pm, ov_ps, ov_movie_req, ov_show_req,
     ov_as_tmdb, ov_ps_tmdb, ov_show_req_tmdb) = overseerr.fetch_active_requests(
        config.get("overseerr_url"), config.get("overseerr_api_key"), protected)

    update_progress(phase="fetching_plex", detail="keep collections")
    movies_keep = plex.fetch_keep_collection(plex_url, plex_token, movies_section,
                                              config.get("plex_movies_keep_collection"))
    tv_keep = plex.fetch_keep_collection(plex_url, plex_token, tv_section,
                                          config.get("plex_tv_keep_collection"))

    # Jellyfin keep collections
    jf_movies_section = config.get("jellyfin_movies_section")
    jf_tv_section = config.get("jellyfin_tv_section")
    if jf_url and jf_key:
        jf_movies_keep_name = config.get("jellyfin_movies_keep_collection")
        jf_tv_keep_name = config.get("jellyfin_tv_keep_collection")
        if jf_movies_keep_name and jf_movies_section:
            update_progress(phase="fetching_jellyfin", detail="movie keep collection")
            movies_keep.update(jellyfin.fetch_keep_collection(jf_url, jf_key, jf_movies_section, jf_movies_keep_name))
        if jf_tv_keep_name and jf_tv_section:
            update_progress(phase="fetching_jellyfin", detail="TV keep collection")
            tv_keep.update(jellyfin.fetch_keep_collection(jf_url, jf_key, jf_tv_section, jf_tv_keep_name))

    # Load play counts and per-user watch sets from our DB
    update_progress(phase="loading_db_plays", detail="watch history play counts")
    conn_plays = get_db()
    db_play_rows = conn_plays.execute("""
        SELECT rating_key, COUNT(*) as plays FROM watch_history GROUP BY rating_key
    """).fetchall()
    db_plays = {str(r["rating_key"]): r["plays"] for r in db_play_rows}
    # Also aggregate by show title (grandparent_title) so show-level lookups work
    show_play_rows = conn_plays.execute("""
        SELECT grandparent_title, COUNT(*) as plays FROM watch_history
        WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
        GROUP BY grandparent_title COLLATE NOCASE
    """).fetchall()
    db_plays_by_title = {r["grandparent_title"].lower(): r["plays"] for r in show_play_rows if r["grandparent_title"]}
    # Last watch dates by rating_key
    last_watch_rows = conn_plays.execute("""
        SELECT rating_key, MAX(watched_at) as last_watched FROM watch_history GROUP BY rating_key
    """).fetchall()
    last_watch_dates = {str(r["rating_key"]): r["last_watched"] for r in last_watch_rows if r["last_watched"]}
    # Last watch dates by show title (grandparent_title)
    show_last_rows = conn_plays.execute("""
        SELECT grandparent_title, MAX(watched_at) as last_watched FROM watch_history
        WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
        GROUP BY grandparent_title COLLATE NOCASE
    """).fetchall()
    last_watch_by_title = {r["grandparent_title"].lower(): r["last_watched"] for r in show_last_rows if r["last_watched"] and r["grandparent_title"]}

    # Build username → set of rating_keys AND show titles they've watched
    user_watch_rows = conn_plays.execute("""
        SELECT u.username, wh.rating_key, wh.grandparent_title FROM watch_history wh
        JOIN users u ON u.id = wh.user_id
    """).fetchall()
    user_watches: dict[str, set] = {}
    for row in user_watch_rows:
        s = user_watches.setdefault(row["username"], set())
        s.add(str(row["rating_key"]))
        # Also add show title so show-level lookups work
        if row["grandparent_title"]:
            s.add(row["grandparent_title"].lower())
    # Season-scoped aggregations for granularity=season collections
    season_play_rows = conn_plays.execute("""
        SELECT grandparent_title, season_number, COUNT(*) as plays
        FROM watch_history
        WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
          AND season_number IS NOT NULL
        GROUP BY grandparent_title COLLATE NOCASE, season_number
    """).fetchall()
    db_plays_by_season: dict[str, int] = {}
    for r in season_play_rows:
        if r["grandparent_title"] and r["season_number"] is not None:
            key = f"{r['grandparent_title'].lower()}:S{r['season_number']}"
            db_plays_by_season[key] = r["plays"]

    season_last_rows = conn_plays.execute("""
        SELECT grandparent_title, season_number, MAX(watched_at) as last_watched
        FROM watch_history
        WHERE grandparent_title IS NOT NULL AND grandparent_title != ''
          AND season_number IS NOT NULL
        GROUP BY grandparent_title COLLATE NOCASE, season_number
    """).fetchall()
    last_watch_by_season: dict[str, str] = {}
    for r in season_last_rows:
        if r["grandparent_title"] and r["season_number"] is not None and r["last_watched"]:
            key = f"{r['grandparent_title'].lower()}:S{r['season_number']}"
            last_watch_by_season[key] = r["last_watched"]

    user_season_rows = conn_plays.execute("""
        SELECT u.username, wh.grandparent_title, wh.season_number
        FROM watch_history wh
        JOIN users u ON u.id = wh.user_id
        WHERE wh.grandparent_title IS NOT NULL AND wh.grandparent_title != ''
          AND wh.season_number IS NOT NULL
    """).fetchall()
    user_watches_by_season: dict[str, set] = {}
    for row in user_season_rows:
        if row["grandparent_title"] and row["season_number"] is not None:
            s = user_watches_by_season.setdefault(row["username"], set())
            s.add(f"{row['grandparent_title'].lower()}:S{row['season_number']}")

    conn_plays.close()
    log.info("Loaded %d items with DB play history, %d users with watches, %d season play records",
             len(db_plays), len(user_watches), len(db_plays_by_season))

    # Extract ratings from Plex/Jellyfin metadata (no external API calls)
    from .core.ratings import extract_ratings
    ratings_cache_map: dict[str, dict] = {}
    for item in plex_movies:
        rk = str(item["ratingKey"])
        r = extract_ratings(item, "plex")
        if r.get("critic_rating") is not None or r.get("audience_rating") is not None:
            ratings_cache_map[rk] = r
    for item in plex_tv:
        rk = str(item["ratingKey"])
        r = extract_ratings(item, "plex")
        if r.get("critic_rating") is not None or r.get("audience_rating") is not None:
            ratings_cache_map[rk] = r
    log.info("Ratings: %d items have rating data from Plex metadata", len(ratings_cache_map))

    # Build contexts
    movie_ctx = EvaluationContext(
        tau_plays=tau_movies, radarr_movies=radarr_movies,
        overseerr_active_movies=ov_am, overseerr_protected_movies=ov_pm,
        plex_keep_keys=movies_keep, db_plays=db_plays, db_plays_by_title=db_plays_by_title,
        movie_requesters=ov_movie_req, user_watches=user_watches,
        last_watch_dates=last_watch_dates, last_watch_by_title=last_watch_by_title,
        ratings_cache=ratings_cache_map,
    )
    tv_ctx = EvaluationContext(
        tau_plays=tau_tv, sonarr_shows=sonarr_shows,
        overseerr_active_shows=ov_as, overseerr_active_shows_tmdb=ov_as_tmdb,
        overseerr_protected_shows=ov_ps, overseerr_protected_shows_tmdb=ov_ps_tmdb,
        plex_keep_keys=tv_keep, db_plays=db_plays, db_plays_by_title=db_plays_by_title,
        show_requesters=ov_show_req, show_requesters_tmdb=ov_show_req_tmdb,
        user_watches=user_watches,
        last_watch_dates=last_watch_dates, last_watch_by_title=last_watch_by_title,
        ratings_cache=ratings_cache_map,
        db_plays_by_season=db_plays_by_season,
        last_watch_by_season=last_watch_by_season,
        user_watches_by_season=user_watches_by_season,
    )

    # Populate the extended EvaluationContext fields (favorites, watchlist,
    # partial-watch aggregates, addedAt, season counts). These are shared
    # across every collection's rule evaluation; show-level protection keys
    # are computed per-rule inside the rule loop because they depend on the
    # rule's own protected_tags/protected_users/etc.
    update_progress(phase="building_context",
                    detail="Building extended evaluation context...")
    try:
        ext = _build_extended_context(
            plex_movies=plex_movies,
            plex_tv=plex_tv,
            radarr_movies=radarr_movies,
            sonarr_shows=sonarr_shows,
            plex_url=plex_url, plex_token=plex_token,
            movies_section=movies_section, tv_section=tv_section,
            overseerr_url=config.get("overseerr_url"),
            overseerr_key=config.get("overseerr_api_key"),
            jf_url=config.get("jellyfin_url"),
            jf_key=config.get("jellyfin_api_key"),
            jf_movies_section=config.get("jellyfin_movies_section"),
            jf_tv_section=config.get("jellyfin_tv_section"),
        )
        _set_ctx_fields(movie_ctx, **ext)
        _set_ctx_fields(tv_ctx, **ext)
    except Exception as e:
        log.exception("Extended context build failed: %s", e)

    # Process all enabled collections from config
    conn = get_db()
    collection_configs = _load_collection_configs(conn)

    # Per-rule filter: restrict to a single rule by exact name. Rules that
    # don't match are skipped entirely for this run -- their staged items
    # aren't re-evaluated, and after-grace actions below will only touch
    # items belonging to the filtered rule.
    if rule_filter:
        before = len(collection_configs)
        collection_configs = [c for c in collection_configs if c.get("name") == rule_filter]
        log.info("rule_filter=%s matched %d of %d enabled rules",
                 rule_filter, len(collection_configs), before)

    # Pre-fetch all unique protected collections referenced by any rule's criteria
    all_protected_col_names: set[str] = set()
    for col_cfg in collection_configs:
        _criteria = CollectionCriteria.from_json(col_cfg.get("criteria"))
        for name in (_criteria.protected_collections or []):
            all_protected_col_names.add(name)

    collection_keys_cache: dict[str, set[str]] = {}
    if all_protected_col_names:
        update_progress(phase="fetching_plex",
                        detail=f"{len(all_protected_col_names)} protected collection(s)")
        for col_name in all_protected_col_names:
            keys: set[str] = set()
            # Plex
            if plex_url and plex_token:
                if movies_section:
                    try:
                        keys.update(plex.fetch_keep_collection(plex_url, plex_token, movies_section, col_name))
                    except Exception as e:
                        log.warning("Protected collection '%s' Plex movies fetch failed: %s",
                                    col_name, e)
                if tv_section:
                    try:
                        keys.update(plex.fetch_keep_collection(plex_url, plex_token, tv_section, col_name))
                    except Exception as e:
                        log.warning("Protected collection '%s' Plex TV fetch failed: %s",
                                    col_name, e)
            # Jellyfin
            if jf_url and jf_key:
                if jf_movies_section:
                    try:
                        keys.update(jellyfin.fetch_keep_collection(jf_url, jf_key, jf_movies_section, col_name))
                    except Exception as e:
                        log.warning("Protected collection '%s' Jellyfin movies fetch failed: %s",
                                    col_name, e)
                if jf_tv_section:
                    try:
                        keys.update(jellyfin.fetch_keep_collection(jf_url, jf_key, jf_tv_section, col_name))
                    except Exception as e:
                        log.warning("Protected collection '%s' Jellyfin TV fetch failed: %s",
                                    col_name, e)
            collection_keys_cache[col_name] = keys
        log.info("Pre-fetched %d protected collections (%d total keys)",
                 len(all_protected_col_names),
                 sum(len(v) for v in collection_keys_cache.values()))

    # Cache for Jellyfin library fetches (library_id -> normalized items)
    _jf_library_cache: dict[str, list[dict]] = {}

    # Deferred collection syncs: {(col_name, section_id, plex_type, source): union_of_want_sets}
    # Multiple rules can contribute to the same collection. We union all and sync once.
    _deferred_syncs: dict[tuple, set[str]] = {}

    for col_cfg in collection_configs:
        name = col_cfg["name"]
        media_type = col_cfg["media_type"]
        criteria = CollectionCriteria.from_json(col_cfg.get("criteria"))

        # Override action/grace_days from the collection_config row if they
        # differ from criteria (the row columns are the canonical source)
        criteria.action = col_cfg.get("action", criteria.action)
        criteria.grace_days = col_cfg.get("grace_days", criteria.grace_days)

        library_source = criteria.library_source or "plex"

        if library_source == "jellyfin" and jf_url and jf_key:
            # Jellyfin-sourced collection
            lib_id = str(criteria.library_section_id or "")
            if not lib_id:
                log.warning("Collection %s targets Jellyfin but has no library_section_id, skipping", name)
                continue

            if lib_id not in _jf_library_cache:
                update_progress(phase="fetching_jellyfin",
                                detail=f"Jellyfin library {lib_id}")
                raw_items = jellyfin.fetch_library(jf_url, jf_key, lib_id)
                _jf_library_cache[lib_id] = [_normalize_jellyfin_item(i) for i in raw_items]
                log.info("Jellyfin library %s: %d items (normalized)", lib_id, len(_jf_library_cache[lib_id]))

            items = _jf_library_cache[lib_id]
            evaluate_fn = evaluate_movie if media_type == "movie" else evaluate_show
            section_id = lib_id
            ctx = movie_ctx if media_type == "movie" else tv_ctx
        else:
            # Plex-sourced collection (default)
            if media_type == "movie":
                items = plex_movies
                evaluate_fn = evaluate_movie
                section_id = criteria.library_section_id or movies_section
                ctx = movie_ctx
            elif criteria.granularity == "season" and media_type == "show":
                # Season granularity: expand each show into individual seasons.
                # Fetching seasons is a ~O(n) round-trip per show -- the
                # sequential loop was dominating wall time for libraries
                # with hundreds of shows. A bounded ThreadPoolExecutor runs
                # ~8 fetches in flight against Plex, which is fine for a
                # local server and keeps the code simple.
                from concurrent.futures import ThreadPoolExecutor, as_completed

                section_id = criteria.library_section_id or tv_section
                ctx = tv_ctx
                update_progress(phase="fetching_seasons",
                                detail=f"Fetching seasons for {name} ({len(plex_tv)} shows)...")
                season_items: list[dict] = []

                def _fetch_one_show(show):
                    show_title = show.get("title", "")
                    show_rk = str(show["ratingKey"])
                    show_guids = show.get("Guid", [])
                    try:
                        seasons = plex.fetch_seasons(plex_url, plex_token, show_rk)
                    except Exception as e:
                        log.warning("Failed to fetch seasons for %s: %s", show_title, e)
                        return []
                    out = []
                    for s in seasons:
                        season_num = s.get("index", 0)
                        s["Guid"] = show_guids  # Inherit show's external IDs
                        s["_show_title"] = show_title
                        s["_show_rating_key"] = show_rk
                        s["_season_number"] = season_num
                        s["title"] = f"{show_title} - Season {season_num}"
                        out.append(s)
                    return out

                completed = 0
                total_shows = len(plex_tv)
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(_fetch_one_show, show) for show in plex_tv]
                    for fut in as_completed(futures):
                        season_items.extend(fut.result())
                        completed += 1
                        if completed % 20 == 0 or completed == total_shows:
                            update_progress(
                                phase="fetching_seasons",
                                detail=f"Fetching seasons ({completed}/{total_shows})...",
                            )
                items = season_items
                # Wrap evaluate_season to pass season_number and show_title
                def _make_season_evaluator():
                    def _eval(item, ctx, criteria=None):
                        return evaluate_season(
                            item, ctx, criteria=criteria,
                            season_number=item.get("_season_number", 0),
                            show_title=item.get("_show_title", ""),
                        )
                    return _eval
                evaluate_fn = _make_season_evaluator()
                log.info("Expanded %d shows to %d seasons for %s",
                         len(plex_tv), len(season_items), name)
            else:
                items = plex_tv
                evaluate_fn = evaluate_show
                section_id = criteria.library_section_id or tv_section
                ctx = tv_ctx

        # Build per-rule plex_keep_keys from this collection's protected_collections
        rule_protected = criteria.protected_collections or []
        if rule_protected:
            rule_keep_keys: set[str] = set()
            for pcol_name in rule_protected:
                rule_keep_keys.update(collection_keys_cache.get(pcol_name, set()))
            ctx = replace(ctx, plex_keep_keys=rule_keep_keys)

        # For season-granularity TV rules, compute ``show_level_protection_keys``
        # fresh using this rule's protection config so ``series_protection``
        # can consult it. Movies and show-level rules don't need it (it only
        # makes sense for season-granularity evaluation).
        if criteria.granularity == "season" and media_type == "show":
            try:
                slp = _compute_show_level_protection_keys(plex_tv, ctx, criteria)
                _set_ctx_fields(ctx, show_level_protection_keys=slp)
                log.info("Rule %s: %d shows flagged by show-level protection",
                         name, len(slp))
            except Exception as e:
                log.warning("show_level_protection_keys computation failed for %s: %s", name, e)

        log.info("Processing collection: %s (source=%s, media_type=%s, action=%s, grace=%dd)",
                 name, library_source, media_type, criteria.action, criteria.grace_days)

        t_rule = time.time()
        want = _process_collection(conn, name, items, evaluate_fn, ctx, criteria,
                            section_id, media_type, dry_run, today,
                            library_source=library_source,
                            deferred_syncs=_deferred_syncs)
        rule_duration = time.time() - t_rule
        _log_activity(conn, "rule_processed", collection=name,
                      detail={"items": len(items), "candidates": len(want),
                              "duration": round(rule_duration, 1), "dry_run": dry_run})

    # Clean up orphan rule_results. Items are unique per (collection,
    # rating_key), so rule_results must be scoped the same way: delete any
    # rule_results row whose (rating_key, collection) pair no longer has a
    # matching items row. Using only rating_key (as before) left orphan
    # results behind when an item was removed from one collection but still
    # lived in another -- those orphan rows then poisoned the next run's
    # "why was this excluded" diagnostics.
    orphan_count = conn.execute("""
        DELETE FROM rule_results
        WHERE (rating_key, collection) NOT IN (
            SELECT rating_key, collection FROM items
        )
    """).rowcount
    if orphan_count:
        log.info("Cleaned %d orphan rule_results", orphan_count)

    # Run after-grace actions on items whose grace period has expired.
    # Done AFTER rule processing so items that gained a protection this scan
    # have already been removed from the staged set before we'd touch them.
    try:
        _execute_after_grace_actions(conn, dry_run, today, rule_filter=rule_filter)
    except Exception as e:
        log.exception("After-grace action executor crashed: %s", e)
        _log_activity(conn, "run_error", detail={"stage": "after_grace", "error": str(e)})

    # Flush deferred collection syncs - union of all rules' want sets per collection
    if not dry_run and _deferred_syncs:
        col_count = len(_deferred_syncs)
        update_progress(
            phase="syncing_collections",
            detail=f"{col_count} Plex/Jellyfin collection{'s' if col_count != 1 else ''}",
        )
        sync_succeeded: list[str] = []
        sync_failed: list[tuple[str, str]] = []  # (name, error)
        for (col_name, sec_id, plex_type, source), want_union in _deferred_syncs.items():
            try:
                if source == "plex" and plex_url and plex_token:
                    plex.sync_collection(plex_url, plex_token, sec_id, col_name, want_union,
                                         media_type=plex_type)
                    log.info("Synced collection '%s': %d items (type=%d)", col_name, len(want_union), plex_type)
                    sync_succeeded.append(col_name)
                elif source == "jellyfin" and jf_url and jf_key:
                    jellyfin.sync_collection(jf_url, jf_key, sec_id, col_name, want_union)
                    log.info("Synced collection '%s': %d items (Jellyfin)", col_name, len(want_union))
                    sync_succeeded.append(col_name)
                else:
                    # No credentials for this source -- record as skipped so
                    # the caller can see partial coverage.
                    sync_failed.append((col_name, f"no credentials for source={source}"))
            except Exception as e:
                log.warning("Failed to sync collection '%s': %s", col_name, e)
                sync_failed.append((col_name, str(e)))
                # Emit an activity_log entry so failures surface in the UI,
                # not only in container logs.
                _log_activity(
                    conn, "collection_sync_failed",
                    collection=col_name,
                    detail={"error": str(e), "source": source,
                            "want_count": len(want_union)},
                )
        log.info(
            "Collection sync summary: %d succeeded, %d failed",
            len(sync_succeeded), len(sync_failed),
        )
        if sync_failed:
            _log_activity(
                conn, "collection_sync_summary",
                detail={
                    "succeeded": sync_succeeded,
                    "failed": [{"name": n, "error": e} for n, e in sync_failed],
                },
            )

    update_progress(phase="finalizing", detail="Saving results...")
    # Retention: trim activity_log to the most recent 90 days at the tail of
    # every run so the table doesn't grow unbounded.
    try:
        from .database import prune_activity_log
        pruned = prune_activity_log(conn, days=90)
        if pruned:
            log.info("Pruned %d activity_log rows older than 90 days", pruned)
    except Exception as e:
        log.warning("activity_log retention prune failed: %s", e)
    _completed_detail = {"dry_run": dry_run, "duration": round(time.time() - t0, 1)}
    if rule_filter:
        _completed_detail["rule_filter"] = rule_filter
    _log_activity(conn, "run_completed", detail=_completed_detail)
    conn.commit()

    # SQLite keeps its query planner stats fresh via PRAGMA optimize; running
    # it at init only leaves the planner blind as the tables grow during the
    # day. A post-run pass is cheap (no-op when nothing has changed enough
    # to need re-analysis) and keeps subsequent runs' plans honest. Failure
    # here must never abort the run -- the data has already been committed.
    try:
        conn.execute("PRAGMA optimize")
    except Exception as e:
        log.debug("PRAGMA optimize failed: %s", e)

    conn.close()

    log.info("=== Done in %.1fs ===", time.time() - t0)


def _sync_users(conn):
    """Sync Plex + Jellyfin users and their watch history directly from the
    media servers.

    Plex uses ``/status/sessions/history/all``; Jellyfin uses per-user
    Played/Resumable item queries. Rows land in ``watch_history``
    (user_id, rating_key, watched_at). ``play_duration`` is stored but
    non-authoritative -- we persist 0 and rely on ``percent_complete``
    (derived from viewOffset / runtime) to drive the partial-watch and
    watch-ratio rules.
    """

    protected = {u.strip() for u in (config.get("protected_requesters") or "").split(",") if u.strip()}

    # ------------------------------------------------------------------
    # Plex -- server-native session history.
    # ------------------------------------------------------------------
    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    if plex_url and plex_token:
        try:
            update_progress(phase="syncing_users", detail="Resolving Plex accounts...")
            accounts = plex.fetch_accounts(plex_url, plex_token)
            log.info("Plex: %d accounts authorised on server", len(accounts))

            # Upsert every authorised account up front so the history inserts
            # below can resolve user_id via plex_user_id without re-querying
            # per row. ``thumb`` is no longer available via /accounts (Plex
            # dropped it in recent API versions); leave blank and let the UI
            # fall back to initials.
            for acct_id, name in accounts.items():
                conn.execute("""
                    INSERT INTO users (plex_user_id, username, thumb, is_protected, source, last_synced)
                    VALUES (?, ?, '', ?, 'plex', datetime('now'))
                    ON CONFLICT(plex_user_id) DO UPDATE SET
                        username = excluded.username,
                        is_protected = excluded.is_protected,
                        source = 'plex',
                        last_synced = datetime('now')
                """, (acct_id, name, name in protected))
            conn.commit()

            update_progress(phase="syncing_users",
                            detail="Fetching Plex session history...")
            history = plex.fetch_session_history(plex_url, plex_token)
            plex_rows_inserted = 0

            # Build a plex_user_id -> internal users.id map once to keep the
            # insert loop tight. Any account_id that doesn't resolve is
            # skipped (logged once) rather than auto-created, since
            # ``fetch_accounts`` has already surfaced the complete list.
            user_id_map: dict[int, int] = {}
            for row in conn.execute(
                "SELECT id, plex_user_id FROM users WHERE plex_user_id IS NOT NULL"
            ).fetchall():
                user_id_map[int(row["plex_user_id"])] = int(row["id"])

            unknown_accounts: set[int] = set()
            batch = 0
            for h in history:
                acct = h["account_id"]
                internal_id = user_id_map.get(acct)
                if internal_id is None:
                    unknown_accounts.add(acct)
                    continue
                rk = h["rating_key"]
                watched_at = h["watched_at"]
                if not rk or not watched_at:
                    continue

                view_offset_ms = h["view_offset_ms"]
                media_duration_ms = h["media_duration_ms"]
                pct = 0
                if media_duration_ms > 0:
                    pct = int(round((view_offset_ms / media_duration_ms) * 100))
                    pct = max(0, min(100, pct))

                # ``play_duration`` is kept on the schema but deliberately 0
                # -- the media-server history doesn't expose a reliable
                # "elapsed seconds this session" value, and the rule engine
                # now reads ``percent_complete`` for both partial-watch and
                # watch-ratio-low decisions.
                conn.execute("""
                    INSERT INTO watch_history
                        (user_id, rating_key, title, grandparent_title, media_type,
                         season_number, episode_number, watched_at,
                         play_duration, media_duration, percent_complete)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    ON CONFLICT(user_id, rating_key, watched_at) DO UPDATE SET
                        media_duration = CASE WHEN excluded.media_duration > watch_history.media_duration
                            THEN excluded.media_duration ELSE watch_history.media_duration END,
                        percent_complete = CASE WHEN excluded.percent_complete > watch_history.percent_complete
                            THEN excluded.percent_complete ELSE watch_history.percent_complete END,
                        grandparent_title = CASE WHEN excluded.grandparent_title != ''
                            THEN excluded.grandparent_title ELSE watch_history.grandparent_title END
                """, (internal_id, rk, h["title"], h["grandparent_title"], h["media_type"],
                      h["season_number"], h["episode_number"], watched_at,
                      media_duration_ms // 1000, pct))
                plex_rows_inserted += 1
                batch += 1
                if batch >= 500:
                    # Commit periodically so we don't hold a multi-minute
                    # write lock on very large histories.
                    conn.commit()
                    batch = 0
            conn.commit()

            if unknown_accounts:
                log.warning("Plex: %d history rows for %d unknown accounts (%s); skipped",
                            len(unknown_accounts), len(unknown_accounts),
                            ", ".join(str(a) for a in sorted(unknown_accounts)[:5]))
            log.info("Plex: inserted/updated %d watch_history rows", plex_rows_inserted)
        except Exception as e:
            log.warning("Plex user/history sync failed: %s", e)

    # ------------------------------------------------------------------
    # Jellyfin -- per-user Played + Resumable queries.
    # ------------------------------------------------------------------
    jf_url = config.get("jellyfin_url")
    jf_key = config.get("jellyfin_api_key")
    if jf_url and jf_key:
        try:
            update_progress(phase="syncing_users",
                            detail="Fetching Jellyfin watch history...")
            jf_rows, jf_users = jellyfin.fetch_watch_history(jf_url, jf_key)
            log.info("Jellyfin: %d users, %d history rows", len(jf_users), len(jf_rows))

            # Upsert Jellyfin users into the shared ``users`` table. The
            # ``plex_user_id`` column is reused for the 63-bit hash of the
            # Jellyfin UUID (see jellyfin._jf_user_id_to_int); the numeric
            # space is enormous relative to Plex's small-int account IDs so
            # collisions aren't a concern.
            for acct_id_str, username in jf_users.items():
                try:
                    acct_id = int(acct_id_str)
                except ValueError:
                    continue
                conn.execute("""
                    INSERT INTO users (plex_user_id, username, thumb, is_protected, source, last_synced)
                    VALUES (?, ?, '', ?, 'jellyfin', datetime('now'))
                    ON CONFLICT(plex_user_id) DO UPDATE SET
                        username = excluded.username,
                        is_protected = excluded.is_protected,
                        source = 'jellyfin',
                        last_synced = datetime('now')
                """, (acct_id, username, username in protected))
            conn.commit()

            user_id_map: dict[int, int] = {}
            for row in conn.execute(
                "SELECT id, plex_user_id FROM users WHERE plex_user_id IS NOT NULL"
            ).fetchall():
                user_id_map[int(row["plex_user_id"])] = int(row["id"])

            jf_rows_inserted = 0
            batch = 0
            for h in jf_rows:
                acct = h["account_id"]
                internal_id = user_id_map.get(acct)
                if internal_id is None:
                    continue
                rk = h["rating_key"]
                watched_at = h["watched_at"]
                if not rk or not watched_at:
                    continue

                view_offset_ms = h["view_offset_ms"]
                media_duration_ms = h["media_duration_ms"]
                pct = 0
                if media_duration_ms > 0:
                    pct = int(round((view_offset_ms / media_duration_ms) * 100))
                    pct = max(0, min(100, pct))

                conn.execute("""
                    INSERT INTO watch_history
                        (user_id, rating_key, title, grandparent_title, media_type,
                         season_number, episode_number, watched_at,
                         play_duration, media_duration, percent_complete)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    ON CONFLICT(user_id, rating_key, watched_at) DO UPDATE SET
                        media_duration = CASE WHEN excluded.media_duration > watch_history.media_duration
                            THEN excluded.media_duration ELSE watch_history.media_duration END,
                        percent_complete = CASE WHEN excluded.percent_complete > watch_history.percent_complete
                            THEN excluded.percent_complete ELSE watch_history.percent_complete END,
                        grandparent_title = CASE WHEN excluded.grandparent_title != ''
                            THEN excluded.grandparent_title ELSE watch_history.grandparent_title END
                """, (internal_id, rk, h["title"], h["grandparent_title"], h["media_type"],
                      h["season_number"], h["episode_number"], watched_at,
                      media_duration_ms // 1000, pct))
                jf_rows_inserted += 1
                batch += 1
                if batch >= 500:
                    conn.commit()
                    batch = 0
            conn.commit()

            log.info("Jellyfin: inserted/updated %d watch_history rows", jf_rows_inserted)
        except Exception as e:
            log.warning("Jellyfin user/history sync failed: %s", e)
