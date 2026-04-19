"""Item detail API - full rule evaluation and action override."""
import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from ..database import get_db

VALID_ACTIONS = {"keep", "delete", "clear"}
BULK_MAX = 500

router = APIRouter(tags=["items"])


class ActionOverride(BaseModel):
    action: str  # keep | migrate | delete | clear
    reason: Optional[str] = None


class AddToKeepBody(BaseModel):
    """Optional target collection name. Defaults to the rule's first
    protected_collection when omitted."""
    collection_name: Optional[str] = None


class WatchlistBody(BaseModel):
    """Optional target Seerr user id. Omitted / None means "the user that owns
    the MCM Seerr API key" (admin). Future multi-user work can populate this
    from the authenticated session or a user picker without changing any
    server/DB shapes."""
    user_id: Optional[int] = None


class BulkActionBody(BaseModel):
    action: str  # 'keep' | 'delete' | 'clear'
    rating_keys: List[str]
    reason: Optional[str] = None


@router.get("/items/search")
def search_items(q: str = Query("", min_length=2)):
    """Search items in rules AND all Plex/Jellyfin libraries."""
    from .. import config
    from ..core import plex
    results = []
    seen_keys = set()

    # 1. Search our items table (items in rule collections)
    conn = get_db()
    db_items = conn.execute("""
        SELECT DISTINCT i.rating_key, i.title, i.media_type, i.collection,
               i.status, i.size_bytes
        FROM items i
        WHERE i.title LIKE ?
        ORDER BY i.title
        LIMIT 20
    """, (f"%{q}%",)).fetchall()
    for item in db_items:
        d = dict(item)
        d["source"] = "rule"
        results.append(d)
        seen_keys.add(str(d["rating_key"]))
    conn.close()

    # 2. Search Plex libraries directly
    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    if plex_url and plex_token:
        try:
            from ..core.clients import get_client
            client = get_client()
            r = client.get(
                f"{plex_url}/hubs/search",
                params={"X-Plex-Token": plex_token, "query": q, "limit": "15"},
                headers={"Accept": "application/json"},
                timeout=10,
            )
            if r.status_code == 200:
                hubs = r.json().get("MediaContainer", {}).get("Hub", [])
                for hub in hubs:
                    if hub.get("type") not in ("movie", "show"):
                        continue
                    for item in hub.get("Metadata", []):
                        rk = str(item.get("ratingKey", ""))
                        if rk and rk not in seen_keys:
                            results.append({
                                "rating_key": rk,
                                "title": item.get("title", ""),
                                "media_type": hub.get("type", ""),
                                "collection": None,
                                "status": None,
                                "size_bytes": 0,
                                "source": "plex",
                            })
                            seen_keys.add(rk)
        except Exception:
            pass

    return {"results": results[:30]}


@router.get("/items/{rating_key}")
def get_item(rating_key: str):
    conn = get_db()

    # Get all collection entries for this item
    entries = conn.execute(
        "SELECT * FROM items WHERE rating_key = ?", (rating_key,)
    ).fetchall()

    if not entries:
        # Item not in any rule - try fetching from Plex directly
        conn.close()
        from .. import config
        plex_url = config.get("plex_url")
        plex_token = config.get("plex_token")
        if plex_url and plex_token:
            try:
                from ..core.plex import _req
                r = _req(plex_url, plex_token, "get", f"/library/metadata/{rating_key}")
                data = r.json().get("MediaContainer", {}).get("Metadata", [{}])[0]
                from ..core.plex import external_id

                # Derive first_seen from Plex addedAt (Unix timestamp)
                added_at = data.get("addedAt")
                first_seen_str = (
                    datetime.datetime.fromtimestamp(added_at).strftime("%Y-%m-%d")
                    if added_at else None
                )

                # Sum size from all Media parts
                total_size = 0
                for media in data.get("Media", []):
                    for part in media.get("Part", []):
                        total_size += part.get("size", 0) or 0

                # Map Plex type to media_type ("show" for TV series, "movie" for movies)
                plex_type = data.get("type", "")

                # Determine watch status from Plex viewCount
                view_count = data.get("viewCount") or 0
                status = "watched" if view_count > 0 else "unwatched"

                plex_entry = {
                    "rating_key": rating_key,
                    "title": data.get("title", ""),
                    "media_type": plex_type,
                    "collection": None,
                    "status": status,
                    "size_bytes": total_size,
                    "first_seen": first_seen_str,
                    "grace_expires": None,
                    "override": None,
                    "tmdb_id": None,
                    "tvdb_id": None,
                    "imdb_id": None,
                    "arr_id": None,
                }
                # Extract external IDs from Guid
                for guid in data.get("Guid", []):
                    gid = guid.get("id", "")
                    if gid.startswith("tmdb://"):
                        plex_entry["tmdb_id"] = int(gid[7:]) if gid[7:].isdigit() else None
                    elif gid.startswith("tvdb://"):
                        plex_entry["tvdb_id"] = int(gid[7:]) if gid[7:].isdigit() else None
                    elif gid.startswith("imdb://"):
                        plex_entry["imdb_id"] = gid[7:]
                # Extract ratings from Plex metadata
                from ..core.ratings import extract_plex_ratings
                plex_ratings = extract_plex_ratings(data)

                # Watch history from our DB - for shows, match by title too
                conn2 = get_db()
                plex_title = plex_entry["title"]
                plex_type = plex_entry["media_type"]
                if plex_type == "show" and plex_title:
                    watch_history = conn2.execute("""
                        SELECT u.username, u.thumb, wh.watched_at, wh.play_duration,
                               wh.media_duration, wh.media_type, wh.season_number, wh.episode_number,
                               wh.title, wh.grandparent_title
                        FROM watch_history wh JOIN users u ON u.id = wh.user_id
                        WHERE wh.rating_key = ? OR wh.grandparent_title = ? COLLATE NOCASE
                        ORDER BY wh.watched_at DESC
                    """, (rating_key, plex_title)).fetchall()
                    watchers = conn2.execute("""
                        SELECT DISTINCT u.username, u.thumb, COUNT(*) as play_count,
                               MAX(wh.watched_at) as last_watched, SUM(wh.play_duration) as total_duration
                        FROM watch_history wh JOIN users u ON u.id = wh.user_id
                        WHERE wh.rating_key = ? OR wh.grandparent_title = ? COLLATE NOCASE
                        GROUP BY u.id ORDER BY MAX(wh.watched_at) DESC
                    """, (rating_key, plex_title)).fetchall()
                else:
                    watch_history = conn2.execute("""
                        SELECT u.username, u.thumb, wh.watched_at, wh.play_duration,
                               wh.media_duration, wh.media_type, wh.season_number, wh.episode_number,
                               wh.title, wh.grandparent_title
                        FROM watch_history wh JOIN users u ON u.id = wh.user_id
                        WHERE wh.rating_key = ? ORDER BY wh.watched_at DESC
                    """, (rating_key,)).fetchall()
                    watchers = conn2.execute("""
                        SELECT DISTINCT u.username, u.thumb, COUNT(*) as play_count,
                               MAX(wh.watched_at) as last_watched, SUM(wh.play_duration) as total_duration
                        FROM watch_history wh JOIN users u ON u.id = wh.user_id
                        WHERE wh.rating_key = ? GROUP BY u.id ORDER BY MAX(wh.watched_at) DESC
                    """, (rating_key,)).fetchall()
                conn2.close()
                return {
                    "entries": [plex_entry],
                    "rules": [],
                    "debrid_cache": [],
                    "activity": [],
                    "watch_history": [dict(w) for w in watch_history],
                    "watchers": [dict(w) for w in watchers],
                    "ratings": plex_ratings,
                    "source": "plex",
                }
            except Exception:
                pass
        return {"error": "Item not found"}

    # Get rule results
    rules = conn.execute("""
        SELECT collection, rule_name, passed, detail, severity, evaluated_at
        FROM rule_results WHERE rating_key = ?
        ORDER BY collection, rule_name
    """, (rating_key,)).fetchall()

    # Get debrid cache status
    debrid = conn.execute(
        "SELECT provider, is_cached, checked_at FROM debrid_cache WHERE rating_key = ?",
        (rating_key,),
    ).fetchall()

    # Get activity for this item
    activity = conn.execute("""
        SELECT timestamp, event_type, detail FROM activity_log
        WHERE rating_key = ? ORDER BY timestamp DESC LIMIT 20
    """, (rating_key,)).fetchall()

    # Get watch history - for TV shows, also match by show title (episodes have different rating_keys)
    item_title = entries[0]["title"] if entries else ""
    item_type = entries[0]["media_type"] if entries else ""

    if item_type == "show" and item_title:
        watch_history = conn.execute("""
            SELECT u.username, u.thumb, wh.watched_at, wh.play_duration,
                   wh.media_duration, wh.media_type, wh.season_number, wh.episode_number,
                   wh.title, wh.grandparent_title
            FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
            WHERE wh.rating_key = ? OR wh.grandparent_title = ? COLLATE NOCASE
            ORDER BY wh.watched_at DESC
        """, (rating_key, item_title)).fetchall()
        watchers = conn.execute("""
            SELECT DISTINCT u.username, u.thumb, COUNT(*) as play_count,
                   MAX(wh.watched_at) as last_watched,
                   SUM(wh.play_duration) as total_duration
            FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
            WHERE wh.rating_key = ? OR wh.grandparent_title = ? COLLATE NOCASE
            GROUP BY u.id
            ORDER BY MAX(wh.watched_at) DESC
        """, (rating_key, item_title)).fetchall()
    else:
        watch_history = conn.execute("""
            SELECT u.username, u.thumb, wh.watched_at, wh.play_duration,
                   wh.media_duration, wh.media_type, wh.season_number, wh.episode_number,
                   wh.title, wh.grandparent_title
            FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
            WHERE wh.rating_key = ?
            ORDER BY wh.watched_at DESC
        """, (rating_key,)).fetchall()
        watchers = conn.execute("""
            SELECT DISTINCT u.username, u.thumb, COUNT(*) as play_count,
                   MAX(wh.watched_at) as last_watched,
                   SUM(wh.play_duration) as total_duration
            FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
            WHERE wh.rating_key = ?
            GROUP BY u.id
        ORDER BY MAX(wh.watched_at) DESC
    """, (rating_key,)).fetchall()

    # Fetch ratings from Plex metadata
    from .. import config as _cfg
    item_ratings = {}
    plex_url = _cfg.get("plex_url")
    plex_token = _cfg.get("plex_token")
    if plex_url and plex_token:
        try:
            from ..core.plex import _req
            from ..core.ratings import extract_plex_ratings
            r = _req(plex_url, plex_token, "get", f"/library/metadata/{rating_key}")
            plex_data = r.json().get("MediaContainer", {}).get("Metadata", [{}])[0]
            item_ratings = extract_plex_ratings(plex_data)
        except Exception:
            pass

    conn.close()
    return {
        "entries": [dict(e) for e in entries],
        "rules": [dict(r) for r in rules],
        "debrid_cache": [dict(d) for d in debrid],
        "activity": [dict(a) for a in activity],
        "watch_history": [dict(w) for w in watch_history],
        "watchers": [dict(w) for w in watchers],
        "ratings": item_ratings,
    }


@router.get("/items/{rating_key}/evaluate")
def evaluate_item_live(rating_key: str):
    """Evaluate all enabled collection rules against an item in real-time."""
    from .. import config
    from ..core import plex, radarr, sonarr, overseerr
    from ..rules.engine import EvaluationContext, evaluate_movie, evaluate_show, is_candidate
    from ..rules.criteria import CollectionCriteria

    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    if not plex_url or not plex_token:
        return {"error": "Plex not configured"}

    # Fetch item from Plex
    try:
        r = plex._req(plex_url, plex_token, "get", f"/library/metadata/{rating_key}",
                       params={"includeGuids": "1"})
        data = r.json().get("MediaContainer", {}).get("Metadata", [])
        if not data:
            return {"error": "Item not found in Plex"}
        item = data[0]
    except Exception:
        return {"error": f"Item {rating_key} not found in Plex or Plex is unreachable"}

    item_type = item.get("type", "")  # "movie" or "show"
    plex_title = item.get("title", "")

    # Pull play count + last watch in a single query per scope instead of
    # four (count-by-rk, last-by-rk, count-by-title, last-by-title).
    conn = get_db()
    rk_row = conn.execute(
        """SELECT COUNT(*) AS c, MAX(watched_at) AS last_watched
           FROM watch_history WHERE rating_key = ?""",
        (rating_key,),
    ).fetchone()
    db_plays = {rating_key: rk_row["c"]} if rk_row and rk_row["c"] else {}
    last_watch_dates = {}
    if rk_row and rk_row["last_watched"]:
        last_watch_dates[rating_key] = rk_row["last_watched"]

    # Show-title-scoped counts only matter for TV shows.
    db_plays_by_title = {}
    last_watch_by_title = {}
    if plex_title and item_type == "show":
        title_row = conn.execute(
            """SELECT COUNT(*) AS c, MAX(watched_at) AS last_watched
               FROM watch_history WHERE grandparent_title = ? COLLATE NOCASE""",
            (plex_title,),
        ).fetchone()
        if title_row and title_row["c"]:
            db_plays_by_title[plex_title.lower()] = title_row["c"]
        if title_row and title_row["last_watched"]:
            last_watch_by_title[plex_title.lower()] = title_row["last_watched"]
    elif plex_title:
        # Movies still need last_watch_by_title lookup for criteria matching
        # (rare, but the old code ran this unconditionally).
        lw_title_row = conn.execute(
            """SELECT MAX(watched_at) AS last_watched
               FROM watch_history WHERE grandparent_title = ? COLLATE NOCASE""",
            (plex_title,),
        ).fetchone()
        if lw_title_row and lw_title_row["last_watched"]:
            last_watch_by_title[plex_title.lower()] = lw_title_row["last_watched"]

    # Play count for this item comes from watch_history (ingested directly
    # from Plex/Jellyfin native session history).
    play_counts = dict(db_plays) if db_plays else {}

    # Get external IDs
    from ..core.plex import external_id
    tmdb_id_str = external_id(item, "tmdb")
    tvdb_id_str = external_id(item, "tvdb")
    imdb_id_str = external_id(item, "imdb")
    tmdb = int(tmdb_id_str) if tmdb_id_str else None
    tvdb = int(tvdb_id_str) if tvdb_id_str else None

    # Fetch external ratings (OMDB)
    # Extract ratings from Plex metadata
    from ..core.ratings import extract_plex_ratings
    ratings = extract_plex_ratings(item)

    # Build ratings_cache for EvaluationContext (keyed by rating_key)
    ratings_cache_ctx = {}
    if ratings.get("critic_rating") is not None or ratings.get("audience_rating") is not None:
        ratings_cache_ctx[rating_key] = ratings

    # Run the independent upstream fetches (Radarr/Sonarr, Overseerr, Plex
    # keep-collection) concurrently. Matches the orchestrator.py pattern for
    # bounded per-run parallelism and cuts wall time from sum() to max() of
    # the individual calls. I/O-bound httpx calls drop the GIL so a thread
    # pool is sufficient.
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_arr():
        try:
            from ..core import arr_instances
            if item_type == "movie" and tmdb:
                inst = arr_instances.default_instance("radarr")
                if inst:
                    return ("radarr", radarr.fetch_movie_by_tmdb(
                        inst["url"], inst["api_key"], tmdb))
            elif item_type == "show" and tvdb:
                inst = arr_instances.default_instance("sonarr")
                if inst:
                    return ("sonarr", sonarr.fetch_show_by_tvdb(
                        inst["url"], inst["api_key"], tvdb))
        except Exception:
            pass
        return (None, None)

    def _fetch_overseerr():
        try:
            ov_url = config.get("overseerr_url")
            ov_key = config.get("overseerr_api_key")
            if not (ov_url and ov_key):
                return None
            protected_str = config.get("protected_requesters") or ""
            protected = {u.strip() for u in protected_str.split(",") if u.strip()}
            return overseerr.fetch_active_requests(ov_url, ov_key, protected)
        except Exception:
            return None

    def _fetch_keep_keys():
        try:
            keep_col = config.get("plex_movies_keep_collection") if item_type == "movie" else config.get("plex_tv_keep_collection")
            section = config.get("plex_movies_section") if item_type == "movie" else config.get("plex_tv_section")
            if keep_col and section:
                return plex.fetch_keep_collection(plex_url, plex_token, section, keep_col)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=4) as pool:
        fut_arr = pool.submit(_fetch_arr)
        fut_ov = pool.submit(_fetch_overseerr)
        fut_keep = pool.submit(_fetch_keep_keys)

        arr_kind, arr_data = fut_arr.result()
        ov_result = fut_ov.result()
        plex_keep_keys = fut_keep.result() or set()

    radarr_movies = arr_data if arr_kind == "radarr" and arr_data else {}
    sonarr_shows = arr_data if arr_kind == "sonarr" and arr_data else {}

    # Unpack Overseerr result (9-tuple) if present, otherwise empty defaults.
    overseerr_active_movies = set()
    overseerr_active_shows = set()
    overseerr_active_shows_tmdb = set()
    overseerr_protected_movies = set()
    overseerr_protected_shows = set()
    overseerr_protected_shows_tmdb = set()
    movie_requesters = {}
    show_requesters = {}
    show_requesters_tmdb = {}
    if ov_result:
        (am, ash, pm, ps, mr, sr,
         as_tmdb, ps_tmdb, sr_tmdb) = ov_result
        overseerr_active_movies = am
        overseerr_active_shows = ash
        overseerr_active_shows_tmdb = as_tmdb
        overseerr_protected_movies = pm
        overseerr_protected_shows = ps
        overseerr_protected_shows_tmdb = ps_tmdb
        movie_requesters = mr
        show_requesters = sr
        show_requesters_tmdb = sr_tmdb

    # Build user_watches from DB (runs alongside the upstream fetches above
    # would be nice, but sqlite3 connection objects aren't thread-safe here;
    # keeping on the main thread is fine since it's a small local query.)
    user_watches = {}
    try:
        uw_rows = conn.execute("""
            SELECT u.username, wh.rating_key, wh.grandparent_title FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
        """).fetchall()
        for row in uw_rows:
            s = user_watches.setdefault(row["username"], set())
            s.add(str(row["rating_key"]))
            if row["grandparent_title"]:
                s.add(row["grandparent_title"].lower())
    except Exception:
        pass

    # Build context
    ctx = EvaluationContext(
        play_counts=play_counts,
        radarr_movies=radarr_movies,
        sonarr_shows=sonarr_shows,
        overseerr_active_movies=overseerr_active_movies,
        overseerr_active_shows=overseerr_active_shows,
        overseerr_active_shows_tmdb=overseerr_active_shows_tmdb,
        overseerr_protected_movies=overseerr_protected_movies,
        overseerr_protected_shows=overseerr_protected_shows,
        overseerr_protected_shows_tmdb=overseerr_protected_shows_tmdb,
        plex_keep_keys=plex_keep_keys,
        db_plays=db_plays, db_plays_by_title=db_plays_by_title,
        movie_requesters=movie_requesters,
        show_requesters=show_requesters,
        show_requesters_tmdb=show_requesters_tmdb,
        user_watches=user_watches,
        last_watch_dates=last_watch_dates,
        last_watch_by_title=last_watch_by_title,
        ratings_cache=ratings_cache_ctx,
    )

    # Evaluate against all enabled collections
    collection_configs = conn.execute(
        "SELECT * FROM collection_config WHERE enabled = 1 ORDER BY id"
    ).fetchall()
    conn.close()

    evaluate_fn = evaluate_movie if item_type == "movie" else evaluate_show

    results_by_collection = []
    for col_cfg in collection_configs:
        col_cfg = dict(col_cfg)
        # Skip if media type doesn't match
        if col_cfg["media_type"] != item_type:
            continue

        criteria = CollectionCriteria.from_json(col_cfg.get("criteria"))
        criteria.action = col_cfg.get("action", criteria.action)
        criteria.grace_days = col_cfg.get("grace_days", criteria.grace_days)

        rule_results = evaluate_fn(item, ctx, criteria=criteria)
        candidate = is_candidate(rule_results)

        results_by_collection.append({
            "collection": col_cfg["name"],
            "action": col_cfg.get("action", "none"),
            "grace_days": col_cfg.get("grace_days", 30),
            "is_candidate": candidate,
            "rules": [
                {"rule_name": r.name, "passed": r.passed, "detail": r.detail, "severity": r.severity}
                for r in rule_results
            ],
        })

    # Build Seerr request info for this item
    seerr_request = None
    if item_type == "movie":
        requester = movie_requesters.get(tmdb)
    else:
        requester = show_requesters.get(tvdb) or show_requesters_tmdb.get(tmdb)
    if requester:
        plex_title = item.get("title", "")
        requester_watched = (requester in user_watches
                             and (rating_key in user_watches[requester]
                                  or plex_title in user_watches[requester]))
        # Get requester's watch details from DB
        watch_details = []
        try:
            conn2 = get_db()
            plex_title = item.get("title", "")
            if item_type == "show" and plex_title:
                rows = conn2.execute("""
                    SELECT wh.watched_at, wh.play_duration, wh.title, wh.season_number, wh.episode_number
                    FROM watch_history wh JOIN users u ON u.id = wh.user_id
                    WHERE u.username = ? AND (wh.rating_key = ? OR wh.grandparent_title = ? COLLATE NOCASE)
                    ORDER BY wh.watched_at DESC LIMIT 10
                """, (requester, rating_key, plex_title)).fetchall()
            else:
                rows = conn2.execute("""
                    SELECT wh.watched_at, wh.play_duration FROM watch_history wh
                    JOIN users u ON u.id = wh.user_id
                    WHERE u.username = ? AND wh.rating_key = ?
                    ORDER BY wh.watched_at DESC LIMIT 5
                """, (requester, rating_key)).fetchall()
            watch_details = [dict(r) for r in rows]
            conn2.close()
        except Exception:
            pass
        seerr_request = {
            "requester": requester,
            "watched": requester_watched,
            "watch_details": watch_details,
        }

    return {
        "rating_key": rating_key,
        "title": item.get("title", ""),
        "type": item_type,
        "evaluations": results_by_collection,
        "seerr_request": seerr_request,
        "ratings": ratings,
    }


@router.get("/items/{rating_key}/seerr-requests")
def get_seerr_requests(rating_key: str):
    """Fetch Seerr request history for this item, with arr tag fallback."""
    from .. import config
    from ..core import plex, overseerr, radarr, sonarr

    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")

    # Resolve IDs and media type
    conn = get_db()
    db_item = conn.execute(
        "SELECT tmdb_id, tvdb_id, media_type FROM items WHERE rating_key = ? LIMIT 1",
        (rating_key,)
    ).fetchone()
    conn.close()

    tmdb_id = None
    tvdb_id = None
    media_type = None

    if db_item:
        tmdb_id = db_item["tmdb_id"]
        tvdb_id = db_item["tvdb_id"]
        media_type = db_item["media_type"]

    if not tmdb_id and plex_url and plex_token:
        try:
            r = plex._req(plex_url, plex_token, "get", f"/library/metadata/{rating_key}",
                          params={"includeGuids": "1"})
            data = r.json().get("MediaContainer", {}).get("Metadata", [{}])[0]
            media_type = data.get("type", "")
            for guid in data.get("Guid", []):
                gid = guid.get("id", "")
                if gid.startswith("tmdb://"):
                    tmdb_id = int(gid[7:]) if gid[7:].isdigit() else None
                elif gid.startswith("tvdb://"):
                    tvdb_id = int(gid[7:]) if gid[7:].isdigit() else None
        except Exception:
            pass

    # 1. Try Seerr API directly
    seerr_requests = []
    ov_url = config.get("overseerr_url")
    ov_key = config.get("overseerr_api_key")
    if ov_url and ov_key and tmdb_id:
        seerr_requests = overseerr.fetch_item_requests(ov_url, ov_key, media_type, tmdb_id)

    # 2. Check arr tags for Seerr-pattern requesters as fallback
    tag_requesters = []
    try:
        from ..core import arr_instances
        if media_type == "movie" and tmdb_id:
            inst = arr_instances.default_instance("radarr")
            if inst:
                movies = radarr.fetch_movie_by_tmdb(
                    inst["url"], inst["api_key"], tmdb_id)
                if tmdb_id in movies:
                    tag_names = movies[tmdb_id].get("_tag_names", [])
                    tag_requesters = overseerr.extract_requesters_from_tags(tag_names)
        elif media_type == "show" and tvdb_id:
            inst = arr_instances.default_instance("sonarr")
            if inst:
                shows = sonarr.fetch_show_by_tvdb(
                    inst["url"], inst["api_key"], tvdb_id)
                if tvdb_id in shows:
                    tag_names = shows[tvdb_id].get("_tag_names", [])
                    tag_requesters = overseerr.extract_requesters_from_tags(tag_names)
    except Exception:
        pass

    return {
        "requests": seerr_requests,
        "tag_requesters": tag_requesters,
        "tmdb_id": tmdb_id,
        "media_type": media_type,
    }


@router.post("/items/{rating_key}/action")
def set_action_override(rating_key: str, override: ActionOverride):
    if override.action not in VALID_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action '{override.action}'. Must be one of: {', '.join(sorted(VALID_ACTIONS))}",
        )
    conn = get_db()

    action = override.action if override.action != "clear" else None
    conn.execute(
        "UPDATE items SET override = ?, override_by = ? WHERE rating_key = ?",
        (action, override.reason, rating_key),
    )

    # Get item info for the log
    item = conn.execute(
        "SELECT title, collection FROM items WHERE rating_key = ? LIMIT 1", (rating_key,)
    ).fetchone()
    item_title = item["title"] if item else rating_key
    item_collection = item["collection"] if item else None

    # Log it
    conn.execute("""
        INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
        VALUES ('item_override', ?, ?, ?, ?)
    """, (item_collection, rating_key, item_title,
          f"Override set to: {override.action}" + (f" ({override.reason})" if override.reason else "")))

    conn.commit()
    conn.close()
    return {"status": "ok", "override": action}


@router.post("/items/{rating_key}/add-to-keep")
def add_item_to_keep_collection(rating_key: str, body: AddToKeepBody):
    """Add this item (or its parent show for seasons) to the rule's keep collection.

    Resolves the rule's ``protected_collections`` list; uses the caller-supplied
    name if given, else the first entry. For season items the parent show's
    rating_key is used so the whole show is protected, not just the season.
    """
    from .. import config as app_config
    from ..core import plex, jellyfin
    from ..rules.criteria import CollectionCriteria

    conn = get_db()
    item = conn.execute(
        "SELECT title, collection, media_type, show_rating_key "
        "FROM items WHERE rating_key = ? LIMIT 1", (rating_key,)
    ).fetchone()
    if not item:
        conn.close()
        raise HTTPException(404, f"Item {rating_key} not found")

    rule_row = conn.execute(
        "SELECT criteria FROM collection_config WHERE name = ?",
        (item["collection"],),
    ).fetchone()
    if not rule_row:
        conn.close()
        raise HTTPException(404, f"Rule '{item['collection']}' not found")

    criteria = CollectionCriteria.from_json(rule_row["criteria"])
    candidates = criteria.protected_collections or []
    if not candidates:
        conn.close()
        raise HTTPException(
            400,
            "Rule has no protected collections configured. Add one on the rule's Protections tab first.",
        )
    target_name = body.collection_name or candidates[0]
    if target_name not in candidates:
        conn.close()
        raise HTTPException(400, f"Collection '{target_name}' is not a protected collection on this rule")

    # Season items add the parent show so the whole series is protected.
    target_key = item["show_rating_key"] or rating_key

    section_id = criteria.library_section_id
    source = criteria.library_source or "plex"

    try:
        if source == "plex":
            plex_url = app_config.get("plex_url")
            plex_token = app_config.get("plex_token")
            if not plex_url or not plex_token:
                raise HTTPException(500, "Plex is not configured")
            media_type = 1 if item["media_type"] == "movie" else 2
            plex.add_to_collection_by_name(
                plex_url, plex_token, section_id, target_name, str(target_key),
                media_type=media_type,
            )
        elif source == "jellyfin":
            jf_url = app_config.get("jellyfin_url")
            jf_key = app_config.get("jellyfin_api_key")
            if not jf_url or not jf_key:
                raise HTTPException(500, "Jellyfin is not configured")
            jellyfin.add_to_collection_by_name(
                jf_url, jf_key, str(section_id), target_name, str(target_key),
            )
        else:
            raise HTTPException(400, f"Unknown library_source: {source}")
    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(502, f"Failed to add to '{target_name}': {e}")

    conn.execute("""
        INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
        VALUES ('item_added_to_keep', ?, ?, ?, ?)
    """, (item["collection"], rating_key, item["title"],
          f"Added to '{target_name}' ({source})"))
    conn.commit()
    conn.close()

    return {"status": "ok", "collection": target_name, "target_rating_key": target_key}


def _resolve_keep_target(rating_key: str, collection_name: Optional[str]):
    """Shared helper: look up the item's rule, pick the protected collection,
    map seasons to show_rating_key, and return everything the add/remove
    endpoints need. Returns (conn, item_row, target_key, target_name, source,
    section_id, media_type) or raises HTTPException."""
    from ..rules.criteria import CollectionCriteria
    conn = get_db()
    item = conn.execute(
        "SELECT title, collection, media_type, show_rating_key "
        "FROM items WHERE rating_key = ? LIMIT 1", (rating_key,)
    ).fetchone()
    if not item:
        conn.close()
        raise HTTPException(404, f"Item {rating_key} not found")
    rule_row = conn.execute(
        "SELECT criteria FROM collection_config WHERE name = ?",
        (item["collection"],),
    ).fetchone()
    if not rule_row:
        conn.close()
        raise HTTPException(404, f"Rule '{item['collection']}' not found")
    criteria = CollectionCriteria.from_json(rule_row["criteria"])
    candidates = criteria.protected_collections or []
    if not candidates:
        conn.close()
        raise HTTPException(
            400,
            "Rule has no protected collections configured. Add one on the rule's Protections tab first.",
        )
    target_name = collection_name or candidates[0]
    if target_name not in candidates:
        conn.close()
        raise HTTPException(400, f"Collection '{target_name}' is not a protected collection on this rule")
    target_key = item["show_rating_key"] or rating_key
    source = criteria.library_source or "plex"
    section_id = criteria.library_section_id
    media_type = 1 if item["media_type"] == "movie" else 2
    return conn, item, str(target_key), target_name, source, section_id, media_type


@router.delete("/items/{rating_key}/keep-collection")
def remove_item_from_keep_collection(rating_key: str, collection_name: Optional[str] = None):
    """Inverse of /add-to-keep. Removes the item (or parent show for seasons)
    from the rule's keep collection."""
    from .. import config as app_config
    from ..core import plex, jellyfin
    conn, item, target_key, target_name, source, section_id, _mt = _resolve_keep_target(
        rating_key, collection_name,
    )
    try:
        if source == "plex":
            plex_url = app_config.get("plex_url")
            plex_token = app_config.get("plex_token")
            if not plex_url or not plex_token:
                raise HTTPException(500, "Plex is not configured")
            plex.remove_from_collection_by_name(
                plex_url, plex_token, section_id, target_name, target_key,
            )
        elif source == "jellyfin":
            jf_url = app_config.get("jellyfin_url")
            jf_key = app_config.get("jellyfin_api_key")
            if not jf_url or not jf_key:
                raise HTTPException(500, "Jellyfin is not configured")
            jellyfin.remove_from_collection_by_name(
                jf_url, jf_key, str(section_id), target_name, target_key,
            )
        else:
            raise HTTPException(400, f"Unknown library_source: {source}")
    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(502, f"Failed to remove from '{target_name}': {e}")
    conn.execute("""
        INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
        VALUES ('item_removed_from_keep', ?, ?, ?, ?)
    """, (item["collection"], rating_key, item["title"],
          f"Removed from '{target_name}' ({source})"))
    conn.commit()
    conn.close()
    return {"status": "ok", "collection": target_name, "target_rating_key": target_key}


def _resolve_watchlist_user(requested: Optional[int]):
    """Resolve the Seerr user id whose watchlist we'll write to / read from.

    - ``requested`` non-None and positive: use as-is. Future auth layer will
      pass the signed-in user's Seerr id here.
    - ``requested`` omitted: fall back to the user that owns the admin API
      key (today: MCM admin; rinseaid). That preserves single-user
      behaviour without the caller having to know the uid.

    Returns (url, key, uid) or raises HTTPException.
    """
    from .. import config as app_config
    from ..core import overseerr
    url = app_config.get("overseerr_url")
    key = app_config.get("overseerr_api_key")
    if not url or not key:
        raise HTTPException(500, "Seerr is not configured")
    if requested is not None and int(requested) > 0:
        return url, key, int(requested)
    uid = overseerr.get_api_user_id(url, key)
    if uid is None:
        raise HTTPException(502, "Could not resolve Seerr admin user id")
    return url, key, uid


@router.post("/items/{rating_key}/watchlist")
def add_item_to_watchlist(rating_key: str, body: WatchlistBody = WatchlistBody()):
    """Add this item's TMDB id to a Seerr user's watchlist.

    ``body.user_id`` targets a specific Seerr user. When omitted, defaults
    to the user that owns the MCM Seerr API key (today the admin). Seasons
    inherit the show's TMDB id, so watchlisting a season effectively
    watchlists the whole show (same behaviour Seerr/Plex themselves exhibit).
    """
    from ..core import overseerr
    conn = get_db()
    item = conn.execute(
        "SELECT title, collection, media_type, tmdb_id "
        "FROM items WHERE rating_key = ? LIMIT 1", (rating_key,)
    ).fetchone()
    if not item:
        conn.close()
        raise HTTPException(404, f"Item {rating_key} not found")
    tmdb = item["tmdb_id"]
    if not tmdb:
        conn.close()
        raise HTTPException(400, "Item has no TMDB id - cannot add to watchlist")
    try:
        url, key, uid = _resolve_watchlist_user(body.user_id)
    except HTTPException:
        conn.close()
        raise
    mt = "movie" if item["media_type"] == "movie" else "tv"
    # Seerr lets the admin API key target another user via X-Api-User.
    on_behalf = uid if body.user_id is not None else None
    ok = overseerr.add_to_watchlist(url, key, int(tmdb), mt, on_behalf_user_id=on_behalf)
    if not ok:
        conn.close()
        raise HTTPException(502, "Seerr rejected the watchlist add")
    conn.execute("""
        INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
        VALUES ('item_added_to_watchlist', ?, ?, ?, ?)
    """, (item["collection"], rating_key, item["title"],
          f"tmdb={tmdb}, type={mt}, user_id={uid}"))
    conn.commit()
    conn.close()
    return {"status": "ok", "tmdb_id": int(tmdb), "media_type": mt, "user_id": uid}


@router.delete("/items/{rating_key}/watchlist")
def remove_item_from_watchlist(rating_key: str, user_id: Optional[int] = None):
    """Inverse of POST /watchlist. ``user_id`` defaults to the admin."""
    from ..core import overseerr
    conn = get_db()
    item = conn.execute(
        "SELECT title, collection, media_type, tmdb_id "
        "FROM items WHERE rating_key = ? LIMIT 1", (rating_key,)
    ).fetchone()
    if not item:
        conn.close()
        raise HTTPException(404, f"Item {rating_key} not found")
    tmdb = item["tmdb_id"]
    if not tmdb:
        conn.close()
        raise HTTPException(400, "Item has no TMDB id")
    try:
        url, key, uid = _resolve_watchlist_user(user_id)
    except HTTPException:
        conn.close()
        raise
    on_behalf = uid if user_id is not None else None
    ok = overseerr.remove_from_watchlist(url, key, int(tmdb), on_behalf_user_id=on_behalf)
    if not ok:
        conn.close()
        raise HTTPException(502, "Seerr rejected the watchlist removal")
    conn.execute("""
        INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
        VALUES ('item_removed_from_watchlist', ?, ?, ?, ?)
    """, (item["collection"], rating_key, item["title"], f"tmdb={tmdb}, user_id={uid}"))
    conn.commit()
    conn.close()
    return {"status": "ok", "tmdb_id": int(tmdb), "user_id": uid}


@router.get("/items/watchlist-members")
def get_watchlist_members(user_id: Optional[int] = None):
    """Return the set of TMDB ids on a Seerr user's watchlist.

    ``user_id`` omitted: returns the admin's watchlist. Used by the rule
    items page to pre-render the +Watchlist toggle state without one lookup
    per row. When multi-user auth lands, the page will call this per
    logged-in user and track per-user toggle state.
    """
    from .. import config as app_config
    from ..core import overseerr
    url = app_config.get("overseerr_url")
    key = app_config.get("overseerr_api_key")
    if not url or not key:
        return {"user_id": None, "tmdb_ids": []}
    if user_id is not None and user_id > 0:
        uid = int(user_id)
    else:
        uid = overseerr.get_api_user_id(url, key)
    if uid is None:
        return {"user_id": None, "tmdb_ids": []}
    ids = overseerr.user_watchlist_tmdb_ids(url, key, uid)
    return {"user_id": uid, "tmdb_ids": sorted(ids)}


@router.post("/items/bulk-action")
def bulk_set_action_override(body: BulkActionBody):
    """Set or clear the override on many items in one transaction.

    Logs one ``item_override`` activity_log row per updated item so the
    activity feed stays per-item rather than a single opaque "bulk" entry.
    """
    if body.action not in VALID_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action '{body.action}'. Must be one of: {', '.join(sorted(VALID_ACTIONS))}",
        )
    if not body.rating_keys:
        return {"status": "ok", "updated": 0}
    if len(body.rating_keys) > BULK_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"Too many items: {len(body.rating_keys)} (max {BULK_MAX})",
        )

    # Dedupe while preserving order - callers occasionally repeat keys.
    seen = set()
    rating_keys = []
    for rk in body.rating_keys:
        if rk and rk not in seen:
            seen.add(rk)
            rating_keys.append(rk)

    action = body.action if body.action != "clear" else None
    reason = body.reason
    detail_suffix = f" ({reason})" if reason else ""
    log_detail = f"Override set to: {body.action}{detail_suffix}"

    conn = get_db()
    try:
        # Look up title/collection for the items we're touching so every
        # activity row has a human-readable title.
        placeholders = ",".join("?" * len(rating_keys))
        rows = conn.execute(
            f"SELECT rating_key, title, collection FROM items WHERE rating_key IN ({placeholders})",
            rating_keys,
        ).fetchall()
        info = {}
        for row in rows:
            # An item may appear in multiple collections - keep the first.
            if row["rating_key"] not in info:
                info[row["rating_key"]] = {
                    "title": row["title"],
                    "collection": row["collection"],
                }

        # Single transaction so a partial failure doesn't leave the table
        # half-updated.
        updated = 0
        for rk in rating_keys:
            cur = conn.execute(
                "UPDATE items SET override = ?, override_by = ? WHERE rating_key = ?",
                (action, reason, rk),
            )
            if cur.rowcount:
                updated += cur.rowcount
            meta = info.get(rk, {})
            conn.execute(
                """INSERT INTO activity_log (event_type, collection, rating_key, title, detail)
                   VALUES ('item_override', ?, ?, ?, ?)""",
                (meta.get("collection"), rk, meta.get("title") or rk, log_detail),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"status": "ok", "updated": updated}
