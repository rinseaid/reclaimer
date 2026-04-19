"""Activity log API."""
from fastapi import APIRouter, Query
from ..database import get_db

router = APIRouter(tags=["activity"])


@router.delete("/activity")
def clear_activity():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) as c FROM activity_log").fetchone()["c"]
    conn.execute("DELETE FROM activity_log")
    conn.commit()
    conn.close()
    return {"deleted": count}


@router.get("/activity")
def get_activity(
    event_type: str = Query(None, description="Filter by event type"),
    collection: str = Query(None, description="Filter by rule/collection name"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    conn = get_db()
    offset = (page - 1) * per_page

    where_clauses = []
    params = []
    if event_type:
        where_clauses.append("event_type = ?")
        params.append(event_type)
    if collection:
        # Accept either the display name ("Seasons Leaving Soon") or the slug
        # form ("seasons-leaving-soon") used in URLs. Rule URLs pass slugs;
        # the activity_log column stores display names.
        where_clauses.append(
            "(collection = ? OR LOWER(REPLACE(collection, ' ', '-')) = LOWER(?))"
        )
        params.append(collection)
        params.append(collection)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    rows = conn.execute(f"""
        SELECT * FROM activity_log{where_sql}
        ORDER BY timestamp DESC LIMIT ? OFFSET ?
    """, params + [per_page, offset]).fetchall()
    total = conn.execute(
        f"SELECT COUNT(*) as count FROM activity_log{where_sql}",
        params,
    ).fetchone()["count"]

    conn.close()
    return {
        "activity": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


_WATCH_SORT_MAP = {
    "watched": "MAX(wh.watched_at)",
    "user": "LOWER(MIN(u.username))",
    "title": "COALESCE(MIN(wh.grandparent_title), MIN(wh.title))",
    "duration": "SUM(wh.play_duration)",
}

def _watch_sort_col(sort: str) -> str:
    return _WATCH_SORT_MAP.get(sort, _WATCH_SORT_MAP["watched"])

def _watch_sort_dir(d: str) -> str:
    return "ASC" if d.lower() == "asc" else "DESC"


@router.get("/activity/watch")
def get_watch_activity(
    user: str = Query("", description="Filter by username"),
    search: str = Query("", description="Search title or show name"),
    media_type: str = Query("", description="Filter by media type (movie, episode)"),
    sort: str = Query("watched", description="Sort: watched, user, title, duration"),
    sort_dir: str = Query("desc", description="asc or desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    """Consolidated watch history across all users.

    Groups watches of the same item (user_id + rating_key) on the same
    calendar day into a single entry with summed duration and play count.
    """
    conn = get_db()
    offset = (page - 1) * per_page

    where_clauses = []
    params: list = []

    if user:
        # Case-insensitive match so a user picked from the dropdown collapses
        # Plex + Jellyfin rows with the same display name.
        where_clauses.append("LOWER(u.username) = LOWER(?)")
        params.append(user)
    if search:
        where_clauses.append(
            "(wh.title LIKE ? OR wh.grandparent_title LIKE ?)"
        )
        params.extend([f"%{search}%", f"%{search}%"])
    if media_type:
        where_clauses.append("wh.media_type = ?")
        params.append(media_type)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # We GROUP BY lowercased username (not user_id) so a single human with a
    # Plex account AND a Jellyfin account collapses into one row per item
    # per day, instead of showing twice.
    count_sql = f"""
        SELECT COUNT(*) as count FROM (
            SELECT 1
            FROM watch_history wh
            JOIN users u ON u.id = wh.user_id
            {where_sql}
            GROUP BY LOWER(u.username), wh.rating_key, DATE(wh.watched_at)
        )
    """
    total = conn.execute(count_sql, params).fetchone()["count"]

    # Count of distinct users (by name, case-insensitive) in the filtered set.
    matching_users_sql = f"""
        SELECT COUNT(DISTINCT LOWER(u.username)) AS c
        FROM watch_history wh
        JOIN users u ON u.id = wh.user_id
        {where_sql}
    """
    matching_users = conn.execute(matching_users_sql, params).fetchone()["c"]

    # Data query.
    #
    # Previously this used two correlated subqueries per output row:
    #   * media_duration fallback -- "max non-zero media_duration for this rk"
    #   * max_percent_complete   -- "max percent_complete for (user, rk)"
    # Both scaled with total watch_history rows rather than page size. We
    # replace them with pre-aggregated CTEs that are each O(table) but only
    # computed once, then LEFT JOIN onto the paged result.
    data_sql = f"""
        WITH rk_media AS (
            SELECT rating_key, MAX(media_duration) AS media_duration
            FROM watch_history
            WHERE media_duration > 0
            GROUP BY rating_key
        ),
        user_rk_pct AS (
            SELECT user_id, rating_key, MAX(percent_complete) AS max_pct
            FROM watch_history
            GROUP BY user_id, rating_key
        )
        SELECT
            MIN(wh.user_id) as user_id,
            MIN(u.username) as username,
            MAX(u.thumb) as thumb,
            -- Combine sources when the same username exists on both servers.
            -- 'plex+jellyfin' indicates the human watched via both.
            CASE
                WHEN COUNT(DISTINCT u.source) > 1 THEN 'plex+jellyfin'
                ELSE MIN(u.source)
            END as source,
            wh.rating_key,
            MIN(wh.title) as title,
            MIN(wh.grandparent_title) as grandparent_title,
            MIN(wh.media_type) as media_type,
            MIN(wh.season_number) as season_number,
            MIN(wh.episode_number) as episode_number,
            MIN(wh.watched_at) as first_watched,
            MAX(wh.watched_at) as last_watched,
            SUM(wh.play_duration) as total_duration,
            SUM(CASE WHEN wh.play_duration > 0 THEN 1 ELSE 0 END) as play_count,
            COALESCE(NULLIF(MAX(wh.media_duration), 0),
                     MAX(rk_media.media_duration),
                     0) as media_duration,
            MAX(user_rk_pct.max_pct) as max_percent_complete
        FROM watch_history wh
        JOIN users u ON u.id = wh.user_id
        LEFT JOIN rk_media     ON rk_media.rating_key = wh.rating_key
        LEFT JOIN user_rk_pct  ON user_rk_pct.user_id = wh.user_id
                               AND user_rk_pct.rating_key = wh.rating_key
        {where_sql}
        GROUP BY LOWER(u.username), wh.rating_key, DATE(wh.watched_at)
        ORDER BY {_watch_sort_col(sort)} {_watch_sort_dir(sort_dir)}
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(data_sql, params + [per_page, offset]).fetchall()

    conn.close()
    return {
        "activity": [dict(r) for r in rows],
        "total": total,
        "matching_users": matching_users,
        "page": page,
        "per_page": per_page,
        "filters": {"user": user, "search": search, "media_type": media_type},
    }


@router.get("/activity/watch/segments")
def get_watch_segments(
    user_id: int = Query(...),
    rating_key: str = Query(...),
    date: str = Query(..., description="Calendar date YYYY-MM-DD to scope segments"),
):
    """Return individual play segments for a consolidated watch entry."""
    conn = get_db()
    rows = conn.execute("""
        SELECT wh.watched_at, wh.play_duration,
               COALESCE(NULLIF(wh.media_duration, 0),
                 (SELECT MAX(wh2.media_duration) FROM watch_history wh2
                  WHERE wh2.rating_key = wh.rating_key AND wh2.media_duration > 0),
               0) as media_duration,
               COALESCE(wh.percent_complete, 0) as percent_complete,
               wh.title
        FROM watch_history wh
        WHERE wh.user_id = ? AND wh.rating_key = ? AND DATE(wh.watched_at) = ?
        ORDER BY wh.watched_at ASC
    """, (user_id, rating_key, date)).fetchall()
    conn.close()
    return {"segments": [dict(r) for r in rows]}
