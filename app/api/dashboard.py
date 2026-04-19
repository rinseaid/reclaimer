"""Dashboard API endpoint."""
import json as _json

from fastapi import APIRouter
from ..database import get_db
from .. import config

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
def get_dashboard():
    """Return aggregated stats for the dashboard landing page.

    Consolidated from 5 SQL round-trips down to 3:
      1. items aggregation -- per (collection, status) counts + per-rating_key
         max size in a single scan (was 3 separate queries).
      2. collection_config snapshot.
      3. most recent run_completed activity_log row.
    ``config.get()`` is an in-memory cache, not a DB call, so the settings
    block is free.
    """
    conn = get_db()

    # One pass over the items table collects everything we need:
    #   * per (collection, status) count + summed size -- for the per-rule
    #     summary cards
    #   * per rating_key MAX(size_bytes) -- for the unique-size aggregate
    #   * flags for "was ever staged?" -- for the pending_actions count
    # We group by rating_key so we can dedup without a second query.
    #
    # Output shape per row:
    #   (rating_key, collection, status, size_bytes, row_count, any_staged)
    # ...where row_count is how many (rating_key, collection, status) pairs
    # exist (usually 1, but defensive in case of schema drift).
    items_rows = conn.execute("""
        SELECT collection,
               status,
               COUNT(*)            AS count,
               SUM(size_bytes)     AS total_bytes,
               rating_key,
               MAX(size_bytes)     AS rk_max_size
        FROM items
        GROUP BY collection, status, rating_key
    """).fetchall()

    col_summary: dict = {}
    # rating_key -> max size across all collection entries (for total_size_bytes)
    rk_max: dict[str, int] = {}
    # rating_key -> True if staged in any collection (for pending_actions)
    rk_staged: set[str] = set()

    for row in items_rows:
        col = row["collection"]
        status = row["status"]
        rk = row["rating_key"]
        rk_size = row["rk_max_size"] or 0
        total_bytes = row["total_bytes"] or 0
        count = row["count"] or 0

        stats = col_summary.get(col)
        if stats is None:
            stats = {"staged": 0, "actioned": 0, "migrated": 0, "kept": 0, "total_bytes": 0}
            col_summary[col] = stats
        # Accumulate -- if a status key isn't pre-seeded we'll add it anyway
        # (schema has a default of 'staged' but nothing pins the vocabulary).
        stats[status] = stats.get(status, 0) + count
        stats["total_bytes"] += total_bytes

        # Record every rating_key seen (any status, any size) so total_tracked
        # matches the unique-keys universe pending_actions draws from. Previous
        # impl only wrote into rk_max when the new size beat the current max,
        # which silently dropped rating_keys whose only entries had size=0
        # (e.g. seasons that don't track their own size). That made
        # pending_actions > total_tracked, which is nonsensical.
        rk_max[rk] = max(rk_max.get(rk, 0), rk_size)

        if status == "staged":
            rk_staged.add(rk)

    unique_count = len(rk_max)
    unique_size = sum(rk_max.values())
    pending = len(rk_staged)

    # Merge collection_config data (action, enabled) into summaries.
    configs = conn.execute(
        "SELECT name, action, grace_days, criteria, enabled FROM collection_config"
    ).fetchall()
    config_map = {r["name"]: r for r in configs}
    for col_name, stats in col_summary.items():
        cfg = config_map.get(col_name)
        if cfg:
            stats["action"] = cfg["action"]
            stats["enabled"] = bool(cfg["enabled"])
            stats["grace_days"] = cfg["grace_days"]
            try:
                criteria = _json.loads(cfg["criteria"]) if cfg["criteria"] else {}
                stats["action_pipeline"] = criteria.get("action_pipeline", [])
            except (ValueError, TypeError):
                stats["action_pipeline"] = []
        else:
            stats["action"] = "none"
            stats["enabled"] = True
            stats["grace_days"] = 30
            stats["action_pipeline"] = []
    # Include configured collections with no items yet
    for name, cfg in config_map.items():
        if name not in col_summary:
            try:
                criteria = _json.loads(cfg["criteria"]) if cfg["criteria"] else {}
                pipeline = criteria.get("action_pipeline", [])
            except (ValueError, TypeError):
                pipeline = []
            col_summary[name] = {
                "staged": 0, "actioned": 0, "migrated": 0, "kept": 0,
                "total_bytes": 0, "action": cfg["action"], "enabled": bool(cfg["enabled"]),
                "grace_days": cfg["grace_days"], "action_pipeline": pipeline,
            }

    # Last run
    last_run = conn.execute("""
        SELECT timestamp, detail FROM activity_log
        WHERE event_type = 'run_completed'
        ORDER BY timestamp DESC LIMIT 1
    """).fetchone()

    conn.close()

    return {
        "collections": col_summary,
        "last_run": dict(last_run) if last_run else None,
        "pending_actions": pending,
        "total_tracked": unique_count,
        "total_size_bytes": unique_size,
        "settings": {
            "movies_action": config.get("movies_action"),
            "tv_action": config.get("tv_action"),
            "ended_action": config.get("ended_action"),
        },
    }
