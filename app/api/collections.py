"""Collections API - browse items with rule explanations, manage collection config."""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Optional
from ..database import get_db

log = logging.getLogger(__name__)

router = APIRouter(tags=["collections"])


# --- Pydantic models ---

class CriteriaModel(BaseModel):
    """Structured criteria payload accepted by the API."""
    rules: dict[str, Any] = {}
    action: str = "none"
    grace_days: int = 30
    delete_files: bool = False
    add_import_exclusion: bool = True
    action_pipeline: list = []
    protected_users: list[str] = []
    protected_tags: list[str] = []
    protected_collections: list[str] = []
    watchlist_protected_users: list[str] = []
    library_section_id: Optional[int | str] = None
    library_source: str = "plex"
    granularity: str = "show"  # "show" or "season"


class CollectionCreate(BaseModel):
    name: str
    media_type: str = "movie"
    action: str = "none"
    grace_days: int = 30
    criteria: Optional[CriteriaModel] = None
    enabled: bool = True
    schedule_cron: Optional[str] = None
    priority: Optional[int] = 100


class CollectionUpdate(BaseModel):
    name: Optional[str] = None
    media_type: Optional[str] = None
    action: Optional[str] = None
    grace_days: Optional[int] = None
    criteria: Optional[CriteriaModel] = None
    enabled: Optional[bool] = None
    schedule_cron: Optional[str] = None
    priority: Optional[int] = None


class ExportRuleModel(BaseModel):
    """One rule in an export/import JSON doc."""
    name: str
    media_type: str = "movie"
    action: str = "none"
    grace_days: int = 30
    enabled: bool = True
    priority: int = 100
    schedule_cron: Optional[str] = None
    criteria: Optional[CriteriaModel] = None


class ImportPayload(BaseModel):
    """Body of POST /api/collections/import."""
    version: int = 1
    mode: str = "merge"  # "merge" | "replace"; default is safe-merge.
    rules: list[ExportRuleModel] = []


VALID_ACTIONS = ("none", "unmonitor", "delete")
VALID_MEDIA_TYPES = ("movie", "show")
VALID_RULE_NAMES = {
    # Criteria
    "never_watched", "not_watched_recently", "show_ended",
    "low_rating", "file_size_min", "release_year_before",
    "watch_ratio_low", "old_season", "old_content",
    "request_fulfilled", "available_on_debrid",
    # Protections
    "no_keep_tag", "no_active_request", "no_protected_request",
    "not_in_keep_collection", "highly_rated",
    "recently_added", "partially_watched", "on_watchlist",
    "plex_favorited", "series_protection",
}

# Columns on collection_config that the PUT endpoint is allowed to update.
# Keys in the `updates` dict are interpolated as column names in the SQL
# `SET` clause, so the list must be kept in sync with the schema and
# treated as a hard allow-list.
ALLOWED_UPDATE_COLUMNS = {
    "media_type",
    "action",
    "grace_days",
    "criteria",
    "enabled",
    "schedule_cron",
    "priority",
}


def _validate_criteria(criteria: CriteriaModel) -> str:
    """Validate a CriteriaModel and return its JSON string for DB storage."""
    for rule_name, rule_cfg in (criteria.rules or {}).items():
        if rule_name not in VALID_RULE_NAMES:
            raise HTTPException(400, f"Unknown rule: {rule_name}")
        if not isinstance(rule_cfg, dict):
            raise HTTPException(400, f"Rule '{rule_name}' config must be an object")
        if "enabled" not in rule_cfg:
            raise HTTPException(400, f"Rule '{rule_name}' must have an 'enabled' field")
    if criteria.action not in VALID_ACTIONS:
        raise HTTPException(400, f"Invalid action in criteria: {criteria.action}")
    return json.dumps(criteria.model_dump())


def _row_to_response(row) -> dict:
    """Convert a collection_config DB row to a response dict with parsed criteria."""
    from ..rules.criteria import CollectionCriteria
    d = dict(row)
    raw = d.get("criteria")
    if raw:
        try:
            parsed = CollectionCriteria.from_json(raw)
            d["criteria"] = parsed.to_dict()
        except Exception:
            # Fallback to raw JSON parse so a malformed row doesn't 500.
            try:
                d["criteria"] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                d["criteria"] = None
    return d


# --- Collection config endpoints ---

@router.post("/collections")
def create_collection(body: CollectionCreate):
    if body.action not in VALID_ACTIONS:
        raise HTTPException(400, f"Invalid action: {body.action}")
    if body.media_type not in VALID_MEDIA_TYPES:
        raise HTTPException(400, f"Invalid media_type: {body.media_type}")

    criteria_json = None
    if body.criteria is not None:
        criteria_json = _validate_criteria(body.criteria)

    priority = body.priority if body.priority is not None else 100
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO collection_config
               (name, media_type, action, grace_days, criteria, enabled,
                schedule_cron, priority)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (body.name, body.media_type, body.action,
             body.grace_days, criteria_json, body.enabled,
             body.schedule_cron, priority),
        )
        conn.commit()
    except Exception:
        conn.close()
        raise HTTPException(409, f"Rule '{body.name}' already exists")
    row = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (body.name,)
    ).fetchone()
    conn.close()

    # If the new rule has a cron schedule, try to register it with the
    # app-wide scheduler. Failure here is non-fatal -- the row is already
    # stored and will be picked up on the next app start.
    if body.schedule_cron:
        _try_reload_per_rule_schedules()

    return _row_to_response(row)


def _try_reload_per_rule_schedules() -> None:
    """Best-effort refresh of the per-rule cron schedules on the running app.

    Imported lazily because main.py imports this module at startup; doing
    the import at module top-level would create a circular import.
    """
    try:
        from ..main import scheduler, reload_per_rule_schedules
        reload_per_rule_schedules(scheduler)
    except Exception as e:  # pragma: no cover - best-effort
        log.debug("reload_per_rule_schedules skipped: %s", e)


@router.get("/collections/{name}/config")
def get_collection_config(name: str):
    display_name = _slug_to_name(name)
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, f"No config for rule '{display_name}'")
    return _row_to_response(row)


@router.get("/collections/{name}/keep-members")
def get_keep_collection_members(name: str):
    """Return the set of rating_keys currently in the rule's primary keep
    collection. Used by the rule-items page to render the +Collection button
    in its added/unadded state without one lookup per row.
    """
    from .. import config as app_config
    from ..core import plex, jellyfin
    from ..rules.criteria import CollectionCriteria
    display_name = _slug_to_name(name)
    conn = get_db()
    row = conn.execute(
        "SELECT criteria FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, f"No config for rule '{display_name}'")
    criteria = CollectionCriteria.from_json(row["criteria"])
    cols = criteria.protected_collections or []
    if not cols:
        return {"collection": None, "members": []}
    target_name = cols[0]
    source = criteria.library_source or "plex"
    section_id = criteria.library_section_id
    members: set[str] = set()
    try:
        if source == "plex":
            plex_url = app_config.get("plex_url")
            plex_token = app_config.get("plex_token")
            if plex_url and plex_token and section_id is not None:
                members = plex.fetch_keep_collection(plex_url, plex_token, int(section_id), target_name)
        elif source == "jellyfin":
            jf_url = app_config.get("jellyfin_url")
            jf_key = app_config.get("jellyfin_api_key")
            if jf_url and jf_key and section_id is not None:
                members = jellyfin.fetch_keep_collection(jf_url, jf_key, str(section_id), target_name)
    except Exception as e:
        log.warning("keep-members fetch failed for rule '%s' / collection '%s': %s",
                    display_name, target_name, e)
    return {"collection": target_name, "source": source, "members": sorted(members)}


@router.put("/collections/{name}/config")
def update_collection_config(name: str, body: CollectionUpdate):
    display_name = _slug_to_name(name)

    if body.action is not None and body.action not in VALID_ACTIONS:
        raise HTTPException(400, f"Invalid action: {body.action}")
    if body.media_type is not None and body.media_type not in VALID_MEDIA_TYPES:
        raise HTTPException(400, f"Invalid media_type: {body.media_type}")

    conn = get_db()
    existing = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(404, f"No config for rule '{display_name}'")

    # Handle rename
    new_name = None
    if body.name is not None and body.name.strip() and body.name.strip() != display_name:
        new_name = body.name.strip()
        # Check for conflict
        conflict = conn.execute(
            "SELECT id FROM collection_config WHERE name = ?", (new_name,)
        ).fetchone()
        if conflict:
            conn.close()
            raise HTTPException(409, f"A rule named '{new_name}' already exists")
        # Rename across all tables
        conn.execute("UPDATE collection_config SET name = ? WHERE name = ?", (new_name, display_name))
        conn.execute("UPDATE items SET collection = ? WHERE collection = ?", (new_name, display_name))
        conn.execute("UPDATE rule_results SET collection = ? WHERE collection = ?", (new_name, display_name))
        display_name = new_name

    updates = {}
    if body.media_type is not None:
        updates["media_type"] = body.media_type
    if body.action is not None:
        updates["action"] = body.action
    if body.grace_days is not None:
        updates["grace_days"] = body.grace_days
    if body.criteria is not None:
        updates["criteria"] = _validate_criteria(body.criteria)
    if body.enabled is not None:
        updates["enabled"] = body.enabled
    if body.schedule_cron is not None:
        # Caller can clear the per-rule schedule by passing an empty string.
        updates["schedule_cron"] = body.schedule_cron or None
    if body.priority is not None:
        updates["priority"] = int(body.priority)

    # Defence-in-depth: reject any column not on the allow-list before
    # interpolating its name into the SET clause. With the current
    # CollectionUpdate model this is unreachable, but guards against
    # future model fields accidentally becoming updatable.
    bad_cols = set(updates) - ALLOWED_UPDATE_COLUMNS
    if bad_cols:
        conn.close()
        raise HTTPException(
            400,
            f"Column(s) not allowed for update: {', '.join(sorted(bad_cols))}",
        )

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates if k in ALLOWED_UPDATE_COLUMNS)
        set_clause += ", updated_at = datetime('now')"
        values = [v for k, v in updates.items() if k in ALLOWED_UPDATE_COLUMNS] + [display_name]
        conn.execute(
            f"UPDATE collection_config SET {set_clause} WHERE name = ?",
            values,
        )

    if updates or new_name:
        conn.commit()

    row = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()
    conn.close()

    if "schedule_cron" in updates or new_name:
        _try_reload_per_rule_schedules()

    return _row_to_response(row)


@router.delete("/collections/{name}")
def delete_collection(name: str):
    display_name = _slug_to_name(name)
    conn = get_db()
    existing = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(404, f"No config for rule '{display_name}'")

    had_schedule = bool(existing["schedule_cron"]) if "schedule_cron" in existing.keys() else False

    conn.execute("DELETE FROM collection_config WHERE name = ?", (display_name,))
    conn.execute("DELETE FROM items WHERE collection = ?", (display_name,))
    conn.execute("DELETE FROM rule_results WHERE collection = ?", (display_name,))
    conn.commit()
    conn.close()

    if had_schedule:
        _try_reload_per_rule_schedules()

    return {"deleted": display_name}


# --- Existing browse endpoints ---

SORT_COLUMNS = {"title", "first_seen", "grace_expires", "size_bytes", "days_tracked"}
SORT_DIRS = {"asc", "desc"}
# Status filter allow-list for GET /api/collections/{name}. "all" is the
# escape hatch that returns items regardless of status.
VALID_STATUSES = {"all", "staged", "actioned", "migrated", "kept"}


@router.get("/collections")
def list_collections():
    conn = get_db()
    # Get item counts per collection
    rows = conn.execute("""
        SELECT collection, COUNT(*) as count,
               SUM(CASE WHEN status = 'staged' THEN 1 ELSE 0 END) as staged,
               SUM(size_bytes) as total_bytes
        FROM items GROUP BY collection
    """).fetchall()

    # Merge with config data
    configs = conn.execute("SELECT * FROM collection_config").fetchall()
    conn.close()

    config_map = {r["name"]: _row_to_response(r) for r in configs}
    result = []
    seen = set()

    for r in rows:
        entry = dict(r)
        name = entry["collection"]
        seen.add(name)
        if name in config_map:
            entry["config"] = config_map[name]
        result.append(entry)

    # Include configured collections with no items yet
    for name, cfg in config_map.items():
        if name not in seen:
            result.append({
                "collection": name,
                "count": 0,
                "staged": 0,
                "total_bytes": 0,
                "config": cfg,
            })

    return result


@router.get("/collections/export")
def export_collections():
    """Download every rule in ``collection_config`` as a JSON doc.

    Returned as an attachment so the browser prompts to save it. Clients
    can re-POST the body (or a trimmed variant) to
    ``/api/collections/import`` to seed another instance.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM collection_config ORDER BY priority ASC, name ASC"
    ).fetchall()
    conn.close()

    from ..rules.criteria import CollectionCriteria
    rules = []
    for row in rows:
        d = dict(row)
        criteria_dict: Optional[dict] = None
        if d.get("criteria"):
            try:
                # Round-trip through CollectionCriteria so the exported blob
                # reflects the canonical shape (with legacy mirrors).
                criteria_dict = CollectionCriteria.from_json(d["criteria"]).to_dict()
            except Exception:
                try:
                    criteria_dict = json.loads(d["criteria"])
                except (json.JSONDecodeError, TypeError):
                    criteria_dict = None

        rules.append({
            "name": d["name"],
            "media_type": d["media_type"],
            "action": d["action"],
            "grace_days": d["grace_days"],
            "enabled": bool(d["enabled"]),
            "priority": int(d.get("priority") or 100),
            "schedule_cron": d.get("schedule_cron"),
            "criteria": criteria_dict,
        })

    now = datetime.now(timezone.utc)
    payload = {
        "version": 1,
        "exported_at": now.isoformat(),
        "rules": rules,
    }
    filename = f"mcm-rules-{now.strftime('%Y-%m-%d')}.json"
    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/collections/import")
def import_collections(payload: ImportPayload):
    """Import rules from an export blob.

    Two modes:
      - ``merge`` (default): insert new rules; rules whose ``name`` already
        exists are skipped and their names returned in ``skipped``.
      - ``replace``: DELETE every existing row, then insert everything in
        the payload. Must be set explicitly -- the default is ``merge`` to
        prevent accidental wipes.

    Validation runs against each rule's criteria via ``_validate_criteria``
    before any writes; any failure aborts the whole import (no partial
    writes).
    """
    mode = (payload.mode or "merge").lower()
    if mode not in ("merge", "replace"):
        raise HTTPException(400, f"Invalid mode: {mode} (expected 'merge' or 'replace')")

    # First pass: validate every rule. Any failure short-circuits before
    # we touch the database.
    validated: list[tuple[ExportRuleModel, Optional[str]]] = []
    for rule in payload.rules:
        if not rule.name or not rule.name.strip():
            raise HTTPException(400, "Import aborted: a rule has an empty name")
        if rule.action not in VALID_ACTIONS:
            raise HTTPException(
                400,
                f"Import aborted: rule '{rule.name}' has invalid action '{rule.action}'",
            )
        if rule.media_type not in VALID_MEDIA_TYPES:
            raise HTTPException(
                400,
                f"Import aborted: rule '{rule.name}' has invalid media_type '{rule.media_type}'",
            )
        criteria_json: Optional[str] = None
        if rule.criteria is not None:
            try:
                criteria_json = _validate_criteria(rule.criteria)
            except HTTPException as e:
                # Reshape detail so the caller sees which rule failed.
                raise HTTPException(
                    400,
                    f"Import aborted: rule '{rule.name}' criteria invalid: {e.detail}",
                )
        validated.append((rule, criteria_json))

    conn = get_db()
    skipped: list[str] = []
    imported = 0
    replaced = mode == "replace"

    try:
        if replaced:
            conn.execute("DELETE FROM collection_config")

        # Load existing names once (for merge-mode conflict detection). In
        # replace mode the table is empty, so this set is always empty.
        existing = {
            row["name"]
            for row in conn.execute("SELECT name FROM collection_config").fetchall()
        }

        for rule, criteria_json in validated:
            if (not replaced) and rule.name in existing:
                skipped.append(rule.name)
                continue
            conn.execute(
                """INSERT INTO collection_config
                   (name, media_type, action, grace_days, criteria, enabled,
                    schedule_cron, priority)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rule.name,
                    rule.media_type,
                    rule.action,
                    rule.grace_days,
                    criteria_json,
                    rule.enabled,
                    rule.schedule_cron,
                    int(rule.priority if rule.priority is not None else 100),
                ),
            )
            imported += 1
            existing.add(rule.name)

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        # _validate_criteria already raised HTTPException above; anything
        # landing here is an integrity/DB error.
        raise HTTPException(500, f"Import failed: {e}")
    conn.close()

    _try_reload_per_rule_schedules()

    return {
        "status": "ok",
        "imported": imported,
        "skipped": skipped,
        "replaced": replaced,
    }


@router.get("/collections/{name}")
def get_collection(
    name: str,
    status: str = Query("staged", description="Filter by status: all|staged|actioned|migrated|kept"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: str = Query("", description="Filter items by title (substring match)"),
    sort: str = Query("first_seen", description="Sort column: title, first_seen, size_bytes, days_tracked"),
    sort_dir: str = Query("asc", description="Sort direction: asc or desc"),
):
    conn = get_db()
    offset = (page - 1) * per_page

    display_name = _slug_to_name(name)

    # Validate sort params (prevent SQL injection)
    if sort not in SORT_COLUMNS:
        sort = "first_seen"
    if sort_dir not in SORT_DIRS:
        sort_dir = "asc"

    # Status filter: "all" skips the status predicate entirely; anything
    # unknown collapses back to the default "staged" for backwards compat.
    if status not in VALID_STATUSES:
        status = "staged"

    # days_tracked is derived from first_seen - sort equivalently
    order_col = "first_seen" if sort == "days_tracked" else sort
    # days_tracked ascending = most recent first_seen (least days), so reverse
    order_dir = sort_dir if sort != "days_tracked" else ("desc" if sort_dir == "asc" else "asc")

    # Defence-in-depth: the values are about to be interpolated directly
    # into the ORDER BY clause, so they MUST be in the allow-lists.
    assert order_col in SORT_COLUMNS, f"order_col {order_col!r} not in allow-list"
    assert order_dir in SORT_DIRS, f"order_dir {order_dir!r} not in allow-list"

    # Build WHERE clause. When status == "all" we drop the predicate and its
    # bound parameter so the query still uses the collection index.
    where_params: list = [display_name]
    if status == "all":
        status_clause = ""
    else:
        status_clause = " AND status = ?"
        where_params.append(status)
    search_clause = ""
    if search.strip():
        search_clause = " AND title LIKE ?"
        where_params.append(f"%{search.strip()}%")

    where = f"collection = ?{status_clause}{search_clause}"

    items = conn.execute(f"""
        SELECT * FROM items
        WHERE {where}
        ORDER BY {order_col} {order_dir}
        LIMIT ? OFFSET ?
    """, (*where_params, per_page, offset)).fetchall()

    total = conn.execute(
        f"SELECT COUNT(*) as count FROM items WHERE {where}",
        where_params,
    ).fetchone()["count"]

    # Get config if available
    config_row = conn.execute(
        "SELECT * FROM collection_config WHERE name = ?", (display_name,)
    ).fetchone()

    # Get rule results for each item
    result = []
    for item in items:
        rules = conn.execute("""
            SELECT rule_name, passed, detail, severity
            FROM rule_results
            WHERE rating_key = ? AND collection = ?
            ORDER BY rule_name
        """, (item["rating_key"], display_name)).fetchall()

        result.append({
            **dict(item),
            "rules": [dict(r) for r in rules],
            "days_tracked": None,  # Computed in frontend from first_seen
        })

    conn.close()
    return {
        "collection": display_name,
        "items": result,
        "total": total,
        "page": page,
        "per_page": per_page,
        "search": search,
        "sort": sort,
        "sort_dir": sort_dir,
        "status": status,
        "config": _row_to_response(config_row) if config_row else None,
    }


# --- Helpers ---


def _slugify(name: str) -> str:
    """Generate a canonical slug from a display name.

    Matches the frontend slugify: lowercase, spaces to dashes, collapse
    runs of dashes (so "Ended Shows - Long Dormant" becomes
    "ended-shows-long-dormant", not "ended-shows---long-dormant").
    """
    return re.sub(r"-{2,}", "-", name.lower().replace(" ", "-"))


def _slug_to_name(slug: str) -> str:
    """Convert a URL slug to the display name, checking the config table."""
    normalised = _slugify(slug)

    # Try matching against collection_config names by slugifying them
    conn = get_db()
    rows = conn.execute("SELECT name FROM collection_config").fetchall()
    conn.close()
    for row in rows:
        if _slugify(row["name"]) == normalised:
            return row["name"]

    # Fallback: title-case the slug
    return slug.replace("-", " ").title()
