"""Manual run trigger API."""
import logging
import re
import threading
import time
from fastapi import APIRouter, HTTPException
from ..database import get_db


def _slugify(name: str) -> str:
    """Lowercase + hyphenate so URL slugs map back to rule names regardless of
    spacing/punctuation.

    Non-alphanumerics collapse to a single hyphen; leading/trailing hyphens
    trimmed. Matches the convention used by the frontend when it builds
    per-rule deep-links.
    """
    if not name:
        return ""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower())
    return s.strip("-")

log = logging.getLogger(__name__)

router = APIRouter(tags=["run"])

_run_lock = threading.Lock()
_running = False

# Cooldown after a run finishes to prevent accidental rapid re-triggers.
_RUN_COOLDOWN_SECONDS = 5.0
_last_run_finished_at: float = 0.0

# Live progress reporting - updated by the orchestrator during a run.
_progress = {
    "phase": "",
    "detail": "",
    "percent": 0,
    "items_processed": 0,
    "items_total": 0,
}
_last_result = {}  # Summary of last completed run


def update_progress(
    phase: str = "",
    detail: str = "",
    percent: int = 0,
    items_processed: int = 0,
    items_total: int = 0,
):
    """Called by the orchestrator to report progress."""
    _progress["phase"] = phase
    _progress["detail"] = detail
    _progress["percent"] = percent
    _progress["items_processed"] = items_processed
    _progress["items_total"] = items_total


def reset_progress():
    """Clear progress state at run start/end."""
    update_progress()


@router.post("/run")
def trigger_run(dry_run: bool = False, rule: str | None = None):
    """Trigger a full orchestrator pass (or a single-rule subset).

    Query params:
      * ``dry_run`` -- skip destructive actions and DB writes.
      * ``rule`` -- optional slug (lowercase, hyphen-separated) identifying a
        single rule. Looked up against ``collection_config.name`` via
        :func:`_slugify`. Mismatches return HTTP 404.
    """
    global _running, _last_run_finished_at
    if _running:
        return {"status": "already_running"}

    # Cooldown: if the previous run finished less than N seconds ago, reject.
    since_last = time.monotonic() - _last_run_finished_at
    if _last_run_finished_at > 0 and since_last < _RUN_COOLDOWN_SECONDS:
        wait = _RUN_COOLDOWN_SECONDS - since_last
        raise HTTPException(
            status_code=429,
            detail=f"Please wait before starting another run ({wait:.1f}s remaining)",
        )

    # Resolve the rule slug to the canonical rule name up-front so we can
    # 404 cleanly instead of starting a background run that processes zero
    # rules. Slug comparison is case/punctuation-insensitive.
    resolved_rule: str | None = None
    if rule:
        conn = get_db()
        rows = conn.execute(
            "SELECT name FROM collection_config WHERE enabled = 1"
        ).fetchall()
        conn.close()
        target = _slugify(rule)
        for r in rows:
            if _slugify(r["name"]) == target:
                resolved_rule = r["name"]
                break
        if resolved_rule is None:
            raise HTTPException(
                status_code=404,
                detail=f"no enabled rule matches slug {rule!r}",
            )

    from ..orchestrator import run_orchestrator

    def _bg():
        global _running, _last_result, _last_run_finished_at
        _running = True
        _last_result = {}
        reset_progress()
        try:
            run_orchestrator(dry_run=dry_run, rule_filter=resolved_rule)
            # Build summary from activity log for this run
            conn = get_db()
            # Use run_started timestamp as base - all events happen between start and complete
            rs = conn.execute(
                "SELECT timestamp FROM activity_log WHERE event_type = 'run_started' ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if rs:
                ts = rs["timestamp"]
                added = conn.execute(
                    "SELECT COUNT(*) as c FROM activity_log WHERE event_type = 'item_added' AND timestamp >= ?", (ts,)
                ).fetchone()["c"]
                removed = conn.execute(
                    "SELECT COUNT(*) as c FROM activity_log WHERE event_type = 'item_removed' AND timestamp >= ?", (ts,)
                ).fetchone()["c"]
                _last_result = {
                    "dry_run": dry_run,
                    "added": added,
                    "removed": removed,
                    "rule": resolved_rule,
                }
            conn.close()
        finally:
            reset_progress()
            _last_run_finished_at = time.monotonic()
            _running = False

    thread = threading.Thread(target=_bg, daemon=True)
    thread.start()

    return {"status": "started", "dry_run": dry_run, "rule": resolved_rule}


@router.post("/run/sync-users")
def trigger_user_sync():
    """Trigger an immediate user sync."""
    from ..orchestrator import _sync_users

    def do_sync():
        conn = get_db()
        try:
            _sync_users(conn)
            conn.commit()
        except Exception as e:
            log.warning("Manual user sync failed: %s", e)
        finally:
            conn.close()

    threading.Thread(target=do_sync, daemon=True).start()
    return {"status": "started"}


@router.get("/run/status")
def run_status():
    return {"running": _running}


@router.get("/run/progress")
def run_progress():
    return {"running": _running, "last_result": _last_result, **_progress}
