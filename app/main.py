"""FastAPI application - Jettison."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import config
from .database import init_db
from .api.router import api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

scheduler = BackgroundScheduler()
templates = Jinja2Templates(directory="/app/app/templates")

# APScheduler job-id prefix for per-rule cron schedules. All jobs with this
# prefix are owned by ``reload_per_rule_schedules`` -- the reload clears
# every matching job before re-registering the current DB state.
PER_RULE_JOB_PREFIX = "rule-cron-"


def _scheduled_run():
    from .orchestrator import run_orchestrator
    try:
        run_orchestrator(dry_run=False)
    except Exception as e:
        log.error("Scheduled run failed: %s", e, exc_info=True)


def _scheduled_run_for_rule(rule_name: str):
    """Run the orchestrator scoped to a single rule (per-rule cron).

    ``run_orchestrator`` gains an optional ``rule_filter`` kwarg in the
    parallel B1 agent's work. We call it with the filter if the signature
    supports it, and fall back to a full run with a TODO warning otherwise
    so this scheduler keeps working while B1 lands.
    """
    from .orchestrator import run_orchestrator
    try:
        try:
            run_orchestrator(dry_run=False, rule_filter=rule_name)
        except TypeError:
            # TODO(b1-agent): run_orchestrator doesn't yet accept rule_filter.
            # Until that lands, fall back to a full run so the schedule still
            # does *something*. Remove the fallback once B1 merges.
            log.warning(
                "run_orchestrator lacks rule_filter kwarg; running full orchestrator for %s",
                rule_name,
            )
            run_orchestrator(dry_run=False)
    except Exception as e:
        log.error("Per-rule scheduled run for %r failed: %s", rule_name, e, exc_info=True)


def reload_per_rule_schedules(sched: BackgroundScheduler) -> None:
    """Re-sync APScheduler's per-rule cron jobs against the DB.

    Drops every existing ``rule-cron-*`` job, then registers one job per
    row in ``collection_config`` that has a non-null ``schedule_cron``.

    Exposed so the create/update/delete handlers in ``api/collections.py``
    (and main.py's startup path) can ask for an immediate refresh when the
    rule set changes. Invalid cron strings log a warning and are skipped --
    they never raise out of this function.
    """
    from .database import get_db

    # Remove existing per-rule jobs first so stale rules don't linger.
    try:
        for job in list(sched.get_jobs()):
            if job.id and job.id.startswith(PER_RULE_JOB_PREFIX):
                sched.remove_job(job.id)
    except Exception as e:
        log.warning("Could not enumerate scheduler jobs for reload: %s", e)
        return

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, name, schedule_cron FROM collection_config "
            "WHERE schedule_cron IS NOT NULL AND schedule_cron != ''"
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        rule_id = row["id"]
        rule_name = row["name"]
        cron_expr = (row["schedule_cron"] or "").strip()
        if not cron_expr:
            continue
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
        except Exception as e:
            log.warning(
                "Skipping per-rule schedule for %r: invalid cron %r (%s)",
                rule_name, cron_expr, e,
            )
            continue
        job_id = f"{PER_RULE_JOB_PREFIX}{rule_id}"
        try:
            sched.add_job(
                _scheduled_run_for_rule,
                trigger,
                args=[rule_name],
                id=job_id,
                replace_existing=True,
            )
            log.info("Registered per-rule schedule %s cron=%r for rule %r",
                     job_id, cron_expr, rule_name)
        except Exception as e:
            log.warning("Failed to register per-rule schedule %s: %s", job_id, e)


def _sync_users_task():
    from .orchestrator import _sync_users
    from .database import get_db
    try:
        conn = get_db()
        _sync_users(conn)
        conn.commit()
        conn.close()
        log.info("Periodic user sync completed")
    except Exception as e:
        log.warning("Periodic user sync failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    log.info("Starting Jettison")
    init_db()
    config.init_settings()

    hour = config.get("schedule_hour")
    minute = config.get("schedule_minute")
    scheduler.add_job(
        _scheduled_run,
        CronTrigger(hour=hour, minute=minute),
        id="nightly_run",
        replace_existing=True,
    )
    sync_hours = config.get("user_sync_interval_hours") or 6
    scheduler.add_job(
        _sync_users_task,
        IntervalTrigger(hours=sync_hours),
        id="periodic_user_sync",
        replace_existing=True,
    )
    scheduler.start()

    # Register any per-rule cron schedules stored in collection_config.
    # Safe to call even before any rules exist -- it's a no-op then.
    try:
        reload_per_rule_schedules(scheduler)
    except Exception as e:
        log.warning("Initial per-rule schedule load failed: %s", e)

    # Run initial user sync in background (non-blocking)
    scheduler.add_job(
        _sync_users_task,
        id="startup_user_sync",
    )
    log.info("Scheduled nightly run at %02d:%02d", hour, minute)
    log.info("Scheduled periodic user sync every %d hours", sync_hours)

    yield

    # Shutdown
    scheduler.shutdown()
    log.info("Shutting down")


app = FastAPI(
    title="Jettison",
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(api_router)

# Static files
try:
    app.mount("/static", StaticFiles(directory="/app/app/static"), name="static")
except Exception:
    pass  # Static dir may not exist yet


# Frontend routes
@app.get("/", response_class=HTMLResponse)
@app.get("/rules", response_class=HTMLResponse)
async def rules_list_page(request: Request):
    return templates.TemplateResponse("collections_list.html", {"request": request})


@app.get("/rules/{name}", response_class=HTMLResponse)
async def rule_page(request: Request, name: str):
    return templates.TemplateResponse("collections.html", {"request": request, "name": name})


@app.get("/rules/{name}/settings", response_class=HTMLResponse)
async def rule_settings_page(request: Request, name: str):
    return templates.TemplateResponse("collection_settings.html", {"request": request, "name": name})



@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse("item_detail.html", {"request": request, "rating_key": ""})


@app.get("/items/{rating_key}", response_class=HTMLResponse)
async def item_page(request: Request, rating_key: str):
    return templates.TemplateResponse("item_detail.html", {"request": request, "rating_key": rating_key})


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    return templates.TemplateResponse("logs.html", {"request": request})


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request):
    return templates.TemplateResponse("activity.html", {"request": request})


@app.get("/settings", response_class=HTMLResponse)
@app.get("/settings/media", response_class=HTMLResponse)
async def settings_media_page(request: Request):
    return templates.TemplateResponse("settings_media.html", {"request": request})


@app.get("/settings/statistics", response_class=HTMLResponse)
async def settings_statistics_page(request: Request):
    return templates.TemplateResponse("settings_statistics.html", {"request": request})


@app.get("/settings/downloads", response_class=HTMLResponse)
async def settings_downloads_page(request: Request):
    return templates.TemplateResponse("settings_downloads.html", {"request": request})


@app.get("/settings/notifications", response_class=HTMLResponse)
async def settings_notifications_page(request: Request):
    return templates.TemplateResponse("settings_notifications.html", {"request": request})


@app.get("/settings/schedule", response_class=HTMLResponse)
async def settings_schedule_page(request: Request):
    return templates.TemplateResponse("settings_schedule.html", {"request": request})


