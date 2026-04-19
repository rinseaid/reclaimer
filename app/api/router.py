"""API router - mounts all sub-routers."""
from fastapi import APIRouter

from . import dashboard, collections, items, users, settings, activity, run, posters, instances

api_router = APIRouter(prefix="/api")
api_router.include_router(dashboard.router)
api_router.include_router(collections.router)
api_router.include_router(items.router)
api_router.include_router(users.router)
api_router.include_router(settings.router)
api_router.include_router(activity.router)
api_router.include_router(run.router)
api_router.include_router(posters.router)
api_router.include_router(instances.router)
