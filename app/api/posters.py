"""Poster proxy + on-disk cache.

Serves scaled thumbnails for items from Plex or Jellyfin via a single
Jettison-side URL that the browser can cache normally. First hit fetches and
writes to disk; subsequent hits are served straight from the cache.
"""
from __future__ import annotations

import email.utils
import hashlib
import logging
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse

from .. import config
from ..core.clients import get_client
from ..database import get_db

log = logging.getLogger(__name__)

router = APIRouter(tags=["posters"])

CACHE_DIR = Path("/app/data/poster-cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Hard cap on poster payloads we'll ingest. Keeps a wedged/misconfigured
# upstream (say, Plex serving a raw 4K artwork asset) from buffering a huge
# blob in memory before the disk write.
MAX_POSTER_BYTES = 2 * 1024 * 1024  # 2 MB

# Size presets -> (width, height). Keep small -- 10-15 KB per thumbnail.
SIZES = {
    "sm": (120, 180),
    "md": (300, 450),
}


def _etag_for(path: Path) -> str:
    """Cheap content-derived ETag: sha256 of the cached file's first 1024
    bytes + mtime. The mtime makes it change if the file is rewritten (even
    with identical prefix bytes), the sha256 lets us return a stable
    identifier that's safe for use in If-None-Match."""
    try:
        stat = path.stat()
        with path.open("rb") as f:
            head = f.read(1024)
    except OSError:
        return ""
    h = hashlib.sha256()
    h.update(head)
    h.update(str(int(stat.st_mtime)).encode("ascii"))
    return f'"{h.hexdigest()[:32]}"'


def _cache_headers(path: Path) -> dict[str, str]:
    """Build caching headers so the browser can 304 on conditional requests
    even after the Cache-Control max-age expires."""
    headers = {"Cache-Control": "public, max-age=604800, immutable"}
    etag = _etag_for(path)
    if etag:
        headers["ETag"] = etag
    try:
        mtime = path.stat().st_mtime
        headers["Last-Modified"] = email.utils.formatdate(mtime, usegmt=True)
    except OSError:
        pass
    return headers


def _maybe_not_modified(request: Request, headers: dict[str, str]) -> Response | None:
    """Return a 304 response if the request's conditional headers match the
    current cache entry; otherwise None so the caller serves the full body."""
    inm = request.headers.get("if-none-match")
    etag = headers.get("ETag")
    if inm and etag and inm.strip() == etag:
        return Response(status_code=304, headers=headers)
    ims = request.headers.get("if-modified-since")
    lm = headers.get("Last-Modified")
    if ims and lm:
        try:
            ims_ts = email.utils.parsedate_to_datetime(ims).timestamp()
            lm_ts = email.utils.parsedate_to_datetime(lm).timestamp()
            if ims_ts >= lm_ts:
                return Response(status_code=304, headers=headers)
        except (TypeError, ValueError):
            pass
    return None


def _cache_path(source: str, rating_key: str, size: str) -> Path:
    # Sanitise rating_key; only allow safe filename characters.
    safe_rk = "".join(c for c in rating_key if c.isalnum() or c in "-_")
    if not safe_rk:
        raise HTTPException(400, "invalid rating_key")
    if size not in SIZES:
        raise HTTPException(400, f"size must be one of: {', '.join(SIZES)}")
    return CACHE_DIR / f"{source}-{safe_rk}-{size}.jpg"


def _resolve_source(rating_key: str) -> str:
    """Figure out whether an item came from Plex or Jellyfin by joining the
    items row to its rule's library_source. Defaults to plex."""
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT cc.criteria
               FROM items i
               JOIN collection_config cc ON cc.name = i.collection
               WHERE i.rating_key = ? LIMIT 1""",
            (rating_key,),
        ).fetchone()
    finally:
        conn.close()
    if row and row["criteria"]:
        try:
            import json as _json
            return _json.loads(row["criteria"]).get("library_source") or "plex"
        except (TypeError, ValueError):
            pass
    return "plex"


def _cap_poster_bytes(data: bytes) -> bytes:
    """Enforce MAX_POSTER_BYTES so a runaway upstream can't stall the client
    with a multi-MB raw artwork asset."""
    if len(data) > MAX_POSTER_BYTES:
        raise HTTPException(502, "upstream poster exceeded size cap")
    return data


def _fetch_plex(rating_key: str, width: int, height: int) -> bytes:
    plex_url = config.get("plex_url")
    plex_token = config.get("plex_token")
    if not plex_url or not plex_token:
        raise HTTPException(503, "Plex is not configured")
    # Plex's photo transcoder scales images server-side and returns JPEG bytes.
    thumb_path = f"/library/metadata/{rating_key}/thumb"
    r = get_client().get(
        f"{plex_url}/photo/:/transcode",
        params={
            "width": width,
            "height": height,
            "minSize": 1,
            "url": thumb_path,
            "X-Plex-Token": plex_token,
        },
        timeout=15,
    )
    if r.status_code == 404:
        raise HTTPException(404, "poster not found on Plex")
    r.raise_for_status()
    return _cap_poster_bytes(r.content)


def _fetch_jellyfin(rating_key: str, width: int, height: int) -> bytes:
    jf_url = config.get("jellyfin_url")
    jf_key = config.get("jellyfin_api_key")
    if not jf_url or not jf_key:
        raise HTTPException(503, "Jellyfin is not configured")
    r = get_client().get(
        f"{jf_url}/Items/{rating_key}/Images/Primary",
        params={"fillWidth": width, "fillHeight": height, "api_key": jf_key},
        timeout=15,
    )
    if r.status_code == 404:
        raise HTTPException(404, "poster not found on Jellyfin")
    r.raise_for_status()
    return _cap_poster_bytes(r.content)


@router.get("/items/{rating_key}/poster")
def get_poster(
    request: Request,
    rating_key: str,
    size: str = Query("sm", pattern="^(sm|md)$"),
):
    # Validate size/rating_key upfront (raises 400 if bad).
    _cache_path("any", rating_key, size)

    # Try each source-specific cache path first to save a DB lookup.
    for src in ("plex", "jellyfin"):
        candidate = _cache_path(src, rating_key, size)
        if candidate.exists() and candidate.stat().st_size > 0:
            headers = _cache_headers(candidate)
            not_modified = _maybe_not_modified(request, headers)
            if not_modified is not None:
                return not_modified
            return FileResponse(
                candidate,
                media_type="image/jpeg",
                headers=headers,
            )

    # Cache miss -- resolve source and fetch.
    source = _resolve_source(rating_key)
    width, height = SIZES[size]
    try:
        if source == "jellyfin":
            data = _fetch_jellyfin(rating_key, width, height)
        else:
            data = _fetch_plex(rating_key, width, height)
    except HTTPException:
        raise
    except Exception as e:
        log.warning("Poster fetch failed for %s/%s: %s", source, rating_key, e)
        raise HTTPException(502, f"upstream poster fetch failed: {e}") from e

    if not data:
        raise HTTPException(502, "empty poster response")

    out = _cache_path(source, rating_key, size)
    tmp = out.with_suffix(".jpg.tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, out)
    except OSError as e:
        log.warning("Poster cache write failed (%s): %s", out, e)
        # Still serve the bytes even if cache write failed.
        return Response(
            content=data,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=604800"},
        )

    return FileResponse(
        out,
        media_type="image/jpeg",
        headers=_cache_headers(out),
    )
