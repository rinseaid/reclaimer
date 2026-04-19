"""Radarr API helpers."""
from __future__ import annotations

import logging
from .clients import get_client

log = logging.getLogger(__name__)


def _api(radarr_url: str, radarr_key: str, method: str, path: str, **kwargs):
    return getattr(get_client(), method)(
        f"{radarr_url}/api/v3{path}",
        headers={"X-Api-Key": radarr_key},
        timeout=30,
        **kwargs,
    )


def fetch_movies(radarr_url: str, radarr_key: str) -> dict[int, dict]:
    """Return {tmdbId → movie_dict} with _tag_names list attached."""
    movies = _api(radarr_url, radarr_key, "get", "/movie").json()
    tag_map = {t["id"]: t["label"] for t in _api(radarr_url, radarr_key, "get", "/tag").json()}
    result: dict[int, dict] = {}
    for m in movies:
        m["_tag_names"] = [tag_map.get(t, str(t)) for t in m.get("tags", [])]
        result[m["tmdbId"]] = m
    log.info("Radarr: %d movies", len(result))
    return result


def unmonitor(radarr_url: str, radarr_key: str, radarr_id: int, title: str) -> None:
    r = _api(radarr_url, radarr_key, "get", f"/movie/{radarr_id}")
    r.raise_for_status()
    movie = r.json()
    movie["monitored"] = False
    _api(radarr_url, radarr_key, "put", f"/movie/{radarr_id}", json=movie).raise_for_status()
    log.info("Unmonitored in Radarr: %s", title)


def delete(radarr_url: str, radarr_key: str, radarr_id: int, title: str,
           delete_files: bool = True, add_exclusion: bool = True) -> None:
    _api(radarr_url, radarr_key, "delete", f"/movie/{radarr_id}",
         params={"deleteFiles": str(delete_files).lower(),
                 "addImportExclusion": str(add_exclusion).lower()}).raise_for_status()
    log.info("Deleted from Radarr: %s (deleteFiles=%s)", title, delete_files)


def fetch_movie_by_tmdb(radarr_url: str, radarr_key: str, tmdb_id: int) -> dict[int, dict]:
    """Fetch a single movie by TMDB ID. Returns {tmdbId -> movie_dict} with _tag_names."""
    if not radarr_url or not radarr_key or not tmdb_id:
        return {}
    try:
        movies = _api(radarr_url, radarr_key, "get", "/movie", params={"tmdbId": tmdb_id}).json()
        if not movies:
            return {}
        tag_map = {t["id"]: t["label"] for t in _api(radarr_url, radarr_key, "get", "/tag").json()}
        result: dict[int, dict] = {}
        for m in movies:
            m["_tag_names"] = [tag_map.get(t, str(t)) for t in m.get("tags", [])]
            result[m["tmdbId"]] = m
        return result
    except Exception:
        return {}


def search(radarr_url: str, radarr_key: str, movie_ids: list[int]) -> None:
    """Trigger a search for specific movies."""
    _api(radarr_url, radarr_key, "post", "/command",
         json={"name": "MoviesSearch", "movieIds": movie_ids}).raise_for_status()


def ensure_tag_id(url: str, key: str, label: str) -> int | None:
    """Return the id of the Radarr tag with ``label``, creating it if needed.

    Radarr stores tags as ``{id, label}`` objects; movies reference tag ids.
    Callers typically supply a human-readable label and don't want to manage
    the create/lookup two-step.
    """
    label = (label or "").strip()
    if not label:
        return None
    r = _api(url, key, "get", "/tag")
    r.raise_for_status()
    for t in r.json():
        if (t.get("label") or "").strip().lower() == label.lower():
            return int(t["id"])
    r2 = _api(url, key, "post", "/tag", json={"label": label})
    r2.raise_for_status()
    return int(r2.json()["id"])


def add_tag(url: str, key: str, movie_id: int, label: str, title: str = "") -> None:
    """Add a tag to a movie by label, creating the tag if missing. No-op
    when the tag is already present (idempotent)."""
    tag_id = ensure_tag_id(url, key, label)
    if tag_id is None:
        return
    r = _api(url, key, "get", f"/movie/{movie_id}")
    r.raise_for_status()
    movie = r.json()
    tags = list(movie.get("tags") or [])
    if tag_id in tags:
        return
    tags.append(tag_id)
    movie["tags"] = tags
    _api(url, key, "put", f"/movie/{movie_id}", json=movie).raise_for_status()
    log.info("Added Radarr tag %r to %s", label, title or movie_id)


def remove_tag(url: str, key: str, movie_id: int, label: str, title: str = "") -> None:
    """Remove a tag from a movie by label. No-op when the tag doesn't
    exist on the movie (or on the server)."""
    label = (label or "").strip()
    if not label:
        return
    tags_r = _api(url, key, "get", "/tag")
    tags_r.raise_for_status()
    want = None
    for t in tags_r.json():
        if (t.get("label") or "").strip().lower() == label.lower():
            want = int(t["id"])
            break
    if want is None:
        return
    r = _api(url, key, "get", f"/movie/{movie_id}")
    r.raise_for_status()
    movie = r.json()
    before = list(movie.get("tags") or [])
    after = [t for t in before if int(t) != want]
    if len(after) == len(before):
        return
    movie["tags"] = after
    _api(url, key, "put", f"/movie/{movie_id}", json=movie).raise_for_status()
    log.info("Removed Radarr tag %r from %s", label, title or movie_id)


def set_root_folder(url: str, key: str, movie_id: int, new_root: str,
                    move_files: bool = True, title: str = "") -> None:
    """Move a movie to a different root folder. Uses Radarr's bulk-edit
    endpoint (``/movie/editor``) so Radarr itself performs the file move.
    """
    new_root = (new_root or "").strip()
    if not new_root:
        raise ValueError("root folder path is required")
    body = {
        "movieIds": [int(movie_id)],
        "rootFolderPath": new_root,
        "moveFiles": bool(move_files),
    }
    r = _api(url, key, "put", "/movie/editor", json=body)
    r.raise_for_status()
    log.info("Moved Radarr movie %s to root %s (moveFiles=%s)",
             title or movie_id, new_root, move_files)


def add_movie(url: str, key: str, tmdb_id: int, title: str,
              quality_profile_id: int, root_folder: str,
              monitored: bool = True, search_on_add: bool = False,
              tags: list[int] | None = None) -> dict:
    """Create a movie on this Radarr. Looks the TMDB id up via
    ``/movie/lookup/tmdb`` so Radarr populates titleSlug, images, year,
    etc. automatically; then POSTs /movie with the full object. Returns
    the created movie dict.
    """
    r = _api(url, key, "get", "/movie/lookup/tmdb", params={"tmdbId": int(tmdb_id)})
    r.raise_for_status()
    lookup = r.json()
    if isinstance(lookup, list):
        if not lookup:
            raise ValueError(f"TMDB id {tmdb_id} not found on Radarr")
        lookup = lookup[0]
    payload = dict(lookup)
    payload["qualityProfileId"] = int(quality_profile_id)
    payload["rootFolderPath"] = root_folder
    payload["monitored"] = bool(monitored)
    payload["minimumAvailability"] = payload.get("minimumAvailability") or "released"
    payload["tags"] = list(tags or [])
    payload["addOptions"] = {
        "searchForMovie": bool(search_on_add),
        "monitor": "movieOnly",
    }
    cr = _api(url, key, "post", "/movie", json=payload)
    cr.raise_for_status()
    created = cr.json()
    log.info("Added movie %r to Radarr (tmdbId=%s, id=%s)",
             title, tmdb_id, created.get("id"))
    return created


def list_quality_profiles(url: str, key: str) -> list[dict]:
    """Return ``[{id, name}, ...]`` from ``/api/v3/qualityprofile``.

    Wrapped in try/except so orchestration doesn't abort when Radarr is
    unreachable -- callers can tell "no profiles known" from an empty list.
    """
    if not url or not key:
        return []
    try:
        r = _api(url, key, "get", "/qualityprofile")
        r.raise_for_status()
        return [{"id": p.get("id"), "name": p.get("name")} for p in r.json()]
    except Exception as e:
        log.warning("Radarr list_quality_profiles failed: %s", e)
        return []


def recycle_bin_path(url: str, key: str) -> str | None:
    """Return Radarr's Media Management recycle-bin path, or ``None`` when it's
    unset / unreachable. Used by the rule editor to warn when a delete-files
    step is scheduled against a Radarr without a safety net configured.
    """
    if not url or not key:
        return None
    try:
        r = _api(url, key, "get", "/config/mediamanagement")
        r.raise_for_status()
        path = (r.json() or {}).get("recycleBin") or ""
        return path.strip() or None
    except Exception as e:
        log.warning("Radarr recycle_bin_path lookup failed: %s", e)
        return None


def get_quality_profile_id(url: str, key: str, name: str) -> int | None:
    """Return the profile ``id`` for the named profile, or ``None`` if not
    found / API unreachable. Name match is case-insensitive.
    """
    if not name:
        return None
    target = name.strip().lower()
    for p in list_quality_profiles(url, key):
        pname = (p.get("name") or "").strip().lower()
        if pname == target and p.get("id") is not None:
            try:
                return int(p["id"])
            except (TypeError, ValueError):
                return None
    return None
