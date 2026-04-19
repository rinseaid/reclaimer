"""Sonarr API helpers."""
from __future__ import annotations

import logging
from .clients import get_client

log = logging.getLogger(__name__)


def _api(sonarr_url: str, sonarr_key: str, method: str, path: str, **kwargs):
    return getattr(get_client(), method)(
        f"{sonarr_url}/api/v3{path}",
        headers={"X-Api-Key": sonarr_key},
        timeout=30,
        **kwargs,
    )


def fetch_shows(sonarr_url: str, sonarr_key: str) -> dict[int, dict]:
    """Return {tvdbId → show_dict} with _tag_names list attached."""
    shows = _api(sonarr_url, sonarr_key, "get", "/series").json()
    tag_map = {t["id"]: t["label"] for t in _api(sonarr_url, sonarr_key, "get", "/tag").json()}
    result: dict[int, dict] = {}
    for s in shows:
        s["_tag_names"] = [tag_map.get(t, str(t)) for t in s.get("tags", [])]
        result[s["tvdbId"]] = s
    log.info("Sonarr: %d shows", len(result))
    return result


def unmonitor(sonarr_url: str, sonarr_key: str, sonarr_id: int, title: str) -> None:
    r = _api(sonarr_url, sonarr_key, "get", f"/series/{sonarr_id}")
    r.raise_for_status()
    show = r.json()
    show["monitored"] = False
    _api(sonarr_url, sonarr_key, "put", f"/series/{sonarr_id}", json=show).raise_for_status()
    log.info("Unmonitored in Sonarr: %s", title)


def delete(sonarr_url: str, sonarr_key: str, sonarr_id: int, title: str,
           delete_files: bool = True) -> None:
    _api(sonarr_url, sonarr_key, "delete", f"/series/{sonarr_id}",
         params={"deleteFiles": str(delete_files).lower()}).raise_for_status()
    log.info("Deleted from Sonarr: %s (deleteFiles=%s)", title, delete_files)


def fetch_show_by_tvdb(sonarr_url: str, sonarr_key: str, tvdb_id: int) -> dict[int, dict]:
    """Fetch a single show by TVDB ID. Returns {tvdbId -> show_dict} with _tag_names."""
    if not sonarr_url or not sonarr_key or not tvdb_id:
        return {}
    try:
        shows = _api(sonarr_url, sonarr_key, "get", "/series", params={"tvdbId": tvdb_id}).json()
        if not shows:
            return {}
        tag_map = {t["id"]: t["label"] for t in _api(sonarr_url, sonarr_key, "get", "/tag").json()}
        result: dict[int, dict] = {}
        for s in shows:
            s["_tag_names"] = [tag_map.get(t, str(t)) for t in s.get("tags", [])]
            result[s["tvdbId"]] = s
        return result
    except Exception:
        return {}


def unmonitor_season(sonarr_url: str, sonarr_key: str, series_id: int,
                     season_number: int, title: str) -> None:
    """Unmonitor a single season without affecting other seasons."""
    r = _api(sonarr_url, sonarr_key, "get", f"/series/{series_id}")
    r.raise_for_status()
    show = r.json()
    for s in show.get("seasons", []):
        if s.get("seasonNumber") == season_number:
            s["monitored"] = False
            break
    _api(sonarr_url, sonarr_key, "put", f"/series/{series_id}", json=show).raise_for_status()
    log.info("Unmonitored season %d in Sonarr: %s", season_number, title)


def delete_season_files(sonarr_url: str, sonarr_key: str, series_id: int,
                        season_number: int, title: str) -> None:
    """Delete all episode files for a specific season."""
    r = _api(sonarr_url, sonarr_key, "get", "/episodefile",
             params={"seriesId": series_id})
    r.raise_for_status()
    files = r.json()
    season_files = [f for f in files if f.get("seasonNumber") == season_number]
    deleted = 0
    for ef in season_files:
        try:
            _api(sonarr_url, sonarr_key, "delete",
                 f"/episodefile/{ef['id']}").raise_for_status()
            deleted += 1
        except Exception as e:
            log.warning("Failed to delete episode file %d for %s S%02d: %s",
                        ef.get("id", 0), title, season_number, e)
    log.info("Deleted %d/%d files for %s season %d",
             deleted, len(season_files), title, season_number)


def search_season(sonarr_url: str, sonarr_key: str, series_id: int, season_number: int) -> None:
    """Trigger a season search."""
    _api(sonarr_url, sonarr_key, "post", "/command",
         json={"name": "SeasonSearch", "seriesId": series_id,
               "seasonNumber": season_number}).raise_for_status()


def search_series(sonarr_url: str, sonarr_key: str, series_id: int) -> None:
    """Trigger a full-series search."""
    _api(sonarr_url, sonarr_key, "post", "/command",
         json={"name": "SeriesSearch", "seriesId": int(series_id)}).raise_for_status()


def ensure_tag_id(url: str, key: str, label: str) -> int | None:
    """Return the id of the Sonarr tag with ``label``, creating it if
    needed. Mirrors ``radarr.ensure_tag_id``."""
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


def add_tag(url: str, key: str, series_id: int, label: str, title: str = "") -> None:
    """Add a tag to a series by label, creating the tag if missing. No-op
    when the tag is already present (idempotent)."""
    tag_id = ensure_tag_id(url, key, label)
    if tag_id is None:
        return
    r = _api(url, key, "get", f"/series/{series_id}")
    r.raise_for_status()
    show = r.json()
    tags = list(show.get("tags") or [])
    if tag_id in tags:
        return
    tags.append(tag_id)
    show["tags"] = tags
    _api(url, key, "put", f"/series/{series_id}", json=show).raise_for_status()
    log.info("Added Sonarr tag %r to %s", label, title or series_id)


def remove_tag(url: str, key: str, series_id: int, label: str, title: str = "") -> None:
    """Remove a tag from a series by label."""
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
    r = _api(url, key, "get", f"/series/{series_id}")
    r.raise_for_status()
    show = r.json()
    before = list(show.get("tags") or [])
    after = [t for t in before if int(t) != want]
    if len(after) == len(before):
        return
    show["tags"] = after
    _api(url, key, "put", f"/series/{series_id}", json=show).raise_for_status()
    log.info("Removed Sonarr tag %r from %s", label, title or series_id)


def set_root_folder(url: str, key: str, series_id: int, new_root: str,
                    move_files: bool = True, title: str = "") -> None:
    """Move a series to a different root folder via Sonarr's bulk-edit
    endpoint (``/series/editor``). Sonarr performs the file move itself."""
    new_root = (new_root or "").strip()
    if not new_root:
        raise ValueError("root folder path is required")
    body = {
        "seriesIds": [int(series_id)],
        "rootFolderPath": new_root,
        "moveFiles": bool(move_files),
    }
    r = _api(url, key, "put", "/series/editor", json=body)
    r.raise_for_status()
    log.info("Moved Sonarr series %s to root %s (moveFiles=%s)",
             title or series_id, new_root, move_files)


def add_series(url: str, key: str, tvdb_id: int, title: str,
               quality_profile_id: int, root_folder: str,
               monitored: bool = True, search_on_add: bool = False,
               tags: list[int] | None = None) -> dict:
    """Create a series on this Sonarr via ``/series/lookup?term=tvdb:ID``.
    Returns the created series dict."""
    r = _api(url, key, "get", "/series/lookup",
             params={"term": f"tvdb:{int(tvdb_id)}"})
    r.raise_for_status()
    lookup = r.json() or []
    if not lookup:
        raise ValueError(f"TVDB id {tvdb_id} not found on Sonarr")
    payload = dict(lookup[0])
    payload["qualityProfileId"] = int(quality_profile_id)
    payload["rootFolderPath"] = root_folder
    payload["monitored"] = bool(monitored)
    payload["seasonFolder"] = payload.get("seasonFolder", True)
    payload["tags"] = list(tags or [])
    payload["addOptions"] = {
        "searchForMissingEpisodes": bool(search_on_add),
        "searchForCutoffUnmetEpisodes": False,
        "monitor": "all",
    }
    cr = _api(url, key, "post", "/series", json=payload)
    cr.raise_for_status()
    created = cr.json()
    log.info("Added series %r to Sonarr (tvdbId=%s, id=%s)",
             title, tvdb_id, created.get("id"))
    return created


def list_quality_profiles(url: str, key: str) -> list[dict]:
    """Return ``[{id, name}, ...]`` from ``/api/v3/qualityprofile``.

    Wrapped in try/except so orchestration doesn't abort when Sonarr is
    unreachable -- callers can tell "no profiles known" from an empty list.
    """
    if not url or not key:
        return []
    try:
        r = _api(url, key, "get", "/qualityprofile")
        r.raise_for_status()
        return [{"id": p.get("id"), "name": p.get("name")} for p in r.json()]
    except Exception as e:
        log.warning("Sonarr list_quality_profiles failed: %s", e)
        return []


def recycle_bin_path(url: str, key: str) -> str | None:
    """Return Sonarr's Media Management recycle-bin path, or ``None`` when
    it's unset / unreachable. Mirrors radarr.recycle_bin_path; used by the
    rule editor to warn when a delete-files step has no safety net.
    """
    if not url or not key:
        return None
    try:
        r = _api(url, key, "get", "/config/mediamanagement")
        r.raise_for_status()
        path = (r.json() or {}).get("recycleBin") or ""
        return path.strip() or None
    except Exception as e:
        log.warning("Sonarr recycle_bin_path lookup failed: %s", e)
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


def build_season_counts(shows: dict[int, dict]) -> dict[int, int]:
    """Compute {series_id -> highest seasonNumber with any episode files}.

    Skips Season 0 (specials). Shows with no populated seasons are omitted so
    callers can treat absence as "unknown" rather than "zero seasons".
    """
    counts: dict[int, int] = {}
    for show in shows.values():
        series_id = show.get("id")
        if series_id is None:
            continue
        highest = -1
        for s in show.get("seasons", []) or []:
            season_num = s.get("seasonNumber", 0)
            if season_num <= 0:  # skip specials / malformed
                continue
            file_count = (s.get("statistics") or {}).get("episodeFileCount", 0) or 0
            if file_count > 0 and season_num > highest:
                highest = season_num
        if highest > 0:
            counts[int(series_id)] = highest
    return counts
