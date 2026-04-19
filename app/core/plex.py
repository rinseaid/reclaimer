"""Plex API helpers."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

from .clients import get_client

log = logging.getLogger(__name__)

_machine_id: Optional[str] = None


def _req(plex_url: str, plex_token: str, method: str, path: str, **kwargs):
    params = kwargs.pop("params", {})
    params["X-Plex-Token"] = plex_token
    return getattr(get_client(), method)(
        f"{plex_url}{path}",
        params=params,
        headers={"Accept": "application/json"},
        **kwargs,
    )


def get_machine_id(plex_url: str, plex_token: str) -> str:
    global _machine_id
    if _machine_id is None:
        r = _req(plex_url, plex_token, "get", "/")
        r.raise_for_status()
        _machine_id = r.json()["MediaContainer"]["machineIdentifier"]
    return _machine_id


def item_uri(plex_url: str, plex_token: str, rating_key: str) -> str:
    mid = get_machine_id(plex_url, plex_token)
    return f"server://{mid}/com.plexapp.plugins.library/library/metadata/{rating_key}"


def fetch_library(plex_url: str, plex_token: str, section_id: int) -> list[dict]:
    r = _req(plex_url, plex_token, "get", f"/library/sections/{section_id}/all",
             params={"includeGuids": "1"})
    r.raise_for_status()
    items = r.json()["MediaContainer"].get("Metadata", [])
    log.info("Plex section %d: %d items", section_id, len(items))
    return items


def fetch_seasons(plex_url: str, plex_token: str, show_rating_key: str) -> list[dict]:
    """Fetch seasons for a TV show, filtering out Season 0 (Specials).

    Each returned dict has ratingKey, index (season number), title, addedAt,
    viewCount, leafCount, viewedLeafCount from the Plex API.
    """
    r = _req(plex_url, plex_token, "get",
             f"/library/metadata/{show_rating_key}/children")
    r.raise_for_status()
    seasons = r.json().get("MediaContainer", {}).get("Metadata", [])
    # Filter out Specials (index 0)
    return [s for s in seasons if s.get("index", 0) != 0]


def external_id(item: dict, id_type: str) -> Optional[str]:
    for g in item.get("Guid", []):
        gid = g.get("id", "")
        if gid.startswith(f"{id_type}://"):
            return gid.split("://", 1)[1]
    guid = item.get("guid", "")
    m = re.search(rf"{id_type}://(\d+)", guid)
    return m.group(1) if m else None


def collection_key(plex_url: str, plex_token: str, section_id: int, name: str) -> Optional[str]:
    r = _req(plex_url, plex_token, "get", f"/library/sections/{section_id}/collections")
    if r.status_code != 200:
        return None
    for col in r.json().get("MediaContainer", {}).get("Metadata", []):
        if col.get("title") == name:
            return col.get("ratingKey")
    return None


def collection_item_keys(plex_url: str, plex_token: str, col_key: str) -> set[str]:
    r = _req(plex_url, plex_token, "get", f"/library/collections/{col_key}/children")
    if r.status_code != 200:
        return set()
    return {
        str(m["ratingKey"])
        for m in r.json().get("MediaContainer", {}).get("Metadata", [])
    }


def fetch_keep_collection(plex_url: str, plex_token: str, section_id: int, name: str) -> set[str]:
    if not name:
        return set()
    ck = collection_key(plex_url, plex_token, section_id, name)
    if not ck:
        # Downgrade from warning: the orchestrator probes both movie and TV
        # sections for each protected collection because it doesn't know which
        # one owns it. The miss is expected; only one section has the hit.
        log.debug("Plex keep collection '%s' not found in section %s", name, section_id)
        return set()
    keys = collection_item_keys(plex_url, plex_token, ck)
    log.info("Plex keep collection '%s': %d items excluded", name, len(keys))
    return keys


def sync_collection(
    plex_url: str, plex_token: str,
    section_id: int, name: str, want: set[str],
    media_type: int | None = None,
) -> None:
    """Sync a Plex collection to match the want set.

    media_type: Plex type (1=movie, 2=show, 3=season, 4=episode).
    If None, guesses from section_id (1=movie, else=show).
    """
    if not want:
        log.info("Collection '%s': no candidates", name)
        return

    ck = collection_key(plex_url, plex_token, section_id, name)

    if media_type is None:
        media_type = 1 if section_id == 1 else 2

    if ck is None:
        keys = list(want)
        log.info("Collection '%s': creating with %d items (type=%d)", name, len(keys), media_type)
        r = _req(plex_url, plex_token, "post", "/library/collections",
                 params={
                     "type": media_type, "title": name, "smart": 0,
                     "sectionId": section_id,
                     "uri": item_uri(plex_url, plex_token, keys[0]),
                 })
        r.raise_for_status()
        ck = str(r.json()["MediaContainer"]["Metadata"][0]["ratingKey"])
        for k in keys[1:]:
            _req(plex_url, plex_token, "put", f"/library/collections/{ck}/items",
                 params={"uri": item_uri(plex_url, plex_token, k)})
        return

    current = collection_item_keys(plex_url, plex_token, ck)
    to_add = want - current
    to_remove = current - want

    log.info("Collection '%s': %d current → %d target (+%d / -%d)",
             name, len(current), len(want), len(to_add), len(to_remove))
    add_errors: list[str] = []
    remove_errors: list[str] = []
    for k in to_add:
        try:
            r = _req(plex_url, plex_token, "put", f"/library/collections/{ck}/items",
                     params={"uri": item_uri(plex_url, plex_token, k)})
            r.raise_for_status()
        except Exception as e:
            add_errors.append(f"{k}: {e}")
            if len(add_errors) <= 3:
                log.warning("Collection '%s': failed to add item %s: %s", name, k, e)
    if len(add_errors) > 3:
        log.warning("Collection '%s': %d total add errors (first 3 logged above)",
                    name, len(add_errors))
    for k in to_remove:
        try:
            r = _req(plex_url, plex_token, "delete",
                     f"/library/collections/{ck}/items/{k}")
            r.raise_for_status()
        except Exception as e:
            remove_errors.append(f"{k}: {e}")
            if len(remove_errors) <= 3:
                log.warning("Collection '%s': failed to remove item %s: %s", name, k, e)
    if len(remove_errors) > 3:
        log.warning("Collection '%s': %d total remove errors (first 3 logged above)",
                    name, len(remove_errors))
    if add_errors or remove_errors:
        raise RuntimeError(
            f"Collection '{name}' sync had {len(add_errors)} add errors "
            f"and {len(remove_errors)} remove errors"
        )


def add_to_collection_by_name(
    plex_url: str, plex_token: str,
    section_id: int, name: str, rating_key: str,
    media_type: int | None = None,
) -> None:
    """Ensure `rating_key` is a member of the named collection.

    Creates the collection if it doesn't exist. Adding an already-present
    item is a no-op on Plex's side, so this is idempotent.
    """
    ck = collection_key(plex_url, plex_token, section_id, name)
    if ck is None:
        if media_type is None:
            media_type = 1 if section_id == 1 else 2
        r = _req(plex_url, plex_token, "post", "/library/collections", params={
            "type": media_type, "title": name, "smart": 0,
            "sectionId": section_id,
            "uri": item_uri(plex_url, plex_token, rating_key),
        })
        r.raise_for_status()
        return
    r = _req(plex_url, plex_token, "put",
             f"/library/collections/{ck}/items",
             params={"uri": item_uri(plex_url, plex_token, rating_key)})
    r.raise_for_status()


def remove_from_collection_by_name(
    plex_url: str, plex_token: str,
    section_id: int, name: str, rating_key: str,
) -> None:
    """Remove `rating_key` from the named collection. Idempotent: if the
    collection or the item isn't present, no-op."""
    ck = collection_key(plex_url, plex_token, section_id, name)
    if ck is None:
        return
    current = collection_item_keys(plex_url, plex_token, ck)
    if str(rating_key) not in current:
        return
    r = _req(plex_url, plex_token, "delete",
             f"/library/collections/{ck}/items/{rating_key}")
    r.raise_for_status()


def fetch_users(plex_url: str, plex_token: str) -> list[dict]:
    """Fetch all users from Plex (server owner + shared users)."""
    r = _req(plex_url, plex_token, "get", "/accounts")
    if r.status_code != 200:
        return []
    return r.json().get("MediaContainer", {}).get("Account", [])


def fetch_accounts(plex_url: str, plex_token: str) -> dict[int, str]:
    """Return {account_id: username} for all Plex accounts authorised on the server.

    Hits ``/accounts`` and maps each ``<Account id="N" name="X"/>`` -- this
    includes the server owner (typically id=1) plus any Plex Home /
    shared users. Returns an empty dict on any failure so the orchestrator
    never crashes on a transient Plex outage.
    """
    if not plex_url or not plex_token:
        return {}
    try:
        r = _req(plex_url, plex_token, "get", "/accounts")
        if r.status_code != 200:
            log.warning("Plex /accounts returned %d", r.status_code)
            return {}
        accounts = r.json().get("MediaContainer", {}).get("Account", []) or []
    except Exception as e:
        log.warning("Plex /accounts fetch failed: %s", e)
        return {}
    out: dict[int, str] = {}
    for a in accounts:
        try:
            acct_id = int(a.get("id"))
        except (TypeError, ValueError):
            continue
        name = a.get("name") or ""
        if not name:
            # Fall back to defaultName/title where the owner's public name isn't set.
            name = a.get("title") or a.get("defaultName") or f"user-{acct_id}"
        out[acct_id] = name
    return out


def fetch_session_history(
    plex_url: str, plex_token: str,
    since_ts: int | None = None,
) -> list[dict]:
    """Walk ``/status/sessions/history/all`` and return a normalized list.

    Each returned dict has:
      - account_id: int
      - rating_key: str (Plex's ratingKey for the item -- episode or movie)
      - title: str (episode title for shows, movie title for movies)
      - grandparent_title: str (show name for episodes; '' for movies)
      - media_type: 'episode' | 'movie'
      - season_number: int | None (parentIndex)
      - episode_number: int | None (index)
      - watched_at: ISO8601 string (UTC, no fractional seconds)
      - view_offset_ms: int (0 if not present)
      - media_duration_ms: int (0 if not present)

    Pagination uses ``X-Plex-Container-Start`` / ``X-Plex-Container-Size=500``
    and stops when the server stops returning entries. ``since_ts`` (Unix
    seconds) is passed to Plex as ``viewedAt>=`` to limit the window.
    Never raises -- logs the error and returns what was gathered so far.
    """
    if not plex_url or not plex_token:
        return []

    page_size = 500
    start = 0
    out: list[dict] = []

    while True:
        params: dict = {
            "X-Plex-Container-Start": start,
            "X-Plex-Container-Size": page_size,
            # Ascending viewedAt is friendlier for incremental syncs
            # (later pages carry later events) but Plex's default is
            # descending; either works as long as we page through fully.
            "sort": "viewedAt:asc",
        }
        if since_ts is not None:
            try:
                params["viewedAt>="] = int(since_ts)
            except (TypeError, ValueError):
                pass
        try:
            r = _req(plex_url, plex_token, "get",
                     "/status/sessions/history/all", params=params, timeout=60)
            if r.status_code != 200:
                log.warning("Plex session history returned %d at start=%d",
                            r.status_code, start)
                break
            container = r.json().get("MediaContainer", {}) or {}
        except Exception as e:
            log.warning("Plex session history fetch failed at start=%d: %s", start, e)
            break

        entries = container.get("Metadata") or []
        if not entries:
            break

        for h in entries:
            try:
                acct = int(h.get("accountID") or 0)
            except (TypeError, ValueError):
                acct = 0
            if not acct:
                continue

            hist_type = (h.get("type") or "").lower()
            if hist_type == "episode":
                media_type = "episode"
            elif hist_type == "movie":
                media_type = "movie"
            else:
                # Unknown / clip / trailer - skip; we only track movies/episodes.
                continue

            viewed_at = h.get("viewedAt")
            watched_at_iso = ""
            if viewed_at:
                try:
                    watched_at_iso = (
                        datetime.utcfromtimestamp(int(viewed_at))
                        .replace(microsecond=0)
                        .isoformat()
                    )
                except (TypeError, ValueError, OSError):
                    watched_at_iso = str(viewed_at)

            try:
                season_number = int(h.get("parentIndex")) if h.get("parentIndex") is not None else None
            except (TypeError, ValueError):
                season_number = None
            try:
                episode_number = int(h.get("index")) if h.get("index") is not None else None
            except (TypeError, ValueError):
                episode_number = None

            try:
                media_duration_ms = int(h.get("duration") or 0)
            except (TypeError, ValueError):
                media_duration_ms = 0
            try:
                view_offset_ms = int(h.get("viewOffset") or 0)
            except (TypeError, ValueError):
                view_offset_ms = 0

            # Apple TV / iPad Plex apps routinely emit "play" pings to the
            # server without any actual playback occurring (screensaver top
            # shelf touches, Continue Watching row scrubs, background
            # foreground events). Plex logs each of these as a history row
            # with viewOffset=null and duration=null. They carry no usable
            # signal and, worse, make items look partially watched on the
            # Watch History page even when the user was asleep. Skip them.
            if view_offset_ms == 0 and media_duration_ms == 0:
                continue

            # Plex only writes completing-scrobble rows to /status/sessions/
            # history/all. If the row has a real media duration but viewOffset
            # is 0, Plex cleared the resume offset on completion -- treat it
            # as "fully watched" so percent_complete derives to 100 instead
            # of 0.
            if view_offset_ms == 0 and media_duration_ms > 0:
                view_offset_ms = media_duration_ms

            out.append({
                "account_id": acct,
                "rating_key": str(h.get("ratingKey") or ""),
                "title": h.get("title") or "",
                "grandparent_title": h.get("grandparentTitle") or "",
                "media_type": media_type,
                "season_number": season_number,
                "episode_number": episode_number,
                "watched_at": watched_at_iso,
                "view_offset_ms": view_offset_ms,
                "media_duration_ms": media_duration_ms,
            })

        # Plex reports total size via ``totalSize`` but that value can lag
        # paging; just stop when a page comes back short.
        if len(entries) < page_size:
            break
        start += page_size

    log.info("Plex session history: %d entries fetched (since_ts=%s)",
             len(out), since_ts)
    return out


def fetch_item_duration(plex_url: str, plex_token: str, rating_key: str) -> int:
    """Fetch the actual media duration in seconds from Plex metadata.

    Plex returns duration in milliseconds; this converts to seconds.
    Returns 0 on any failure so callers can skip the update.
    """
    try:
        r = _req(plex_url, plex_token, "get", f"/library/metadata/{rating_key}",
                 timeout=10)
        r.raise_for_status()
        metadata = r.json().get("MediaContainer", {}).get("Metadata", [{}])[0]
        duration_ms = metadata.get("duration", 0)
        return int(duration_ms / 1000) if duration_ms else 0
    except Exception:
        return 0


def fetch_watchlist(plex_token: str) -> list[dict]:
    """Fetch the authenticated user's watchlist from Plex Discover API."""
    try:
        r = get_client().get(
            "https://metadata.provider.plex.tv/library/sections/watchlist/all",
            headers={"Accept": "application/json", "X-Plex-Token": plex_token},
        )
        if r.status_code == 200:
            return r.json().get("MediaContainer", {}).get("Metadata", [])
    except Exception as e:
        log.warning("Failed to fetch watchlist: %s", e)
    return []


def fetch_favorited_keys(plex_url: str, plex_token: str, section_id: int) -> set[str]:
    """Fetch rating_keys of items in a section that the admin has hearted.

    Plex exposes the admin's heart rating as ``userRating`` on library items;
    any positive value (typically 10) counts as favorited. We query the section
    with a ``userRating>0`` filter so we only pull the matching subset.

    Returns an empty set on any failure -- favorites are optional metadata and
    the orchestrator should not crash if this call fails.
    """
    if not plex_url or not plex_token or not section_id:
        return set()
    try:
        r = _req(plex_url, plex_token, "get",
                 f"/library/sections/{section_id}/all",
                 params={"userRating>": "0"})
        if r.status_code != 200:
            log.warning("Plex favorites fetch (section %s) returned %d",
                        section_id, r.status_code)
            return set()
        items = r.json().get("MediaContainer", {}).get("Metadata", [])
        keys = {str(i["ratingKey"]) for i in items if i.get("ratingKey")}
        log.info("Plex favorites (section %s): %d hearted items", section_id, len(keys))
        return keys
    except Exception as e:
        log.warning("Plex favorites fetch (section %s) failed: %s", section_id, e)
        return set()
