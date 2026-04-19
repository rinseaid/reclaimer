"""Jellyfin API helpers."""
from __future__ import annotations

import logging
from typing import Optional

from .clients import get_client

log = logging.getLogger(__name__)


def _req(url: str, api_key: str, method: str, path: str, **kwargs):
    """Thin wrapper around httpx that injects the Jellyfin API key."""
    params = kwargs.pop("params", {})
    params["api_key"] = api_key
    return getattr(get_client(), method)(
        f"{url}{path}",
        params=params,
        headers={"Accept": "application/json"},
        **kwargs,
    )


def fetch_library(url: str, api_key: str, library_id: str) -> list[dict]:
    """Fetch all items from a Jellyfin library."""
    r = _req(url, api_key, "get", "/Items", params={
        "ParentId": library_id,
        "Recursive": "true",
        "IncludeItemTypes": "Movie,Series",
        "Fields": "ProviderIds,DateCreated,Overview",
    }, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("Items", [])
    log.info("Jellyfin library %s: %d items", library_id, len(items))
    return items


def fetch_libraries(url: str, api_key: str) -> list[dict]:
    """Fetch all media libraries from Jellyfin."""
    r = _req(url, api_key, "get", "/Library/VirtualFolders", timeout=10)
    r.raise_for_status()
    folders = r.json()
    libraries = []
    for folder in folders:
        collection_type = folder.get("CollectionType", "")
        if collection_type in ("movies", "tvshows"):
            libraries.append({
                "id": folder.get("ItemId", ""),
                "title": folder.get("Name", ""),
                "type": "movie" if collection_type == "movies" else "show",
            })
    return libraries


def fetch_users(url: str, api_key: str) -> list[dict]:
    """Fetch all users from Jellyfin."""
    r = _req(url, api_key, "get", "/Users", timeout=10)
    if r.status_code != 200:
        return []
    return r.json()


def fetch_collections(url: str, api_key: str, library_id: str) -> list[dict]:
    """Fetch all BoxSet collections in a Jellyfin library."""
    r = _req(url, api_key, "get", "/Items", params={
        "IncludeItemTypes": "BoxSet",
        "ParentId": library_id,
        "Recursive": "true",
    }, timeout=15)
    if r.status_code != 200:
        return []
    return r.json().get("Items", [])


def collection_item_ids(url: str, api_key: str, collection_id: str) -> set[str]:
    """Return the set of item IDs inside a Jellyfin collection (BoxSet)."""
    r = _req(url, api_key, "get", "/Items", params={
        "ParentId": collection_id,
    }, timeout=15)
    if r.status_code != 200:
        return set()
    return {item["Id"] for item in r.json().get("Items", []) if "Id" in item}


def sync_collection(
    url: str, api_key: str,
    library_id: str, name: str, want: set[str],
) -> None:
    """Ensure a Jellyfin BoxSet collection matches the desired item set.

    Creates the collection if it doesn't exist, then adds/removes items to
    match *want*.  Mirrors the behaviour of plex.sync_collection().
    """
    if not want:
        log.info("Collection '%s': no candidates", name)
        return

    # Find existing collection by name
    existing_id: Optional[str] = None
    for col in fetch_collections(url, api_key, library_id):
        if col.get("Name") == name:
            existing_id = col["Id"]
            break

    if existing_id is None:
        ids_csv = ",".join(want)
        log.info("Collection '%s': creating with %d items", name, len(want))
        r = _req(url, api_key, "post", "/Collections", params={
            "Name": name,
            "Ids": ids_csv,
            "ParentId": library_id,
        }, timeout=30)
        r.raise_for_status()
        return

    current = collection_item_ids(url, api_key, existing_id)
    to_add = want - current
    to_remove = current - want

    log.info("Collection '%s': %d current -> %d target (+%d / -%d)",
             name, len(current), len(want), len(to_add), len(to_remove))

    if to_add:
        r = _req(url, api_key, "post",
                 f"/Collections/{existing_id}/Items",
                 params={"Ids": ",".join(to_add)}, timeout=30)
        r.raise_for_status()
    if to_remove:
        r = _req(url, api_key, "delete",
                 f"/Collections/{existing_id}/Items",
                 params={"Ids": ",".join(to_remove)}, timeout=30)
        r.raise_for_status()


def add_to_collection_by_name(
    url: str, api_key: str,
    library_id: str, name: str, item_id: str,
) -> None:
    """Ensure `item_id` is a member of the named BoxSet. Creates it if missing.

    Idempotent: adding an item that's already present is a no-op on Jellyfin.
    """
    existing_id: Optional[str] = None
    for col in fetch_collections(url, api_key, library_id):
        if col.get("Name") == name:
            existing_id = col["Id"]
            break
    if existing_id is None:
        r = _req(url, api_key, "post", "/Collections", params={
            "Name": name, "Ids": item_id, "ParentId": library_id,
        }, timeout=30)
        r.raise_for_status()
        return
    r = _req(url, api_key, "post",
             f"/Collections/{existing_id}/Items",
             params={"Ids": item_id}, timeout=30)
    r.raise_for_status()


def remove_from_collection_by_name(
    url: str, api_key: str,
    library_id: str, name: str, item_id: str,
) -> None:
    """Remove `item_id` from the named BoxSet. Idempotent: if the BoxSet or
    the item isn't present, no-op."""
    existing_id: Optional[str] = None
    for col in fetch_collections(url, api_key, library_id):
        if col.get("Name") == name:
            existing_id = col["Id"]
            break
    if existing_id is None:
        return
    current = collection_item_ids(url, api_key, existing_id)
    if item_id not in current:
        return
    r = _req(url, api_key, "delete",
             f"/Collections/{existing_id}/Items",
             params={"Ids": item_id}, timeout=30)
    r.raise_for_status()


def fetch_keep_collection(
    url: str, api_key: str, library_id: str, name: str,
) -> set[str]:
    """Return item IDs in a named keep collection, or empty set if not found."""
    if not name:
        return set()
    for col in fetch_collections(url, api_key, library_id):
        if col.get("Name") == name:
            keys = collection_item_ids(url, api_key, col["Id"])
            log.info("Jellyfin keep collection '%s': %d items excluded", name, len(keys))
            return keys
    log.warning("Jellyfin keep collection '%s' not found in library %s", name, library_id)
    return set()


def external_id(item: dict, provider: str) -> Optional[str]:
    """Extract a provider ID from a Jellyfin item's ProviderIds dict.

    Provider keys in Jellyfin are capitalised ("Tmdb", "Tvdb", "Imdb").
    The *provider* argument uses the lowercase form used by Plex GUIDs
    (e.g. "tmdb", "tvdb", "imdb") and is normalised here.
    """
    provider_map = {"tmdb": "Tmdb", "tvdb": "Tvdb", "imdb": "Imdb"}
    jf_key = provider_map.get(provider.lower(), provider.capitalize())
    return item.get("ProviderIds", {}).get(jf_key)


def fetch_seasons(url: str, api_key: str, series_id: str) -> list[dict]:
    """Fetch season objects for a Jellyfin series."""
    r = _req(url, api_key, "get", f"/Shows/{series_id}/Seasons", timeout=15)
    if r.status_code != 200:
        return []
    return r.json().get("Items", [])


def fetch_item_duration(url: str, api_key: str, item_id: str) -> int:
    """Fetch the actual media duration in seconds from Jellyfin metadata."""
    try:
        r = _req(url, api_key, "get", f"/Items/{item_id}", timeout=10)
        r.raise_for_status()
        data = r.json()
        # Jellyfin returns RunTimeTicks in 100-nanosecond intervals
        ticks = data.get("RunTimeTicks", 0)
        return int(ticks / 10_000_000) if ticks else 0
    except Exception:
        return 0


def test_connection(url: str, api_key: str) -> dict:
    """Test Jellyfin connection, return server info."""
    r = _req(url, api_key, "get", "/System/Info", timeout=10)
    r.raise_for_status()
    data = r.json()
    return {
        "ok": True,
        "detail": f"Jellyfin {data.get('Version', '?')} - {data.get('ServerName', '?')}",
    }


# Ticks-per-ms conversion: Jellyfin reports durations in "ticks" (100 ns
# intervals); 10_000 ticks = 1 ms.
_TICKS_PER_MS = 10_000


def _jf_user_id_to_int(user_id: str) -> int:
    """Collapse a Jellyfin user UUID into a stable 64-bit signed int.

    The ``users`` table stores ``plex_user_id`` as INTEGER, and we want
    Jellyfin users to coexist in the same table without a schema migration.
    We hash the UUID with SHA-1 and take the low 63 bits (signed-positive),
    which is a one-way but stable mapping -- the same UUID always lands on
    the same int.
    """
    import hashlib
    h = hashlib.sha1(user_id.encode("utf-8")).digest()
    # 63-bit to stay strictly positive in SQLite's signed INTEGER space.
    return int.from_bytes(h[:8], "big") & ((1 << 63) - 1)


def fetch_watch_history(
    jf_url: str, jf_key: str,
) -> tuple[list[dict], dict[str, str]]:
    """Return (rows, user_map) of per-user watch history for every Jellyfin user.

    ``rows`` entries have the same shape as ``plex.fetch_session_history``:
      - account_id: int (stable hash of the Jellyfin UUID)
      - rating_key: str (Jellyfin item id)
      - title: str
      - grandparent_title: str
      - media_type: 'episode' | 'movie'
      - season_number: int | None
      - episode_number: int | None
      - watched_at: ISO8601 string
      - view_offset_ms: int
      - media_duration_ms: int

    ``user_map`` is ``{account_id_int: username}`` so the orchestrator can
    upsert the ``users`` table. We return the map alongside the rows (rather
    than a full Jellyfin user row) to keep the contract narrow -- the
    orchestrator only needs id + username.

    Never raises; on any HTTP / parsing failure we log and return ([], {}).
    The function synthesizes ``view_offset_ms = media_duration_ms`` for items
    flagged ``UserData.Played=True`` so fully-watched items show up as
    100% complete even when Jellyfin's PlaybackPositionTicks is 0.
    """
    if not jf_url or not jf_key:
        return [], {}

    try:
        users = fetch_users(jf_url, jf_key)
    except Exception as e:
        log.warning("Jellyfin: failed to fetch users for watch history: %s", e)
        return [], {}

    rows: list[dict] = []
    user_map: dict[str, str] = {}  # keyed by the synthesised int as str

    for u in users or []:
        uuid = u.get("Id") or ""
        username = u.get("Name") or ""
        if not uuid or not username:
            continue
        acct_int = _jf_user_id_to_int(uuid)
        user_map[str(acct_int)] = username

        # Jellyfin exposes the entire play history only via per-user queries.
        # Played + Resumable together cover "fully watched" and "in-progress"
        # items; newer Jellyfin versions expose both via IsPlayed/IsResumable
        # filter flags.
        for played_flag, label in (("IsPlayed", "played"), ("IsResumable", "resumable")):
            try:
                r = _req(jf_url, jf_key, "get", f"/Users/{uuid}/Items", params={
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Episode",
                    "Filters": played_flag,
                    "Fields": "UserData,SeriesName,ParentIndexNumber,IndexNumber,RunTimeTicks",
                    "Limit": 0,  # Jellyfin treats 0 as "no limit"
                }, timeout=60)
                if r.status_code != 200:
                    log.warning("Jellyfin %s history for %s returned %d",
                                label, username, r.status_code)
                    continue
                items = r.json().get("Items", []) or []
            except Exception as e:
                log.warning("Jellyfin %s history fetch failed for %s: %s",
                            label, username, e)
                continue

            for it in items:
                ud = it.get("UserData", {}) or {}
                last_played = ud.get("LastPlayedDate") or ""
                # Without a timestamp we can't de-dupe or reason about recency;
                # skip rather than stuff a zero in.
                if not last_played:
                    continue

                item_type = (it.get("Type") or "").lower()
                if item_type == "episode":
                    media_type = "episode"
                    grandparent_title = it.get("SeriesName") or ""
                elif item_type == "movie":
                    media_type = "movie"
                    grandparent_title = ""
                else:
                    continue

                pos_ticks = int(ud.get("PlaybackPositionTicks") or 0)
                run_ticks = int(it.get("RunTimeTicks") or 0)
                view_offset_ms = pos_ticks // _TICKS_PER_MS
                media_duration_ms = run_ticks // _TICKS_PER_MS

                # Fully-watched items may not retain a playback offset. Plex/
                # Jellyfin both treat ``Played=True`` as authoritative; mirror
                # that by synthesising 100% completion.
                if ud.get("Played") and media_duration_ms > 0:
                    view_offset_ms = media_duration_ms

                # Strip fractional seconds + trailing Z for consistency with
                # plex.fetch_session_history. Jellyfin hands us ISO-8601
                # already; be tolerant of both 'Z' and '+00:00' suffixes.
                watched_at = last_played.rstrip("Z")
                if "." in watched_at:
                    watched_at = watched_at.split(".", 1)[0]

                season_number = it.get("ParentIndexNumber")
                episode_number = it.get("IndexNumber")
                try:
                    season_number = int(season_number) if season_number is not None else None
                except (TypeError, ValueError):
                    season_number = None
                try:
                    episode_number = int(episode_number) if episode_number is not None else None
                except (TypeError, ValueError):
                    episode_number = None

                rows.append({
                    "account_id": acct_int,
                    "rating_key": str(it.get("Id") or ""),
                    "title": it.get("Name") or "",
                    "grandparent_title": grandparent_title,
                    "media_type": media_type,
                    "season_number": season_number,
                    "episode_number": episode_number,
                    "watched_at": watched_at,
                    "view_offset_ms": view_offset_ms,
                    "media_duration_ms": media_duration_ms,
                })

    log.info("Jellyfin watch history: %d rows across %d users",
             len(rows), len(user_map))
    return rows, user_map
