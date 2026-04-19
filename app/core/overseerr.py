"""Seerr (Overseerr) API helpers."""
from __future__ import annotations

import logging
from .clients import get_client

log = logging.getLogger(__name__)


def fetch_active_requests(
    overseerr_url: str, overseerr_key: str, protected_requesters: set[str],
) -> tuple[set[int], set[int], set[int], set[int], dict[int, str], dict[int, str],
           set[int], set[int], dict[int, str]]:
    """
    Return (active_movie_ids, active_show_ids, protected_movie_ids, protected_show_ids,
            movie_requesters, show_requesters,
            active_shows_tmdb, protected_shows_tmdb, show_requesters_tmdb).

    movie_requesters map tmdb ID → requester username.
    show_requesters map tvdb ID → requester username.
    show_requesters_tmdb map tmdb ID → requester username (fallback for shows missing tvdbId).
    """
    empty = set(), set(), set(), set(), {}, {}, set(), set(), {}
    if not overseerr_url or not overseerr_key:
        return empty

    client = get_client()
    active_movies: set[int] = set()
    active_shows: set[int] = set()
    active_shows_tmdb: set[int] = set()
    protected_movies: set[int] = set()
    protected_shows: set[int] = set()
    protected_shows_tmdb: set[int] = set()
    movie_requesters: dict[int, str] = {}
    show_requesters: dict[int, str] = {}
    show_requesters_tmdb: dict[int, str] = {}
    page, total_pages = 1, 1

    while page <= total_pages:
        r = client.get(
            f"{overseerr_url}/api/v1/request",
            headers={"X-Api-Key": overseerr_key},
            params={"take": 100, "skip": (page - 1) * 100, "filter": "all"},
        )
        r.raise_for_status()
        body = r.json()
        total = body["pageInfo"]["results"]
        total_pages = max(1, (total + 99) // 100)

        for req in body.get("results", []):
            status = req.get("status")
            media = req.get("media", {})
            media_type = media.get("mediaType")
            tmdb_id = media.get("tmdbId")
            tvdb_id = media.get("tvdbId")
            requester = (req.get("requestedBy", {}).get("plexUsername", "")
                         or req.get("requestedBy", {}).get("displayName", ""))

            # Track ALL requests regardless of status for requester lookup
            # The rule engine checks if requester has watched - that's the real gate
            if status and status not in (3,):  # Exclude declined (3)
                if media_type == "movie" and tmdb_id:
                    active_movies.add(tmdb_id)
                    if requester:
                        movie_requesters[tmdb_id] = requester
                elif media_type == "tv":
                    if tvdb_id:
                        active_shows.add(tvdb_id)
                        if requester:
                            show_requesters[tvdb_id] = requester
                    # Always index by TMDB as fallback (Seerr sometimes has tmdbId but no tvdbId)
                    if tmdb_id:
                        active_shows_tmdb.add(tmdb_id)
                        if requester:
                            show_requesters_tmdb[tmdb_id] = requester

            if protected_requesters and requester in protected_requesters:
                if media_type == "movie" and tmdb_id:
                    protected_movies.add(tmdb_id)
                elif media_type == "tv":
                    if tvdb_id:
                        protected_shows.add(tvdb_id)
                    if tmdb_id:
                        protected_shows_tmdb.add(tmdb_id)

        page += 1

    log.info("Seerr: %d active movies, %d active TV (tvdb), %d active TV (tmdb)",
             len(active_movies), len(active_shows), len(active_shows_tmdb))
    return (active_movies, active_shows, protected_movies, protected_shows,
            movie_requesters, show_requesters,
            active_shows_tmdb, protected_shows_tmdb, show_requesters_tmdb)


import re

# Seerr adds tags like "18 - angus.d0" to Radarr/Sonarr when it sends a request
SEERR_TAG_PATTERN = re.compile(r"^\d+\s*-\s*(.+)$")


def parse_seerr_tag(tag: str) -> str | None:
    """Extract the username from a Seerr-style arr tag like '18 - angus.d0'.
    Returns the username or None if the tag doesn't match."""
    m = SEERR_TAG_PATTERN.match(tag)
    return m.group(1).strip() if m else None


def extract_requesters_from_tags(tag_names: list[str]) -> list[str]:
    """Extract all Seerr requester usernames from a list of arr tags."""
    requesters: list[str] = []
    for t in tag_names:
        username = parse_seerr_tag(t)
        if username:
            requesters.append(username)
    return requesters


STATUS_LABELS = {
    1: "Pending Approval",
    2: "Approved",
    3: "Declined",
    4: "Processing",
    5: "Available",
}


def add_to_watchlist(
    overseerr_url: str, overseerr_key: str,
    tmdb_id: int, media_type: str,
    on_behalf_user_id: int | None = None,
) -> bool:
    """Add a TMDB id to a Seerr user's watchlist.

    Without ``on_behalf_user_id`` the entry goes to the user that owns the
    admin API key (typically the Seerr admin). Pass a user id to target
    another user's watchlist.

    ``media_type`` must be "movie" or "tv".
    Returns True on success (201), False otherwise.
    """
    if not overseerr_url or not overseerr_key:
        return False
    headers = {"X-Api-Key": overseerr_key, "Content-Type": "application/json"}
    if on_behalf_user_id is not None:
        headers["X-Api-User"] = str(on_behalf_user_id)
    try:
        r = get_client().post(
            f"{overseerr_url}/api/v1/watchlist",
            headers=headers,
            json={"tmdbId": int(tmdb_id), "mediaType": media_type},
            timeout=15,
        )
        return r.status_code == 201
    except Exception as e:
        log.warning("Seerr add-to-watchlist failed (tmdb=%s, type=%s): %s",
                    tmdb_id, media_type, e)
        return False


def remove_from_watchlist(
    overseerr_url: str, overseerr_key: str,
    tmdb_id: int,
    on_behalf_user_id: int | None = None,
) -> bool:
    """Remove a TMDB id from a Seerr user's watchlist. Idempotent: unknown
    entries silently succeed.

    Returns True when the entry is known to be gone."""
    if not overseerr_url or not overseerr_key:
        return False
    headers = {"X-Api-Key": overseerr_key}
    if on_behalf_user_id is not None:
        headers["X-Api-User"] = str(on_behalf_user_id)
    try:
        r = get_client().delete(
            f"{overseerr_url}/api/v1/watchlist/{int(tmdb_id)}",
            headers=headers,
            timeout=15,
        )
        # 204 = removed; 401/404 for "already gone" we treat as success too.
        return r.status_code in (204, 404)
    except Exception as e:
        log.warning("Seerr remove-from-watchlist failed (tmdb=%s): %s", tmdb_id, e)
        return False


def user_watchlist_tmdb_ids(
    overseerr_url: str, overseerr_key: str,
    user_id: int,
) -> set[int]:
    """Return the set of TMDB ids currently on the named user's watchlist.

    Used by the Reclaimer UI to pre-render the +Watchlist toggle's state. Failure
    modes fall through as an empty set; the caller treats "unknown" as "not
    on the watchlist" and the user can still click to add.
    """
    out: set[int] = set()
    if not overseerr_url or not overseerr_key:
        return out
    client = get_client()
    headers = {"X-Api-Key": overseerr_key}
    try:
        page, total_pages = 1, 1
        while page <= total_pages:
            r = client.get(
                f"{overseerr_url}/api/v1/user/{user_id}/watchlist",
                headers=headers, params={"page": page}, timeout=15,
            )
            if r.status_code != 200:
                break
            body = r.json()
            total_pages = max(1, int(body.get("totalPages") or 1))
            for e in body.get("results", []):
                tmdb = e.get("tmdbId")
                if tmdb is not None:
                    try:
                        out.add(int(tmdb))
                    except (TypeError, ValueError):
                        continue
            page += 1
    except Exception as e:
        log.warning("Seerr user watchlist fetch failed (uid=%s): %s", user_id, e)
    return out


def get_api_user_id(overseerr_url: str, overseerr_key: str) -> int | None:
    """Return the user id that owns the admin API key. Used by +Watchlist to
    pre-populate the toggle state against the right user without hardcoding
    a uid.
    """
    if not overseerr_url or not overseerr_key:
        return None
    try:
        r = get_client().get(
            f"{overseerr_url}/api/v1/auth/me",
            headers={"X-Api-Key": overseerr_key}, timeout=10,
        )
        if r.status_code == 200:
            uid = r.json().get("id")
            if uid is not None:
                return int(uid)
    except Exception as e:
        log.warning("Seerr /auth/me lookup failed: %s", e)
    return None


def fetch_all_watchlists(
    overseerr_url: str, overseerr_key: str,
) -> list[dict]:
    """Fetch the union of every Overseerr user's watchlist.

    Returns a list of dicts with at least ``tmdbId`` and ``mediaType`` keys
    (``"movie"`` or ``"tv"``). Duplicates across users are naturally included
    -- callers should de-duplicate by (mediaType, tmdbId) if they care.

    Returns an empty list if Overseerr isn't configured or any call fails.
    """
    if not overseerr_url or not overseerr_key:
        return []

    client = get_client()
    headers = {"X-Api-Key": overseerr_key}

    # Pull all users (paged).
    users: list[dict] = []
    try:
        page, total_pages = 1, 1
        while page <= total_pages:
            r = client.get(
                f"{overseerr_url}/api/v1/user",
                headers=headers,
                params={"take": 100, "skip": (page - 1) * 100},
            )
            r.raise_for_status()
            body = r.json()
            total = body.get("pageInfo", {}).get("results", 0)
            total_pages = max(1, (total + 99) // 100)
            users.extend(body.get("results", []))
            page += 1
    except Exception as e:
        log.warning("Seerr: failed to list users for watchlist fetch: %s", e)
        return []

    # Per-user watchlist -- Seerr paginates these, but typical lists are small.
    items: list[dict] = []
    for u in users:
        uid = u.get("id")
        if uid is None:
            continue
        try:
            # Seerr's watchlist endpoint only accepts `page`; anything else
            # (skip/take/pageSize) is rejected with 400. Page size is fixed
            # server-side, and the response carries ``totalPages`` at the
            # top level to drive pagination.
            page, total_pages = 1, 1
            while page <= total_pages:
                r = client.get(
                    f"{overseerr_url}/api/v1/user/{uid}/watchlist",
                    headers=headers,
                    params={"page": page},
                )
                if r.status_code in (403, 404):
                    # Overseerr returns 403 for users without a linked Plex
                    # watchlist, 404 for non-existent users. Either way, just
                    # skip that user -- not an error worth logging.
                    break
                r.raise_for_status()
                body = r.json()
                total_pages = max(1, int(body.get("totalPages") or 1))
                for entry in body.get("results", []):
                    tmdb = entry.get("tmdbId")
                    mt = entry.get("mediaType")
                    if tmdb and mt:
                        items.append({"tmdbId": tmdb, "mediaType": mt})
                page += 1
        except Exception as e:
            # A single user's watchlist failing shouldn't blow up the whole run.
            log.warning("Seerr: failed to fetch watchlist for user %s: %s", uid, e)
            continue

    log.info("Seerr watchlist: %d entries across %d users", len(items), len(users))
    return items


def fetch_item_requests(
    overseerr_url: str, overseerr_key: str,
    media_type: str, tmdb_id: int,
) -> list[dict]:
    """Fetch all Seerr requests for a specific item by TMDB ID.

    Uses Seerr's /movie/{tmdbId} or /tv/{tmdbId} endpoint to get the media
    record, then fetches each request's details.

    Returns a list of dicts with: requester, status, status_label,
    requested_at, updated_at, media_type, tmdb_id, tvdb_id.
    """
    if not overseerr_url or not overseerr_key or not tmdb_id:
        return []

    client = get_client()
    headers = {"X-Api-Key": overseerr_key}

    seerr_type = "tv" if media_type == "show" else "movie"

    try:
        r = client.get(f"{overseerr_url}/api/v1/{seerr_type}/{tmdb_id}", headers=headers)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("Seerr: failed to fetch %s/%s: %s", seerr_type, tmdb_id, e)
        return []

    # Extract media info for IDs
    media_info = data.get("mediaInfo") or {}
    tvdb_id = media_info.get("tvdbId") or data.get("externalIds", {}).get("tvdbId")

    requests = media_info.get("requests") or []
    results = []
    for req in requests:
        status = req.get("status", 0)
        requester_obj = req.get("requestedBy") or {}
        requester = (requester_obj.get("plexUsername", "")
                     or requester_obj.get("displayName", "")
                     or requester_obj.get("email", ""))

        results.append({
            "request_id": req.get("id"),
            "requester": requester,
            "requester_avatar": requester_obj.get("avatar", ""),
            "status": status,
            "status_label": STATUS_LABELS.get(status, f"Unknown ({status})"),
            "requested_at": req.get("createdAt", ""),
            "updated_at": req.get("updatedAt", ""),
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "tvdb_id": tvdb_id,
        })

    return results
