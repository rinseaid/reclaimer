"""Settings API."""
from __future__ import annotations

import ipaddress
import logging
from urllib.parse import urlparse

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Any

from .. import config
from ..core.clients import get_client

router = APIRouter(tags=["settings"])
log = logging.getLogger(__name__)


# --- SSRF guard for /settings/test-connection ---
#
# Services whose target URL is hard-coded (not user-supplied). We ignore the
# body.url for these - only the pinned remote host is ever contacted.
FIXED_SERVICE_HOSTS = {
    "torbox": "https://api.torbox.app",
    "rd": "https://api.real-debrid.com",
}

# Services that accept a user-supplied URL (internal services on the docker
# network). For these we still validate the URL doesn't point at a private
# address UNLESS the host is one of our known docker service names.
# Radarr/Sonarr are NOT listed here -- their connection tests live at
# /api/instances/test (multi-instance aware).
USER_URL_SERVICES = {
    "plex", "overseerr",
    "jellyfin", "apprise",
}

# Allow-listed docker service hostnames (these resolve to RFC1918 addresses
# but are legitimate targets on our compose network).
ALLOWED_DOCKER_HOSTS = {
    "plex", "radarr", "sonarr", "overseerr", "seerr",
    "jellyfin", "apprise", "apprise-api",
}


def _is_private_host(host: str) -> bool:
    """Return True if host resolves to a private/loopback/link-local IP."""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def _validate_user_url(url: str) -> str | None:
    """Validate a user-supplied URL for SSRF safety.

    Returns None if the URL is safe to fetch, otherwise an error string.
    """
    try:
        parsed = urlparse(url)
    except Exception as e:
        return f"Invalid URL: {e}"
    if parsed.scheme not in ("http", "https"):
        return f"Only http/https URLs are allowed (got {parsed.scheme!r})"
    host = parsed.hostname
    if not host:
        return "URL has no host"
    # Allow known docker service hostnames even if they resolve privately.
    if host.lower() in ALLOWED_DOCKER_HOSTS:
        return None
    # Reject literal private/loopback IPs.
    if _is_private_host(host):
        return f"Refusing to fetch private/loopback address: {host}"
    return None


class SettingsUpdate(BaseModel):
    settings: dict[str, Any]


class TestConnectionRequest(BaseModel):
    service: str
    url: str | None = None
    api_key: str | None = None


@router.get("/settings")
def get_settings():
    return config.get_all(redact=True)


@router.put("/settings")
def update_settings(body: SettingsUpdate):
    config.update(body.settings)
    return {"status": "ok", "updated": list(body.settings.keys())}


@router.get("/settings/plex-libraries")
def get_plex_libraries():
    """Fetch available Plex libraries for the per-rule library dropdown."""
    try:
        url = config.get("plex_url")
        token = config.get("plex_token")
        if not url or not token:
            return {"libraries": [], "machine_id": ""}
        client = get_client()
        # Get machine identifier
        machine_id = ""
        try:
            r_id = client.get(f"{url}/", params={"X-Plex-Token": token},
                              headers={"Accept": "application/json"}, timeout=10)
            r_id.raise_for_status()
            machine_id = r_id.json().get("MediaContainer", {}).get("machineIdentifier", "")
        except Exception:
            pass
        # Get libraries
        r = client.get(
            f"{url}/library/sections",
            params={"X-Plex-Token": token},
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        sections = data.get("MediaContainer", {}).get("Directory", [])
        libraries = [
            {"id": int(s["key"]), "title": s["title"], "type": s["type"]}
            for s in sections
            if s.get("type") in ("movie", "show")
            and s.get("agent", "") != "com.plexapp.agents.none"
        ]
        return {"libraries": libraries, "machine_id": machine_id}
    except Exception as e:
        log.warning("Failed to fetch Plex libraries: %s", e)
        return {"libraries": [], "machine_id": "", "error": str(e)}


@router.get("/settings/plex-collections")
def get_plex_collections():
    """Fetch Plex collections for the protection dropdown."""
    try:
        url = config.get("plex_url")
        token = config.get("plex_token")
        if not url or not token:
            return {"collections": []}
        client = get_client()
        collections = []
        for section_id in [config.get("plex_movies_section"), config.get("plex_tv_section")]:
            if not section_id:
                continue
            r = client.get(
                f"{url}/library/sections/{section_id}/collections",
                params={"X-Plex-Token": token},
                headers={"Accept": "application/json"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                for c in data.get("MediaContainer", {}).get("Metadata", []):
                    title = c.get("title", "")
                    if title:
                        collections.append(title)
        return {"collections": sorted(set(collections))}
    except Exception as e:
        log.warning("Failed to fetch Plex collections: %s", e)
        return {"collections": [], "error": str(e)}


@router.get("/settings/jellyfin-collections")
def get_jellyfin_collections():
    """Fetch Jellyfin BoxSet collections for the protection dropdown."""
    try:
        url = config.get("jellyfin_url")
        key = config.get("jellyfin_api_key")
        if not url or not key:
            return {"collections": []}
        from ..core.jellyfin import fetch_collections
        collections = []
        for section_id in [config.get("jellyfin_movies_section"), config.get("jellyfin_tv_section")]:
            if not section_id:
                continue
            for c in fetch_collections(url, key, section_id):
                name = c.get("Name", "")
                if name:
                    collections.append(name)
        return {"collections": sorted(set(collections))}
    except Exception as e:
        log.warning("Failed to fetch Jellyfin collections: %s", e)
        return {"collections": [], "error": str(e)}


@router.get("/settings/recycle-bin-status")
def get_recycle_bin_status():
    """Report whether each Radarr/Sonarr instance has a recycle-bin path
    configured.

    Response shape keeps the legacy top-level ``radarr``/``sonarr`` keys
    (pointing at the default instance for back-compat with the rule
    editor) and adds a per-instance ``instances`` list so future UI can
    show status for every connected arr.
    """
    from ..core import arr_instances, radarr, sonarr
    result: dict = {"instances": []}
    per_kind_default: dict[str, dict] = {}
    for inst in arr_instances.list_instances():
        kind = inst["kind"]
        mod = radarr if kind == "radarr" else sonarr
        entry = {
            "id": inst["id"], "kind": kind, "name": inst["name"],
            "configured": False, "path": None, "reason": None,
            "is_default": bool(inst["is_default"]),
        }
        try:
            path = mod.recycle_bin_path(inst["url"], inst["api_key"])
            entry["configured"] = bool(path)
            entry["path"] = path
        except Exception as e:
            entry["reason"] = f"error: {e}"
        result["instances"].append(entry)
        if entry["is_default"] and kind not in per_kind_default:
            per_kind_default[kind] = entry
    # Legacy fields so the existing rule-editor JS keeps working unchanged.
    for kind in ("radarr", "sonarr"):
        d = per_kind_default.get(kind)
        if d:
            result[kind] = {
                "configured": d["configured"], "path": d["path"],
                "reason": d["reason"],
            }
        else:
            result[kind] = {"configured": False, "path": None,
                            "reason": "not_connected"}
    return result


@router.get("/settings/arr-tags")
def get_arr_tags():
    """Fetch tags from every configured Radarr/Sonarr instance,
    deduplicated for the protection dropdown."""
    from ..core import arr_instances
    tags: set[str] = set()
    for inst in arr_instances.list_instances():
        try:
            r = get_client().get(
                f"{inst['url'].rstrip('/')}/api/v3/tag",
                headers={"X-Api-Key": inst["api_key"]},
                timeout=10,
            )
            if r.status_code == 200:
                for t in r.json():
                    label = t.get("label", "")
                    if label:
                        tags.add(label)
        except Exception:
            continue
    return {"tags": sorted(tags)}


@router.get("/settings/jellyfin-libraries")
def get_jellyfin_libraries():
    """Fetch Jellyfin libraries for the per-rule library dropdown."""
    try:
        url = config.get("jellyfin_url")
        key = config.get("jellyfin_api_key")
        if not url or not key:
            return {"libraries": []}
        from ..core.jellyfin import fetch_libraries
        return {"libraries": fetch_libraries(url, key)}
    except Exception as e:
        log.warning("Failed to fetch Jellyfin libraries: %s", e)
        return {"libraries": [], "error": str(e)}


def _fail(detail: str) -> dict:
    """Build a failure response for test-connection.

    Includes both ``detail`` (the FastAPI-convention field) and ``error``
    (retained for backward-compat with existing templates).
    """
    return {"ok": False, "detail": detail, "error": detail}


@router.post("/settings/test-connection")
def test_connection(body: TestConnectionRequest):
    """Test connectivity to an external service.

    If url/api_key are provided they are used directly (for testing unsaved
    values). Otherwise the current saved settings are used.

    Response shape:
      {"ok": true,  "detail": "<human-readable status>"}
      {"ok": false, "detail": "<error message>", "error": "<legacy alias>"}

    This endpoint is user-facing (connection refused / bad API key are
    expected outcomes), so it does not raise HTTPException - the ``ok``
    flag communicates success/failure.
    """
    service = body.service

    # --- SSRF guards ---
    # 1. Pinned-host services: ignore body.url, use the hard-coded public URL.
    # 2. User-URL services: validate URL passes urlparse and isn't a private
    #    IP (unless it's one of our known docker service hostnames).
    # 3. Any other service: reject - unknown services may not be safe.
    if service in FIXED_SERVICE_HOSTS:
        # body.url is ignored for these; we always use the pinned host.
        url = FIXED_SERVICE_HOSTS[service]
    elif service in USER_URL_SERVICES:
        configured_url = config.get(f"{service}_url")
        url = body.url or configured_url
        if not url:
            return _fail("URL not configured")
        # Trust URLs the user has already stored in settings -- they've
        # explicitly configured them (often private LAN IPs in homelabs).
        # Only apply SSRF checks to URLs the caller supplied in this request
        # that differ from the stored configuration.
        if url != configured_url:
            ssrf_err = _validate_user_url(url)
            if ssrf_err is not None:
                log.warning("Rejected test-connection URL for %s: %s", service, ssrf_err)
                return _fail(ssrf_err)
    else:
        return _fail(f"Unknown service: {service}")

    key = body.api_key or config.get(f"{service}_api_key") or config.get(f"{service}_token")
    client = get_client()

    try:
        if service == "plex":
            token = body.api_key or config.get("plex_token")
            if not token:
                return _fail("Token not configured")
            r = client.get(
                f"{url}/",
                params={"X-Plex-Token": token},
                headers={"Accept": "application/json"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            name = data.get("MediaContainer", {}).get("friendlyName", "Plex")
            version = data.get("MediaContainer", {}).get("version", "?")
            return {"ok": True, "detail": f"{name} v{version}"}

        elif service == "overseerr":
            if not key:
                return _fail("API key not configured")
            r = client.get(
                f"{url}/api/v1/status",
                headers={"X-Api-Key": key},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            return {"ok": True, "detail": f"Seerr v{data.get('version', '?')}"}

        elif service == "torbox":
            api_key = body.api_key or config.get("torbox_api_key")
            if not api_key:
                return _fail("API key not configured")
            r = client.get(
                f"{url}/v1/api/user/me",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json().get("data", {})
            plan = data.get("plan", "?")
            return {"ok": True, "detail": f"TorBox ({plan} plan)"}

        elif service == "rd":
            api_key = body.api_key or config.get("rd_api_key")
            if not api_key:
                return _fail("API key not configured")
            r = client.get(
                f"{url}/rest/1.0/user",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            username = data.get("username", "?")
            return {"ok": True, "detail": f"Real-Debrid ({username})"}

        elif service == "jellyfin":
            if not key:
                return _fail("API key not configured")
            from ..core.jellyfin import test_connection as jf_test
            result = jf_test(url, key)
            # Normalise helper's response shape to include both detail + error.
            if not result.get("ok") and "error" in result and "detail" not in result:
                result["detail"] = result["error"]
            return result

        elif service == "apprise":
            r = client.get(f"{url}/", timeout=10)
            # Apprise returns various codes; a connection is enough
            return {"ok": True, "detail": f"Apprise reachable (HTTP {r.status_code})"}

        else:
            # Unreachable: SSRF guard above already rejected unknown services.
            return _fail(f"Unknown service: {service}")

    except Exception as exc:
        log.warning("Connection test for %s failed: %s", service, exc)
        msg = str(exc)
        # Trim httpx exception noise
        if "ConnectError" in msg or "ConnectTimeout" in msg:
            msg = "Connection refused or timed out"
        elif "401" in msg or "403" in msg:
            msg = "Authentication failed (check API key)"
        return _fail(msg)
