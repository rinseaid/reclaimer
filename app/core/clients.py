"""Shared HTTP client for all API integrations."""
from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)

_client: httpx.Client | None = None


def _try_http2() -> bool:
    """httpx only enables HTTP/2 when the optional ``h2`` package is
    available. Detect at import time so we don't blow up on machines that
    didn't pull ``httpx[http2]``."""
    try:
        import h2  # noqa: F401
        return True
    except ImportError:
        return False


def get_client() -> httpx.Client:
    global _client
    if _client is None:
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Bounded connection pool -- enough concurrency for the orchestrator's
        # parallel Plex/Radarr/Sonarr/Tautulli fan-out without letting a
        # misbehaving integration exhaust local sockets.
        limits = httpx.Limits(
            max_connections=50,
            max_keepalive_connections=20,
            keepalive_expiry=30.0,
        )
        # Split connect vs read timeouts so a wedged integration can't stall
        # the whole orchestrator run. 5s connect weeds out unreachable hosts
        # quickly; 30s read covers the slow Overseerr/Plex history endpoints.
        timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
        http2 = _try_http2()
        if not http2:
            log.debug("httpx h2 package not installed; HTTP/2 disabled")
        _client = httpx.Client(
            timeout=timeout,
            verify=ctx,
            follow_redirects=True,
            limits=limits,
            http2=http2,
        )
    return _client
