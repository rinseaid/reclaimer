"""Radarr/Sonarr instance management API.

CRUD endpoints backed by ``app.core.arr_instances``. API keys are
redacted on read; the PATCH handler treats empty/omitted ``api_key``
as "leave unchanged" to match the pattern used by ``/api/settings`` for
sensitive fields.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..core import arr_instances
from ..core.clients import get_client

router = APIRouter(prefix="/instances", tags=["instances"])
log = logging.getLogger(__name__)

REDACTED = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"


def _redact(row: dict) -> dict:
    out = dict(row)
    if out.get("api_key"):
        out["api_key"] = REDACTED
    return out


class InstanceCreate(BaseModel):
    kind: str
    name: str
    url: str
    api_key: str
    public_url: str = ""
    is_default: bool = False


class InstanceUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    api_key: str | None = None
    public_url: str | None = None
    is_default: bool | None = None


class InstanceTest(BaseModel):
    kind: str
    url: str | None = None
    api_key: str | None = None
    instance_id: int | None = None


@router.get("")
def list_all(kind: str | None = None) -> dict[str, Any]:
    rows = arr_instances.list_instances(kind=kind)
    return {"instances": [_redact(r) for r in rows]}


@router.get("/{instance_id}")
def get_one(instance_id: int):
    row = arr_instances.get_instance(instance_id)
    if not row:
        raise HTTPException(status_code=404, detail="Instance not found")
    return _redact(row)


@router.post("")
def create(body: InstanceCreate):
    try:
        new_id = arr_instances.create_instance(
            kind=body.kind,
            name=body.name,
            url=body.url,
            api_key=body.api_key,
            public_url=body.public_url,
            is_default=body.is_default,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    row = arr_instances.get_instance(new_id)
    return _redact(row) if row else {"id": new_id}


@router.patch("/{instance_id}")
def update(instance_id: int, body: InstanceUpdate):
    fields = body.model_dump(exclude_none=True)
    # Empty-string api_key from the UI means "leave unchanged" (the input
    # ships "" when the user didn't retype a redacted value). Strip it so
    # we don't clobber the stored key with an empty string.
    if fields.get("api_key") == "":
        fields.pop("api_key")
    try:
        arr_instances.update_instance(instance_id, **fields)
    except KeyError:
        raise HTTPException(status_code=404, detail="Instance not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    row = arr_instances.get_instance(instance_id)
    return _redact(row) if row else {"id": instance_id}


@router.delete("/{instance_id}")
def delete(instance_id: int):
    ok = arr_instances.delete_instance(instance_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Instance not found")
    return {"status": "ok", "deleted": instance_id}


@router.post("/test")
def test(body: InstanceTest):
    """Reach the arr ``/system/status`` endpoint with the supplied
    credentials. If ``instance_id`` is provided and ``api_key`` is
    absent, the stored key for that row is used so users can re-test
    a saved instance without retyping its key.
    """
    kind = body.kind
    if kind not in arr_instances.VALID_KINDS:
        return {"ok": False, "detail": f"Unknown kind: {kind}"}
    url = (body.url or "").strip()
    key = (body.api_key or "").strip()
    if body.instance_id is not None and (not url or not key):
        existing = arr_instances.get_instance(body.instance_id)
        if existing:
            url = url or existing["url"]
            key = key or existing["api_key"]
    if not url:
        return {"ok": False, "detail": "URL not configured"}
    if not key:
        return {"ok": False, "detail": "API key not configured"}
    try:
        r = get_client().get(
            f"{url.rstrip('/')}/api/v3/system/status",
            headers={"X-Api-Key": key},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        label = "Radarr" if kind == "radarr" else "Sonarr"
        return {"ok": True, "detail": f"{label} v{data.get('version', '?')}"}
    except Exception as e:
        log.warning("Instance test failed (%s): %s", kind, e)
        msg = str(e)
        if "ConnectError" in msg or "ConnectTimeout" in msg:
            msg = "Connection refused or timed out"
        elif "401" in msg or "403" in msg:
            msg = "Authentication failed (check API key)"
        return {"ok": False, "detail": msg}
