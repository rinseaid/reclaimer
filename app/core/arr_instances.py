"""Multi-instance registry for Radarr and Sonarr connections.

Stored in the ``arr_instances`` table. Each row represents a distinct
arr connection (HD vs 4K Radarr, cloud-only Sonarr, etc.). At most one
row per kind carries ``is_default=1`` (enforced by a partial unique
index); legacy call sites that don't know which instance they want
fall through to the default.

All DB access is confined to this module; callers outside work with
plain ``dict`` rows and integer ids.
"""
from __future__ import annotations

from datetime import datetime

from ..database import get_db

VALID_KINDS = ("radarr", "sonarr")


def _to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()}


def list_instances(kind: str | None = None) -> list[dict]:
    """Return all instances (optionally filtered to a kind), defaults first."""
    conn = get_db()
    try:
        if kind:
            rows = conn.execute(
                "SELECT * FROM arr_instances WHERE kind = ? "
                "ORDER BY is_default DESC, id",
                (kind,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM arr_instances ORDER BY kind, is_default DESC, id"
            ).fetchall()
        return [_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_instance(instance_id: int) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM arr_instances WHERE id = ?", (int(instance_id),)
        ).fetchone()
        return _to_dict(row) if row else None
    finally:
        conn.close()


def default_instance(kind: str) -> dict | None:
    """Return the default instance of ``kind``, falling back to the
    lowest-id row if no row is explicitly default (e.g. all were
    toggled off manually). Returns ``None`` when the kind has no rows.
    """
    if kind not in VALID_KINDS:
        return None
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM arr_instances WHERE kind = ? AND is_default = 1",
            (kind,),
        ).fetchone()
        if row:
            return _to_dict(row)
        row = conn.execute(
            "SELECT * FROM arr_instances WHERE kind = ? ORDER BY id LIMIT 1",
            (kind,),
        ).fetchone()
        return _to_dict(row) if row else None
    finally:
        conn.close()


def create_instance(kind: str, name: str, url: str, api_key: str,
                    public_url: str = "", is_default: bool = False) -> int:
    if kind not in VALID_KINDS:
        raise ValueError(f"Invalid kind: {kind!r}")
    name = (name or "").strip()
    url = (url or "").strip()
    api_key = (api_key or "").strip()
    public_url = (public_url or "").strip()
    if not (name and url and api_key):
        raise ValueError("name, url, api_key are required")
    conn = get_db()
    try:
        now = datetime.now().isoformat()
        if is_default:
            conn.execute(
                "UPDATE arr_instances SET is_default = 0, updated_at = ? "
                "WHERE kind = ? AND is_default = 1",
                (now, kind),
            )
        cur = conn.execute(
            """INSERT INTO arr_instances
               (kind, name, url, api_key, public_url, is_default, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (kind, name, url, api_key, public_url,
             1 if is_default else 0, now, now),
        )
        new_id = cur.lastrowid
        # If no default exists for this kind yet, promote the new row so
        # callers that ask for default_instance() always get something.
        if not is_default:
            has_default = conn.execute(
                "SELECT 1 FROM arr_instances WHERE kind = ? AND is_default = 1 LIMIT 1",
                (kind,),
            ).fetchone()
            if not has_default:
                conn.execute(
                    "UPDATE arr_instances SET is_default = 1, updated_at = ? WHERE id = ?",
                    (now, new_id),
                )
        conn.commit()
        return new_id
    finally:
        conn.close()


def update_instance(instance_id: int, **fields) -> None:
    """Patch selected columns. Pass ``is_default=True`` to promote this
    row and demote any existing default of the same kind."""
    allowed = {"name", "url", "api_key", "public_url", "is_default"}
    updates: dict = {k: v for k, v in fields.items()
                     if k in allowed and v is not None}
    if not updates:
        return
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT kind FROM arr_instances WHERE id = ?", (int(instance_id),)
        ).fetchone()
        if not row:
            raise KeyError(f"Instance {instance_id} not found")
        kind = row["kind"]
        now = datetime.now().isoformat()
        if "is_default" in updates:
            if updates["is_default"]:
                conn.execute(
                    "UPDATE arr_instances SET is_default = 0, updated_at = ? "
                    "WHERE kind = ? AND is_default = 1 AND id != ?",
                    (now, kind, int(instance_id)),
                )
                updates["is_default"] = 1
            else:
                updates["is_default"] = 0
        for k in ("name", "url", "api_key", "public_url"):
            if k in updates and isinstance(updates[k], str):
                updates[k] = updates[k].strip()
        set_clause = ", ".join(f"{k} = ?" for k in updates) + ", updated_at = ?"
        vals = list(updates.values()) + [now, int(instance_id)]
        conn.execute(f"UPDATE arr_instances SET {set_clause} WHERE id = ?", vals)
        conn.commit()
    finally:
        conn.close()


def delete_instance(instance_id: int) -> bool:
    """Delete an instance. Returns ``True`` if a row was removed. If the
    deleted row was the default, promotes the lowest-id remaining row of
    the same kind to default so callers relying on ``default_instance``
    don't suddenly see ``None``."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT kind, is_default FROM arr_instances WHERE id = ?",
            (int(instance_id),),
        ).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM arr_instances WHERE id = ?", (int(instance_id),))
        if row["is_default"]:
            promote = conn.execute(
                "SELECT id FROM arr_instances WHERE kind = ? ORDER BY id LIMIT 1",
                (row["kind"],),
            ).fetchone()
            if promote:
                conn.execute(
                    "UPDATE arr_instances SET is_default = 1, updated_at = ? WHERE id = ?",
                    (datetime.now().isoformat(), promote["id"]),
                )
        conn.commit()
        return True
    finally:
        conn.close()


def resolve(instance_id: int | None, kind: str) -> dict | None:
    """Return a concrete instance dict.

    If ``instance_id`` is provided and matches the kind, return it.
    Otherwise fall back to ``default_instance(kind)``. Callers use this
    as the single resolution point when a pipeline step or API call
    may or may not carry an explicit instance id.
    """
    if instance_id is not None:
        inst = get_instance(int(instance_id))
        if inst and inst["kind"] == kind:
            return inst
    return default_instance(kind)
