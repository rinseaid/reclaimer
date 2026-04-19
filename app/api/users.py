"""Users API -- list of Plex/Jellyfin users with play totals.

Consumers:
- collection_settings.html chip input for protected_users suggestions
- activity.html user filter dropdown

The legacy per-user detail page was removed; the per-user history,
protected toggle, and watched-lookup endpoints that only it consumed
went with it.
"""
from fastapi import APIRouter

from ..database import get_db

router = APIRouter(tags=["users"])


@router.get("/users")
def list_users():
    conn = get_db()
    users = conn.execute("""
        SELECT u.*, COUNT(wh.id) as total_plays,
               MAX(wh.watched_at) as last_watched
        FROM users u
        LEFT JOIN watch_history wh ON u.id = wh.user_id
        GROUP BY u.id
        ORDER BY LOWER(u.username), u.id
    """).fetchall()
    conn.close()
    return [dict(u) for u in users]
