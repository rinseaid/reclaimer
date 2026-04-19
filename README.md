# Media Collection Manager (MCM)

FastAPI app that watches Plex/Jellyfin libraries, evaluates configurable
rules (never-watched, keep-tags, Seerr requests, file size, age, ratings,
etc.) against them, and runs ordered action pipelines -- tagging,
searching, moving, migrating, or deleting items in Radarr and Sonarr.

Previously lived under `stacks/media-collection-manager/` in the
rinseaid/komodo GitOps repo; split out so the app can evolve on its
own release cycle. Deployment is still managed by Komodo -- the
compose + TOML live in the komodo repo, while this repo provides the
image built from the Dockerfile here.

## Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

Point a browser at http://localhost:8080. The first run seeds defaults
from environment variables (see `app/config.py` / `app/database.py`);
after that SQLite at `/app/data/mcm.db` is the source of truth.

## Build the image

```bash
docker build -t mcm:dev .
```

## Key integrations

- Plex / Jellyfin -- library inventory + watch history
- Radarr / Sonarr -- metadata, tagging, monitoring, deletes, migrations
  (multi-instance support: register as many arrs as you want)
- Seerr (Overseerr) -- active-request protection
- Apprise -- notifications for rule hits + pipeline outcomes
- Real-Debrid / TorBox -- cache availability checks for download decisions
