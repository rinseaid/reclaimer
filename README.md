# Reclaimer

Rule-driven lifecycle manager for Plex/Jellyfin libraries. Watches
your collections, evaluates configurable rules (never-watched,
keep-tags, Seerr requests, file size, age, ratings, etc.), and runs
ordered action pipelines in Radarr and Sonarr -- tagging, searching,
moving to other root folders, migrating between arr instances, or
deleting.

Deployment lives in rinseaid/komodo -- the compose + TOML there
reference the image published by the GitHub Actions workflow in this
repo.

## Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

UI at http://localhost:8080. First run seeds defaults from environment
variables (see `app/config.py` / `app/database.py`); after that the
SQLite DB at `/app/data/reclaimer.db` is the source of truth.

## Build the image

```bash
docker build -t reclaimer:dev .
```

## Published images

GitHub Actions builds and pushes `ghcr.io/rinseaid/reclaimer:<tag>` on
every push to `main`. Tags:

- `latest` -- tip of main
- `sha-<short>` -- pinned to the exact commit (Renovate-friendly)

## Integrations

- Plex / Jellyfin -- library inventory + watch history
- Radarr / Sonarr -- metadata, tagging, monitoring, deletes,
  cross-instance migrations (multi-instance support built in)
- Seerr (Overseerr) -- active-request protection
- Apprise -- notifications for rule hits and pipeline outcomes
- Real-Debrid / TorBox -- cache-availability checks
