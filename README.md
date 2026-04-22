# Reclaimer

**Reclaimer** is a rule-driven lifecycle manager for personal media libraries. It watches what you actually watch, evaluates each item against rules you define, and either leaves it alone, lets you know, or runs an ordered pipeline of actions against your upstream managers — tag, move, swap quality profile, migrate to another instance, delete.

It runs as a single container, stores its state in a local SQLite file, and has no other moving parts.

---

## The problem

Libraries grow faster than attention. One-time clicks become forty-gigabyte eyesores; every season of every show gets fetched automatically; old 4K transfers sit next to new ones. The content you love is indistinguishable from the content that downloaded once and never got touched — until storage pressure or upgrade cycles force you to wade through it by hand.

Existing cleanup tools either nuke indiscriminately, refuse to integrate with your upstream managers, or treat every item the same. None understand that "nobody has watched this for two years" is a different problem from "this is actively requested" is a different problem from "this belongs to a show that just ended."

Reclaimer models each of those as a separate dimension of a rule, runs them on a schedule, and lets you chain actions as an ordered pipeline — per rule.

---

## How it works

```
            ┌───────────────────────────────────────────┐
            │  Reclaimer runs nightly (or on per-rule   │
            │  cron), reading state from your upstreams │
            └───────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
  Library server(s)       Library manager(s)       Request tracker
  metadata + views        files, tags, quality     active requests

                                │
                                ▼
           For each rule, every item is evaluated against
           N criteria and M protections. Matches enter a
           staging queue with a per-rule grace clock.
                                │
                                ▼
           When grace expires (or the item is force-actioned),
           the rule's pipeline runs: ordered steps, mixing
           immediate and after-grace timing, per your config.
```

Every decision is auditable. Each item carries a first-seen timestamp, the rule that flagged it, the criteria it met, and the grace clock. Nothing is actioned without explicitly clearing both criteria and protections, and nothing happens on first sight — staging gives you a visible queue you can intervene in.

---

## Key concepts

### Rules

A **rule** is a named config that targets a single content type (movie, show, or season) in one library section. Every rule has:

- **Criteria** — predicates that an item must satisfy to qualify. Never watched, file size above X, released before year Y, rated below Z, partially-watched-but-abandoned, ended show with all episodes watched, etc.
- **Protections** — predicates that exempt items regardless of criteria. Tagged `keep`, member of a named collection, actively requested, on a protected user's watchlist, recently watched by anyone.
- **Action pipeline** — an ordered list of steps that run when the item clears criteria, is not protected, and has survived grace.

Rules are independent. Nothing stops you from running a "never-watched → notify" rule next to "4K over 80 GB → swap to 1080p" next to "old seasons of ended shows → offload local copy." Each has its own grace, its own schedule, its own pipeline.

### Grace period

When an item first matches a rule, it is **staged**, not actioned. It sits in a queue with a visible countdown. If it gets watched, requested, tagged, or added to a keep-collection before grace expires, it exits the queue quietly. Only items that survive their full grace window reach the pipeline.

Grace is per-rule. A 30-day grace on a "flagged for deletion" rule and a 365-day grace on "cold archive" coexist.

### Action pipeline

Pipeline steps run in the order you write them. Each step has a timing:

- **Immediate** — runs the moment an item first enters the staging queue. Used for notifications, collection sync, tagging — things that should give a viewer a heads-up or mark state in your upstream managers right away.
- **After grace** — runs only when the grace clock expires. Used for destructive or expensive operations (delete, move, migrate, search).

Most step types have a sensible default and can be pinned to either timing.

#### Available steps

| Step | What it does |
|---|---|
| `notify` | Send a formatted message through your notification gateway. Templated with `{title}`, `{collection}`, `{grace_expires}`, `{action_summary}`, etc. Best-effort; failures don't abort the pipeline. |
| `sync_collection` | Add matches to a named collection on the library server so viewers can see "leaving soon". |
| `add_arr_tag` / `remove_arr_tag` | Add or remove a tag on the item in the relevant library manager. Tags are how many managers route grabs through specific indexers or download clients. |
| `set_root_folder` | Tell the library manager to move the item to a different root path. The manager itself performs the file move. |
| `swap_quality_profile` | Change the quality profile, so the next search or upgrade targets a different tier. |
| `trigger_search` | Kick off a search in the library manager. Season-scoped for season-granularity rules, series-scoped otherwise. |
| `migrate_to_instance` | Copy the item to a different registered library manager. Target root folder, profile (matched by name on the target), and tags (by label) are carried over; the source is optionally retained. |
| `script` | Shell out to a custom command. Receives the item's identifiers as environment variables (see below). |
| `unmonitor` | Stop the library manager from tracking new episodes / upgrades. |
| `delete` | Remove the item record from the library manager. |
| `delete_files` | Remove the files from disk. |
| `import_exclusion` | After deletion, prevent automatic re-download of the same release. |

Custom scripts receive these env vars:

```
RECLAIMER_TITLE          RECLAIMER_RATING_KEY
RECLAIMER_MEDIA_TYPE     RECLAIMER_COLLECTION
RECLAIMER_ARR_ID         RECLAIMER_SEASON_NUMBER
```

### Multi-instance registry

Reclaimer treats library managers as a registry, not a single global connection. Register as many as you want — one per quality tier, one per storage class, one per media type. Rules default to the primary instance of the relevant kind, and individual pipeline steps can override to target a specific instance.

This is what lets a single rule move content from an HD instance into a 4K instance, or split a pipeline so the tag lives on one manager and the delete fires on another.

---

## Examples

The tag labels, thresholds, and collection names below are illustrative. Everything is configurable per rule.

### 1. Notify-only

> "Tell me what's at risk. Don't touch anything."

**Criteria:** never watched, not in a `keep` collection, no active request.

**Pipeline:**
```
notify (immediate, template: 'Flagged by {collection}: {title} ({size})')
```

Run it for a week. Read the notifications. Tighten the rule once you trust it.

### 2. Soft delete

> "Show viewers it's leaving, then quietly remove it after 30 days if nobody acts."

**Criteria:** never watched, not tagged `keep`, no active request.

**Pipeline:**
```
sync_collection: Leaving Soon       (immediate)
add_arr_tag:    leaving-soon        (immediate)
delete_files                        (after 30-day grace)
import_exclusion                    (after grace)
```

A viewer seeing "Leaving Soon" in their library can start it, and the view event pulls the item out of staging. Anything untouched vanishes cleanly on day 31.

### 3. Offload local copy

> "For old seasons, I want to delete the bytes on my local disk but still have the show play. My download client produces symlinks into a remote-backed mount when a specific tag is present on the series — I want to route the re-grab through that client."

**Criteria (season granularity):** not the most recent season, no active request, no views in the last 180 days.

**Pipeline:**
```
add_arr_tag:     cold                (immediate)
delete_files                         (after grace)
trigger_search                       (after grace)
```

The root folder doesn't change. Deleting the files leaves the season "missing"; the search re-grabs through the tagged download-client path, which drops in symlinks. Disk reclaimed, the show still plays.

### 4. Promote to a higher-quality instance

> "When I've actually watched a film more than once, move it from the HD library to the 4K library."

**Criteria:** view count ≥ 2 by a primary user, rating ≥ 8.0.

**Pipeline:**
```
migrate_to_instance:
  target:          <4K library manager>
  target_root:     /mnt/4k/Movies
  target_profile:  Ultra-HD
  keep_source:     false             (after grace)
add_arr_tag:       promoted-to-4k    (after grace, on target)
```

The item is created on the target by external-id lookup. Tags are carried across by label (ids differ per instance). The 4K manager's next search pulls a 4K-tier release into the right folder.

### 5. Seasonal pruning of ended shows

> "Once a show ends and I've watched the season, stop monitoring it and clean up the files."

**Criteria (season granularity):** series status is "ended", every episode in the season has been watched, no views in last 90 days.

**Pipeline:**
```
unmonitor                            (after 60-day grace)
delete_files                         (after grace)
```

---

## Protections

Protections are checked in parallel with criteria. Any match pulls an item out of consideration entirely — it never enters staging, never gets notified about, never reaches the pipeline.

| Protection | Effect |
|---|---|
| `protected_tags` | Item has any tag in the listed set on its library manager |
| `protected_collections` | Item is a member of a named collection on the library server |
| `protected_users` | Watched by one of the listed users |
| `active_request` | Has an open request in the request tracker |
| `recently_added` | Arrived in the library within the last N days |
| `partially_watched` | Someone started it within the last N days (protects mid-watch items) |
| `series_protection` | (season rules) Every season in a show is protected if the show carries any protection |

Protections stack. You can have "the primary user's watchlist is sacred" AND "anything tagged `keep` survives" AND "anything in the Classics collection is off-limits" all active at once.

---

## Scheduling

Rules run on the global nightly schedule by default (hour + minute, local time). Each rule can optionally carry a cron expression that overrides the global schedule — useful for expensive rules that should run less often, or time-sensitive ones that should fire at a specific hour.

Rules also carry a priority. When two rules match the same item, lower numbers win. Most rules target disjoint criteria and never collide; the field is there for the edge cases.

---

## Quick start

### 1. Connect your upstreams

Reclaimer reads from and writes to external services. At minimum you need:

- **A library server** — source of truth for what content exists, what's been watched, and by whom
- **A library manager** — owner of the files, tags, and quality profiles

Optional but useful:

- **A request tracker** — so active requests protect in-flight content
- **A notification gateway** — for immediate-timing notifications
- **A secondary library server** — if you run more than one

### 2. Run the container

```yaml
services:
  reclaimer:
    image: ghcr.io/rinseaid/reclaimer:latest
    container_name: reclaimer
    ports:
      - "8080:8080"
    volumes:
      - ./data:/app/data
    environment:
      # Required: library server
      PLEX_URL: "https://library.example.com:32400"
      PLEX_TOKEN: "your-token"

      # Required: at least one library manager
      RADARR_URL: "http://movies:7878"
      RADARR_API_KEY: "..."
      SONARR_URL: "http://tv:8989"
      SONARR_API_KEY: "..."

      # Optional upstreams
      SEERR_URL: "http://requests:5055"
      SEERR_API_KEY: "..."
      APPRISE_URL: "http://apprise:8000/notify/media"
    restart: unless-stopped
```

First boot seeds these values into SQLite. After that, the UI is the source of truth — changes in the UI don't get overwritten by env vars on subsequent boots.

### 3. Dry-run a rule

Open `http://localhost:8080`, go to **Rules**, pick a template (or start from scratch), fill in criteria and pipeline. Before enabling, click **Dry run**: Reclaimer reports which items match, which are protected and why, and exactly what the pipeline would do — without touching anything.

### 4. Enable and walk away

Enable the rule. The nightly run picks it up. Watch the **Logs** page the next day to confirm you got the outcome you expected.

---

## Operational model

### Auth

Reclaimer has no built-in authentication. It assumes it runs behind a reverse proxy that enforces access control — a forward-auth proxy, OIDC gateway, or plain HTTP basic-auth sidecar. Exposing it directly would give any visitor full control over your library managers.

### Single user

The app is single-tenant. All rules, overrides, and settings are shared globally by whoever can reach the UI. There is no per-user scope, no permissions, no audit trail of "who changed this."

### Data

Everything lives at `/app/data/reclaimer.db` (SQLite, WAL mode). Typical size for a library of a few thousand items is under 100 MB. Back it up by stopping the container and copying the file.

### Failure handling

Pipeline steps execute in sequence. A failed step stops the pipeline for that item; prior steps are not rolled back. Failures are logged to the activity stream and surface in the Logs page with the item title and error. Notifications are best-effort — a failed send does not abort the pipeline.

---

## Configuration

Most settings live in the UI under **Settings**. The reference below covers environment seeds used on first boot.

### Library server

| Variable | Description |
|---|---|
| `PLEX_URL` | Library server base URL |
| `PLEX_TOKEN` | Library server token |
| `PLEX_PUBLIC_URL` | Public-facing URL for browser links (optional) |
| `PLEX_MOVIES_SECTION` / `PLEX_TV_SECTION` | Section IDs used as defaults |
| `JELLYFIN_URL` / `JELLYFIN_API_KEY` | Secondary library server (optional) |

### Library managers

On first boot, the single-instance env vars below are promoted into the multi-instance registry as default instances. After that, add more via the UI — there's no cap on how many of each kind you can register.

| Variable | Description |
|---|---|
| `RADARR_URL` / `RADARR_API_KEY` | Movie library manager — seeds the default instance |
| `SONARR_URL` / `SONARR_API_KEY` | TV library manager — seeds the default instance |
| `RADARR_PUBLIC_URL` / `SONARR_PUBLIC_URL` | Public-facing URLs for browser links |

### Optional

| Variable | Description |
|---|---|
| `SEERR_URL` / `SEERR_API_KEY` | Request tracker — gates the `active_request` protection |
| `APPRISE_URL` | Notification gateway (any Apprise-compatible endpoint) |
| `PROTECTED_REQUESTERS` | Comma-separated usernames whose requests block deletion |
| `TORBOX_API_KEY` / `RD_API_KEY` | Cache-availability checks used by the offload flow |

### Scheduling

| Variable | Default | Description |
|---|---|---|
| `SCHEDULE_HOUR` | `2` | Global nightly run hour (local time) |
| `SCHEDULE_MINUTE` | `30` | Global nightly run minute |
| `USER_SYNC_INTERVAL_HOURS` | `6` | How often to sync watch-history users |

### Deletion defaults

| Variable | Default | Description |
|---|---|---|
| `DELETE_FILES` | `true` | Whether `delete` removes files by default (per-rule override available) |
| `ADD_IMPORT_EXCLUSION` | `true` | Whether `delete` adds an import-exclusion block |

---

## Pipeline templates

Templates in the rule editor insert skeleton steps with placeholders. You fill in the specifics.

| Template | Shape |
|---|---|
| **Migrate to cloud storage** | swap profile → set root folder → search → retag |
| **Migrate to local storage** | swap profile → set root folder → search → retag |
| **Promote to 4K instance** | migrate to instance → add tag |
| **Retag after migration** | remove one tag → add another |
| **Offload local copy** | add tag → delete files → search — works with a tag-gated download client that produces symlinks into a remote-backed mount |

---

## Published images

GitHub Actions builds and pushes on every commit to `main`:

- `ghcr.io/rinseaid/reclaimer:latest` — tip of main
- `ghcr.io/rinseaid/reclaimer:sha-<short>` — pinned to the exact commit
- `ghcr.io/rinseaid/reclaimer:vX.Y.Z` — published when a semver tag is cut

Digest-pin in production (`ghcr.io/rinseaid/reclaimer:sha-abc1234@sha256:...`) so deploys are reproducible.

---

## Development

```sh
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

Open `http://localhost:8080`.

### Build the image locally

```sh
docker build -t reclaimer:dev .
```

---

## License

MIT
