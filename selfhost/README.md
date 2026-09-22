# Self-host pipeline — download → HEVC encode → Cloudflare R2 → Supabase

Converts top-scored movies from their existing download links (anonsharing /
mediafire), re-encodes them to **H.265 (HEVC, hvc1 tag, two-pass)** so they are
much smaller than the source, uploads them to **Cloudflare R2** (zero egress),
and repoints the movie's `Downloadurls` in Supabase at your R2 URL with the
`hosted` flag set to `true`. Existing watch links stay untouched.

Files are served as **downloads** (HEVC does not play in a plain `<video>` tag
on Chrome/Firefox; that is intentional).

## Layout

```
selfhost/
  config.js         env-driven config + defaults
  run.js            CLI orchestrator
  lib/
    state.js        tiny logger + resume state (storage/selfhost/state.json)
    pick.js         pick top-N movies with a downloadable source
    download.js     resolve anonsharing/mediafire → stream file
    convert.js      ffprobe + two-pass libx265 encode sized to budget
    upload.js       S3-compatible upload to R2 (idempotent)
    deploy.js       rewrite moviesv2.Downloadurls + hosted flag
  migration.sql     run once in Supabase SQL editor (adds hosted column)
```

## 1. Oracle VM prerequisites (Ubuntu/Debian, ARM)

```bash
sudo apt update && sudo apt install -y ffmpeg aria2
ffmpeg -version            # confirm libx265 is compiled in
```

If `ffmpeg` lacks libx265:
```bash
sudo add-apt-repository ppa:strukturag/libde265
sudo apt update && sudo apt install -y ffmpeg
```

Install Node 20+:
```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
```

Clone / copy the repo onto the VM, then run `npm install`.

## 2. Cloudflare R2

1. Cloudflare dashboard → **R2** → create a bucket (e.g. `filimehome-videos`).
2. **Manage R2 API Tokens** → create a token with **Object Read & Write** on that
   bucket. Copy the Access Key ID and Secret Access Key.
3. Set the endpoint: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
   (find your Account ID under R2 dashboard).
4. For public delivery, either use the bucket's `r2.dev` URL or, better, attach
   a **custom domain** (e.g. `videos.filimehome.com`) to the bucket through the
   Cloudflare zone — you then serve via the global CDN. Use that as
   `R2_PUBLIC_BASE_URL`.

## 3. Environment

Copy the block below into `moviescraper/.env` (all self-host vars opt-in):

```
# --- self-host pipeline ---
SELFHOST_TOP_N=20
SELFHOST_BUDGET_GB=15
SELFHOST_MIN_TARGET_MB=400
SELFHOST_MAX_TARGET_MB=1500
SELFHOST_CODEC=hevc
SELFHOST_PRESET=medium
SELFHOST_AUDIO_BITRATE=128k
SELFHOST_AUDIO_CHANNELS=2
SELFHOST_WORK_DIR=/mnt/selfhost        # put on the 200GB block volume, not boot
SELFHOST_LOG_LEVEL=info

R2_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET=filimehome-videos
R2_PREFIX=movies
R2_PUBLIC_BASE_URL=https://videos.filimehome.com
```

`SUPABASE_URL` / `SUPABASE_KEY` must already be in `.env` (the pipeline reuses
`services/supabaseClient.js` — use a **service_role** key so row updates work).

> Budget math: it divides `SELFHOST_BUDGET_GB` evenly across `SELFHOST_TOP_N`
> and computes a video bitrate per movie, clamped to the MIN/MAX bounds. With
> 9.5 GB over 20 movies you get ~475 MB each — sized to stay inside Cloudflare
> R2's 10 GB free tier.

### When the budget is full (and newer movies appear later)

`run.js` keeps a **rolling budget**. Before processing each movie it checks
`used + slice ≤ budget`:

- if there is room → process normally;
- if not → it **evicts** the already-hosted movie with the **lowest relevance
  score** (deletes the R2 object, removes its `(Server HD)` entry, sets
  `hosted=false`), then retries until the new movie fits.

So once your first 20 files fill the 9.5 GB, a later run that finds brand-new
high-score movies will swap out the oldest/lowest-priority hosted ones rather
than exceeding the limit. Evicted movies simply fall back to their original
download links — no data is lost, and a future run can re-host them if it wants
(turn on `--force` for that movie).

Controls: budget and top-N via env vars; forced swap of a specific movie with
`--id <uuid> --force`.

## 4. One-time DB migration

Run `selfhost/migration.sql` in the Supabase SQL Editor. It adds the `hosted`
boolean; until then, `deploy.js` still updates `Downloadurls` and warns.

## 5. Run

```bash
node selfhost/run.js --list            # show which movies would process
node selfhost/run.js --top 4           # process the top 4 not-yet-hosted
node selfhost/run.js --id <uuid>       # one movie by id
node selfhost/run.js --resume          # continue after an interruption
node selfhost/run.js --merge           # fold duplicates into one row, then run
node selfhost/run.js --dry-merge       # only report would-be merges
node selfhost/run.js --force           # re-encode + re-upload + re-deploy
```

Progress is saved to `storage/selfhost/state.json`, so `--resume` skips done
ids and retries nothing on its own (a failed id stays in `state.failed[]`).

### Archived original links (no re-scrape on eviction)

When a movie/episode is hosted, `deploy.js` stores the original
anonsharing/mediafire URL **inside the served entry** as `oldDownloadUrl`:

```json
{
  "title": "Episode 2 (Server HD)",
  "watchUrl": "https://rumble.com/e2",
  "downloadUrl": "https://videos.filimehome.com/…",
  "oldDownloadUrl": "https://anonsharing.com/file/a2.mp4",
  "direct": true
}
```

Eviction (`dehostMovie`) then **restores** that entry to its ordinary slow link
(`downloadUrl: oldDownloadUrl`) — no re-scraping needed. Entries created before
this feature have no archive and are dropped instead (a one-time manual
re-scrape covers those).

### Smart duplicate merge

TV/series scrapes often arrive as several rows over time (a season released in
batches). `lib/merge.js` collapses them safely:

- groups rows by a normalized "core title" (strips years, `S01E02`, "Episode N",
  "Part N") **and** type;
- refuses to merge rows with different narrators (different dubs/versions of the
  same name);
- the survivor is a `hosted=true` row if one exists (self-hosted rows are never
  deleted), otherwise the highest-scored row;
- `Downloadurls` from all rows are combined and deduped by
  `downloadUrl`/`watchUrl`/`title` (survivor's entries come first);
- survivor inherits `hosted=true` if any merged row was hosted, and the
  duplicate rows are deleted.

Run it standalone (dry-run is the default) or fold it into a pipeline run:

```bash
node selfhost/merge.js --apply              # really merge + delete dupes
node selfhost/merge.js --limit 20 --apply   # cap to 20 groups this run
node selfhost/run.js --merge --top 5        # merge, then process top 5
```

## 6. Scheduling

Start as a persistent job (so a slow batch survives SSH logs-out):

```bash
screen -S selfhost
node --max-old-space-size=1024 selfhost/run.js --top 20 --resume
# then detach: Ctrl+A D
```

For weekly runs, `crontab -e`:
```
0 3 * * 1 cd /path/to/moviescraper && /usr/bin/node selfhost/run.js --top 5 --resume >> logs/selfhost.log 2>&1
```

## 7. Notes / gotchas

- The A1 VM encodes with pure CPU (~1–2× realtime, HEVC slower than AVC).
  `--preset medium` (default) is a reasonable speed/quality/size balance.
- `download.js` resolves mediafire via the public `get_info` API and tries
  anonsharing direct/href strategies. If a host changes format, that file is
  the only place to fix.
- Files are removed from the VM after upload (the 200 GB volume is only a
  scratch space).
- Watch links (Rumble etc.) are left as-is — this pipeline only replaces the
  **Download** target.

### Self-hosted entries are write-protected

Three layers stop the scraper from clobbering your hosted movies:

1. **GH Actions scrapes are insert-only** (`SUPABASE_INSERT_ONLY=true` in both
   workflows) → `upsert(onConflict: link, ignoreDuplicates: true)` means
   existing rows are never updated, hosted or not. New episodes/titles land as
   **new rows**; old ones are untouched.
2. **Hosted-shield in `services/saveMoviesToSupabase.js`**: even a local,
   non-insert-only run first checks which incoming links are `hosted=true` and
   **drops them from the upsert** — the shield logs `Protected N self-hosted
   movie(s)…`. It activates once `migration.sql` has added the `hosted` column;
   without it (older rows) the shield is best-effort.
3. **Episode-aware deploy**: a TV row holds each episode as its own
   `Downloadurls` entry. Deploy labels the entry with the *episode* title
   (`"Episode 3 (Server HD)"`) and replaces only that exact entry, so hosting a
   new episode of a series keeps the already-hosted episodes intact. Eviction
   removes only `(Server HD)`-tagged entries, never the original ones.

Caveat: the manual one-off utilities (`npm run normalize:movies --apply`,
`utils/mergeDuplicateTitles.js`, `utils/dedupeSupabase.js`,
`utils/normalizeDatabase.js`) upsert/delete rows **by id without the shield** —
do not run those on a database that now has `hosted` rows unless you re-apply
the migration and accept they may rewrite `Downloadurls`.