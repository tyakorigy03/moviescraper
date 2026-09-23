# igiti.net — Scraper Research (metadata done, video needs subscription)

> Status: **paused**. Metadata is fully scrapable today. Video (`{GUID}`) only
> reveals itself to a signed-in, subscribed account. Revisit after the user
> obtains a **igiti.net paid subscription** to enumerate GUIDs.
>
> Research date: 2026-09-23

## Site identity
- Name: IGITI / "IGITI NET" — "Watch Movies and Series Online"
- Canonical: `https://igiti.net` → **308 redirect → `https://www.igiti.net`** (always use `www`; PowerShell 5.1 `Invoke-WebRequest` does not follow 308s).
- Content: international mainstream movies/series (USA, some Asia/Africa), marketed with Kinyarwanda tags (`agasobanuye`, `Zisobanuye`, `film zisobanuye`).
- Homepage SSR embeds 91 catalog records (53 movie + 38 serie).

## Tech stack
- **Next.js App Router** (RSC `self.__next_f.push` payloads; buildId `6ufc6miSW19FozwPaFicH`). No `__NEXT_DATA__`.
- Route groups: `app/(DefaultMode)/...` and `app/(TVMode)/...`.
- Tailwind CSS, shadcn/ui (Radix), lucide-react, Sonner Toaster, next/image optimizer (`/_next/image`), PWA manifest.
- Analytics: Umami (`umami-o6verj.rw1.hubfly.app`) + GA4 (`G-NY0KE9Y1V1`); OneSignal push.
- Image CDN: `https://image-munch.igiti.net/{filename}?width=W&random=3` (slug-based thumbs like `ambulance-2022.webp`).
- **No WordPress** (no wp-json/wp-content), no burst-cache, no `__INITIAL_STATE__`.

## Catalog data (free, no auth)
Catalog lives as structured JSON inside the RSC payload:
`self.__next_f.push([1,"5:[[["$","$L...",null,{"Contents":[ ... ]}...` (chunk 12 on homepage, ~24 KB). Consistent schema across `/`, `/tv`, `/search`:
```json
{ "id": 437, "slug": "ambulance-2022", "title": "Ambulance",
  "contentType": "movie" | "serie", "lastUpdated": "2026-09-22 00:00:00",
  "thumbnail": "ambulance-2022.webp", "country": "USA",
  "tags": "Action", "isPrivate": 0, "releaseYear": 2022 }
```
Homepage sections: "Recent Movies", "Recent Series", "Today Selections", "Random Selections", "Recent Contents".

## URL structure
| Route | Purpose |
|---|---|
| `/` | Homepage (all sections) |
| `/tv?page=N` | Series listing, paginated `?page=2 … ?page=18+` |
| `/search?query=&genre=` | Search page (GET form, input `query`; genre chips Action/Drama/Comedy/Horror) |
| `/watch/{slug}` | Movie detail+watch, SSR with JSON-LD `@type:"Movie"` |
| `/tv-watch/{slug}?season=N&episode=M` | Series episode watch — **"Sign in to your account" gate** |
| `/api/heptapay/intent` | POST `{months}` → `{data:{amount, paymentReference}}` (subscription) |
| `/api/auth/me` | Auth/session endpoint (POST; GET → `"Bad request."`) |

`robots.txt`: `Disallow: /admin`, `/api/`, `/tv`, `/tv-watch`.
Probed catalog endpoints `/api/{contents,movies,series,search,tv,home,...}` → all 404 (no public API); only `/api/heptapay` and `/api/auth/*` live.

## Video delivery (paywalled) — direct MP4s on BunnyCDN
- Viewer/download component: `/_next/static/chunks/app/(DefaultMode)/watch/[slug]/page-904c4b11bc812d85.js`.
- Quality ladder: `["720p","480p","240p","360p"]`.
- Probe: `fetch("https://vz-8bff0eee-c7f.b-cdn.net/{GUID}/play_{QUALITY}.mp4")` — 200 = exists.
- **Download (Bunny Storage API key hardcoded in public JS):**
  `https://storage.bunnycdn.com/vz-8bff0eee-c7f/{GUID}/play_{Q}.mp4?accessKey=f9133a44-f719-4c9c-939713da39a0-3fe1-44c1&download`
- **Critical:** `{GUID}` = per-title Bunny folder; **not in any public SSR payload, only after auth/subscription.** Probes with numeric id (`437`), slug, random GUID → all **403** (token-protected zone / unknown path).
- Series episodes use same `play_{quality}.mp4` scheme per episode via `/tv-watch/{slug}?season=N&episode=M`.
- No HLS/DASH/`.ts`/embedders.

## Scraping plan
1. **Metadata (do now, no auth):** parse `Contents[]` from `/`, `/tv?page=N`, `/search` → id, slug, title, contentType, releaseYear, country, tags, thumbnail, lastUpdated. Poster base: `https://image-munch.igiti.net/{thumbnail}`.
2. **Video (requires subscription):** sign in with subscribed account → capture per-title `{GUID}` from post-auth page/JS → build `https://vz-8bff0eee-c7f.b-cdn.net/{GUID}/play_{quality}.mp4`. Bunny storage key already known, so no extra session tokens needed once GUID is known (assuming play files aren't additionally token-signed; current 403s are pre-GUID, need re-test with a real GUID).
3. Subscriptions via HeptaPay (base ~6,000 RWF; seller `invoice-noreply@igiti.net`).