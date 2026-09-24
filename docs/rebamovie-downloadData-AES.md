# rebamovie /downloadData — AES-encrypted request (THE key finding)

> 2026-09-23. Root cause of "downloadData returns wrong file" and its fix.

## Symptom
`POST https://api.rebamovie.com/downloadData` with a plain JSON body
`{url, server, name, time}` returned a **stale cached mp4 of a different video**:
- 971/971 saved download entries had a media GUID that did **not** match their own
  watch URL's GUID (audit of live DB).
- JWT `dis.filename` decoded to unrelated titles: "One Last Shot",
  "Vishwanath and Sons A", "S01 - EP07 : I Will Find You".
- Pattern looked like a rate-limit/caching bug; it was not — it was an
  **undecryptable request** falling back to a cached response.

## The fix (reverse-engineered from the site's client bundle)
The site's download modal (`assets/index-*.js`, `DownloadModal` component)
**AES-encrypts the body** before POSTing. Plaintext bodies hit a degraded path.

Wire request (JSON):
```json
{ "iv": "<base64 16-byte IV>", "encrypted": "<base64 AES-256-CBC ciphertext>" }
```

Ciphertext plaintext (keep this exact key order):
```json
{ "url": "<video URL from cinemaData, verbatim incl. query>",
  "server": "<episode.server, fallback 'rebamovie'>",
  "name": "<see name rules>",
  "time": 1 }
```

Crypto params:
- Cipher: **AES-256-CBC**, PKCS#7 padding
- Key: UTF-8 bytes of the **first 32 chars** of
  `"hdsdgfudekwoqmdzonasdiowm23r4egtynh"` → `hdsdgfudekwoqmdzonasdiowm23r4egt`
- IV: fresh random 16 bytes per request, base64'd alongside in the envelope
- Response is plain (not encrypted): `{url}` or a raw `"http..."` string.

Name rules (must match the site exactly):
- series: `<Title> - S<ss>E<ee>` + `: <partName>` if partName non-empty, e.g.
  `Nero The Assassin - S01E03`
- movie: `www.rebamovie.com_<Title>`

`server` fallback is the literal `"rebamovie"` (NOT `"2"`).

Headers: only `Content-Type: application/json` (no UA/Referer needed).

## Verification
Encrypted request for Choti Bahu S1E1 → `download-video.wixmp.com/video/{same GUID}
/720p/mp4/file.mp4?token=...` → HEAD `200`, `video/mp4`, 397.5 MB,
`attachment; filename="Choti Bahu - S01E01.mp4"`. GUID matches the watch URL GUID.

## Implementation
`scrapers/rebamovie/api.js`:
- `aesEnvelope(obj)` — Node `crypto.createCipheriv('aes-256-cbc', AES_KEY, iv)`.
- `fetchDownloadLink` posts `aesEnvelope({url, server, name, time:1})`.
- `mediaGuid(url)` extracts the Wix media GUID from either URL shape; a returned
  download is only trusted when its GUID equals the requested watch URL's GUID.
- Global pacing (REBA_DL_MIN_GAP_MS) + circuit-breaker cooldown on empties remain.

## Guard (data safety)
`index.js` marks an item "not fresh" when cached entries are missing download
links, so the next delta run retries resolution. Merged entries keep validated
GUID-matched downloads or nothing — wrong files can never reach the DB again.