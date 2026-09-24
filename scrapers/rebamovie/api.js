/**
 * rebamovie.com JSON API client — no auth required.
 *
 *   POST /movies        { pageNumber, languageCode, deviceType }  → catalog
 *   POST /cinemaData    { MovieId, userId, tool, addedToList, deviceType } → episodes
 *   POST /downloadData  { url, server, name, time }  → direct progressive MP4
 *
 * Content is Kinyarwanda when languageCode="rn" (titles "Agasobanuye X By <narrator>").
 * Video links are Wix-signed (DASH .mpd on cdn-video.rebamovie.com, direct
 * .mp4 on download-video.wixmp.com) and expire after ~24h, so the scraper
 * re-resolves stale items to keep stored links fresh.
 */
const axios = require('axios');
const crypto = require('crypto');
const { logError } = require('../../utils/logger');

const API_BASE = 'https://api.rebamovie.com';
const SITE_BASE = 'https://www.rebamovie.com';
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';

/**
 * /downloadData requires its JSON body wrapped in an AES-256-CBC envelope
 * ({iv, encrypted}) using this hardcoded key from the site's client bundle.
 * Sent plain, the backend can't decrypt and serves stale cached URLs for the
 * WRONG video — same-host, different GUID. So we MUST encrypt, exactly as the
 * site does: randomized 16-byte IV, PKCS#7, key = first 32 chars of S9.
 */
const S9 = 'hdsdgfudekwoqmdzonasdiowm23r4egtynh';
const AES_KEY = Buffer.from(S9.slice(0, 32));

function aesEnvelope(obj) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', AES_KEY, iv);
  const enc = Buffer.concat([cipher.update(JSON.stringify(obj), 'utf8'), cipher.final()]);
  return { iv: iv.toString('base64'), encrypted: enc.toString('base64') };
}

async function post(path, body, { retries = 2, timeout = 30000 } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await axios.post(`${API_BASE}${path}`, body, {
        headers: { 'Content-Type': 'application/json' },
        timeout,
      });
      if (res.status >= 200 && res.status < 300) return res.data;
      lastErr = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastErr = err;
    }
    if (attempt < retries) await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
  }
  throw lastErr;
}

/** Language is fixed to Kinyarwanda (site default for rw). */
function languageCode() {
  return String(process.env.REBA_LANGUAGE_CODE || 'rn');
}

// --- downloadData throttle ---------------------------------------------------
// The /downloadData endpoint rate-limits hard when hit in bursts (responds 200
// with an empty payload). We pace every call globally and treat consecutive
// empties as a rate-limit signal: a cooldown that grows with each strike, so
// the API gets room to recover instead of being hammered by the retry loop.
let lastAt = 0;
let cooldownUntil = 0;
let emptyStrikes = 0;
const DL_MIN_GAP_MS = parseInt(process.env.REBA_DL_MIN_GAP_MS, 10) || 500;

// watch-GUID → download URL (or '' for a known placeholder). Ensures we never
// hit /downloadData twice for the same underlying file in one process.
const guidCache = new Map();

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Enforce the global minimum spacing + any active cooldown before a download call. */
async function paceDownload() {
  const now = Date.now();
  let wait = Math.max(0, lastAt + DL_MIN_GAP_MS - now);
  if (cooldownUntil > now) wait = Math.max(wait, cooldownUntil - now);
  if (wait) await sleep(wait);
  lastAt = Date.now();
}

function noteEmpty() {
  emptyStrikes++;
  cooldownUntil = Date.now() + Math.min(2000 + emptyStrikes * 1500, 20000);
}

function noteOk() {
  emptyStrikes = 0;
  cooldownUntil = 0;
}

/**
 * Catalog page.
 * @param {number} pageNumber 0-based.
 * @returns {{allMovies:{items:[],totalCount:number,totalPages:number,pageSize:number}, trendingMovies:[], newMovies:[], mostPopularMovies:[]}}
 */
function fetchPage(pageNumber) {
  return post('/movies', { pageNumber, languageCode: languageCode(), deviceType: UA });
}

/**
 * Episodes/parts for one movieId. `data.episodes` is nested per season:
 *   data.episodes[seasonIndex][episodeIndex]
 * @returns {{isSeason:boolean, ads:string, data:{seasons:[], episodes:[[{}]]}}}
 */
function fetchCinemaData(MovieId) {
  return post('/cinemaData', {
    MovieId,
    userId: '',
    addedToList: 'added',
    deviceType: UA,
    source: '',
  });
}

/**
 * Extract the Wix media GUID (e.g. "d7f9fb_9127dc02...") from a rebamovie URL.
 * Both the watch manifest (cdn-video.rebamovie.com/{GUID}/...) and the direct
 * MP4 (download-video.wixmp.com/video/{GUID}/...) carry the same GUID for the
 * same underlying file — so a mismatch proves downloadData returned a STALE URL
 * for a different video.
 *
 * NOTE: 720p and 480p renditions share the same GUID; they only differ by the
 * `/720p/` vs `/480p/` path segment. Use guidKey() (GUID + rendition) whenever
 * the returned rendition matters, e.g. the download cache.
 */
function mediaGuid(u) {
  const m = /(?:cdn-video\.rebamovie\.com|download-video\.wixmp\.com)\/(?:video\/)?([^/]+?)\//.exec(String(u || ''));
  return m ? m[1] : '';
}

/** "…/{GUID}/,480p,/…" (watch) or "…/{GUID}/480p/…" (download) → "480p". */
function renditionOf(u) {
  const m = /(\d{3}p)/.exec(String(u || ''));
  return m ? m[1] : '';
}

/** GUID + rendition — distinguishes 720p/480p variants of the same file. */
function guidKey(u) {
  const guid = mediaGuid(u);
  return guid ? `${guid}|${renditionOf(u) || '?'}` : '';
}

/**
 * Resolve a watch URL to a direct progressive MP4.
 *
 * IMPORTANT: the body must be the {iv, encrypted} AES envelope built from
 * {url, server, name, time:1} — the exact shape the site's download modal sends.
 * Plaintext bodies are not decryptable server-side and produce stale cached URLs
 * for other videos (wrong GUID). We additionally verify the returned URL's media
 * GUID equals the requested one and discard anything else.
 * @returns {Promise<string>} verified mp4 URL or '' on failure.
 */
async function fetchDownloadLink({ url, server = '', name = '', time = 1 }) {
  // Empty response = rate-limit: back off and retry (3s then 8s + jitter).
  // GUID mismatch = the episode genuinely maps to a broken/placeholder video on
  // rebamovie's side: one fast retry, then give up so we never store a stale
  // URL for a different file and don't burn minutes hammering a dead episode.
  const expected = mediaGuid(url);
  const cacheKey = guidKey(url);

  // The site shares ONE placeholder GUID across many episodes of a broken title
  // (e.g. KUIFI S01E32-E77 all map to the same stub). Memoize the outcome per
  // watch GUID+rendition so we only resolve each underlying file once per run —
  // that turns a 46-episode placeholder run into a single API call instead of 46.
  if (cacheKey && guidCache.has(cacheKey)) return guidCache.get(cacheKey);

  const body = aesEnvelope({ url, server, name, time });
  let resolved = '';
  for (let attempt = 0; attempt < 3; attempt++) {
    await paceDownload();
    try {
      const res = await post('/downloadData', body, { retries: 0 });
      const dl = String(res?.url || res || '').trim();
      if (dl && (!expected || mediaGuid(dl) === expected)) {
        noteOk();
        resolved = dl;
        break;
      }
      const mismatch = !!dl;
      if (mismatch) {
        // Persistent per-file state, not a rate-limit signal: don't raise the
        // cooldown (that would stall unrelated good episodes for 20s each).
        logError(`downloadData mismatch for ${name} — broken/placeholder video, giving up`);
        if (attempt === 0) await sleep(1500 + Math.floor(Math.random() * 1000));
        break; // real file problem — stop retrying
      }
      logError(`downloadData empty for ${name} (attempt ${attempt + 1})`);
      noteEmpty();
    } catch (err) {
      logError(`downloadData failed for ${name}: ${err.message}`);
    }
    if (attempt < 2) {
      await sleep((attempt === 0 ? 3000 : 8000) + Math.floor(Math.random() * 1500));
    }
  }
  if (cacheKey) guidCache.set(cacheKey, resolved);
  return resolved;
}

module.exports = { API_BASE, SITE_BASE, UA, fetchPage, fetchCinemaData, fetchDownloadLink, languageCode, mediaGuid, renditionOf, guidKey };