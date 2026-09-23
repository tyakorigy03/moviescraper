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
const { logError } = require('../../utils/logger');

const API_BASE = 'https://api.rebamovie.com';
const SITE_BASE = 'https://www.rebamovie.com';
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';

async function post(path, body, { retries = 2, timeout = 30000 } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await axios.post(`${API_BASE}${path}`, body, {
        headers: {
          'Content-Type': 'application/json',
          Accept: '*/*',
          'User-Agent': UA,
          Referer: `${SITE_BASE}/`,
        },
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
 * Resolve a watch URL (DASH manifest) to a direct progressive MP4.
 * The endpoint occasionally returns an empty payload (rate limit) — retry a few
 * times with backoff, then give up (entry stays watch-only).
 * @returns {Promise<string>} mp4 URL or '' on failure.
 */
async function fetchDownloadLink({ url, server = '', name = '', time = '' }) {
  // Attempt 1 hits right away; retries back off 3s then 8s (+jitter) so the
  // rate-limit window can drain before we ask again.
  const backoffs = [3000, 8000];
  for (let attempt = 0; attempt <= backoffs.length; attempt++) {
    await paceDownload();
    try {
      const res = await post('/downloadData', { url, server, name, time });
      const dl = String(res?.url || '').trim();
      if (dl) {
        noteOk();
        return dl;
      }
      logError(`downloadData empty for ${name} (attempt ${attempt + 1})`);
      noteEmpty();
    } catch (err) {
      logError(`downloadData failed for ${name}: ${err.message}`);
    }
    if (attempt < backoffs.length) {
      await sleep(backoffs[attempt] + Math.floor(Math.random() * 1500));
    }
  }
  return '';
}

module.exports = { API_BASE, SITE_BASE, UA, fetchPage, fetchCinemaData, fetchDownloadLink, languageCode };