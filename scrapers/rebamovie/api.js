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
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await post('/downloadData', { url, server, name, time });
      const dl = String(res?.url || '').trim();
      if (dl) return dl;
      logError(`downloadData empty for ${name} (attempt ${attempt + 1})`);
    } catch (err) {
      logError(`downloadData failed for ${name}: ${err.message}`);
    }
    await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
  }
  return '';
}

module.exports = { API_BASE, SITE_BASE, UA, fetchPage, fetchCinemaData, fetchDownloadLink, languageCode };