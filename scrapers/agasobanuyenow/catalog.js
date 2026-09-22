/**
 * agasobanuyenow.com catalog — the site exposes a clean paginated JSON API:
 *
 *   POST /ajax/get-homepage-mixed.php { page, limit }
 *
 * Example item:
 * {
 *   type: "movie" | "series",
 *   title: "The Mongoose",
 *   slug: "the-mongoose-by-gaheza",        // narrator is baked into the slug
 *   image: "https://media.agasobanuyenow.com/assets/images/...",
 *   year: "2026",
 *   genre: "Action",
 *   interpreter: "Gaheza",                  // the narrator/translator
 *   csv_id: "af42e396-...",                 // series only
 *   latest_episode_badge: "S1EP17F",        // series only
 *   last_episode_added_at: "2026-04-11 18:48:14", // series only
 *   has_new_episode_24h: false
 * }
 */
const axios = require('axios');
const { logInfo, logError } = require('../../utils/logger');

const API_URL = 'https://agasobanuyenow.com/ajax/get-homepage-mixed.php';
const CATALOG_LIMIT = 20;
const MAX_PAGES = 25;

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';

function fetchPage(page, limit = CATALOG_LIMIT) {
  return axios.post(
    API_URL,
    { page, limit },
    {
      headers: {
        'Content-Type': 'application/json',
        Accept: '*/*',
        Referer: 'https://agasobanuyenow.com/',
        'User-Agent': UA,
      },
      timeout: 30000,
    }
  );
}

/**
 * Page the whole catalog. Dedupes by slug (and csv_id for series) because the
 * site can return the same title with different narrator variants — those are
 * distinct tracks and are NOT deduped (handled later by the matcher).
 */
async function fetchCatalog({ maxPages = MAX_PAGES, limit = 0, delayMs = 0 } = {}) {
  const items = [];
  const seen = new Set();

  for (let page = 1; page <= maxPages; page++) {
    let data;
    try {
      const res = await fetchPage(page);
      data = res.data?.items || [];
    } catch (err) {
      logError(`catalog page ${page} failed: ${err.message}`);
      break;
    }

    if (!Array.isArray(data) || data.length === 0) {
      logInfo(`catalog finished at page ${page} (no more items).`);
      break;
    }

    let added = 0;
    for (const item of data) {
      const key = item.csv_id ? `csv:${item.csv_id}` : `slug:${item.slug}`;
      if (seen.has(key)) continue;
      seen.add(key);
      items.push(item);
      added++;
    }

    logInfo(`catalog page ${page}: ${added} new item(s) (total ${items.length}).`);

    if (limit > 0 && items.length >= limit) break;
    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  return items;
}

module.exports = { fetchCatalog, CATALOG_LIMIT, MAX_PAGES };