/**
 * rebamovie catalog fetch — pages the /movies endpoint (Kinyarwanda "rn").
 *
 * Items (per catalog response. allMovies.items):
 * {
 *   id: "<movieId uuid>",                     // unique per upload (use for link + cinemaData)
 *   movieDataId: {
 *     id: "<movieDataId uuid>",               // canonical title id (shared across languages/narrators)
 *     title, description, genre: [], country: [],
 *     rereaseDate: "2026", fullReleaseDate: "21/09/2026",
 *     typeFull: ["Filme"] | ["Series"],
 *     image, longCover, hdimage
 *   },
 *   interpreter: { title: "<narrator>" },
 *   seoTitle: "Agasobanuye <Title> By <Narrator>",
 *   totalTime: "S1 EP6F" | "Filme"
 * }
 */
const { fetchPage } = require('./api');
const { logInfo, logError } = require('../../utils/logger');

const MAX_PAGES = 68; // rn totalPages as observed; overridden by first response
const PAGE_SIZE = 49;

function isSeries(item) {
  const tfull = Array.isArray(item?.movieDataId?.typeFull) ? item.movieDataId.typeFull : [];
  if (tfull.includes('Series')) return true;
  const tt = String(item?.totalTime || '');
  if (/^S\d/i.test(tt)) return true;
  return tfull.includes('Series');
}

/**
 * Page the whole catalog. Dedupes by movieId (item.id) — the same
 * movieDataId.id can appear multiple times with different narrators/dubs and
 * those must stay as distinct items (handled later by the matcher).
 */
async function fetchCatalog({ maxPages = 0, limit = 0, delayMs = 0 } = {}) {
  const items = [];
  const seen = new Set();
  let totalPages = maxPages || MAX_PAGES;

  for (let page = 0; page < totalPages; page++) {
    let data;
    try {
      data = await fetchPage(page);
    } catch (err) {
      logError(`catalog page ${page} failed: ${err.message}`);
      break;
    }

    const all = data?.allMovies || {};
    const batch = Array.isArray(all.items) ? all.items : [];
    if (all.totalPages && !maxPages) totalPages = all.totalPages;

    if (!batch.length) {
      logInfo(`catalog finished at page ${page} (no more items).`);
      break;
    }

    let added = 0;
    for (const item of batch) {
      if (!item?.id || !item?.movieDataId?.title) continue;
      if (seen.has(item.id)) continue;
      if (limit > 0 && items.length >= limit) break;
      seen.add(item.id);
      items.push(item);
      added++;
    }

    logInfo(`catalog page ${page}: ${added} new item(s) (total ${items.length}).`);

    if (limit > 0 && items.length >= limit) break;
    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  return items;
}

module.exports = { fetchCatalog, isSeries, MAX_PAGES, PAGE_SIZE };