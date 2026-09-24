/**
 * Persist run state under storage/ (committed by the GH Actions workflow so
 * the next scheduled run can do a cheap delta pass). Shape:
 *
 * {
 *   version: 1,
 *   lastRunAt: iso,
 *   lastFullScanAt: iso,
 *   movies: { "<movieId>": {
 *       at: iso,                              // last time we pulled cinemaData
 *       type: "tv" | "movie",
 *       badge: "<totalTime>",                 // e.g. "S1 EP6F", freshness hint
 *       episodes: {
 *         "s1e3": {                    // episode locator (or "t:<coreTitle>" for movies)
 *           watchUrl, downloadUrl, name, server
 *         }
 *       }
 *   } },
 *   failed: { "<movieId>": { at: iso, attempts: n, error: "..." } },
 *   pendingWrites: [ { link, Downloadurls, modifiedAt, ... } ]  // update patches
 * }
 */
const fs = require('fs-extra');
const path = require('path');

const STATE_PATH = path.join(__dirname, '..', '..', 'storage', 'rebamovie-state.json');

async function loadState() {
  try {
    if (await fs.pathExists(STATE_PATH)) {
      const s = await fs.readJson(STATE_PATH);
      if (s && s.version === 1) {
        return {
          lastRunAt: s.lastRunAt || null,
          lastFullScanAt: s.lastFullScanAt || null,
          movies: s.movies || {},
          failed: s.failed || {},
          pendingWrites: Array.isArray(s.pendingWrites) ? s.pendingWrites : [],
        };
      }
    }
  } catch (err) {
    // corrupt file — start fresh
  }
  return { lastRunAt: null, lastFullScanAt: null, movies: {}, failed: {}, pendingWrites: [] };
}

async function saveState(state) {
  await fs.ensureDir(path.dirname(STATE_PATH));
  await fs.writeJson(
    STATE_PATH,
    { version: 1, movies: state.movies, ...state, lastRunAt: new Date().toISOString() },
    { spaces: 2 }
  );
}

module.exports = { loadState, saveState, STATE_PATH };