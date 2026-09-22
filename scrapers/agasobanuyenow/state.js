/**
 * Persist run state under storage/ (committed by the GH Actions workflow so
 * the next scheduled run can do a cheap delta pass). Shape:
 *
 * {
 *   version: 1,
 *   lastRunAt: iso,
 *   movies:   { "<slug>": { ok: true, at: iso } },          // enrichment done
 *   series:   { "<slug>": {
 *                badge, lastEpisodeAddedAt, at,
 *                episodes: { "s1e3": { dl, at } },          // resolved CDN urls
 *              } },
 *   insertedEntities: { "<entityKey>": true }               // rows we created
 * }
 */
const fs = require('fs-extra');
const path = require('path');

const STATE_PATH = path.join(__dirname, '..', '..', 'storage', 'agasobanuyenow-state.json');

async function loadState() {
  try {
    if (await fs.pathExists(STATE_PATH)) {
      const s = await fs.readJson(STATE_PATH);
      if (s && s.version === 1) {
        return {
          lastRunAt: s.lastRunAt || null,
          lastFullScanAt: s.lastFullScanAt || null,
          movies: s.movies || {},
          series: s.series || {},
          insertedEntities: s.insertedEntities || {},
        };
      }
    }
  } catch (err) {
    // corrupt file — start fresh
  }
  return { lastRunAt: null, lastFullScanAt: null, movies: {}, series: {}, insertedEntities: {} };
}

async function saveState(state) {
  await fs.ensureDir(path.dirname(STATE_PATH));
  await fs.writeJson(STATE_PATH, { version: 1, ...state, lastRunAt: new Date().toISOString() }, { spaces: 2 });
}

module.exports = { loadState, saveState, STATE_PATH };