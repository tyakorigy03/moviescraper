/**
 * Hot-signal helpers — externally-sourced popularity used as a capped, additive
 * boost term inside computeRelevanceScore (see utils/relevanceScore.js).
 *
 * Snapshot file (storage/hot-signals.json):
 * {
 *   version: 2,
 *   updatedAt: "<ISO>",
 *   signals: {
 *     "<moviesv2 link>": {
 *       hot: 4.2,
 *       points: 17.6,                       // legacy additive points, for reporting
 *       sources: ["reba-trending#0", "aglive#3"],
 *       family: "beauty in black|tv",
 *       match: "family+narrator"            // how the work resolved to this row
 *     }
 *   }
 * }
 *
 * v1 records only carried { hot, sources }; both are read unchanged, so an old
 * snapshot still loads.
 *
 * The hotsignals scraper (npm run scrape5) builds this snapshot from:
 *   - rebamovie /movies trending/new/mostPopular ranked lists (the same lists
 *     agasobanuyebox.com surfaces — same API),
 *   - agasobanuyelive + oshakurfilms storage order (recency),
 *   - agasobanuyenow catalog order (recency),
 * then folds `hot` into each matched row's score.
 */
const fs = require('fs-extra');
const path = require('path');

const SNAPSHOT_FILE = path.join(__dirname, '..', 'storage', 'hot-signals.json');
const SNAPSHOT_VERSION = 2;
const EMPTY = { version: SNAPSHOT_VERSION, updatedAt: null, signals: {} };

/** Clamp hot to the same 0..10 cap computeRelevanceScore applies. */
function clampHot(value) {
  return Math.max(0, Math.min(Number(value) || 0, 10));
}

async function loadHotSnapshot() {
  try {
    if (!(await fs.pathExists(SNAPSHOT_FILE))) return { ...EMPTY };
    return await fs.readJson(SNAPSHOT_FILE);
  } catch {
    return { ...EMPTY };
  }
}

async function saveHotSnapshot(signals) {
  const snapshot = { version: SNAPSHOT_VERSION, updatedAt: new Date().toISOString(), signals };
  await fs.ensureFile(SNAPSHOT_FILE);
  await fs.writeJson(SNAPSHOT_FILE, snapshot, { spaces: 2 });
  return snapshot;
}

/** Number of signals currently tracked for a link (0 when absent). */
function hotOf(snapshot, link) {
  const rec = snapshot && snapshot.signals && snapshot.signals[link];
  return rec ? clampHot(rec.hot) : 0;
}

module.exports = { loadHotSnapshot, saveHotSnapshot, hotOf, clampHot, SNAPSHOT_FILE };