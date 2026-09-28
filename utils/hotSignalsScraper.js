/**
 * Hot-signal scorer (npm run scrape5) — "hot agasobanuye" boost.
 *
 * Collects external popularity signals and folds them into moviesv2 scores as a
 * capped additive hot term (see utils/relevanceScore.js). Sources:
 *
 *   1. rebamovie.com /movies ranked lists — `trendingMovies`, `newMovies`,
 *      `mostPopularMovies`. agasobanuyebox.com (the CineBeta/streamchat network)
 *      serves the exact same API, so reading rebamovie covers agasobanuyebox too.
 *   2. agasobanuyelive.json storage order (progressLink2 = discovery order,
 *      newest first → earlier index = hotter).
 *   3. oshakurfilms.json storage order (same shape).
 *   4. agasobanuyenow.json catalog order (array order = listing order).
 *
 * Resolution and weighting live in utils/hotMatching.js. In short: a ranked list
 * names a *work*, so signals resolve narrator-blind and season-blind to
 * `coreTitle|type` and the boost is attributed to that family's newest row —
 * a show is boosted once, on its current season. Points combine as a noisy-OR
 * of per-list rank weights so no single list can peg the cap, and every
 * observation keeps its provenance (`reba-trending#2`, `aglive#0`, …) in the
 * snapshot.
 *
 * Scores are recomputed for rows that gain a boost AND for rows that lose one
 * (a title dropping off the lists must fall back to hot=0, otherwise its
 * inflated score would stick forever). Writes happen only when a score actually
 * changes, so `modifiedAt` does not churn.
 *
 * Usage:
 *   node utils/hotSignalsScraper.js          # build snapshot + apply scores
 *   node utils/hotSignalsScraper.js --dry    # build snapshot, don't touch DB
 *   node utils/hotSignalsScraper.js --report # log unattached ranked works
 *   node utils/hotSignalsScraper.js --limit 60
 */
require('dotenv').config();
const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');
const { loadHotSnapshot, saveHotSnapshot } = require('./hotSignals');
const {
  RANKS, buildIndex, createLedger, applyLedger,
} = require('./hotMatching');
const { fetchPage } = require('../scrapers/rebamovie/api');
const { loadStorageJson } = require('./storageOrders');

const TABLE = 'moviesv2';
const BATCH_SIZE = 200;

/* ------------------------------ signal gathering --------------------------- */

const REBA_LISTS = {
  trendingMovies: 'reba-trending',
  newMovies: 'reba-new',
  mostPopularMovies: 'reba-popular',
};

/** rebamovie ranked lists -> ledger. */
async function collectReba(ledger, pages = 1) {
  for (let p = 0; p < pages; p++) {
    let data;
    try {
      data = await fetchPage(p);
    } catch (err) {
      logError(`hotsignals rebamovie page ${p} failed: ${err.message}`);
      break;
    }
    for (const [listName, tag] of Object.entries(REBA_LISTS)) {
      const list = Array.isArray(data?.[listName]) ? data[listName] : [];
      list.slice(0, RANKS).forEach((item, idx) => {
        if (!item?.id || !item?.movieDataId?.title) return;
        const type =
          Array.isArray(item.movieDataId.typeFull) && item.movieDataId.typeFull.includes('Series') ? 'tv' : 'movie';
        ledger.add(
          item.movieDataId.title,
          type,
          idx,
          `${tag}#${idx}`,
          item.interpreter?.title || '',
          Math.max(0, 10 - idx * 0.9)
        );
      });
    }
  }
}

/** Storage-order lists: earlier item in the list => higher hot. */
function collectOrder(ledger, items, tag) {
  for (const [idx, item] of (items || []).slice(0, RANKS).entries()) {
    const title = String(item?.title || '').trim();
    if (!title) continue;
    const type = item?.type || (/\bs0\d+\b/i.test(title) ? 'tv' : 'movie');
    // genre strings sometimes carry the narrator; harmless when absent, since
    // resolution is narrator-blind (see utils/hotMatching.js).
    const narrator = Array.isArray(item?.genres)
      ? item.genres.filter((g) => g && /\b(?:by|kwa)\s+/i.test(String(g))).join(', ')
      : '';
    ledger.add(title, type, idx, `${tag}#${idx}`, narrator, Math.max(0, 9 - idx * 0.45));
  }
}

/* ------------------------------ matching ----------------------------------- */

async function loadRows() {
  const all = [];
  const pageSize = 1000;
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(TABLE)
      .select('link,title,type,narrator,popularity,publishedAt,modifiedAt,tmdb_rating,score')
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`rows: ${error.message}`);
    all.push(...(data || []));
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

const scoreWith = (row, hot) =>
  computeRelevanceScore({
    tmdb_rating: row.tmdb_rating || 0,
    popularity: row.popularity || 0,
    publishedAt: row.publishedAt || '',
    modifiedAt: row.modifiedAt || '',
    narrator: row.narrator || '',
    title: row.title || '',
    hot,
  });

/* ------------------------------ main --------------------------------------- */

async function main() {
  const args = process.argv.slice(2);
  const dry = args.includes('--dry');
  const report = args.includes('--report');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) || 0 : 0;

  const ledger = createLedger();

  logInfo('hotsignals: collecting ranked lists from rebamovie (+ agasobanuyebox)...');
  await collectReba(ledger, 1);

  logInfo('hotsignals: reading storage-order signals (agasobanuyelive, oshakurfilms, agasobanuyenow)...');
  const [aglive, oshakur, agnow] = await Promise.all([
    loadStorageJson('agasobanuyelive.json'),
    loadStorageJson('oshakurfilms.json'),
    loadStorageJson('agasobanuyenow.json'),
  ]);
  collectOrder(ledger, aglive, 'aglive');
  collectOrder(ledger, oshakur, 'oshakur');
  collectOrder(ledger, agnow, 'agnow');

  let rows = await loadRows();
  if (limit > 0) rows = rows.slice(0, limit);
  logInfo(
    `hotsignals: ${ledger.size} ranked works from ${rows.length} existing moviesv2 row(s).`
  );

  const byLink = new Map(rows.map((r) => [r.link, r]));
  // Score at hot=0 selects which row of a show gets the boost; it must be the
  // same function the database is ranked by, or the boost could land on a row
  // the frontend's show-level dedupe then discards.
  const { boosts, unresolved } = applyLedger(ledger, buildIndex(rows), {
    scoreRow: (row) => scoreWith(row, 0),
  });

  // link -> { hot, sources, ... }
  const signals = {};
  const changes = [];
  const bump = (row, hot, rec) => {
    const newScore = scoreWith(row, hot);
    if (Math.abs(newScore - (Number(row.score) || 0)) > 0.05) {
      changes.push({ link: row.link, score: newScore });
    }
    if (rec) signals[row.link] = rec;
  };

  for (const boost of boosts) {
    const row = byLink.get(boost.link);
    if (!row) continue;
    bump(row, boost.hot, {
      hot: boost.hot,
      points: boost.points,
      sources: boost.sources,
      show: boost.show,
      match: boost.match,
    });
  }

  // A title that falls off every list must lose its boost, or the inflated
  // score would persist indefinitely (hotSignalsScraper used to skip these).
  const previous = (await loadHotSnapshot()).signals || {};
  const released = Object.keys(previous).filter((link) => !signals[link]);
  for (const link of released) {
    const row = byLink.get(link);
    if (row) bump(row, 0, null);
  }
  if (released.length) {
    logInfo(`hotsignals: ${released.length} previously-boosted row(s) dropped off the lists and will fall back to hot=0.`);
  }

  if (report && unresolved.length) {
    logInfo(`hotsignals: ${unresolved.length} ranked work(s) not in the catalogue:`);
    for (const u of unresolved.sort((a, b) => b.hot - a.hot)) {
      logInfo(`  ${String(u.hot.toFixed(1)).padStart(4)}  ${u.show}  [${u.sources.join(' ')}]`);
    }
  }

  const snapshot = { version: 2, updatedAt: new Date().toISOString(), signals };
  const boostedCount = Object.keys(signals).length;
  logInfo(
    `hotsignals: ${boostedCount} work(s) boosted, ${changes.length} score change(s), ` +
      `${unresolved.length} unattached.`
  );

  if (dry) {
    // Persisting the snapshot without applying the scores would leave the file
    // describing boosts the database does not have, so --dry writes nothing.
    logInfo('hotsignals: --dry, skipping snapshot write and DB writes.');
    return { boosted: boostedCount, changes: changes.length, unattached: unresolved.length, changes_: changes };
  }

  await saveHotSnapshot(signals);

  for (let i = 0; i < changes.length; i += BATCH_SIZE) {
    const { error } = await supabase.from(TABLE).upsert(changes.slice(i, i + BATCH_SIZE), { onConflict: 'link' });
    if (error) logError(`hotsignals upsert batch failed: ${error.message}`);
  }
  logInfo(`hotsignals: applied to ${changes.length} row(s).`);
  return { boosted: boostedCount, changes: changes.length, unattached: unresolved.length };
}

if (require.main === module) {
  main()
    .then((res) =>
      logInfo(
        `hotsignals finished — boosted ${res?.boosted} work(s), ` +
          `${res?.changes} score change(s), ${res?.unattached} unattached.`
      )
    )
    .catch((err) => {
      logError(`hotsignals fatal: ${err.message}`);
      process.exit(1);
    });
}

module.exports = { main };
