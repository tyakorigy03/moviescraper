/**
 * Hot-score predictor (diagnostic, read-only) — answers "why does this title
 * have the score it has?" without waiting for the next scrape5 run.
 *
 * It replays the exact resolution and weighting from utils/hotMatching.js
 * (which is what hotSignalsScraper.js applies), so for every work the source
 * sites currently rank it shows:
 *
 *   live position on each ranked list -> hot -> predicted score
 *   vs the score actually stored in moviesv2
 *
 * Flags worth reading:
 *   UNATTACHED the work is ranked but has no catalogue row
 *   AT-CAP     hot is pinned at the cap, so it no longer discriminates
 *   VOLATILE   the lists rotated since the last snapshot
 *   DRIFT      stored score disagrees with the model
 *   NO-TMDB    no tmdb_rating/popularity, so the score is recency+hot only
 *   MANY-SEASONS  the work is stored as N season rows but only 1 is boosted
 *
 * Usage:
 *   node utils/predictHotScores.js                 # everything the sites rank
 *   node utils/predictHotScores.js "beauty in black"
 *   node utils/predictHotScores.js --reba-only
 *   node utils/predictHotScores.js --no-live       # storage sources only
 *   node utils/predictHotScores.js --verbose       # + source list per row
 */
require('dotenv').config();
const supabase = require('../services/supabaseClient');
const { logInfo } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');
const { loadHotSnapshot } = require('./hotSignals');
const { loadStorageJson } = require('./storageOrders');
const { RANKS, HOT_CAP, showKey, buildIndex, createLedger, applyLedger } = require('./hotMatching');
const { fetchPage } = require('../scrapers/rebamovie/api');

const TABLE = 'moviesv2';
const REBA_LISTS = {
  trendingMovies: 'reba-trending',
  newMovies: 'reba-new',
  mostPopularMovies: 'reba-popular',
};

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

async function main() {
  const argv = process.argv.slice(2);
  const filter = (argv.find((a) => !a.startsWith('--')) || '').toLowerCase();
  const live = !argv.includes('--no-live');
  const rebaOnly = argv.includes('--reba-only');
  const verbose = argv.includes('--verbose');

  const ledger = createLedger();

  if (live) {
    let data = null;
    try {
      data = await fetchPage(0);
    } catch (err) {
      logInfo(`predict: rebamovie fetch failed (${err.message}) — storage sources only`);
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
          item.interpreter?.title || ''
        );
      });
    }
  }

  if (!rebaOnly) {
    for (const [name, tag] of [
      ['agasobanuyelive.json', 'aglive'],
      ['oshakurfilms.json', 'oshakur'],
      ['agasobanuyenow.json', 'agnow'],
    ]) {
      const items = await loadStorageJson(name);
      for (const [idx, item] of items.slice(0, RANKS).entries()) {
        const title = String(item?.title || '').trim();
        if (!title) continue;
        const type = item?.type || (/\bs0\d+\b/i.test(title) ? 'tv' : 'movie');
        ledger.add(title, type, idx, `${tag}#${idx}`);
      }
    }
  }

  const rows = await loadRows();
  const byLink = new Map(rows.map((r) => [r.link, r]));
  const { boosts, unresolved } = applyLedger(ledger, buildIndex(rows), {
    scoreRow: (row) => scoreWith(row, 0),
  });
  const signals = (await loadHotSnapshot()).signals || {};

  const shown = boosts.filter((b) => !filter || b.show.includes(filter));
  const shownUnattached = unresolved.filter((u) => !filter || u.show.includes(filter));

  if (!shown.length && !shownUnattached.length) {
    logInfo(filter ? `predict: no ranked work matches "${filter}"` : 'predict: nothing is currently ranked');
    return { matched: 0, unattached: 0 };
  }

  const head = `  ${'WORK'.padEnd(38)} ${'HOT'.padStart(5)} ${'STORED'.padStart(7)} ${'PREDICTED'.padStart(10)}  ${'SOURCES'}`;
  logInfo('');
  logInfo(
    `  hot-score prediction — ${ledger.size} ranked work(s)` +
      `${filter ? ` matching "${filter}"` : ''}, ${rows.length} catalogue row(s) scanned`
  );
  logInfo(head);
  logInfo('  ' + '-'.repeat(head.length - 2));

  let atCap = 0;
  let drift = 0;
  for (const boost of shown.sort((a, b) => b.hot - a.hot)) {
    const row = byLink.get(boost.link);
    if (!row) continue;
    const stored = Number(row.score) || 0;
    const pred = scoreWith(row, boost.hot);
    const prevHot = signals[row.link] ? Number(signals[row.link].hot) || 0 : 0;
    const flags = [];
    if (boost.hot >= HOT_CAP - 0.5) flags.push('AT-CAP');
    if (Math.abs(prevHot - boost.hot) > 0.05) flags.push('VOLATILE');
    if (Math.abs(pred - stored) > 0.5) flags.push('DRIFT');
    if (!row.tmdb_rating && !row.popularity) flags.push('NO-TMDB');
    if (boost.rows > 1) flags.push(`${boost.rows}-SEASONS`);
    if (flags.includes('AT-CAP')) atCap++;
    if (flags.includes('DRIFT')) drift++;

    logInfo(
      `  ${row.title.slice(0, 38).padEnd(38)} ${boost.hot.toFixed(1).padStart(5)} ` +
        `${stored.toFixed(2).padStart(7)} ${pred.toFixed(2).padStart(10)}  ` +
        `${verbose ? boost.sources.join(' ') : boost.sources.slice(0, 3).join(' ')}${flags.length ? '  ' + flags.join(',') : ''}`
    );
  }

  for (const u of shownUnattached.sort((a, b) => b.hot - a.hot)) {
    logInfo(
      `  ${u.show.slice(0, 38).padEnd(38)} ${u.hot.toFixed(1).padStart(5)} ` +
        `${'--'.padStart(7)} ${'--'.padStart(10)}  ${verbose ? u.sources.join(' ') : u.sources.slice(0, 3).join(' ')}  UNATTACHED`
    );
  }

  logInfo('');
  logInfo(`  matched: ${shown.length}   unattached: ${shownUnattached.length}   at cap: ${atCap}   drifted: ${drift}`);
  return { matched: shown.length, unattached: shownUnattached.length };
}

if (require.main === module) {
  main()
    .then((r) => logInfo(`predict: done (${r.matched} matched, ${r.unattached} unattached).`))
    .catch((err) => {
      logInfo(`predict failed: ${err.message}`);
      process.exit(1);
    });
}

module.exports = { main };
