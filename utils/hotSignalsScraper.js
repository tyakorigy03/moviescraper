/**
 * Hot-signal scorer (npm run scrape5) — "hot agasobanuye" boost.
 *
 * Collects external popularity signals and folds them into moviesv2 scores as a
 * capped additive hot term (see utils/relevanceScore.js). Sources:
 *
 *   1. rebamovie.com /movies ranked lists — `trendingMovies`, `newMovies`,
 *      `mostPopularMovies` (each top-10, per catalog page). agasobanuyebox.com
 *      (the CineBeta/streamchat network) serves the exact same API, so reading
 *      rebamovie covers agasobanuyebox too.
 *   2. agasobanuyelive.json storage order (progressLink2 = discovery order,
 *      newest first → earlier index = hotter).
 *   3. oshakurfilms.json storage order (same shape).
 *   4. agasobanuyenow.json catalog order (array order = listing order).
 *
 * Matching: titles are normalized to an entity key (coreTitle + type +
 * normalized narrator) and matched against existing moviesv2 rows, mirroring
 * the conservative entity matching used by the merge scrapers. Signals are
 * aggregated per entity key (max/sum, capped at 10) and stored in
 * storage/hot-signals.json. Scores are recomputed ONLY for matched rows and
 * written only when the score actually changes (no modifiedAt churn).
 *
 * Usage:
 *   node utils/hotSignalsScraper.js          # build snapshot + apply scores
 *   node utils/hotSignalsScraper.js --dry    # build snapshot, don't touch DB
 *   node utils/hotSignalsScraper.js --limit 60
 */
require('dotenv').config();
const fs = require('fs-extra');
const path = require('path');
const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');
const { clampHot, saveHotSnapshot } = require('./hotSignals');
const { fetchPage } = require('../scrapers/rebamovie/api');
const { coreTitle } = require('../scrapers/agasobanuyenow/keys');
const { normType, normNarrator } = require('../scrapers/rebamovie/mergeEntries');

const TABLE = 'moviesv2';
const BATCH_SIZE = 200;
const RANKS = 25; // how deep into any ranked list a title still counts as hot
const STORAGE_DIR = path.join(__dirname, '..', 'storage');

/* ------------------------------ identity keys ------------------------------ */

/** "Agasobanuye X By Rocky" row titles can carry different narrator spellings. */
function entityKey(title, type, narrator) {
  const t = coreTitle(title);
  if (!t) return null;
  return `${t}|${normType(type) || 'movie'}|${normNarrator(narrator)}`;
}

/* ------------------------------ signal gathering --------------------------- */

/** rebamovie ranked lists -> [{ key, hot }]. */
async function rebaSignals(pages = 1) {
  const out = new Map(); // key -> hot
  for (let p = 0; p < pages; p++) {
    let data;
    try {
      data = await fetchPage(p);
    } catch (err) {
      logError(`hotsignals rebamovie page ${p} failed: ${err.message}`);
      break;
    }
    for (const listName of ['trendingMovies', 'newMovies', 'mostPopularMovies']) {
      const list = Array.isArray(data?.[listName]) ? data[listName] : [];
      list.slice(0, RANKS).forEach((item, idx) => {
        if (!item?.id || !item?.movieDataId?.title) return;
        const type = Array.isArray(item.movieDataId.typeFull) && item.movieDataId.typeFull.includes('Series') ? 'tv' : 'movie';
        const key = entityKey(item.movieDataId.title, type, item.interpreter?.title || '');
        if (!key) return;
        const pts = Math.max(0, 10 - idx * 0.9);
        out.set(key, (out.get(key) || 0) + pts);
      });
    }
  }
  return out;
}

/** Storage-order signals: earlier item in the list => higher hot. */
function orderSignals(items, { max = RANKS, skipMissingTitle = true } = {}) {
  const out = new Map();
  (items || []).slice(0, max).forEach((item, idx) => {
    const title = String(item?.title || '').trim();
    if (skipMissingTitle && !title) return;
    const type = item?.type || (/\bs0\d+\b/i.test(title) ? 'tv' : 'movie');
    const narrator = Array.isArray(item?.genres)
      ? item.genres.filter((g) => g && /\b(?:by|kwa)\s+/i.test(String(g))).join(', ')
      : '';
    const key = entityKey(title, type, narrator);
    if (!key) return;
    const pts = Math.max(0, 9 - idx * 0.45);
    out.set(key, (out.get(key) || 0) + pts);
  });
  return out;
}

async function loadStorageJson(name) {
  const file = path.join(STORAGE_DIR, name);
  try {
    if (!(await fs.pathExists(file))) return [];
    const data = await fs.readJson(file);
    // oshakur/agasobanuyelive store { progressLink2: [...] }.
    if (Array.isArray(data.progressLink2)) return data.progressLink2;
    if (Array.isArray(data)) return data;
    return [];
  } catch (err) {
    logError(`hotsignals failed reading storage/${name}: ${err.message}`);
    return [];
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

/* ------------------------------ main --------------------------------------- */

async function main() {
  const args = process.argv.slice(2);
  const dry = args.includes('--dry');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) || 0 : 0;

  logInfo('hotsignals: collecting ranked lists from rebamovie (+ agasobanuyebox)...');
  const reba = await rebaSignals(1);

  logInfo('hotsignals: reading storage-order signals (agasobanuyelive, oshakurfilms, agasobanuyenow)...');
  const [aglive, oshakur, agnow] = await Promise.all([
    loadStorageJson('agasobanuyelive.json'),
    loadStorageJson('oshakurfilms.json'),
    loadStorageJson('agasobanuyenow.json'),
  ]);
  const order = new Map([...orderSignals(aglive), ...orderSignals(oshakur), ...orderSignals(agnow)]);

  const byKey = new Map();
  for (const [key, hot] of reba) byKey.set(key, (byKey.get(key) || 0) + hot);
  for (const [key, hot] of order) byKey.set(key, (byKey.get(key) || 0) + hot);

  logInfo(`hotsignals: ${byKey.size} hot entity keys (${reba.size} from rebamovie/agasobanuyebox, ${order.size} from storage order).`);

  let rows = await loadRows();
  if (limit > 0) rows = rows.slice(0, limit);
  logInfo(`hotsignals: evaluating ${rows.length} existing moviesv2 rows.`);

  const signals = {}; // link -> { hot, sources[] }
  const changes = [];

  for (const row of rows) {
    const key = entityKey(row.title, row.type, row.narrator);
    if (!key) continue;
    const hot = clampHot(byKey.get(key));
    if (hot <= 0) continue;

    const oldScore = Number(row.score) || 0;
    const newScore = computeRelevanceScore({
      tmdb_rating: row.tmdb_rating || 0,
      popularity: row.popularity || 0,
      publishedAt: row.publishedAt || '',
      modifiedAt: row.modifiedAt || '',
      narrator: row.narrator || '',
      title: row.title || '',
      hot,
    });

    signals[row.link] = { hot: Math.round(hot * 10) / 10, sources: ['recomputed'] };
    if (Math.abs(newScore - oldScore) > 0.05) changes.push({ link: row.link, score: newScore });
  }

  const snapshot = await saveHotSnapshot(signals);
  logInfo(`hotsignals: snapshot updated — ${Object.keys(snapshot.signals).length} link(s) boosted.`);

  if (dry) {
    logInfo(`hotsignals: --dry, skipping DB writes (${changes.length} score change(s) would be applied).`);
    return { boosted: Object.keys(signals).length, changes: changes.length };
  }

  for (let i = 0; i < changes.length; i += BATCH_SIZE) {
    const { error } = await supabase.from(TABLE).upsert(changes.slice(i, i + BATCH_SIZE), { onConflict: 'link' });
    if (error) logError(`hotsignals upsert batch failed: ${error.message}`);
  }
  logInfo(`hotsignals: applied hot boost to ${changes.length} row(s).`);
  return { boosted: Object.keys(signals).length, changes: changes.length };
}

if (require.main === module) {
  main()
    .then((res) => logInfo(`hotsignals finished — boosted ${res?.boosted} row(s), ${res?.changes} score change(s).`))
    .catch((err) => {
      logError(`hotsignals fatal: ${err.message}`);
      process.exit(1);
    });
}

module.exports = { main };