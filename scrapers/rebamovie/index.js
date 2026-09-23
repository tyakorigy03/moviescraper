/**
 * rebamovie (Kinyarwanda) enrich+insert scraper.
 *
 * Pure JSON API — no browser. Merges rebamovie's fast Wix-CDN links into
 * existing moviesv2 rows (matched by normalized title + type + narrator, so a
 * different dub stays a separate row) and inserts brand-new titles the other
 * sites don't have.
 *
 * Link freshness: Wix-signed video URLs expire after ~24h, so delta runs keep
 * stored links alive by re-resolving items older than REBA_REFRESH_HOURS while
 * skipping everything still fresh (no API calls, no writes for unchanged rows).
 *
 * Usage:
 *   node scrapers/rebamovie/index.js              # delta (GH Actions default)
 *   node scrapers/rebamovie/index.js --full       # force re-resolve everything
 *   node scrapers/rebamovie/index.js --limit 60
 *   node scrapers/rebamovie/index.js --no-downloads   # watch-only (DASH), skip MP4 resolution
 *   node scrapers/rebamovie/index.js --no-insert      # only enrich existing rows
 */
const supabase = require('../../services/supabaseClient');
const { logInfo, logError } = require('../../utils/logger');
const { fetchCatalog } = require('./catalog');
const { fetchCinemaData, fetchDownloadLink, SITE_BASE } = require('./api');
const { loadState, saveState } = require('./state');
const { matchEntity, makeMovieEntry, makeEpisodeEntry, mergeEntries, normType } = require('./mergeEntries');
const { coreTitle } = require('../agasobanuyenow/keys');
const { enrichWithTMDB } = require('../../services/enrichWithTmdb');
const { computeRelevanceScore } = require('../../utils/relevanceScore');

const TABLE = 'moviesv2';
const BATCH_SIZE = parseInt(process.env.REBA_BATCH_SIZE, 10) || 50;
const MAX_EPISODES = parseInt(process.env.REBA_MAX_EPISODES, 10) || 200;

const epKey = (s, e) => `s${s}e${e}`;

function chunkArr(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function makeCollector() {
  return { newRows: [], updates: [] };
}

async function flushWrites(collector) {
  if (!collector.newRows.length && !collector.updates.length) return;
  const batches = [];
  if (collector.newRows.length) {
    batches.push(...chunkArr(collector.newRows, BATCH_SIZE).map((rows) => ({ kind: 'insert', rows })));
  }
  if (collector.updates.length) {
    batches.push(...chunkArr(collector.updates, BATCH_SIZE).map((rows) => ({ kind: 'update', rows })));
  }

  let ok = 0;
  let failed = 0;
  for (const batch of batches) {
    const opts =
      batch.kind === 'insert'
        ? { onConflict: 'link', ignoreDuplicates: true }
        : { onConflict: 'link' };
    const { error } = await supabase.from(TABLE).upsert(batch.rows, opts);
    if (error) {
      failed += batch.rows.length;
      logError(`${batch.kind} batch (${batch.rows.length}) failed: ${error.message}`);
      continue;
    }
    ok += batch.rows.length;
  }
  collector.newRows = [];
  collector.updates = [];
  logInfo(failed ? `flushed ${ok} write(s) to Supabase (${failed} failed)` : `flushed ${ok} write(s) to Supabase`);
}

function parseArgs(argv) {
  const args = {
    full: false,
    insertNew: true,
    downloads: String(process.env.REBA_DOWNLOADS || '').toLowerCase() !== 'false',
    limit: 0,
    pages: undefined,
    delayMs: parseInt(process.env.REBA_DELAY_MS, 10) || 120,
    refreshHours: parseInt(process.env.REBA_REFRESH_HOURS, 10) || 6,
  };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--full') args.full = true;
    else if (argv[i] === '--no-insert') args.insertNew = false;
    else if (argv[i] === '--no-downloads') args.downloads = false;
    else if (argv[i] === '--limit') args.limit = parseInt(argv[++i], 10) || 0;
    else if (argv[i] === '--pages') args.pages = parseInt(argv[++i], 10);
    else if (argv[i] === '--delay-ms') args.delayMs = parseInt(argv[++i], 10) || 120;
    else if (argv[i] === '--refresh-hours') args.refreshHours = parseInt(argv[++i], 10) || 6;
  }
  return args;
}

async function loadAllRows() {
  const all = [];
  const pageSize = 1000;
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(TABLE)
      .select('id,title,type,narrator,release_year,link,Downloadurls,hosted,tmdb_id,genres,image,poster')
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`rows: ${error.message}`);
    all.push(...(data || []));
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

/** "24/02/2026" → "2026-02-24" (ISO) for publishedAt. */
function fullDateToIso(value) {
  if (!value) return '';
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(String(value).trim());
  if (!m) return '';
  const [_, day, month, year] = m;
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`.trim();
}

function pageUrl(item) {
  return `${SITE_BASE}/movie/${item.id}`;
}

function normNarratorName(item) {
  return item.interpreter?.title || '';
}

/** Flatten cinemaData episodes (nested per season) into {s,e,video,server}. */
function extractEpisodes(cinema) {
  const perSeason = Array.isArray(cinema?.data?.episodes) ? cinema.data.episodes : [];
  const out = [];
  for (let si = 0; si < perSeason.length; si++) {
    const list = Array.isArray(perSeason[si]) ? perSeason[si] : [];
    for (let ei = 0; ei < list.length; ei++) {
      const ep = list[ei];
      if (!ep) continue;
      const s = (ep.position?.seasonIndex ?? si) + 1;
      const e = ep.episode || (ep.position?.episodeIndex ?? ei) + 1;
      const video = ep.video?.hdVideo || ep.video?.midVideo || ep.video?.lowVideo || '';
      if (!video) continue;
      out.push({
        s,
        e,
        video,
        server: ep.server || '2',
        title: ep.title || '',
        movieId: ep.episodeId || ep.movieId || '',
      });
    }
  }
  // Sort by season+episode so entries are deterministic.
  return out.sort((a, b) => a.s - b.s || a.e - b.e);
}

/** Build the rebamovie entry list for one catalog item. */
async function buildSpecs(item, isSeason, cinema, args) {
  const showTitle = String(item.movieDataId?.title || '').trim();
  const episodes = extractEpisodes(cinema).slice(0, MAX_EPISODES);

  if (!episodes.length) return { specs: [], singleSeason: false };

  const singleSeason = new Set(episodes.map((e) => e.s)).size === 1;

  const specs = [];
  if (isSeason) {
    for (const ep of episodes) {
      const name = `S${String(ep.s).padStart(2, '0')}E${String(ep.e).padStart(2, '0')}`;
      const downloadUrl = args.downloads
        ? await fetchDownloadLink({ url: ep.video, server: ep.server, name: `${name} - ${showTitle}`, time: '' })
        : '';
      specs.push(makeEpisodeEntry(showTitle, ep.s, ep.e, { watchUrl: ep.video, downloadUrl }));
      await new Promise((r) => setTimeout(r, args.delayMs));
    }
  } else {
    // Movie: single-part entry titled with the film name.
    const ep = episodes[0];
    const downloadUrl = args.downloads
      ? await fetchDownloadLink({ url: ep.video, server: ep.server, name: showTitle, time: '' })
      : '';
    specs.push(makeMovieEntry(item, { watchUrl: ep.video, downloadUrl }));
  }

  return { specs, singleSeason };
}

/** Enrich + score + shape a brand-new row for upsert. */
async function buildInsertRow(item, entries, { publishedAt = '', narrator = '', type = 'movie' } = {}) {
  const title = String(item.movieDataId?.title || '').trim();
  const published = publishedAt || fullDateToIso(item.movieDataId?.fullReleaseDate) || null;
  const enrich = await enrichWithTMDB({ title, publishedAt: published, type });
  const year = parseInt(item.movieDataId?.rereaseDate, 10) || null;

  const base = {
    link: pageUrl(item),
    title,
    type,
    narrator,
    release_year: year,
    genres: Array.isArray(item.movieDataId?.genre) ? item.movieDataId.genre.filter(Boolean) : null,
    country: Array.isArray(item.movieDataId?.country)
      ? item.movieDataId.country.filter(Boolean).join(', ')
      : '',
    image: item.movieDataId?.hdimage || item.movieDataId?.image || null,
    poster: item.movieDataId?.hdimage || item.movieDataId?.image || null,
    publishedAt: published,
    modifiedAt: new Date().toISOString(),
    Downloadurls: entries,
  };
  const enrichRow = {
    ...enrich,
    tmdb_overview: enrich.tmdb_overview || item.movieDataId?.description || null,
  };
  return {
    ...base,
    ...enrichRow,
    score: computeRelevanceScore({
      tmdb_rating: enrich.tmdb_rating || 0,
      popularity: enrich.popularity || 0,
      publishedAt: published || '',
      modifiedAt: base.modifiedAt,
      narrator,
      title,
    }),
  };
}

function enqueueNewRow(collector, rowObj, rows, insertedThisRun) {
  const key = `${coreTitle(rowObj.title)}|${rowObj.type}|${String(rowObj.narrator || '').toLowerCase()}`;
  insertedThisRun.set(key, { ...rowObj });
  rows.push({ ...rowObj });
  collector.newRows.push(rowObj);
  logInfo(`++ new row queued: ${rowObj.title} (${rowObj.type}) - ${rowObj.Downloadurls.length} link(s)`);
}

function enqueueRowUpdate(collector, row, updatedEntries, item) {
  const patch = {
    link: row.link,
    Downloadurls: updatedEntries,
    modifiedAt: new Date().toISOString(),
  };
  const narrator = normNarratorName(item);
  if (!row.narrator && narrator) patch.narrator = narrator;
  if (!row.image && item.movieDataId?.hdimage) patch.image = item.movieDataId.hdimage;
  if (!row.poster && item.movieDataId?.hdimage) patch.poster = item.movieDataId.hdimage;

  collector.updates.push(patch);
  row.Downloadurls = updatedEntries;
  if (patch.narrator) row.narrator = patch.narrator;
  if (patch.image) row.image = patch.image;
  if (patch.poster) row.poster = patch.poster;
  logInfo(`~ enriched ${row.title}: ${updatedEntries.length} download entries`);
}

async function processItem(item, row, rows, insertedThisRun, collector, state, args) {
  const id = item.id;
  const badge = item.totalTime || '';
  const cached = state.movies[id];

  // Delta: skip a fresh resolution that already produced a row — no API call,
  // no write. If a fresh entry exists but the DB row is missing (e.g. state was
  // committed ahead of writes), rebuild it from the cached links.
  const fresh = cached && Date.now() - new Date(cached.at).getTime() < args.refreshHours * 3600e3;
  if (!args.full && fresh && cached.badge === badge) {
    if (row) return { skipped: true };
    if (args.insertNew && Array.isArray(cached.entries) && cached.entries.length) {
      const rowObj = await buildInsertRow(item, cached.entries, {
        publishedAt: fullDateToIso(item.movieDataId?.fullReleaseDate),
        narrator: normNarratorName(item),
        type: cached.type || 'movie',
      });
      enqueueNewRow(collector, rowObj, rows, insertedThisRun);
    }
    return { skipped: true };
  }

  let cinema;
  try {
    cinema = await fetchCinemaData(id);
  } catch (err) {
    logError(`cinemaData failed for ${item.movieDataId?.title} (${id}): ${err.message}`);
    return { skipped: false };
  }

  const isSeason = Boolean(cinema.isSeason);
  const type = isSeason ? 'tv' : 'movie';
  const narrator = normNarratorName(item);

  const { specs, singleSeason } = await buildSpecs(item, isSeason, cinema, args);

  if (!specs.length) {
    logInfo(`- no playable video for ${item.movieDataId?.title} (${id})`);
    return { skipped: false };
  }

  if (row) {
    const { entries, changed } = mergeEntries(row.Downloadurls, specs, { singleSeason });
    if (changed) enqueueRowUpdate(collector, row, entries, item);
  } else if (args.insertNew) {
    const { entries } = mergeEntries([], specs, { singleSeason });
    const rowObj = await buildInsertRow(item, entries, {
      publishedAt: fullDateToIso(item.movieDataId?.fullReleaseDate),
      narrator,
      type,
    });
    enqueueNewRow(collector, rowObj, rows, insertedThisRun);
  }

  state.movies[id] = { at: new Date().toISOString(), badge, type, singleSeason, entries: specs };
  await saveState(state);

  return { skipped: false };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const state = await loadState();
  const rows = await loadAllRows();
  const insertedThisRun = new Map();

  // Delta always pages the full catalog; per-item freshness (REBA_REFRESH_HOURS)
  // decides whether a cinemaData re-pull is needed to keep stored Wix links
  // under their ~24h expiry. --full forces a re-resolve of everything.
  logInfo(
    `rebamovie scraper started — mode: ${args.full ? 'FULL' : 'delta'}, downloads: ${args.downloads}, ` +
      `insert-new: ${args.insertNew}, refresh-hours: ${args.refreshHours}\n` +
      `existing rows loaded: ${rows.length}`
  );

  const items = (await fetchCatalog({ maxPages: args.pages || 0, limit: args.limit, delayMs: 0 })).map(
    (item) => ({
      ...item,
      title: String(item.movieDataId?.title || '').trim(),
    })
  );

  const collector = makeCollector();
  let processed = 0;
  let resolved = 0;
  let skipped = 0;

  for (const item of items) {
    const type = normType(isSeasonByItem(item) ? 'tv' : 'movie');
    const key = `${coreTitle(item.movieDataId?.title || '')}|${type}|${normNarratorName(item).toLowerCase()}`;
    const row = matchEntity({ ...item, type }, rows) || insertedThisRun.get(key) || null;

    try {
      const res = await processItem(item, row, rows, insertedThisRun, collector, state, args);
      if (res.skipped) skipped++;
      else processed++;
      if (!res.skipped) resolved++;
    } catch (err) {
      logError(`failed ${item.movieDataId?.title} (${item.id}): ${err.message}`);
    }

    if (collector.newRows.length + collector.updates.length >= BATCH_SIZE) {
      await flushWrites(collector);
    }

    await new Promise((r) => setTimeout(r, args.delayMs));
  }

  await flushWrites(collector);
  await saveState(state);
  logInfo(
    `rebamovie scraper finished — resolved ${resolved}, skipped ${skipped} (unchanged), ` +
      `of ${items.length} catalog items.`
  );
}

function isSeasonByItem(item) {
  const tfull = Array.isArray(item?.movieDataId?.typeFull) ? item.movieDataId.typeFull : [];
  if (tfull.includes('Series')) return true;
  return /^S\d/i.test(String(item?.totalTime || ''));
}

if (require.main === module) {
  main().catch((err) => {
    logError(`fatal: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { main };