/**
 * AgasobanuyeNow enrich+insert scraper.
 *
 * Rewrites/enriches moviesv2 rows with agasobanuyenow's own-CDN MP4 download
 * links (better than the old mediafire/anonsharing ones), merges episodes and
 * "Part A/Part B" splits into existing rows without duplicates, and can insert
 * brand-new titles that the other sites don't have.
 *
 * Usage:
 *   node scrapers/agasobanuyenow/index.js             # delta (GH Actions default)
 *   node scrapers/agasobanuyenow/index.js --full      # ignore state, re-scrape everything
 *   node scrapers/agasobanuyenow/index.js --limit 40 --pages 2
 *   node scrapers/agasobanuyenow/index.js --type movie
 *   node scrapers/agasobanuyenow/index.js --slug the-mongoose-by-gaheza --deep
 *   node scrapers/agasobanuyenow/index.js --no-insert  # only enrich, never insert
 */
const supabase = require('../../services/supabaseClient');
const { logInfo, logError } = require('../../utils/logger');
const { fetchCatalog } = require('./catalog');
const { loadState, saveState } = require('./state');
const { fetchHtml, resolveMovie, resolveEpisode, BASE } = require('./resolver');
const {
  matchEntity,
  makeMovieEntry,
  makeEpisodeEntry,
  mergeEntries,
  normType,
} = require('./mergeEntries');
const { coreTitle } = require('./keys');
const { enrichWithTMDB } = require('../../services/enrichWithTmdb');
const { computeRelevanceScore } = require('../../utils/relevanceScore');

const TABLE = 'moviesv2';
const epKey = (s, e) => `s${s}e${e}`;

function parseArgs(argv) {
  const args = {
    full: false,
    watch: String(process.env.AGNOW_WATCH || '').toLowerCase() !== 'false',
    insertNew: true,
    deep: false,
    limit: 0,
    pages: undefined,
    type: '',
    slug: null,
    watchPages: parseInt(process.env.AGNOW_WATCH_PAGES, 10) || 3,
    fullScanHours: parseInt(process.env.AGNOW_FULL_SCAN_HOURS, 10) || 24,
    delayMs: parseInt(process.env.AGNOW_DELAY_MS, 10) || 350,
  };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--full') args.full = true;
    else if (argv[i] === '--watch') args.watch = true;
    else if (argv[i] === '--no-watch') args.watch = false;
    else if (argv[i] === '--no-insert') args.insertNew = false;
    else if (argv[i] === '--deep') args.deep = true;
    else if (argv[i] === '--limit') args.limit = parseInt(argv[++i], 10) || 0;
    else if (argv[i] === '--pages') args.pages = parseInt(argv[++i], 10);
    else if (argv[i] === '--type') args.type = argv[++i] || '';
    else if (argv[i] === '--slug') args.slug = argv[++i] || null;
    else if (argv[i] === '--delay-ms') args.delayMs = parseInt(argv[++i], 10) || 350;
  }
  if (process.env.AGNOW_INSERT_NEW && String(process.env.AGNOW_INSERT_NEW).toLowerCase() === 'false') {
    args.insertNew = false;
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
      .select('id,title,type,narrator,release_year,year,link,Downloadurls,hosted,tmdb_id,genres,image,poster')
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`rows: ${error.message}`);
    all.push(...(data || []));
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

function pageUrl(item) {
  return item.type === 'movie'
    ? `${BASE}/movies/${item.slug}`
    : `${BASE}/tv/${item.slug}`;
}

function parseEpisodeGuide(html, slug) {
  const esc = slug.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`/watch/tv/${esc}/(\\d+)/(\\d+)`, 'gi');
  const seen = new Map();
  let m;
  while ((m = re.exec(html)) !== null) {
    seen.set(epKey(+m[1], +m[2]), { s: +m[1], e: +m[2] });
  }
  return [...seen.values()].sort((a, b) => a.s - b.s || a.e - b.e);
}

/** Enrich + score + shape a brand-new row for upsert. */
async function buildInsertRow(item, entries) {
  const type = normType(item.type);
  const enrich = await enrichWithTMDB({
    title: item.title,
    publishedAt: item.year ? `${item.year}-01-01` : undefined,
    type,
  });
  const year = parseInt(item.year, 10) || null;
  const base = {
    link: pageUrl(item),
    title: String(item.title || '').trim(),
    type,
    narrator: item.interpreter || '',
    release_year: year,
    genres: item.genre
      ? String(item.genre).split(',').map((g) => g.trim()).filter(Boolean)
      : null,
    image: item.image || null,
    poster: item.image || null,
    publishedAt: null,
    modifiedAt: new Date().toISOString(),
    Downloadurls: entries,
  };
  return {
    ...base,
    ...enrich,
    score: computeRelevanceScore({
      tmdb_rating: enrich.tmdb_rating || 0,
      popularity: enrich.popularity || 0,
      publishedAt: '',
      modifiedAt: base.modifiedAt,
      narrator: base.narrator || '',
      title: base.title || '',
    }),
  };
}

async function upsertNewRow(rowObj, rows, insertedThisRun) {
  const { error } = await supabase.from(TABLE).upsert(rowObj, {
    onConflict: 'link',
    ignoreDuplicates: true,
  });
  if (error) {
    logError(`insert failed for ${rowObj.title}: ${error.message}`);
    return null;
  }
  const key = `${coreTitle(rowObj.title)}|${rowObj.type}`;
  insertedThisRun.set(key, { ...rowObj });
  rows.push({ ...rowObj }); // keep matcher-aware for subsequent items in this run
  logInfo(`++ inserted new: ${rowObj.title} (${rowObj.type}) — ${rowObj.Downloadurls.length} link(s)`);
  return rowObj;
}

async function applyRowUpdate(row, updatedEntries, item) {
  const patch = {
    Downloadurls: updatedEntries,
    modifiedAt: new Date().toISOString(),
  };
  if (!row.narrator && item.interpreter) patch.narrator = item.interpreter;
  if ((!row.genres || !row.genres.length) && item.genre) {
    patch.genres = String(item.genre).split(',').map((g) => g.trim()).filter(Boolean);
  }
  if (!row.image && item.image) patch.image = item.image;
  if (!row.poster && item.image) patch.poster = item.image;

  const { error } = await supabase.from(TABLE).update(patch).eq('id', row.id);
  if (error) {
    logError(`update failed for ${row.title}: ${error.message}`);
    return;
  }
  // keep in-memory copy fresh for later items in the same run
  row.Downloadurls = updatedEntries;
  if (patch.narrator) row.narrator = patch.narrator;
  if (patch.genres) row.genres = patch.genres;
  if (patch.image) row.image = patch.image;
  if (patch.poster) row.poster = patch.poster;
  logInfo(`~ enriched ${row.title}: ${updatedEntries.length} download entries`);
}

async function processMovie(item, row, rows, insertedThisRun, state, args) {
  const slug = item.slug;
  const cached = state.movies[slug];
  const cacheUrl = () => ({ downloadUrl: cached?.dl || '', watchUrl: `${BASE}/movies/${slug}` });

  if (!args.full && cached) {
    // Already visited before: if a row exists nothing new to add; otherwise
    // (insert mode) rebuild the row from the cached link.
    if (row) return;
    if (!args.insertNew) return;
    const spec = makeMovieEntry(item, cacheUrl());
    const rowObj = await buildInsertRow(item, [spec]);
    await upsertNewRow(rowObj, rows, insertedThisRun);
    return;
  }

  const resolved = await resolveMovie({ slug, title: item.title, narrator: item.interpreter, deep: args.deep });
  state.movies[slug] = { ok: resolved.ok, dl: resolved.downloadUrl, at: new Date().toISOString() };
  await saveState(state);

  if (!resolved.ok && !args.deep) {
    // Pattern-drift signal — leave a trace for a deeper manual pass.
    logInfo(`? no direct link for ${item.title} — watch-only (re-run with --deep)`);
  }

  if (row) {
    const spec = makeMovieEntry(item, resolved);
    const { entries, changed } = mergeEntries(row.Downloadurls, [spec]);
    if (changed) await applyRowUpdate(row, entries, item);
  } else if (args.insertNew) {
    const spec = makeMovieEntry(item, resolved);
    const rowObj = await buildInsertRow(item, [spec]);
    await upsertNewRow(rowObj, rows, insertedThisRun);
  }
}

async function processSeries(item, row, rows, insertedThisRun, state, args) {
  const slug = item.slug;
  const sInfo = state.series[slug];
  const badgeChanged = !sInfo || sInfo.badge !== item.latest_episode_badge;

  if (!args.full && sInfo && !badgeChanged) {
    // Nothing new (no new episodes since last run): if a row exists, done.
    if (row) return;
    if (!args.insertNew) return;
    // Rebuild the row from cached episodes (e.g. after a merge wiped it).
    const specs = Object.entries(sInfo.episodes || {}).map(([k, v]) => {
      const [s, e] = k.replace('s', '').split('e').map(Number);
      return { title: 's' + k, downloadUrl: v.dl || '', watchUrl: `${BASE}/watch/tv/${slug}/${s}/${e}` };
    });
    if (specs.length) {
      const entries = mergeEntries([], specs, { singleSeason: false }).entries;
      const rowObj = await buildInsertRow(item, entries);
      await upsertNewRow(rowObj, rows, insertedThisRun);
    }
    return;
  }

  // Walk the series guide and resolve episode CDN links.
  const episodes = state.series[slug]?.episodes || {};
  const showTitle = item.title;
  const narrator = item.interpreter;
  let singleSeason = false;
  let guide;
  try {
    const html = await fetchHtml(`${BASE}/tv/${slug}`);
    guide = parseEpisodeGuide(html, slug);
    singleSeason = new Set(guide.map((g) => g.s)).size === 1;
  } catch (err) {
    logError(`series guide failed for ${item.title}: ${err.message}`);
    guide = [];
  }

  const specs = [];
  for (const { s, e } of guide) {
    const k = epKey(s, e);
    const cachedEp = episodes[k];
    if (!args.full && cachedEp) {
      // Keep using the archived resolution (dl may be '' for watch-only).
      const dl = cachedEp.dl || '';
      specs.push(makeEpisodeEntry(showTitle, s, e, { downloadUrl: dl, watchUrl: `${BASE}/watch/tv/${slug}/${s}/${e}` }));
      continue;
    }
    const resolved = await resolveEpisode({
      slug, s, e, showTitle, narrator, deep: args.deep,
    });
    episodes[k] = { dl: resolved.downloadUrl, at: new Date().toISOString() };
    specs.push(makeEpisodeEntry(showTitle, s, e, resolved));
    await new Promise((r) => setTimeout(r, args.delayMs));
  }

  state.series[slug] = {
    badge: item.latest_episode_badge || sInfo?.badge || null,
    lastEpisodeAddedAt: item.last_episode_added_at || null,
    at: new Date().toISOString(),
    episodes,
  };
  await saveState(state);

  if (!specs.length) {
    logInfo(`- no episodes resolved for ${item.title}`);
    return;
  }

  if (row) {
    const { entries, changed } = mergeEntries(row.Downloadurls, specs, { singleSeason });
    if (changed) await applyRowUpdate(row, entries, item);
  } else if (args.insertNew) {
    const { entries } = mergeEntries([], specs, { singleSeason });
    const rowObj = await buildInsertRow(item, entries);
    await upsertNewRow(rowObj, rows, insertedThisRun);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const state = await loadState();
  const rows = await loadAllRows();
  const insertedThisRun = new Map();

  // Watch mode is the normal high-frequency pass: only the newest catalog
  // window is looked at, and anything we already handled is skipped without a
  // single page fetch. A full delta pass still runs periodically so older
  // titles and drifted URL patterns get re-checked.
  let maxPages = args.pages;
  let scan = 'delta';
  if (args.watch && !args.full && !args.pages && !args.slug) {
    const hoursSinceFull = state.lastFullScanAt
      ? (Date.now() - new Date(state.lastFullScanAt).getTime()) / 3600e3
      : Infinity;
    if (hoursSinceFull > args.fullScanHours) {
      scan = 'full';
      state.lastFullScanAt = new Date().toISOString();
    } else {
      scan = `watch (${args.watchPages} pages)`;
      maxPages = args.watchPages;
    }
  }

  logInfo(
    `agasobanuyenow scraper started — mode: ${args.full ? 'FULL' : scan}, ` +
      `insert-new: ${args.insertNew}, deep: ${args.deep}\n` +
      `existing rows loaded: ${rows.length}`
  );

  let items;
  if (args.slug) {
    const all = await fetchCatalog({ maxPages: args.pages || 60, delayMs: 0 });
    items = all.filter((i) => i.slug === args.slug);
    logInfo(`asked for ${args.slug} — found ${items.length} catalog item(s)`);
  } else {
    items = await fetchCatalog({ maxPages, limit: args.limit, delayMs: 0 });
  }

  let processed = 0;
  let skipped = 0;
  for (const item of items) {
    const type = normType(item.type);
    if (args.type && args.type !== type) continue;

    const key = `${coreTitle(item.title)}|${type}`;
    const row = matchEntity(item, rows) || insertedThisRun.get(key) || null;

    // Cheap short-circuit matching the site's own freshness signals so a
    // frequent run touches nothing that hasn't changed.
    if (args.watch && !args.full && row && !item.has_new_episode_24h) {
      if (type === 'movie' && state.movies[item.slug]?.ok) {
        skipped++;
        continue;
      }
      if (type === 'tv' && state.series[item.slug]?.badge === item.latest_episode_badge) {
        skipped++;
        continue;
      }
    }

    try {
      if (type === 'movie') {
        await processMovie(item, row, rows, insertedThisRun, state, args);
      } else if (type === 'tv') {
        await processSeries(item, row, rows, insertedThisRun, state, args);
      } else {
        continue;
      }
      processed++;
    } catch (err) {
      logError(`failed ${item.title} (${item.slug}): ${err.message}`);
    }

    await new Promise((r) => setTimeout(r, args.delayMs));
  }

  await saveState(state);
  logInfo(
    `agasobanuyenow scraper finished — processed ${processed}, skipped ${skipped} (unchanged), ` +
      `of ${items.length} catalog items.`
  );
}

if (require.main === module) {
  main().catch((err) => {
    logError(`fatal: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { main };