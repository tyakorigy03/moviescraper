const supabase = require('./supabaseClient');
const { computeRelevanceScore } = require('../utils/relevanceScore');
const { loadScraperState, saveScraperState } = require('../utils/stateManager');
const { logInfo, logError } = require('../utils/logger');
const { collapsePartRows, mergeEntryLists } = require('./dedupeAgLiveParts');

const SITE_KEY = 'agasobanuyelive';
const BAD_HOST = 'anonsharing.com';

function deduplicateByLink(movies) {
  const map = new Map();
  for (const movie of movies) {
    if (movie.link) {
      map.set(movie.link, movie);
    }
  }
  return Array.from(map.values());
}

function normalizeLink(url = '') {
  return url.replace(/^https?:\/\/(www\.)?/, 'https://');
}

function normalizeTimestamp(value) {
  return value || null;
}

function isTruthyEnv(value) {
  return String(value || '').toLowerCase() === 'true' || String(value || '').toLowerCase() === '1';
}

/**
 * Shield: rows we self-host (hosted=true) must never be overwritten by the
 * scraper — even in a non-insert-only run. Returns the set of links that are
 * self-hosted. Best-effort: if the `hosted` column is missing (migration not
 * run yet) it returns empty and the shield stays off.
 */
async function fetchHostedLinks(links) {
  if (!links.length) return new Set();
  const { data, error } = await supabase.from('moviesv2').select('link,hosted').in('link', links);
  if (error) return new Set();
  return new Set((data || []).filter((r) => r.hosted === true).map((r) => r.link));
}

async function saveMoviesToSupabase(moviesInput, options = {}) {
  const insertOnly = Boolean(options.insertOnly) || isTruthyEnv(process.env.SUPABASE_INSERT_ONLY);
  const filteredMovies = moviesInput.filter(
    (item) =>
      !item?.Downloadurls?.some(
        (dl) => dl?.watchUrl?.includes(BAD_HOST) || dl?.downloadUrl?.includes(BAD_HOST)
      )
  );

  const skippedMovies = moviesInput.filter((item) =>
    item?.Downloadurls?.some(
      (dl) => dl?.watchUrl?.includes(BAD_HOST) || dl?.downloadUrl?.includes(BAD_HOST)
    )
  );

  if (skippedMovies.length > 0) {
    await markMoviesAsIgnoredInState(
      skippedMovies.map((movie) => movie.link),
      `Skipped because download/watch URL matched blocked host: ${BAD_HOST}`
    );
    logInfo(`Skipped ${skippedMovies.length} movies because they point to ${BAD_HOST}.`);
  }

  // Collapse agasobanuyelive part pages into a single row per show+season
  // (Pt.1/Pt.2/Pt.3 are sequential episode ranges of the SAME title — merge
  // their Downloadurls instead of creating "duplicate" rows).
  const collapsed = collapsePartRows(filteredMovies);
  if (collapsed.merged > 0) {
    logInfo(`Merged ${collapsed.merged} agasobanuyelive part row(s) into their show's consolidated row.`);
  }

  const batchSize = 200;
  let count = 0;

  for (let i = 0; i < collapsed.movies.length; i += batchSize) {
    const chunk = collapsed.movies.slice(i, i + batchSize);
    const uniqueChunk = deduplicateByLink(chunk);

    // Cross-run part merge: when a collapsed record points at a link that
    // already exists in the DB (an earlier part saved during a previous run),
    // `insertOnly` would silently skip it and lose the new part's episodes.
    // Re-pull the stored row and union its entries into the incoming record.
    const collapsedRecords = uniqueChunk.filter((m) => Array.isArray(m._sourceLinks) && m._sourceLinks.length > 1);
    const standalone = uniqueChunk.filter((m) => !(Array.isArray(m._sourceLinks) && m._sourceLinks.length > 1));
    if (collapsedRecords.length) {
      const { data: existingRows, error: existErr } = await supabase
        .from('moviesv2')
        .select('link,Downloadurls')
        .in('link', collapsedRecords.map((m) => normalizeLink(m.link) || ''));
      if (existErr) {
        logError(`Failed reading existing rows for part merge: ${existErr.message}`);
      } else {
        const existingByLink = new Map((existingRows || []).map((r) => [normalizeLink(r.link), r]));
        for (const movie of collapsedRecords) {
          const existing = existingByLink.get(normalizeLink(movie.link) || '');
          if (existing && Array.isArray(existing.Downloadurls) && existing.Downloadurls.length) {
            movie.Downloadurls = mergeEntryLists([existing.Downloadurls, movie.Downloadurls]);
            movie._existing = existing;
          }
        }
      }
    }

    // Never overwrite self-hosted rows, whatever the insertOnly mode is.
    const hostedLinks = await fetchHostedLinks(uniqueChunk.map((m) => normalizeLink(m.link) || ''));
    const safeStandalone = standalone.filter(
      (m) => !hostedLinks.has(normalizeLink(m.link) || '')
    );
    const shieldedCount = uniqueChunk.length - safeStandalone.length - collapsedRecords.length;
    if (shieldedCount > 0) {
      logInfo(`Protected ${shieldedCount} self-hosted movie(s) from being overwritten.`);
    }

    const toInsert = safeStandalone.map((movie) => ({
      ...movie,
      link: normalizeLink(movie.link) || '',
      publishedAt: normalizeTimestamp(movie.publishedAt),
      modifiedAt: normalizeTimestamp(movie.modifiedAt),
      release_date: normalizeTimestamp(movie.release_date),
      score: computeRelevanceScore({
        tmdb_rating: movie.tmdb_rating || 0,
        popularity: movie.popularity || 0,
        publishedAt: movie.publishedAt || '',
        modifiedAt: movie.modifiedAt || '',
        narrator: movie.narrator || '',
        title: movie.title || ''
      })
    }));

    const { error, data } = await supabase
      .from('moviesv2')
      .upsert(toInsert, { onConflict: 'link', ignoreDuplicates: insertOnly })
      .select();

    let collapsedSaved = 0;
    for (const movie of collapsedRecords) {
      if (hostedLinks.has(normalizeLink(movie.link) || '')) continue;
      const patch = {
        ...movie,
        link: normalizeLink(movie.link) || '',
        publishedAt: normalizeTimestamp(movie.publishedAt),
        modifiedAt: new Date().toISOString(),
        release_date: normalizeTimestamp(movie.release_date),
        score: computeRelevanceScore({
          tmdb_rating: movie.tmdb_rating || 0,
          popularity: movie.popularity || 0,
          publishedAt: movie.publishedAt || '',
          modifiedAt: movie.modifiedAt || '',
          narrator: movie.narrator || '',
          title: movie.title || ''
        })
      };
      delete patch._sourceLinks;
      delete patch._existing;
      const res = movie._existing
        ? await supabase.from('moviesv2').update(patch).eq('link', patch.link)
        : await supabase.from('moviesv2').insert(patch);
      if (!res.error) collapsedSaved++;
      else logError(`Failed merged part row ${patch.link}: ${res.error.message}`);
    }

    if (error) {
      logError(`Failed inserting batch ${i / batchSize + 1}: ${error.message}`);
    } else {
      count += data.length;
      count += collapsedSaved;
      logInfo(`Saved ${count} movies so far...`);
      await markMoviesAsSavedInState(
        chunk.flatMap((movie) => movie._sourceLinks || [movie.link])
      );
    }
  }

  logInfo(`Finished saving ${count} movies to Supabase.`);
}

async function markMoviesAsSavedInState(movieLinks) {
  const state = await loadScraperState(SITE_KEY);
  const progressLink2 = state.progressLink2 || [];
  let updated = false;

  for (const movie of progressLink2) {
    if (movieLinks.includes(movie.link) && movie.saved === false) {
      movie.saved = true;
      updated = true;
    }
  }

  if (updated) {
    await saveScraperState(SITE_KEY, { ...state, progressLink2 });
    logInfo(`Updated saved status in local state for ${movieLinks.length} movies.`);
  }
}

async function markMoviesAsIgnoredInState(movieLinks, reason) {
  const state = await loadScraperState(SITE_KEY);
  const progressLink2 = state.progressLink2 || [];
  let updated = false;

  for (const movie of progressLink2) {
    if (movieLinks.includes(movie.link) && !movie.saved) {
      movie.ignored = true;
      movie.ignoreReason = reason;
      updated = true;
    }
  }

  if (updated) {
    await saveScraperState(SITE_KEY, { ...state, progressLink2 });
    logInfo(`Updated ignored status in local state for ${movieLinks.length} movies.`);
  }
}

module.exports = { saveMoviesToSupabase };
