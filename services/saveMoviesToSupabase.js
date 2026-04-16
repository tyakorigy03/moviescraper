const supabase = require('./supabaseClient');
const { computeRelevanceScore } = require('../utils/relevanceScore');
const { loadScraperState, saveScraperState } = require('../utils/stateManager');
const { logInfo, logError } = require('../utils/logger');

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

  const batchSize = 200;
  let count = 0;

  for (let i = 0; i < filteredMovies.length; i += batchSize) {
    const chunk = filteredMovies.slice(i, i + batchSize);
    const uniqueChunk = deduplicateByLink(chunk);

    const toInsert = uniqueChunk.map((movie) => ({
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

    if (error) {
      logError(`Failed inserting batch ${i / batchSize + 1}: ${error.message}`);
    } else {
      count += data.length;
      logInfo(`Saved ${count} movies so far...`);
      await markMoviesAsSavedInState(uniqueChunk.map((movie) => movie.link));
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
