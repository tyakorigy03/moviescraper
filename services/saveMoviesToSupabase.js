const supabase = require('./supabaseClient');
const { computeRelevanceScore } = require('../utils/relevanceScore');
const { loadScraperState, saveScraperState } = require('../utils/stateManager');
const { logInfo, logError } = require("../utils/logger");
const { link } = require('fs-extra');

const SITE_KEY = 'agasobanuyelive';

/**
 * Remove duplicates by `link` (keep last occurrence)
 * @param {Array<Object>} movies
 * @returns {Array<Object>} deduplicated array
 */
function deduplicateByLink(movies) {
  const map = new Map();
  for (const movie of movies) {
    if (movie.link) {
      map.set(movie.link, movie); // overwrites duplicates, keep last
    }
  }
  return Array.from(map.values());
}
/**
 * Save or update movie data in Supabase (PostgreSQL)
 * Uses UPSERT based on unique `link` or custom ID
 *
 * @param {Array<Object>} movies - List of movie objects to save
 */
const BAD_HOST = "anonsharing.com";
function normalizeLink(url) {
  return url.replace(/^https?:\/\/(www\.)?/, 'https://');
}
async function saveMoviesToSupabase(moviesinput) {
  const filteredMovies = moviesinput.filter((item) =>
    !item?.Downloadurls?.some(
      (dl) => dl?.watchUrl?.includes(BAD_HOST) || dl?.downloadUrl?.includes(BAD_HOST)
    ));
  let movies=filteredMovies;
  const batchSize = 200;
  let count = 0;
  for (let i = 0; i < movies.length; i += batchSize) {
    const chunk = movies.slice(i, i + batchSize);

    // Deduplicate chunk by link
    const uniqueChunk = deduplicateByLink(chunk);

    const toInsert = uniqueChunk.map(movie => {
      return {
        ...movie,
          link: normalizeLink(movie.link) || '',
          score: computeRelevanceScore({
          tmdb_rating: movie.tmdb_rating || 0,
          popularity: movie.popularity || 0,
          publishedAt: movie.publishedAt || '',
          modifiedAt: movie.modifiedAt || '',
          narrator: movie.narrator || '',
          title: movie.title || ''
        })
      };
    });

    const { error, data } = await supabase
      .from('moviesv2')
      .upsert(toInsert, { onConflict: ['link'] })
      .select();

    if (error) {
      console.error(`❌ Failed inserting batch ${i / batchSize + 1}:`, error);
      logError(`Failed inserting batch ${i / batchSize + 1}: ${error.message}`);
    } else {
      count += data.length;
      logInfo(`✅ Saved ${count} movies so far...`);
      // After successful save, update local state saved flags:
      await markMoviesAsSavedInState(uniqueChunk.map(m => m.link));
    }
  }
  logInfo(`🎉 Finished saving ${count} movies to Supabase.`);
}

// Helper to update saved = true in your state JSON for these links
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
    logInfo(`🔄 Updated saved status in local state for ${movieLinks.length} movies`);
  }
}

module.exports = { saveMoviesToSupabase };
