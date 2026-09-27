const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');
const { loadHotSnapshot, hotOf } = require('./hotSignals');

const BATCH_SIZE = 200;

async function updateMovieScores() {
  // Preserve the hot boost term on the weekly full recalc: snapshot built by
  // the hotsignals scraper (npm run scrape5) maps link -> capped 0..10 hot.
  const hotSnapshot = await loadHotSnapshot();
  let from = 0;
  let totalUpdated = 0;

  while (true) {
    const { data: movies, error } = await supabase
      .from('moviesv2')
      .select('*')
      .range(from, from + BATCH_SIZE - 1);

    if (error) {
      logError('❌ Error fetching movies:', error);
      break;
    }

    if (!movies || movies.length === 0) break;

    const updatedMovies = movies
      .filter((movie) => movie.link)
      .map((movie) => ({
        link: movie.link, // assuming 'link' is the unique key
        score: computeRelevanceScore({
        tmdb_rating: movie.tmdb_rating || 0,
        popularity: movie.popularity || 0,
        publishedAt: movie.publishedAt || '',
        modifiedAt: movie.modifiedAt || '',
        narrator: movie.narrator || '',
        title: movie.title || '',
        hot: hotOf(hotSnapshot, movie.link || ''),
      }),
    }));

    const { error: updateError } = await supabase
      .from('moviesv2')
      .upsert(updatedMovies, { onConflict: ['link'] });

    if (updateError) {
      logError('❌ Error updating relevance scores:', updateError);
      break;
    }

    totalUpdated += updatedMovies.length;
    logInfo(`✅ Updated relevance score for ${totalUpdated} movies so far...`);

    if (movies.length < BATCH_SIZE) break;
    from += BATCH_SIZE;
  }

  logInfo(`🎉 Finished updating relevance scores for ${totalUpdated} movies.`);
  return totalUpdated;
}

module.exports = updateMovieScores;
