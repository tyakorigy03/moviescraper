const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');

const BATCH_SIZE = 200;

async function updateMovieScores() {
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
