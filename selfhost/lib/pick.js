const supabase = require('../../services/supabaseClient');
const config = require('../config');
const { logger } = require('./state');

// Hosts we can actually download source files from.
const GOOD_HOSTS = ['anonsharing.com', 'mediafire.com'];

const HOST_PRIORITY = ['anonsharing.com', 'mediafire.com'];

function hostOf(url = '') {
  try {
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

/**
 * Pick one Downloadurls entry we can fetch a source file from. Prefers
 * anonsharing (direct .mp4) over mediafire (bot-protected but resolvable).
 */
function pickDownloadEntry(movie) {
  if (!Array.isArray(movie.Downloadurls) || movie.Downloadurls.length === 0) return null;
  for (const host of HOST_PRIORITY) {
    const entry = movie.Downloadurls.find(
      (e) => e && typeof e.downloadUrl === 'string' && hostOf(e.downloadUrl) === host
    );
    if (entry) return entry;
  }
  return null;
}

function hasSelfHostedEntry(movie) {
  return (
    Array.isArray(movie.Downloadurls) &&
    movie.Downloadurls.some((e) => e && /\(server hd\)/i.test(String(e.title || '')))
  );
}

/**
 * Choose up to `topN` movies: highest relevance score first, that still have a
 * usable external download URL, skipping ones already self-hosted.
 */
async function pickMovies() {
  const limit = 500;
  const { data, error } = await supabase
    .from('moviesv2')
    .select('id,title,link,score,popularity,Downloadurls,hosted')
    .order('score', { ascending: false, nullsFirst: false })
    .limit(limit);

  if (error) throw new Error(`pick: ${error.message}`);

  const candidates = [];
  for (const movie of data || []) {
    const entry = pickDownloadEntry(movie);
    if (!entry) {
      logger.debug(`skip (no good source): ${movie.title}`);
      continue;
    }
    candidates.push({ movie, entry });
  }

  logger.info(
    `${candidates.length} of ${(data || []).length} top candidates have a downloadable source.`
  );
  // NOTE: do NOT slice to config.topN here — run.js filters out already-hosted
  // movies first, then takes the top N from what actually needs processing
  // (otherwise later runs would keep re-selecting the same high-score movies).
  return candidates;
}

/** Re-check a single movie (used by --force to ignore already-hosted). */
async function pickById(id) {
  const { data, error } = await supabase
    .from('moviesv2')
    .select('id,title,link,score,popularity,Downloadurls,hosted')
    .eq('id', id)
    .maybeSingle();
  if (error) throw new Error(`pickById: ${error.message}`);
  if (!data) return null;
  const entry = pickDownloadEntry(data);
  if (!entry) return null;
  return { movie: data, entry };
}

module.exports = { pickMovies, pickById, hasSelfHostedEntry, GOOD_HOSTS };