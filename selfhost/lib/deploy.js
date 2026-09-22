const supabase = require('../../services/supabaseClient');
const { logger } = require('./state');

const SELF_TAG = /\(server hd\)/i;

/**
 * Point a movie's Downloadurls at our R2 file:
 *  - archives the original download link (oldDownloadUrl) so eviction can
 *    restore it without re-scraping
 *  - replaces the previous "(Server HD)" entry for that same episode, or
 *    prepends a new one
 *  - sets hosted = true and bumps modifiedAt (if the column exists)
 */
async function deployMovieEntry({ movie, entry, publicUrl }) {
  const entries = Array.isArray(movie.Downloadurls) ? movie.Downloadurls.map((e) => ({ ...e })) : [];

  const watchUrl =
    (entry && typeof entry.watchUrl === 'string' && entry.watchUrl) ||
    (entries.find((e) => e.watchUrl) || {}).watchUrl ||
    '';

  // A TV row can hold many episodes as separate Downloadurls entries. Label the
  // self-hosted entry with the EPISODE's title (not the show's) and replace
  // only that one entry, so hosting episode 3 never clobbers episode 2's.
  const episodeTitle =
    entry && typeof entry.title === 'string' && entry.title.trim() ? entry.title.trim() : movie.title;
  const selfTitle = `${episodeTitle} (Server HD)`;

  const selfEntry = {
    title: selfTitle,
    watchUrl,
    downloadUrl: publicUrl,
    // Archive the slow/original source link so we can restore it on eviction.
    oldDownloadUrl: (entry && typeof entry.downloadUrl === 'string' && entry.downloadUrl) || '',
    direct: true,
  };

  const idx = entries.findIndex((e) => e?.title === selfTitle);
  if (idx >= 0) entries[idx] = selfEntry;
  else entries.unshift(selfEntry);

  const update = {
    Downloadurls: entries,
    modifiedAt: new Date().toISOString(),
  };

  const withHosted = { ...update, hosted: true };
  const { data, error } = await supabase
    .from('moviesv2')
    .update(withHosted)
    .eq('id', movie.id)
    .select('id,title,hosted')
    .single();

  if (error) {
    // 'hosted' may not exist yet — apply the migration in selfhost/migration.sql.
    if (/column.*hosted.*does not exist/i.test(error.message)) {
      logger.warn('  "hosted" column missing — applying Downloadurls update only (run migration.sql)');
      const { data: d2, error: e2 } = await supabase
        .from('moviesv2')
        .update(update)
        .eq('id', movie.id)
        .select('id,title,hosted')
        .single();
      if (e2) throw new Error(`deploy (no hosted): ${e2.message}`);
      return d2;
    }
    throw new Error(`deploy: ${error.message}`);
  }

  logger.info(`  deployed ${movie.title} → ${publicUrl}`);
  return data;
}

/**
 * Undo self-hosting: restore the archived original link inside "(Server HD)"
 * entries (no re-scrape needed), clear the hosted flag, so the movie falls
 * back to its ordinary mediafire/anonsharing download.
 */
async function dehostMovie(id) {
  const { data, error } = await supabase
    .from('moviesv2')
    .select('id,title,Downloadurls,hosted')
    .eq('id', id)
    .maybeSingle();

  if (error) throw new Error(`dehost fetch: ${error.message}`);
  if (!data) {
    logger.warn(`  movie ${id} not found while de-hosting — skipping`);
    return null;
  }

  const restored = [];
  let restoredCount = 0;
  for (const e of Array.isArray(data.Downloadurls) ? data.Downloadurls : []) {
    if (!SELF_TAG.test(String(e.title || ''))) {
      restored.push({ ...e });
      continue;
    }
    const old = e.oldDownloadUrl;
    if (typeof old === 'string' && old) {
      // Swap back to the archived source link, keep the episode's identity.
      restored.push({
        title: String(e.title).replace(SELF_TAG, '').trim(),
        watchUrl: e.watchUrl || '',
        downloadUrl: old,
        direct: true,
      });
      restoredCount++;
    } else {
      // No archive (pre-change entries) — the entry is simply dropped.
      logger.warn(`  no archived link for "${e.title}" — entry dropped`);
    }
  }

  const anyLeft = restored.some((e) => SELF_TAG.test(String(e.title || '')));
  const update = {
    Downloadurls: restored,
    modifiedAt: new Date().toISOString(),
  };

  const { data: d, error: e } = await supabase
    .from('moviesv2')
    .update({ ...update, hosted: anyLeft })
    .eq('id', id)
    .select('id,title,hosted')
    .single();

  if (e) {
    if (/column.*hosted.*does not exist/i.test(e.message)) {
      logger.warn('  "hosted" column missing — applied Downloadurls change only (run migration.sql)');
      const { error: e2 } = await supabase
        .from('moviesv2')
        .update(update)
        .eq('id', id)
        .select('id,title')
        .single();
      if (e2) throw new Error(`dehost (no hosted): ${e2.message}`);
      return d;
    }
    throw new Error(`dehost: ${e.message}`);
  }

  logger.info(
    `  un-hosted ${data.title}` +
      (restoredCount ? ` — restored ${restoredCount} archived download link(s)` : '')
  );
  return d;
}

module.exports = { deployMovieEntry, dehostMovie };