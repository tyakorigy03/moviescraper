const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { decodeHtmlEntities } = require('./decodeHtml');
const { sanitizeUrl } = require('./sanitizeVideoLinks');
const { computeRelevanceScore } = require('./relevanceScore');

const PAGE_SIZE = 1000;
const UPDATE_BATCH_SIZE = 50;
const BLOCKED_WATCH_HOSTS = ['mediafire.com'];

function parseArgs(argv) {
  const args = new Set(argv.slice(2));
  return {
    dryRun: !args.has('--apply'),
    apply: args.has('--apply'),
  };
}

function normalizeLink(url = '') {
  const sanitized = sanitizeUrl(url);
  return sanitized.replace(/^https?:\/\/(www\.)?/, 'https://');
}

function decodeText(value) {
  if (typeof value !== 'string') return value || '';
  return decodeHtmlEntities(value).trim();
}

function extractRedirectTarget(value = '') {
  const sanitized = sanitizeUrl(value);
  if (!sanitized) return '';

  try {
    const parsed = new URL(sanitized);
    const redirectTarget = parsed.searchParams.get('url');
    return redirectTarget ? sanitizeUrl(redirectTarget) : sanitized;
  } catch {
    return sanitized;
  }
}

function isBlockedWatchUrl(url = '') {
  if (!url) return false;

  try {
    const parsed = new URL(url);
    return BLOCKED_WATCH_HOSTS.some((host) => parsed.hostname.includes(host));
  } catch {
    return false;
  }
}

function normalizeGenres(genres) {
  if (typeof genres === 'string') {
    return genres.split(/[|,]/).map((genre) => genre.trim()).filter(Boolean);
  }

  if (Array.isArray(genres)) {
    return genres.map((genre) => String(genre).trim()).filter(Boolean);
  }

  return [];
}

function mergeLegacyDownloadUrls(movie) {
  const merged = [];

  if (Array.isArray(movie.Downloadurls)) merged.push(...movie.Downloadurls);
  if (Array.isArray(movie.downloadurls)) merged.push(...movie.downloadurls);

  return merged;
}

function normalizeVideoEntries(entries = []) {
  const stats = {
    total: Array.isArray(entries) ? entries.length : 0,
    removedEmpty: 0,
    removedDuplicate: 0,
    fixedRedirect: 0,
    removedBlockedWatchOnly: 0,
    cleanedFields: 0,
  };

  const deduped = new Map();

  for (const entry of Array.isArray(entries) ? entries : []) {
    const title = decodeText(entry?.title) || 'Download';
    const rawWatchUrl = typeof entry?.watchUrl === 'string' ? entry.watchUrl : '';
    const rawDownloadUrl = typeof entry?.downloadUrl === 'string' ? entry.downloadUrl : '';

    const watchUrl = extractRedirectTarget(rawWatchUrl);
    const downloadUrl = extractRedirectTarget(rawDownloadUrl);

    if (watchUrl !== sanitizeUrl(rawWatchUrl) || downloadUrl !== sanitizeUrl(rawDownloadUrl)) {
      stats.fixedRedirect += 1;
    }

    let normalizedWatchUrl = watchUrl;
    let normalizedDownloadUrl = downloadUrl;

    if (isBlockedWatchUrl(normalizedWatchUrl)) {
      normalizedWatchUrl = '';
    }

    if (!normalizedWatchUrl && !normalizedDownloadUrl) {
      stats.removedEmpty += 1;
      continue;
    }

    if (!normalizedWatchUrl && normalizedDownloadUrl && isBlockedWatchUrl(watchUrl)) {
      stats.removedBlockedWatchOnly += 1;
    }

    if (
      title !== (entry?.title || '') ||
      normalizedWatchUrl !== rawWatchUrl ||
      normalizedDownloadUrl !== rawDownloadUrl
    ) {
      stats.cleanedFields += 1;
    }

    const normalizedEntry = {
      title,
      watchUrl: normalizedWatchUrl,
      downloadUrl: normalizedDownloadUrl,
      direct: Boolean(entry?.direct),
    };

    const dedupeKey = [
      normalizedEntry.title.toLowerCase(),
      normalizedEntry.watchUrl,
      normalizedEntry.downloadUrl,
    ].join('||');

    if (deduped.has(dedupeKey)) {
      stats.removedDuplicate += 1;
      continue;
    }

    deduped.set(dedupeKey, normalizedEntry);
  }

  return {
    entries: Array.from(deduped.values()),
    stats,
  };
}

function inferStatus(normalizedMovie, videoStats, context = {}) {
  const hasTitle = Boolean(normalizedMovie.title);
  const hasVideos = normalizedMovie.Downloadurls.length > 0;
  const hasStreamable = normalizedMovie.Downloadurls.some((entry) => entry.watchUrl);
  const hasDownloadable = normalizedMovie.Downloadurls.some((entry) => entry.downloadUrl);
  const hasLinkCollision = Boolean(context.linkCollision);

  if (!hasTitle && !hasVideos) return 'junk';
  if (!hasVideos) return 'needs_review';
  if (!hasStreamable && hasDownloadable) return 'download_only';
  if (hasLinkCollision) return 'needs_review';
  if (videoStats.removedEmpty > 0 || videoStats.removedDuplicate > 0 || videoStats.fixedRedirect > 0) {
    return 'cleaned';
  }
  return 'clean';
}

function buildLinkNormalizationPlan(movies) {
  const groups = new Map();
  const blockedIds = new Set();
  const canonicalByLink = new Map();

  for (const movie of Array.isArray(movies) ? movies : []) {
    const candidate = normalizeLink(movie?.link || '');
    if (!candidate) continue;
    if (!groups.has(candidate)) groups.set(candidate, []);
    groups.get(candidate).push(movie);
  }

  for (const [candidate, group] of groups.entries()) {
    if (group.length < 2) continue;

    const existingExact = group.find((item) => (item?.link || '') === candidate);

    const sorted = existingExact
      ? [existingExact, ...group.filter((item) => item?.id !== existingExact?.id)]
      : [...group].sort((a, b) => {
          const aId = Number(a?.id);
          const bId = Number(b?.id);
          if (!Number.isNaN(aId) && !Number.isNaN(bId)) return aId - bId;
          return String(a?.id ?? '').localeCompare(String(b?.id ?? ''));
        });

    const canonical = sorted[0];
    canonicalByLink.set(candidate, canonical?.id);

    for (const dup of sorted.slice(1)) {
      blockedIds.add(dup?.id);
    }
  }

  return { blockedIds, canonicalByLink };
}

function buildNormalizedMovie(movie, options = {}) {
  const normalizedGenres = normalizeGenres(movie.genres);
  const mergedEntries = mergeLegacyDownloadUrls(movie);
  const { entries: normalizedEntries, stats: videoStats } = normalizeVideoEntries(mergedEntries);
  const linkUpdateBlockedIds = options.linkUpdateBlockedIds instanceof Set
    ? options.linkUpdateBlockedIds
    : new Set();
  const linkCollision = linkUpdateBlockedIds.has(movie.id);

  const normalizedMovie = {
    id: movie.id,
    link: linkCollision ? (movie.link || '') : normalizeLink(movie.link || ''),
    title: decodeText(movie.title),
    narrator: decodeText(movie.narrator),
    country: decodeText(movie.country),
    genres: normalizedGenres,
    Downloadurls: normalizedEntries,
    downloadurls: null,
    publishedAt: movie.publishedAt || null,
    modifiedAt: movie.modifiedAt || null,
    release_date: movie.release_date || null,
  };

  const streamableCount = normalizedEntries.filter((entry) => entry.watchUrl).length;
  const downloadableCount = normalizedEntries.filter((entry) => entry.downloadUrl).length;
  const status = inferStatus(normalizedMovie, videoStats, { linkCollision });
  const score = computeRelevanceScore({
    ...movie,
    title: normalizedMovie.title,
    narrator: normalizedMovie.narrator,
    genres: normalizedMovie.genres,
    Downloadurls: normalizedMovie.Downloadurls,
    publishedAt: normalizedMovie.publishedAt || '',
    modifiedAt: normalizedMovie.modifiedAt || '',
  });

  return {
    normalizedMovie: {
      ...normalizedMovie,
      score,
    },
    derivedStatus: {
      normalization_status: status,
      usable_video_count: normalizedEntries.length,
      has_streamable_video: streamableCount > 0,
      has_downloadable_video: downloadableCount > 0,
    },
    videoStats,
    linkCollision,
  };
}

function diffMovie(original, normalized, derivedStatus) {
  const changedFields = [];

  const originalDownloadurls = JSON.stringify(original.Downloadurls || []);
  const originalLegacyDownloadurls = JSON.stringify(original.downloadurls || null);
  const normalizedDownloadurls = JSON.stringify(normalized.Downloadurls || []);

  if ((original.link || '') !== normalized.link) changedFields.push('link');
  if ((original.title || '') !== normalized.title) changedFields.push('title');
  if ((original.narrator || '') !== normalized.narrator) changedFields.push('narrator');
  if ((original.country || '') !== normalized.country) changedFields.push('country');
  if (JSON.stringify(normalizeGenres(original.genres)) !== JSON.stringify(normalized.genres)) changedFields.push('genres');
  if (originalDownloadurls !== normalizedDownloadurls || originalLegacyDownloadurls !== 'null') changedFields.push('Downloadurls');
  if ((original.score || 0) !== normalized.score) changedFields.push('score');
  if ((original.normalization_status || '') !== derivedStatus.normalization_status) changedFields.push('normalization_status');
  if ((original.usable_video_count || 0) !== derivedStatus.usable_video_count) changedFields.push('usable_video_count');
  if (Boolean(original.has_streamable_video) !== derivedStatus.has_streamable_video) changedFields.push('has_streamable_video');
  if (Boolean(original.has_downloadable_video) !== derivedStatus.has_downloadable_video) changedFields.push('has_downloadable_video');

  return changedFields;
}

function buildSummary() {
  return {
    totalMovies: 0,
    changedMovies: 0,
    statusCounts: {
      clean: 0,
      cleaned: 0,
      download_only: 0,
      needs_review: 0,
      junk: 0,
    },
    issues: {
      emptyVideoEntriesRemoved: 0,
      duplicateVideoEntriesRemoved: 0,
      redirectUrlsFixed: 0,
      blockedWatchUrlsRemoved: 0,
      linkUpdatesBlocked: 0,
      changedDownloadurlRecords: 0,
      changedTitles: 0,
      changedLinks: 0,
    },
    samples: {
      junk: [],
      needs_review: [],
      download_only: [],
      link_collisions: [],
      changed: [],
    },
  };
}

function pushSample(bucket, sample, limit = 5) {
  if (bucket.length < limit) bucket.push(sample);
}

async function fetchAllMovies() {
  let allMovies = [];
  let from = 0;

  while (true) {
    logInfo(`Fetching movies ${from} to ${from + PAGE_SIZE - 1}...`);
    const { data: chunk, error } = await supabase
      .from('moviesv2')
      .select('*')
      .range(from, from + PAGE_SIZE - 1);

    if (error) {
      throw new Error(`Failed to fetch movies: ${error.message}`);
    }

    if (!chunk || chunk.length === 0) break;
    allMovies = allMovies.concat(chunk);
    if (chunk.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  return allMovies;
}

async function applyUpdates(updates) {
  let updatedCount = 0;

  for (let i = 0; i < updates.length; i += UPDATE_BATCH_SIZE) {
    const batch = updates.slice(i, i + UPDATE_BATCH_SIZE);
    const { error } = await supabase
      .from('moviesv2')
      .upsert(batch, { onConflict: 'id' });

    if (error) {
      throw new Error(`Failed to update batch starting at ${i}: ${error.message}`);
    }

    updatedCount += batch.length;
    logInfo(`Updated ${updatedCount}/${updates.length} records...`);
  }
}

async function main() {
  const { dryRun, apply } = parseArgs(process.argv);
  const summary = buildSummary();

  logInfo(`Starting movie record normalization in ${dryRun ? 'dry-run' : 'apply'} mode...`);

  const movies = await fetchAllMovies();
  const updates = [];

  summary.totalMovies = movies.length;
  const linkPlan = buildLinkNormalizationPlan(movies);

  for (const movie of movies) {
    const { normalizedMovie, derivedStatus, videoStats, linkCollision } = buildNormalizedMovie(movie, {
      linkUpdateBlockedIds: linkPlan.blockedIds
    });
    const changedFields = diffMovie(movie, normalizedMovie, derivedStatus);

    summary.statusCounts[derivedStatus.normalization_status] += 1;
    summary.issues.emptyVideoEntriesRemoved += videoStats.removedEmpty;
    summary.issues.duplicateVideoEntriesRemoved += videoStats.removedDuplicate;
    summary.issues.redirectUrlsFixed += videoStats.fixedRedirect;
    summary.issues.blockedWatchUrlsRemoved += videoStats.removedBlockedWatchOnly;
    if (linkCollision) {
      summary.issues.linkUpdatesBlocked += 1;
      pushSample(summary.samples.link_collisions, {
        id: movie.id,
        link: movie.link,
        normalized_candidate: normalizeLink(movie.link || '')
      });
    }

    if (changedFields.includes('Downloadurls')) summary.issues.changedDownloadurlRecords += 1;
    if (changedFields.includes('title')) summary.issues.changedTitles += 1;
    if (changedFields.includes('link')) summary.issues.changedLinks += 1;

    if (derivedStatus.normalization_status === 'junk') {
      pushSample(summary.samples.junk, {
        id: movie.id,
        title: movie.title,
        link: movie.link,
      });
    }

    if (derivedStatus.normalization_status === 'needs_review') {
      pushSample(summary.samples.needs_review, {
        id: movie.id,
        title: movie.title,
        link: movie.link,
        usable_video_count: derivedStatus.usable_video_count,
      });
    }

    if (derivedStatus.normalization_status === 'download_only') {
      pushSample(summary.samples.download_only, {
        id: movie.id,
        title: movie.title,
        link: movie.link,
        usable_video_count: derivedStatus.usable_video_count,
      });
    }

    if (changedFields.length > 0) {
      summary.changedMovies += 1;
      pushSample(summary.samples.changed, {
        id: movie.id,
        title: normalizedMovie.title || movie.title,
        changedFields,
      });
      updates.push(normalizedMovie);
    }
  }

  logInfo('Normalization summary:');
  logInfo(JSON.stringify(summary, null, 2));

  if (dryRun) {
    logInfo(`Dry run complete. ${updates.length} records would be updated.`);
    return;
  }

  if (!apply) {
    logInfo('No apply flag provided. Exiting without changes.');
    return;
  }

  if (updates.length === 0) {
    logInfo('No updates needed.');
    return;
  }

  await applyUpdates(updates);
  logInfo('Movie record normalization complete.');
}

main().catch((err) => {
  logError(`Normalization failed: ${err.message}`);
  process.exit(1);
});
