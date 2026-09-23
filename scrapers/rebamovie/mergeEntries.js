/**
 * Entity matching + Downloadurls merging for the rebamovie enrich pass.
 *
 * Deliberately conservative (previous agnow merge produced duplicate rows):
 *  - entity key = coreTitle(title) + type, narrator must match when both sides
 *    have one — different dubs of the same film stay as separate rows;
 *  - narrator is normalized (strip "By "/"Kwa " prefixes) because sources
 *    store narrators differently (e.g. "Rocky" vs "By Rocky");
 *  - year mismatch only lowers confidence;
 *  - "(Server HD)" self-hosted slots are never collapsed here.
 *
 * CDN awareness: a link is "good CDN" when it lives on agasobanuyenow's own
 * CDN or on rebamovie/Wix media hosts — an existing entry is only upgraded
 * (with the old link archived into oldDownloadUrl) when its download is
 * missing or slow (anonsharing/mediafire) or on an unknown host.
 */
const { coreTitle, locator, isSlowHost, isServerEntry } = require('../agasobanuyenow/keys');

function hostOf(url = '') {
  try {
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

const GOOD_CDN_HOSTS = new Set([
  'media.agasobanuyenow.com',
  'cdn-video.rebamovie.com',
  'download-video.wixmp.com',
  'repackager.wixmp.com',
  'video.wixstatic.com',
]);

const isGoodCdn = (url = '') => GOOD_CDN_HOSTS.has(hostOf(url));

function normType(type) {
  const t = String(type || '').toLowerCase();
  return t === 'series' || t === 'tv' ? 'tv' : t === 'movie' ? 'movie' : '';
}

/** "By Rocky" / "By gaheza" / "Kwa Rocky" → "rocky". */
function normNarrator(name = '') {
  return String(name || '')
    .toLowerCase()
    .replace(/^(by|kwa|na|s)\s+/i, '')
    .replace(/^by\s+/i, '')
    .trim();
}

/**
 * Find the existing moviesv2 row this rebamovie item belongs to.
 * @param {{title:string, type:string, interpreter:{title?:string}, movieDataId?:{rereaseDate?:string}}} item
 */
function matchEntity(item, rows, { matchThreshold = 0.8 } = {}) {
  const entityCore = coreTitle(item.title);
  const type = normType(item.type);
  const narrator = normNarrator(item.interpreter?.title);
  const year = parseInt(item.movieDataId?.rereaseDate, 10) || null;

  if (!entityCore || !type) return null;

  let best = null;
  let bestScore = 0;

  for (const row of rows || []) {
    if (normType(row.type) !== type) continue;
    if (!row.title || coreTitle(row.title) !== entityCore) continue;

    let score = 1.0;
    const rowNar = normNarrator(row.narrator);

    if (narrator && rowNar && narrator !== rowNar) continue; // different dub — never merge
    if (narrator && !rowNar) score -= 0.1;
    if (!narrator && rowNar) score -= 0.1;

    const rowYear = parseInt(row.release_year || row.year, 10) || null;
    if (year && rowYear && year !== rowYear) score -= 0.15;

    if (score > bestScore) {
      bestScore = score;
      best = row;
    }
  }

  return bestScore >= matchThreshold ? best : null;
}

/** Movie → a single download entry titled with the film name. */
function makeMovieEntry(item, spec) {
  return {
    title: String(item.title || '').trim() || 'Movie',
    watchUrl: spec.watchUrl || '',
    downloadUrl: spec.downloadUrl || '',
    direct: true,
  };
}

const pad = (n) => String(n).padStart(2, '0');

/** Series episode → an entry titled S##E## (locator-friendly). */
function makeEpisodeEntry(showTitle, s, e, spec) {
  return {
    title: `S${pad(s)}E${pad(e)}`,
    watchUrl: spec.watchUrl || '',
    downloadUrl: spec.downloadUrl || '',
    direct: true,
  };
}

/**
 * Merge new specs into an existing row's Downloadurls.
 *
 * Guarantees:
 *  - "(Server HD)" flagged slots are never overwritten;
 *  - a missing/slow/unknown-host download gets UPGRADED to the new CDN link,
 *    archiving the old one in oldDownloadUrl;
 *  - already-good CDN links are left alone;
 *  - untracked episodes/parts get APPENDED (no duplicates by locator/title).
 *
 * @returns {{entries: Array, changed: boolean}}
 */
function mergeEntries(existing, specs, { singleSeason = false } = {}) {
  const originals = Array.isArray(existing) ? existing : [];

  const flagged = originals.filter((e) => isServerEntry(e)).map((e) => ({ ...e }));
  const plain = originals.filter((e) => !isServerEntry(e)).map((e) => ({ ...e }));

  const keyFor = (title) => {
    const lk = locator(title);
    if (lk) {
      if (singleSeason && /^s1e/.test(lk)) return `e${lk.replace(/^s1e/, '')}`;
      return lk;
    }
    return `t:${coreTitle(title)}`;
  };

  const byKey = new Map();
  for (const entry of plain) {
    const k = keyFor(entry.title);
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(entry);
  }

  let touched = false;

  for (const spec of specs) {
    const specTitle = String(spec.title || '').trim();
    const sk = keyFor(specTitle);

    if (sk) {
      const matches = byKey.get(sk) || [];
      const target = matches.find(
        (m) => !m.downloadUrl || isSlowHost(m.downloadUrl) || !isGoodCdn(m.downloadUrl)
      );
      if (target) {
        const prior = target.downloadUrl;
        if (spec.downloadUrl && isSlowHost(prior) && !target.oldDownloadUrl) {
          target.oldDownloadUrl = prior;
        }
        if (spec.downloadUrl && prior !== spec.downloadUrl) {
          target.downloadUrl = spec.downloadUrl;
          if (prior && !isSlowHost(prior) && !isGoodCdn(prior) && !target.oldDownloadUrl) {
            target.oldDownloadUrl = prior;
          }
          touched = true;
        }
        if (spec.watchUrl && target.watchUrl !== spec.watchUrl) {
          target.watchUrl = spec.watchUrl;
          touched = true;
        }
        continue;
      }
    }

    // New episode/part/variant — append unless a URL duplicate already exists.
    const dup = plain.some(
      (e) => e.downloadUrl && spec.downloadUrl && e.downloadUrl === spec.downloadUrl
    ) || plain.some((e) => e.watchUrl && spec.watchUrl && e.watchUrl === spec.watchUrl);
    if (dup) continue;

    if (spec.downloadUrl || spec.watchUrl) {
      plain.push({
        title: specTitle,
        watchUrl: spec.watchUrl || '',
        downloadUrl: spec.downloadUrl || '',
        direct: !!spec.direct,
      });
      touched = true;
    }
  }

  const entries = [...flagged, ...plain];
  const changed = touched || JSON.stringify(entries) !== JSON.stringify(originals);
  return { entries, changed };
}

module.exports = { matchEntity, makeMovieEntry, makeEpisodeEntry, mergeEntries, normType, normNarrator, isGoodCdn };