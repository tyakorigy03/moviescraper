/**
 * Entity matching + Downloadurls merging for the agasobanuyenow enrich pass.
 *
 * Matching is multi-signal and deliberately conservative:
 *  - entity key = coreTitle(title) + type (parts/episodes/years stripped)
 *  - same narrator required when both sides have one (different tracks = different
 *    agasobanuyenow items, e.g. Moana-by-gaheza vs Moana-by-perfect — never merged)
 *  - year mismatch only lowers confidence
 *  - rows with a "(Server HD)" entry are never collapsed here
 */
const { coreTitle, locator, isSlowHost, isCdnHost, isServerEntry, CDN_HOST } = require('./keys');

function normType(type) {
  const t = String(type || '').toLowerCase();
  return t === 'series' || t === 'tv' ? 'tv' : t === 'movie' ? 'movie' : '';
}

/**
 * Find the existing moviesv2 row this catalog item belongs to.
 */
function matchEntity(item, rows, { matchThreshold = 0.8 } = {}) {
  const entityCore = coreTitle(item.title);
  const type = normType(item.type);
  const narrator = String(item.interpreter || '').trim().toLowerCase();
  const year = parseInt(item.year, 10) || null;

  if (!entityCore || !type) return null;

  let best = null;
  let bestScore = 0;

  for (const row of rows || []) {
    if (normType(row.type) !== type) continue;
    if (!row.title || coreTitle(row.title) !== entityCore) continue;

    let score = 1.0;
    const rowNar = String(row.narrator || '').trim().toLowerCase();

    if (narrator && rowNar && narrator !== rowNar) continue; // different track
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

/** Movie → a single download entry. Keeps part markers so Part 1/Part 2 stay distinct. */
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
 *  - "(Server HD)" entries are never overwritten (their watch/download stay);
 *  - an existing entry with a slow (anonsharing/mediafire) or missing download
 *    gets UPGRADED to the CDN link, keeping the old link in `oldDownloadUrl`;
 *  - untracked episodes/parts get APPENDED (no duplicates by locator);
 *  - if the row is self-hosted, its Server HD entry's oldDownloadUrl is bumped
 *    to the CDN link so eviction restores the fast link.
 *
 * @param {Array} existing  current Downloadurls
 * @param {Array} specs     entries to merge in ({title, watchUrl, downloadUrl, direct})
 * @param {Object} opts     { singleSeason }
 * @returns {{entries: Array, changed: boolean}}
 */
function mergeEntries(existing, specs, { singleSeason = false } = {}) {
  const originals = Array.isArray(existing) ? existing : [];

  // Split slots: flagged entries must stay byte-for-byte identical (except the
  // Server HD oldDownloadUrl bump handled at the end).
  const flagged = originals.filter((e) => isServerEntry(e)).map((e) => ({ ...e }));
  const plain = originals.filter((e) => !isServerEntry(e)).map((e) => ({ ...e }));

  // Key function: collapse S1E3 → E3 when a single-season show, so the old
  // "Episode 3" style titles match the new "S1E3" ones.
  const keyFor = (title, nonSplit) => {
    const lk = locator(title);
    if (lk) {
      if (singleSeason && /^s1e/.test(lk)) return `e${lk.replace(/^s1e/, '')}`;
      return lk;
    }
    return nonSplit({ title });
  };

  const byKey = new Map();
  for (const entry of plain) {
    const k = keyFor(entry.title, (o) => `t:${coreTitle(o.title)}`);
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(entry);
  }

  let touched = false;

  for (const spec of specs) {
    const specTitle = String(spec.title || '').trim();
    const sk = keyFor(specTitle, (o) => `t:${coreTitle(o.title)}`);

    if (sk) {
      const matches = byKey.get(sk) || [];
      const target = matches.find(
        (m) => !m.downloadUrl || isSlowHost(m.downloadUrl) || !isCdnHost(m.downloadUrl)
      );
      if (target) {
        // Upgrade: swap slow/missing download link for the CDN one.
        const prior = target.downloadUrl;
        if (spec.downloadUrl && isSlowHost(prior) && !target.oldDownloadUrl) {
          target.oldDownloadUrl = prior;
        }
        if (spec.downloadUrl && prior !== spec.downloadUrl) {
          target.downloadUrl = spec.downloadUrl;
          touched = true;
        }
        if (!target.watchUrl && spec.watchUrl) {
          target.watchUrl = spec.watchUrl;
          touched = true;
        }
        continue;
      }
    }

    // New episode/part/variant — append unless a CDN-duplicate already exists.
    const dup = plain.some(
      (e) => e.downloadUrl && spec.downloadUrl && e.downloadUrl === spec.downloadUrl
    );
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

  // Self-hosted rows: point the archived fallback at the CDN link.
  for (const spec of specs) {
    if (!spec.downloadUrl) continue;
    for (const fe of flagged) {
      if (isServerEntry(fe) && !isCdnHost(fe.oldDownloadUrl) && fe.oldDownloadUrl !== spec.downloadUrl) {
        fe.oldDownloadUrl = spec.downloadUrl;
        touched = true;
      }
    }
  }

  const entries = [...flagged, ...plain];
  const changed = touched || JSON.stringify(entries) !== JSON.stringify(originals);
  return { entries, changed };
}

module.exports = { matchEntity, makeMovieEntry, makeEpisodeEntry, mergeEntries, normType, CDN_HOST };