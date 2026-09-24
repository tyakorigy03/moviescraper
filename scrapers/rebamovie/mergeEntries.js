/**
 * Entity matching + Downloadurls merging for the rebamovie enrich pass.
 *
 * Matching is multi-signal and deliberately conservative (previous agnow merge
 * produced duplicate rows):
 *  - entity key = coreTitle(title) + type, narrator must match when both sides
 *    have one — different dubs of the same film stay as separate rows;
 *  - narrator is normalized (strip "By "/"Kwa " prefixes) because sources
 *    store narrators differently (e.g. "Rocky" vs "By Rocky");
 *  - year mismatch only lowers confidence;
 *  - "(Server HD)" self-hosted slots are never collapsed here.
 *
 * URL priority (trusted CDN first):
 *  1. rebamovie/Wix (download-video.wixmp.com, cdn-video.rebamovie.com, …)
 *  2. agasobanuyenow's own media.agasobanuyenow.com CDN
 *  3. unknown hosts / direct links
 *  4. slow hosts (mediafire / anonsharing) — always replaced when the spec
 *     carries a trusted CDN link; kept as the only fallback otherwise.
 *
 * Season awareness (aglive per-season rows vs rebamovie whole-series items):
 *  - aglive rows are titled "Show S05" with entries "EP01".."EP10" (locator e1).
 *    rebamovie items cover S01..S05 with entries "S01E01".. (locator s5e1).
 *  - when a row has a season (rowSeason) the merger maps "EPxx" → that season's
 *    episode so rebamovie "S05E01" REPLACES the aglive "EP01" instead of
 *    appending a duplicate;
 *  - episodes whose season differs from rowSeason are skipped (they belong to
 *    the other per-season rows, never dumped into this one);
 *  - whole movies collapse part-suffixed entries ("Shelter A"/"Shelter B" +
 *    rebamovie whole "Shelter") into a single CDN entry.
 */
const {
  coreTitle, locator, isSlowHost, isServerEntry, hostOf,
  seasonOfTitle, familyKey, isPartSuffixTitle,
} = require('../agasobanuyenow/keys');

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
 * All moviesv2 rows (same title core + type + narrator) this item could
 * belong to, scored like matchEntity. A rebamovie whole-series item routinely
 * matches several aglive per-season rows — distribution merges each season
 * into its own row instead of dumping everything into one.
 */
function matchEntityRows(item, rows, { matchThreshold = 0.8 } = {}) {
  const entityCore = coreTitle(item.title);
  const type = normType(item.type);
  const narrator = normNarrator(item.interpreter?.title);
  const year = parseInt(item.movieDataId?.rereaseDate, 10) || null;

  if (!entityCore || !type) return [];

  const scored = [];
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

    if (score >= matchThreshold) scored.push({ row, score });
  }

  return scored.sort((a, b) => b.score - a.score).map((s) => s.row);
}

/** Find the single best existing moviesv2 row this item belongs to. */
function matchEntity(item, rows, opts) {
  const all = matchEntityRows(item, rows, opts);
  return all.length ? all[0] : null;
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

/** "s5e1" → {s:5, e:1}. */
function seasonEpOf(loc = '') {
  const m = /^s(\d{1,3})e(\d{1,3})$/.exec(loc);
  return m ? { s: +m[1], e: +m[2] } : null;
}

/**
 * Merge new specs into an existing row's Downloadurls.
 *
 * Guarantees:
 *  - "(Server HD)" flagged slots are never overwritten;
 *  - a missing/slow/unknown-host download gets UPGRADED to CDN, archiving the
 *    old one in oldDownloadUrl; CDN→CDN only in `replace` (repair) mode;
 *  - watchUrl prefers a trusted CDN (rebamovie cdn-video) over rumble/mediafire
 *    so the player doesn't hit dead rumble embeds — but never wipes a watchUrl
 *    when the spec has none;
 *  - season-aware: with rowSeason set, "EPxx" entries are treated as that
 *    season's episodes (S<rowSeason>E<xx>) and cross-season specs are skipped;
 *  - movie parts ("Shelter A"/"Shelter B") collapse into the whole rebamovie
 *    CDN entry when one arrives (with the parts archived, not appended as a
 *    third row);
 *  - untracked episodes/parts get APPENDED (no duplicates by locator/title).
 *
 * @param {Array} existing  current Downloadurls
 * @param {Array} specs     entries to merge in ({title, watchUrl, downloadUrl, direct})
 * @param {Object} opts     { singleSeason, replace, rowSeason }
 * @returns {{entries: Array, changed: boolean}}
 */
function mergeEntries(existing, specs, { singleSeason = false, replace = false, rowSeason = '' } = {}) {
  const originals = Array.isArray(existing) ? existing : [];

  const flagged = originals.filter((e) => isServerEntry(e)).map((e) => ({ ...e }));
  const plain = originals.filter((e) => !isServerEntry(e)).map((e) => ({ ...e }));

  const keyFor = (title) => {
    const lk = locator(title);
    if (lk) {
      if (/^e\d+$/.test(lk) && rowSeason) return `s${rowSeason}e${lk.slice(1)}`;
      if (singleSeason && /^s1e/.test(lk)) return `e${lk.replace(/^s1e/, '')}`;
      return lk;
    }
    return `t:${familyKey(title)}`;
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
    const lk = locator(specTitle);

    // Season-scoped row: ignore episodes belonging to other seasons (they have
    // their own per-season rows). Whole-movie entries always pass.
    if (rowSeason) {
      const se = seasonEpOf(lk);
      if (se && String(se.s) !== String(rowSeason)) continue;
    }

    const sk = keyFor(specTitle);
    let matches = byKey.get(sk) || [];

    // Whole-movie spec colliding with part entries ("Shelter A"/"Shelter B"):
    // collapse to a single entry when we have a trusted link to replace the
    // parts. The parts' downloads are archived into oldDownloadUrl (never
    // silently lost), and nothing is appended as a "third part".
    if (lk === null && (spec.downloadUrl || spec.watchUrl) && matches.length) {
      const whole = matches.find((m) => !isPartSuffixTitle(m.title)) || null;
      const parts = matches.filter((m) => isPartSuffixTitle(m.title));
      const collapsing = whole || (parts.length && parts.length === matches.length);

      if (collapsing) {
        const target = whole || matches[0];
        const archived = [
          ...parts.map((p) => p.downloadUrl).filter(Boolean),
          ...(target.oldDownloadUrl || '').split(' | ').filter(Boolean),
          ...(whole ? [] : [target.downloadUrl].filter(Boolean)),
        ].filter(Boolean);

        if (!whole && spec.downloadUrl && isGoodCdn(spec.downloadUrl)) {
          const prior = target.downloadUrl;
          if (prior && prior !== spec.downloadUrl) {
            target.oldDownloadUrl = [...new Set(archived)].join(' | ') || prior;
          }
          if (target.downloadUrl !== spec.downloadUrl) {
            target.downloadUrl = spec.downloadUrl;
            touched = true;
          }
          target.title = specTitle;
        }
        if (spec.watchUrl && target.watchUrl !== spec.watchUrl) {
          target.watchUrl = spec.watchUrl;
          touched = true;
        }
        if (parts.length) touched = true;

        // Drop the part entries (other than the target) from the list.
        const dropTitles = new Set(parts.filter((p) => p !== target).map((p) => p.title));
        const kept = plain.filter((e) => e === target || !dropTitles.has(e.title));
        plain.length = 0;
        plain.push(...kept);
        continue;
      }
    }

    const target = replace
      ? matches[0]
      : matches.find(
          (m) => !m.downloadUrl || isSlowHost(m.downloadUrl) || !isGoodCdn(m.downloadUrl)
        );
    if (target) {
      const prior = target.downloadUrl;
      if (spec.downloadUrl && isSlowHost(prior) && !target.oldDownloadUrl) {
        target.oldDownloadUrl = prior;
      }
      if (spec.downloadUrl && prior !== spec.downloadUrl) {
        const newBetter = !prior || isSlowHost(prior) || !isGoodCdn(prior);
        const sameTrust = isGoodCdn(prior) && isGoodCdn(spec.downloadUrl);
        if (replace || newBetter || sameTrust) {
          if (replace && prior && !target.oldDownloadUrl) {
            target.oldDownloadUrl = prior;
          } else if (prior && (isSlowHost(prior) || !isGoodCdn(prior)) && !target.oldDownloadUrl) {
            target.oldDownloadUrl = prior;
          }
          target.downloadUrl = spec.downloadUrl;
          touched = true;
        }
      }
      // Watch: trusted CDN over rumble/mediafire; never wipe when spec has none.
      if (spec.watchUrl && target.watchUrl !== spec.watchUrl) {
        const priorWatch = hostOf(target.watchUrl);
        const newWatch = hostOf(spec.watchUrl);
        const shouldSwap =
          !target.watchUrl ||
          isGoodCdn(spec.watchUrl) ||
          (/rumble\.com|mediafire\.com/i.test(priorWatch || '') && newWatch) ||
          replace;
        if (shouldSwap) {
          target.watchUrl = spec.watchUrl;
          touched = true;
        }
      }
      continue;
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

module.exports = { matchEntity, matchEntityRows, makeMovieEntry, makeEpisodeEntry, mergeEntries, normType, normNarrator, isGoodCdn, seasonOfTitle };