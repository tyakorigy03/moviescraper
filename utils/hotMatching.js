/**
 * Entity resolution for hot signals (see utils/hotSignalsScraper.js).
 *
 * A ranked source list identifies a *work*, not a database row. Two gaps made
 * the old exact-key match `coreTitle|type|narrator` discard most of the data:
 *
 *   1. Narrator is optional upstream. Reba sends `interpreter.title` only when
 *      the entry has one; the storage order files never have it (their genre
 *      strings are sometimes mined for it, which is why 0 of 72 storage keys
 *      carried one). Meanwhile 2216 of 2340 catalogue rows DO have a narrator,
 *      so a narrator-less key can never match them. That alone dropped 71 of
 *      98 ranked entries — including the largest single storage signal,
 *      "beauty in black|tv" at 17.55 raw points.
 *   2. A series is listed once but stored per season. "outer banks|tv|rocky"
 *      addresses 5 rows, so a boost either smeared across S01..S05 or had to
 *      pick arbitrarily.
 *
 * Signals therefore aggregate per *show* — `coreTitle`, ignoring type, narrator
 * and season — and are attributed to a single row of that show.
 *
 * Attribution deliberately mirrors the frontend (src/lib/showKey.ts, both
 * apps): it groups rows by `coreTitle` and keeps the highest-scoring one, so the
 * scorer boosts exactly the row that will be displayed. This matters — picking
 * the newest season instead is not equivalent. "Beauty In Black S03" is the
 * newest row but has no tmdb_rating/popularity, so it scores ~22 while S02
 * scores ~50; boosting S03 would put the boost on a row the UI never shows and
 * the entire hot signal would be invisible. Row choice is therefore argmax of
 * the caller's score function, with data-quality/recency order breaking ties.
 *
 * Aggregation is a noisy-OR over per-list rank weights rather than a sum that
 * saturates the cap:
 *
 *   weight(i) = (RANKS - 1 - i) / RANKS      #1 -> 0.96 ... #25 -> 0
 *   hot       = 10 * (1 - PROD(1 - w))
 *
 * No single list can peg the cap on its own, and a title has to be corroborated
 * across lists/sites to approach it — which is the property the old
 * `sum then clamp` lacked, where 12 titles all sat pinned at exactly 10.
 */
const { coreTitle, seasonOfTitle } = require('../scrapers/agasobanuyenow/keys');
const { normType, normNarrator } = require('../scrapers/rebamovie/mergeEntries');

const RANKS = 25;
const HOT_CAP = 10;

const clamp01 = (v) => Math.max(0, Math.min(1, Number(v) || 0));

/* ------------------------------ identity ---------------------------------- */

/**
 * Show identity: what the UI groups by. Mirrors showKey() in the frontends,
 * including the trailing "(...)" qualifier drop that coreTitle also does.
 */
function showKey(title) {
  return coreTitle(title);
}

/** Narrator-aware key. Kept for diagnostics; no longer the matching key. */
function entityKey(title, type, narrator) {
  const core = coreKey(title);
  if (!core) return null;
  return `${core}|${normType(type) || 'movie'}|${normNarrator(narrator)}`;
}

/** Narrator-blind, season-blind work identity including media type. */
function familyKey(title, type) {
  const core = coreKey(title);
  if (!core) return null;
  return `${core}|${normType(type) || 'movie'}`;
}

const coreKey = (title) => coreTitle(title);

/* -------------------------------- weighting -------------------------------- */

/** Rank (0-based) -> 0..1 weight. #1 is 0.96, the last ranked slot is 0. */
function rankWeight(index, ranks = RANKS) {
  return clamp01((ranks - 1 - Number(index)) / ranks);
}

/** Noisy-OR combination of independent corroborations, capped at HOT_CAP. */
function combineHot(weights, cap = HOT_CAP) {
  let inverse = 1;
  for (const w of weights) inverse *= 1 - clamp01(w);
  return Math.round(Math.min(cap, cap * (1 - inverse)) * 10) / 10;
}

/* ------------------------------- index ------------------------------------ */

const push = (map, key, value) => {
  const list = map.get(key);
  if (list) list.push(value);
  else map.set(key, [value]);
};

/** Index catalogue rows by show (what the UI groups by) plus family/exact. */
function buildIndex(rows) {
  const byShow = new Map();
  const byFamily = new Map();
  const byExact = new Map();
  for (const row of rows || []) {
    const core = showKey(row.title);
    if (!core) continue;
    const family = `${core}|${normType(row.type) || 'movie'}`;
    push(byShow, core, row);
    push(byFamily, family, row);
    push(byExact, `${family}|${normNarrator(row.narrator)}`, row);
  }
  return { byShow, byFamily, byExact };
}

/* ------------------------------- resolution -------------------------------- */

const parseTime = (value) => {
  const t = Date.parse(value || '');
  return Number.isFinite(t) ? t : 0;
};

/**
 * Order candidate rows, best first.
 *
 * Metadata completeness leads, because a row with a blank `type`/`narrator` is
 * exactly the defect that made a work unattributable in the first place. It also
 * guards the recency tie-break: duplicate uploads of one work carry
 * `publishedAt` values a day apart, so recency alone would happily hand the
 * boost to a defective row (e.g. "Mutiny" with a blank `type` and a stale score).
 *
 * Recency then decides between genuinely equivalent rows — which is what
 * separates S05 from S01.
 */
function byPreference(rows) {
  return rows.slice().sort(
    (a, b) =>
      (b.type ? 1 : 0) - (a.type ? 1 : 0) ||
      (b.narrator ? 1 : 0) - (a.narrator ? 1 : 0) ||
      parseTime(b.publishedAt) - parseTime(a.publishedAt) ||
      parseTime(b.modifiedAt) - parseTime(a.modifiedAt) ||
      (Number(seasonOfTitle(b.title)) || 0) - (Number(seasonOfTitle(a.title)) || 0) ||
      String(a.title).localeCompare(String(b.title))
  );
}

/**
 * The single row a show's boost lands on. Returns null when the show is absent.
 *
 * `scoreRow` must be the same score function the database is ranked by. It is
 * the primary criterion, so the boosted row is the one the UI keeps; ties fall
 * back to data-quality/recency order. `reason` records which rule won so the
 * snapshot stays auditable.
 */
function pickPrimary(rows, preferredNarrator = '', scoreRow = null) {
  if (!rows || !rows.length) return null;
  // byPreference first, then a STABLE sort on score: equal scores keep the
  // quality/recency order.
  const ordered = byPreference(rows);
  const wanted = preferredNarrator ? normNarrator(preferredNarrator) : '';
  if (wanted) {
    const exact = rows.filter((r) => normNarrator(r.narrator) === wanted);
    if (exact.length === 1) return { row: exact[0], reason: 'narrator' };
  }
  if (scoreRow) {
    ordered.sort((a, b) => (Number(scoreRow(b)) || 0) - (Number(scoreRow(a)) || 0));
    return { row: ordered[0], reason: 'score' };
  }
  return { row: ordered[0], reason: 'preference' };
}

/** Map a show key onto the rows that back it. */
function resolveShow(index, key, preferredNarrator = '', scoreRow = null) {
  const rows = index.byShow.get(key) || [];
  const picked = pickPrimary(rows, preferredNarrator, scoreRow);
  if (!picked) return null;
  return { ...picked, rows, reason: picked.reason === 'preference' ? 'preference' : picked.reason };
}

/* ------------------------------ signal ledger ------------------------------ */

/**
 * Accumulates ranked observations per show. Keyed by show identity so a
 * movie/series pair sharing a title, or the same show listed on several sites,
 * corroborates one signal instead of racing to overwrite another.
 */
function createLedger() {
  const entries = new Map();
  return {
    /**
     * @param {string} title    as the source spelled it
     * @param {string} type     media type, for provenance only
     * @param {number} index    0-based position in the ranked list
     * @param {string} source   provenance tag, e.g. "reba-trending#2"
     * @param {string} narrator optional narrator the source reported
     * @param {number} points   legacy additive points, kept for reporting
     */
    add(title, type, index, source, narrator = '', points = 0) {
      const key = showKey(title);
      if (!key) return;
      let rec = entries.get(key);
      if (!rec) {
        rec = { key, families: new Set(), weights: [], sources: [], points: 0, narrators: new Set() };
        entries.set(key, rec);
      }
      if (familyKey(title, type)) rec.families.add(familyKey(title, type));
      rec.weights.push(rankWeight(index));
      rec.sources.push(source);
      rec.points += Number(points) || 0;
      if (narrator) rec.narrators.add(normNarrator(narrator));
    },
    /** @returns {IterableIterator<object>} one record per show */
    entries() {
      return entries.values();
    },
    get size() {
      return entries.size;
    },
  };
}

/**
 * Resolve a ledger into per-link boost records.
 *
 * @param ledger    from createLedger()
 * @param index     from buildIndex(rows)
 * @param scoreRow  optional (row) => score; selects the row the UI will show
 */
function applyLedger(ledger, index, { scoreRow = null } = {}) {
  const boosts = [];
  const unresolved = [];
  for (const rec of ledger.entries()) {
    const hot = combineHot(rec.weights);
    if (hot <= 0) continue;
    const narrator = rec.narrators.size ? [...rec.narrators][0] : '';
    const target = resolveShow(index, rec.key, narrator, scoreRow);
    if (!target) {
      unresolved.push({ show: rec.key, families: [...rec.families], hot, points: rec.points, sources: rec.sources });
      continue;
    }
    boosts.push({
      link: target.row.link,
      hot,
      points: Math.round(rec.points * 10) / 10,
      show: rec.key,
      families: [...rec.families],
      match: target.reason,
      rows: target.rows.length,
      sources: rec.sources,
    });
  }
  return { boosts, unresolved };
}

module.exports = {
  RANKS,
  HOT_CAP,
  showKey,
  entityKey,
  familyKey,
  rankWeight,
  combineHot,
  buildIndex,
  byPreference,
  pickPrimary,
  resolveShow,
  createLedger,
  applyLedger,
};
