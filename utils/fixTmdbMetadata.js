/**
 * TMDB metadata repair (npm run fix:tmdb).
 *
 * An audit of moviesv2 (utils/fixTmdbMetadata.js --audit) found that the
 * enricher does not reach every row of a work:
 *
 *   - a season/part row often has no TMDB data at all while a sibling row of
 *     the same show carries it ("Beauty In Black  S03" had nothing, S01/S02
 *     both carried the show entry 246246);
 *   - a duplicate row of the same show often loses its artwork ("Beauty In
 *     Black  S02" had poster_small/medium/large but a null image/poster);
 *   - a few rows are matched to the wrong film entirely ("Black Box" matched
 *     1650803, an unrelated film about a photographer, instead of 1321008).
 *
 * Only the unambiguous cases are repaired automatically:
 *
 *   fill-tmdb   copy the show's TMDB block onto a row that has none, but only
 *               when the group resolves to exactly ONE distinct tmdb_id. If a
 *               show points at several ids they may genuinely be different
 *               films ("Re:Born" 408866 vs "Reborn" 542330), so those are left
 *               for a human.
 *   fill-image  copy artwork from the group's canonical row (the one whose
 *               title carries no season/part marker).
 *   override    explicit, hand-verified remaps, listed below with a reason.
 *
 * Nothing is deleted and no existing value is overwritten: every fill targets a
 * column that is currently null. Affected rows are re-scored, because a rating
 * and popularity feed computeRelevanceScore.
 *
 * Usage:
 *   node utils/fixTmdbMetadata.js --audit     # report only
 *   node utils/fixTmdbMetadata.js --dry       # report the planned writes
 *   node utils/fixTmdbMetadata.js             # apply
 */
require('dotenv').config();
const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { computeRelevanceScore } = require('./relevanceScore');
const { loadHotSnapshot } = require('./hotSignals');
const { showKey } = require('./hotMatching');

const TABLE = 'moviesv2';
const BATCH_SIZE = 200;

/** TMDB columns a show-level match provides. `image`/`poster` are site-owned. */
const TMDB_FIELDS = ['tmdb_id', 'tmdb_type', 'tmdb_title', 'tmdb_year', 'tmdb_rating', 'popularity', 'tmdb_overview'];
const ART_FIELDS = ['image', 'poster'];

/**
 * Hand-verified wrong matches. Each entry names the TMDB entry the row should
 * point at; the correct data is copied from `from` (any sibling row already
 * matched to it), so nothing is invented here.
 */
const OVERRIDES = [
  {
    link: 'https://pub-e93ca026964e48dab84e209dd9964abd.r2.dev/movies/self/5a09bf7c-4d6a-413a-a116-eefd3d1df467/black-box-2026-perfect.mp4',
    title: 'Black Box',
    toTmdbId: 1321008,
    reason:
      'was matched to TMDB 1650803 ("An elderly photographer studies a series of photographs"), an unrelated film. ' +
      '1321008 is the 2026 Korean disaster film this row is, and the sibling Black Box row already matches it. ' +
      'Both rows share the same wix poster asset, confirming one film from two providers.',
  },
];

/* --------------------------------- load ------------------------------------ */

async function loadRows() {
  const all = [];
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(TABLE)
      .select(
        [
          'link', 'title', 'type', 'score',
          'tmdb_id', 'tmdb_type', 'tmdb_title', 'tmdb_year', 'tmdb_rating', 'popularity', 'tmdb_overview',
          'image', 'poster', 'poster_small', 'poster_medium', 'poster_large',
          'publishedAt', 'modifiedAt', 'tmdb_id',
        ].join(',')
      )
      .range(from, from + 999);
    if (error) throw new Error(`rows: ${error.message}`);
    all.push(...(data || []));
    if (!data || data.length < 1000) break;
    from += 1000;
  }
  return all;
}

/* -------------------------------- planning --------------------------------- */

/** Title that represents the whole work, i.e. no season/part/episode marker. */
const isCanonical = (title) => !/\b(?:s\d{1,2}|season|episode|ep|pt|part|igice|gbice)\b/i.test(String(title || ''));

function plan(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = showKey(row.title);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const patches = new Map(); // link -> { link, ...fields }
  const notes = [];
  const touchedLinks = new Set();

  const patch = (row, fields, why) => {
    const empty = {};
    for (const [k, v] of Object.entries(fields)) {
      if (v === null || v === undefined || v === '') continue;
      if (row[k] !== null && row[k] !== undefined && row[k] !== '') continue; // never overwrite
      empty[k] = v;
    }
    if (!Object.keys(empty).length) return;
    patches.set(row.link, { link: row.link, ...(patches.get(row.link) || {}), ...empty });
    touchedLinks.add(row.link);
    notes.push({ link: row.link, title: row.title, fields: Object.keys(empty), why });
  };

  // --- explicit wrong-match overrides -------------------------------------
  const byLink = new Map(rows.map((r) => [r.link, r]));
  for (const o of OVERRIDES) {
    const row = byLink.get(o.link);
    if (!row) {
      notes.push({ link: o.link, title: o.title, fields: [], why: `OVERRIDE SKIPPED: row not found (${o.reason})` });
      continue;
    }
    const donor = rows.find((r) => r.tmdb_id === o.toTmdbId && r.link !== o.link);
    if (!donor) {
      notes.push({ link: o.link, title: o.title, fields: [], why: `OVERRIDE SKIPPED: no row already matched to ${o.toTmdbId}` });
      continue;
    }
    if (row.tmdb_id === o.toTmdbId) continue;
    // a wrong tmdb_id is a real value, so replace it explicitly rather than
    // going through the fill-only path
    const fields = {};
    for (const f of TMDB_FIELDS) if (donor[f] !== null && donor[f] !== undefined && donor[f] !== '') fields[f] = donor[f];
    for (const f of ['poster_small', 'poster_medium', 'poster_large']) {
      if (!row[f] && donor[f]) fields[f] = donor[f];
    }
    if (!row.image && donor.image) fields.image = donor.image;
    if (row.tmdb_id) delete patches.get(row.link)?.tmdb_id;
    patches.set(row.link, { link: row.link, ...(patches.get(row.link) || {}), ...fields });
    touchedLinks.add(row.link);
    notes.push({ link: o.link, title: row.title, fields: Object.keys(fields), why: `OVERRIDE -> ${o.toTmdbId} (was ${row.tmdb_id}): ${o.reason}` });
  }

  // --- fill missing TMDB from a sibling -----------------------------------
  for (const [key, members] of groups) {
    const withId = members.filter((r) => r.tmdb_id);
    const distinct = [...new Set(withId.map((r) => r.tmdb_id))];
    if (distinct.length !== 1) continue; // 0 = nothing to copy, >1 = ambiguous
    const donor = withId.reduce((a, b) => (b.tmdb_rating ? b : a), withId[0]);
    for (const row of members) {
      if (row.tmdb_id) continue;
      const fields = {};
      for (const f of TMDB_FIELDS) if (donor[f] !== null && donor[f] !== undefined && donor[f] !== '') fields[f] = donor[f];
      if (!fields.tmdb_id) continue;
      patch(row, fields, `fill-tmdb from ${donor.title} (tmdb_id ${donor.tmdb_id})`);
    }
  }

  // --- fill missing artwork from the best donor in the group ---------------
  // Preference, best first: a donor for the *same* season (a duplicate upload
  // of this very card), then the canonical whole-work row, then the row with the
  // most TMDB metadata. Picking "whichever row came back first" would hand
  // "Beauty In Black S02" the S03 poster file, which is misleading on the card.
  const seasonOf = (title) => {
    const m = /\bs(\d{1,2})\b/i.exec(String(title || '')) || /\bseason\s*(\d{1,2})\b/i.exec(String(title || ''));
    return m ? Number(m[1]) : 0;
  };
  const richness = (r) => TMDB_FIELDS.filter((f) => r[f] !== null && r[f] !== undefined && r[f] !== '').length;

  for (const [, members] of groups) {
    const withImage = members.filter((r) => r.image);
    if (!withImage.length) continue;
    for (const row of members) {
      if (row.image) continue;
      const want = seasonOf(row.title);
      const donor = withImage
        .filter((d) => d !== row)
        .sort((a, b) => {
          const sa = seasonOf(a.title) === want && want ? 0 : 1;
          const sb = seasonOf(b.title) === want && want ? 0 : 1;
          return sa - sb || (isCanonical(b.title) ? 1 : 0) - (isCanonical(a.title) ? 1 : 0) || richness(b) - richness(a);
        })[0];
      if (donor) patch(row, { image: donor.image, poster: donor.poster || donor.image }, `fill-image from ${donor.title}`);
    }
  }

  return { patches: [...patches.values()], notes, groups, touched: touchedLinks };
}

/* --------------------------------- audit ----------------------------------- */

async function audit(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = showKey(row.title);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  const count = { 'conflicting tmdb ids': 0, 'id but no rating': 0, 'missing tmdb (sibling has it)': 0, 'missing image (sibling has it)': 0, 'missing tmdb poster': 0 };
  const examples = [];
  const add = (kind, key, detail) => {
    count[kind]++;
    if (examples.length < 400 && examples.filter((e) => e.kind === kind).length < 6) examples.push({ kind, key, detail });
  };
  for (const [key, members] of groups) {
    const ids = [...new Set(members.map((r) => r.tmdb_id).filter(Boolean))];
    if (ids.length > 1) add('conflicting tmdb ids', key, members.map((r) => `${r.title} -> ${r.tmdb_id}`).join(' || '));
    for (const r of members) if (r.tmdb_id && !r.tmdb_rating) add('id but no rating', key, `${r.title} -> ${r.tmdb_id} "${r.tmdb_title}"`);
    const rated = members.filter((r) => r.tmdb_id && r.tmdb_rating);
    if (rated.length) {
      for (const r of members) {
        if (!r.tmdb_id) add('missing tmdb (sibling has it)', key, `${r.title}; ${rated[0].title} -> ${rated[0].tmdb_id}`);
        else if (!r.poster_small) add('missing tmdb poster', key, `${r.title} id ${r.tmdb_id}`);
      }
    }
    const withImg = members.filter((r) => r.image);
    if (withImg.length) for (const r of members) if (!r.image) add('missing image (sibling has it)', key, `${r.title}; ${withImg[0].title} has one`);
  }
  logInfo('\n  TMDB metadata audit');
  for (const [k, n] of Object.entries(count)) logInfo(`    ${String(n).padStart(4)}  ${k}`);
  logInfo('\n  examples:');
  for (const e of examples.filter((e) => ['conflicting tmdb ids', 'missing tmdb (sibling has it)', 'missing image (sibling has it)'].includes(e.kind))) {
    logInfo(`    [${e.kind}] ${e.key}: ${e.detail}`);
  }
  return count;
}

/* --------------------------------- main ------------------------------------ */

async function main() {
  const args = process.argv.slice(2);
  const dry = args.includes('--dry');
  const only = args.includes('--audit');

  const rows = await loadRows();
  logInfo(`fix:tmdb: ${rows.length} row(s) loaded.`);

  if (only) {
    await audit(rows);
    return { patched: 0 };
  }

  const { patches, notes, touched } = plan(rows);
  logInfo(`fix:tmdb: ${patches.length} row(s) to update across ${touched.size} show(s).`);
  for (const n of notes) {
    logInfo(`  ${n.title}  [${n.fields.join(', ') || 'none'}]`);
    logInfo(`      ${n.why}`);
  }
  if (!patches.length) return { patched: 0 };

  if (dry) {
    logInfo('fix:tmdb: --dry, no writes.');
    return { patched: patches.length };
  }

  // Metadata feeds the score, so re-score every touched row with the hot value
  // the snapshot already assigned it.
  const signals = (await loadHotSnapshot()).signals || {};
  const byLink = new Map(rows.map((r) => [r.link, r]));
  const withScores = [];
  for (const p of patches) {
    const row = { ...byLink.get(p.link), ...p };
    const hot = signals[p.link] ? Number(signals[p.link].hot) || 0 : 0;
    p.score = computeRelevanceScore({
      tmdb_rating: row.tmdb_rating || 0,
      popularity: row.popularity || 0,
      publishedAt: row.publishedAt || '',
      modifiedAt: row.modifiedAt || '',
      narrator: row.narrator || '',
      title: row.title || '',
      hot,
    });
    withScores.push(p);
  }

  for (let i = 0; i < withScores.length; i += BATCH_SIZE) {
    const { error } = await supabase.from(TABLE).upsert(withScores.slice(i, i + BATCH_SIZE), { onConflict: 'link' });
    if (error) logError(`fix:tmdb upsert batch failed: ${error.message}`);
  }
  logInfo(`fix:tmdb: applied ${withScores.length} row(s).`);
  return { patched: withScores.length };
}

if (require.main === module) {
  main()
    .then((r) => logInfo(`fix:tmdb finished — ${r.patched} row(s) patched.`))
    .catch((err) => {
      logError(`fix:tmdb fatal: ${err.message}`);
      process.exit(1);
    });
}

module.exports = { main, plan, loadRows };
