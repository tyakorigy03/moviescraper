/**
 * AgLive part-row de-dup.
 *
 * agasobanuyelive.com splits long shows and big movies into sequential pages
 * ("Wild Heart S01 (Yabani) Pt.1/2/3", "Jodhaa Akbar Part 1/2",
 * "The Secret Circle S01 [Pt.1]/[Pt.2]"). The scraper stores one moviesv2 row
 * per page, so one show becomes 2-3 "duplicates".
 *
 * Parts are additive and non-overlapping (Pt.1 = EP01-40, Pt.2 = EP41-80, …),
 * so unioning a part group's Downloadurls (deduped by locator) reconstructs the
 * full episode list under ONE row. Different seasons stay separate rows.
 */
const supabase = require('./supabaseClient');
const { coreTitle, locator } = require('../scrapers/agasobanuyenow/keys');
const { normNarrator } = require('../scrapers/rebamovie/mergeEntries');

const AGLIVE_BASE = 'agasobanuyelive.com';

/** Extract a season number from a title like "Show S01 ..." / "Show Season 3". Returns '' when none. */
function seasonOf(title = '') {
  const m = /\bS(\d{1,2})\b/i.exec(title) || /\bSeason\s*(\d{1,2})\b/i.exec(title);
  return m ? String(+m[1]) : '';
}

/** "Show S01 (X) Pt.1" → 1 ; "Show Part 2" → 2 ; "Show" → 0 (no part). */
function partNumOf(title = '') {
  const m = /\b(?:Pt\.?|Part)\s*(\d{1,3})\b/i.exec(title);
  return m ? +m[1] : 0;
}

/** Strip the trailing part marker: "Wild Heart S01 (Yabani) Pt.3" → "Wild Heart S01 (Yabani)". */
function baseTitleOf(title = '') {
  return String(title || '')
    .replace(/\s*(?:\(|\[)?\s*(?:Pt\.?|Part)\s*\d{1,3}\s*(?:\)|\])?$/i, '')
    .replace(/[\s\-–]+$/, '')
    .trim();
}

/** True for scraper-artifact rows (empty title / category link) we should delete. */
function isJunkRow(row = {}) {
  const link = String(row.link || '');
  const title = String(row.title || '').trim();
  if (!title) return true;
  if (/\/category\//i.test(link)) return true;
  return false;
}

/** Group key: same show, same type, same dub (narrator), same season. */
function partGroupKey(row = {}) {
  const season = seasonOf(String(row.title || ''));
  const type = String(row.type || 'movie').toLowerCase() === 'tv' ? 'tv' : 'movie';
  return `${coreTitle(row.title)}|${type}|${normNarrator(row.narrator)}|${season}`;
}

function normLocator(title = '') {
  return locator(String(title || '').trim()) || `t:${coreTitle(title)}`;
}

/** Book-keeping: "S01E02" style sort key keeps episodes in natural order. */
function sortKeyOf(loc = '') {
  const e = /^e(\d{1,3})$/.exec(loc);
  if (e) return `01-${String(+e[1]).padStart(4, '0')}`;
  const se = /^s(\d{1,3})e(\d{1,3})$/.exec(loc);
  if (se) return `${String(+se[1]).padStart(3, '0')}-${String(+se[2]).padStart(4, '0')}`;
  const p = /^p(.+)$/.exec(loc);
  if (p) return `09-${p[1]}`;
  return `zz-${loc}`;
}

/**
 * Collapse a list of scraped movie records: same-show+season part rows are
 * merged into ONE record (canonical = lowest part number, or part-less if any).
 * Returns a deduplicated array + stats. Pure — no DB access.
 */
function collapsePartRows(movies = []) {
  const groups = new Map();
  for (const movie of movies) {
    if (isJunkRow(movie)) continue;
    const key = partGroupKey(movie);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(movie);
  }

  const out = [];
  let merged = 0;
  for (const group of groups.values()) {
    if (group.length === 1) {
      out.push(group[0]);
      continue;
    }
    const parts = [...group].sort((a, b) => partNumOf(a.title) - partNumOf(b.title));
    const canonical = parts[0];
    const full = { ...canonical };
    full.title = baseTitleOf(canonical.title);
    full.link = canonical.link;
    full._sourceLinks = [...new Set(parts.map((p) => p.link))];
    full.Downloadurls = mergeEntryLists(parts.map((p) => p.Downloadurls));
    out.push(full);
    merged += parts.length - 1;
  }
  return { movies: out, merged };
}

/** Union of Downloadurls across part rows, deduped by locator, in natural order. */
function mergeEntryLists(lists = []) {
  const byLoc = new Map();
  for (const list of lists) {
    for (const entry of list || []) {
      const loc = normLocator(entry.title);
      const existing = byLoc.get(loc);
      // Prefer the entry that carries a download link.
      if (!existing || (!existing.downloadUrl && entry.downloadUrl)) {
        byLoc.set(loc, { ...entry });
      }
    }
  }
  return [...byLoc.entries()]
    .sort((a, b) => (sortKeyOf(a[0]) < sortKeyOf(b[0]) ? -1 : 1))
    .map(([, entry]) => entry);
}

/**
 * One-time in-DB cleanup: read all aglive rows, merge part groups, delete the
 * superseded rows. dryRun=true only reports what would happen.
 */
async function dedupeAgLivePartsInDb({ dryRun = false } = {}) {
  const rows = [];
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from('moviesv2')
      .select('id,title,type,narrator,link,release_year,Downloadurls,image,poster,publishedAt,modifiedAt')
      .like('link', `%${AGLIVE_BASE}%`)
      .range(from, from + 999);
    if (error) throw new Error(`read aglive rows: ${error.message}`);
    rows.push(...(data || []));
    if (!data || data.length < 1000) break;
    from += 1000;
  }

  const groups = new Map();
  const junkIds = [];
  for (const row of rows) {
    if (isJunkRow(row)) {
      junkIds.push(row.id);
      continue;
    }
    const key = partGroupKey(row);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const mergedRows = [];
  const deletedIds = [...junkIds];
  for (const group of groups.values()) {
    if (group.length === 1) continue;
    const parts = [...group].sort((a, b) => partNumOf(a.title) - partNumOf(b.title));
    const canonical = parts[0];
    const others = parts.slice(1);
    mergedRows.push({
      id: canonical.id,
      link: canonical.link,
      title: baseTitleOf(canonical.title),
      Downloadurls: mergeEntryLists(parts.map((p) => p.Downloadurls)),
      individualRows: parts.map((p) => ({ id: p.id, title: p.title, entries: (p.Downloadurls || []).length })),
    });
    deletedIds.push(...others.map((o) => o.id));
  }

  console.log(`aglive rows: ${rows.length}`);
  console.log(`junk rows to delete (no title / category link): ${junkIds.length}`);
  console.log(`part groups to merge: ${mergedRows.length}`);
  console.log(`rows to delete (superseded parts + junk): ${deletedIds.length}`);

  if (dryRun || !mergedRows.length) {
    for (const m of mergedRows.slice(0, 50)) {
      console.log(`  -> "${m.title}" (${m.link}) entries ${m.Downloadurls.length}: ${m.individualRows.map((r) => `${r.title}(${r.entries})`).join(' + ')}`);
    }
    return { mergedRows, deletedIds };
  }

  // Persist: update the canonical row, delete the superseded ones.
  let ok = 0;
  for (const m of mergedRows) {
    const { error } = await supabase
      .from('moviesv2')
      .update({
        title: m.title,
        Downloadurls: m.Downloadurls,
        modifiedAt: new Date().toISOString(),
      })
      .eq('link', m.link);
    if (error) {
      console.error(`  update failed "${m.link}": ${error.message}`);
      continue;
    }
    ok++;
  }
  if (deletedIds.length) {
    const { error } = await supabase.from('moviesv2').delete().in('id', deletedIds);
    if (error) console.error(`  delete failed: ${error.message}`);
  }
  console.log(`updated ${ok}/${mergedRows.length} canonical rows, deleted ${deletedIds.length} part rows`);
  return { mergedRows, deletedIds };
}

module.exports = { partGroupKey, partNumOf, baseTitleOf, seasonOf, collapsePartRows, mergeEntryLists, dedupeAgLivePartsInDb };