/**
 * Smart duplicate merge for moviesv2.
 *
 * Problem: TV/movie scrapes can arrive as multiple rows over time — e.g. a
 * season released in batches ("S01E01", then later "S01E02"). Each row owns a
 * slice of the show. We want ONE canonical row holding ALL Downloadurls
 * entries, with no conflicts and never deleting a self-hosted (hosted=true)
 * row.
 *
 * Merge rules (conservative):
 *  - group by normalized "core title" + type (years / S##E## / Episode N /
 *    Part N stripped);
 *  - never treat two rows with different narrators as the same (avoid merging
 *    different dubs/versions);
 *  - the survivor is the hosted row if any exists (self-hosted rows are never
 *    deleted), else the row with the highest score;
 *  - Downloadurls entries are merged and deduped by downloadUrl, watchUrl or
 *    title, with the survivor's entries kept first;
 *  - the survivor inherits hosted=true if ANY merged row was hosted;
 *  - non-survivor rows are deleted.
 */
const supabase = require('../../services/supabaseClient');
const { logger } = require('./state');

function coreTitle(raw = '') {
  return String(raw)
    .toLowerCase()
    .replace(/\b(?:19|20)\d{2}\b/g, ' ')                  // 2001, 1999 …
    .replace(/\bs\d{1,2}(?:\s*e\d{1,2})?\b/g, ' ')        // s01 / s01e02
    .replace(/\bepisode\s*\d+\b/g, ' ')                   // episode 5
    .replace(/\bpart\s*\d+\b/g, ' ')                      // part 2
    .replace(/\be\s*\d{1,3}\b/g, ' ')                     // e3
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function dedupeUrl(e) {
  return e?.downloadUrl || e?.watchUrl || String(e?.title || '');
}

/**
 * @returns {Promise<{merges: Array, deleted: Array<string>, changedSurvivors: Array<string>}>}
 */
async function mergeDuplicates({ dryRun = true, limit = 50 } = {}) {
  const { data: rows, error } = await supabase
    .from('moviesv2')
    .select('id,title,type,narrator,score,link,Downloadurls,hosted')
    .order('score', { ascending: false, nullsFirst: false });

  if (error) throw new Error(`merge fetch: ${error.message}`);

  const groups = new Map();
  for (const r of rows || []) {
    if (!r.title) continue;
    const key = `${coreTitle(r.title)}|${String(r.type || '').toLowerCase()}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(r);
  }

  const candidates = [...groups.values()]
    .filter((g) => g.length > 1)
    .sort((a, b) => b.length - a.length)
    .slice(0, limit);

  const deleted = [];
  const survivorsChanged = [];
  const merges = [];

  for (const group of candidates) {
    const core = coreTitle(group[0].title);

    // Conservative: skip when narrators disagree (different dubs/series of same name).
    const narrators = new Set(group.map((r) => (r.narrator || '').trim()).filter(Boolean));
    if (narrators.size > 1) {
      logger.debug(`merge skip (narrator mismatch): ${core}`);
      continue;
    }

    const survivors = group.filter((r) => r.hosted === true);
    const survivor =
      survivors[0] || group[0]; // group order: score desc, hosted row forced first

    const seen = new Set();
    const merged = [];
    for (const r of group) {
      const entries = Array.isArray(r.Downloadurls) ? r.Downloadurls : [];
      for (const e of entries) {
        const key = dedupeUrl(e);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        merged.push({ ...e });
      }
    }

    const dupes = group.filter((r) => r.id !== survivor.id);
    const hostedNow = group.some((r) => r.hosted === true);
    const entriesChanged = JSON.stringify(merged) !== JSON.stringify(survivor.Downloadurls || []);

    merges.push({
      key: core,
      type: survivor.type || null,
      survivorId: survivor.id,
      survivorTitle: survivor.title,
      rows: group.length,
      dupedIds: dupes.map((r) => r.id),
      hosted: hostedNow,
      changed: entriesChanged || hostedNow !== (survivor.hosted === true),
    });

    logger.info(
      `${dryRun ? '[DRY]' : '[APPLY]'} ${core} (${survivor.type || '?'}): ` +
        `${group.length} rows → merge into ${survivor.title} (${survivor.id}), ` +
        `${merged.length} download entries, hosted=${hostedNow}`
    );

    if (dryRun) continue;

    const update = {
      Downloadurls: merged,
      modifiedAt: new Date().toISOString(),
    };
    if (hostedNow) update.hosted = true;

    const { error: uErr } = await supabase
      .from('moviesv2')
      .update(update)
      .eq('id', survivor.id);

    if (uErr) {
      // 'hosted' may be missing — still apply Downloadurls merge.
      if (/column.*hosted.*does not exist/i.test(uErr.message)) {
        const { error: u2 } = await supabase
          .from('moviesv2')
          .update({ Downloadurls: merged, modifiedAt: update.modifiedAt })
          .eq('id', survivor.id);
        if (u2) {
          logger.error(`merge update failed for ${survivor.title}: ${u2.message}`);
          continue;
        }
      } else {
        logger.error(`merge update failed for ${survivor.title}: ${uErr.message}`);
        continue;
      }
    }

    if (dupes.length) {
      const { error: dErr } = await supabase
        .from('moviesv2')
        .delete()
        .in('id', dupes.map((r) => r.id));
      if (dErr) {
        logger.error(`merge delete failed for ${dupes.length} dupes of ${survivor.title}: ${dErr.message}`);
        continue;
      }
      deleted.push(...dupes.map((r) => `${r.title} (${r.id})`));
    }
    survivorsChanged.push(survivor.id);
  }

  return { merges, deleted, survivorsChanged, total: (rows || []).length };
}

module.exports = { mergeDuplicates, coreTitle };