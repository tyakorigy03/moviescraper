/**
 * One-time cleanup for the duplicate-append bug in rebamovie's mergeEntries.
 *
 * The append guard used to compare FULL download/watch URLs, so every re-minted
 * Wix signature looked like a brand-new episode and got appended as a copy.
 * Rows ballooned (one title held 313 entries for 25 real episodes) and each
 * copy rendered its own play button. mergeEntries now compares media identity
 * (GUID + rendition) and self-heals duplicate slots, so the scheduled scraper
 * repairs rows as it visits them — but that drains slowly.
 *
 * This script applies the exact same self-heal to every row up front:
 *   node utils/backfillRebaEntries.js            # dry run (default, no writes)
 *   node utils/backfillRebaEntries.js --apply    # actually write
 *   node utils/backfillRebaEntries.js --apply --limit 20
 *
 * Nothing is deleted outright: every dropped downloadUrl is archived into the
 * surviving entry's `oldDownloadUrl` (" | "-joined), matching the convention
 * mergeEntries already uses. The script refuses to touch rows whose downloads
 * would lose an unarchived URL.
 */
require('dotenv').config();
const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { mergeEntries } = require('../scrapers/rebamovie/mergeEntries');

const TABLE = 'moviesv2';
const BATCH_SIZE = 200;

/* ------------------------------- helpers -------------------------------- */

const archivedParts = (e) => ((e && e.oldDownloadUrl) || '').split(' | ').filter(Boolean);

/**
 * Safety check: every downloadUrl present before the collapse must either
 * survive on a kept entry or be recoverable from some entry's oldDownloadUrl.
 * A row that fails this is reported and skipped, never written.
 */
function lossesUnarchived(before, after) {
  const kept = new Set(after.map((e) => e.downloadUrl).filter(Boolean));
  const archived = new Set();
  for (const e of after) for (const p of archivedParts(e)) archived.add(p);
  const missing = [];
  for (const e of before) {
    const u = e.downloadUrl;
    if (!u) continue;
    if (kept.has(u) || archived.has(u)) continue;
    missing.push(u);
  }
  return [...new Set(missing)];
}

/* -------------------------------- main ---------------------------------- */

async function main() {
  const args = process.argv.slice(2);
  const apply = args.includes('--apply');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) || 0 : 0;

  logInfo(`backfill: mode = ${apply ? 'APPLY (writes to Supabase)' : 'DRY RUN (no writes)'}`);

  let from = 0;
  let scanned = 0;
  let changed = 0;
  let entriesBefore = 0;
  let entriesAfter = 0;
  let skipped = 0;
  let applied = 0;
  const pending = [];

  while (true) {
    const { data: rows, error } = await supabase
      .from(TABLE)
      .select('id,title,Downloadurls')
      .order('id')
      .range(from, from + BATCH_SIZE - 1);

    if (error) {
      logError(`backfill: fetch failed at offset ${from}: ${error.message}`);
      break;
    }
    if (!rows || !rows.length) break;

    for (const row of rows) {
      scanned++;
      if (limit > 0 && changed >= limit) break;

      const existing = Array.isArray(row.Downloadurls) ? row.Downloadurls : [];
      if (existing.length < 2) continue;

      const { entries, changed: didChange } = mergeEntries(existing, []);
      if (!didChange) continue;

      const lost = lossesUnarchived(existing, entries);
      if (lost.length) {
        skipped++;
        logError(`backfill: SKIPPED "${row.title}" — ${lost.length} download(s) would be unarchived`);
        continue;
      }

      changed++;
      entriesBefore += existing.length;
      entriesAfter += entries.length;
      pending.push({ id: row.id, title: row.title, from: existing.length, to: entries.length, entries });
    }

    if (rows.length < BATCH_SIZE) break;
    if (limit > 0 && changed >= limit) break;
    from += BATCH_SIZE;
    logInfo(`backfill: scanned ${scanned} row(s), ${changed} would shrink...`);
  }

  logInfo('─'.repeat(60));
  logInfo(`scanned rows:        ${scanned}`);
  logInfo(`rows to collapse:    ${changed}`);
  logInfo(`skipped (unsafe):    ${skipped}`);
  logInfo(`entries:             ${entriesBefore} -> ${entriesAfter}`);
  if (entriesBefore) {
    logInfo(`reduction:           ${((1 - entriesAfter / entriesBefore) * 100).toFixed(1)}%`);
  }
  logInfo('─'.repeat(60));

  const worst = pending.slice().sort((a, b) => b.from - b.to - (a.from - a.to)).slice(0, 10);
  for (const p of worst) {
    logInfo(`  ${String(p.title).slice(0, 32).padEnd(32)} ${String(p.from).padStart(3)} -> ${String(p.to).padStart(3)}`);
  }

  if (!apply) {
    logInfo('DRY RUN — nothing was written. Re-run with --apply to persist.');
    return { changed, skipped };
  }

  for (let i = 0; i < pending.length; i += BATCH_SIZE) {
    const chunk = pending.slice(i, i + BATCH_SIZE);
    const { error: upErr } = await supabase
      .from(TABLE)
      .upsert(chunk.map((p) => ({ id: p.id, Downloadurls: p.entries })), { onConflict: ['id'] });
    if (upErr) {
      logError(`backfill: write failed for batch at ${i}: ${upErr.message}`);
      break;
    }
    applied += chunk.length;
    logInfo(`backfill: wrote ${applied}/${pending.length} row(s)...`);
  }

  logInfo(`backfill finished — ${applied} row(s) collapsed.`);
  return { changed, skipped, applied };
}

if (require.main === module) {
  main().catch((err) => {
    logError(`backfill failed: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { main, lossesUnarchived };
