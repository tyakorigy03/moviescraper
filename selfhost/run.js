/**
 * Self-host one (or more) movies: download source → HEVC encode → upload to R2
 * → update Supabase Downloadurls + hosted flag.
 *
 * Usage:
 *   node selfhost/run.js --top 4 --budget-gb 12      # pick top 4 movies not yet hosted
 *   node selfhost/run.js --id <movie-uuid>           # process a single movie by id
 *   node selfhost/run.js --resume                     # resume after an interrupt
 *   node selfhost/run.js --force                      # re-process even if done/deployed
 *   node selfhost/run.js --list                       # just list this run's candidates
 */
const fs = require('fs-extra');
const path = require('path');
const config = require('./config');
const { logger, loadState, saveState } = require('./lib/state');
const { pickMovies, pickById, hasSelfHostedEntry } = require('./lib/pick');
const { resolveDownloadUrl, downloadToFile } = require('./lib/download');
const { convertFile } = require('./lib/convert');
const { uploadFile, objectKey, deleteObject } = require('./lib/upload');
const { deployMovieEntry, dehostMovie } = require('./lib/deploy');
const { mergeDuplicates } = require('./lib/merge');

const GB = 1024 * 1024 * 1024;
const MB = 1024 * 1024;

function parseArgs(argv) {
  const args = {
    id: null,
    top: config.topN,
    budgetGb: config.budgetGb,
    force: false,
    resume: false,
    list: false,
    merge: false,
    dryMerge: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--id') args.id = argv[++i] || null;
    else if (a === '--top') args.top = parseInt(argv[++i], 10) || config.topN;
    else if (a === '--budget-gb') args.budgetGb = parseFloat(argv[++i]) || config.budgetGb;
    else if (a === '--force') args.force = true;
    else if (a === '--resume') args.resume = true;
    else if (a === '--list') args.list = true;
    else if (a === '--merge') args.merge = true;
    else if (a === '--dry-merge') args.dryMerge = true;
  }
  return args;
}

function slugify(title = '') {
  const slug = title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  return slug || 'movie';
}

async function cleanup(paths) {
  for (const p of paths) {
    try {
      await fs.remove(p);
    } catch {
      /* best effort */
    }
  }
}

async function processMovie({ movie, entry, args, state }) {
  const id = movie.id;
  logger.info(`======== ${movie.title} (${id}) ========`);

  await fs.ensureDir(config.rawDir);
  await fs.ensureDir(config.outDir);

  const rawPath = path.join(config.rawDir, `${id}.src`);
  const finalName = `${slugify(movie.title)}-${id.slice(0, 8)}.mp4`;
  const outPath = path.join(config.outDir, finalName);

  try {
    // 1. Resolve + download the source file.
    const resolved = await resolveDownloadUrl(entry.downloadUrl);
    logger.info(`  source ${entry.downloadUrl}`);
    logger.info(`  resolved → ${resolved}`);
    const rawSize = await downloadToFile(resolved, rawPath);
    logger.info(`  downloaded ${Math.round(rawSize / MB)} MB`);

    // 2. Size the encode to the per-movie budget.
    const budgetMb = Math.max(
      config.minTargetMb,
      Math.min(config.maxTargetMb, (args.budgetGb * GB) / Math.max(1, args.top) / MB)
    );
    logger.info(`  per-movie size target: ${Math.round(budgetMb)} MB`);

    // 3. Encode H.265 (hvc1) at that size.
    const finalSize = await convertFile(rawPath, outPath, budgetMb * MB);

    // 4. Upload to R2 (skip if already present unless --force).
    const url = await uploadFile({ id, filename: finalName, filePath: outPath, forceUpload: args.force });

    // 5. Point the DB row at our file.
    await deployMovieEntry({ movie, entry, publicUrl: url });

    state.done[id] = {
      title: movie.title,
      file: objectKey(id, finalName),
      bytes: finalSize,
      url,
      score: movie.score ?? null,
      doneAt: new Date().toISOString(),
    };
    delete state.failed[id];
    await saveState(state);
    logger.info(`✔ ${movie.title} complete (${Math.round(finalSize / MB)} MB)`);
    return true;
  } catch (err) {
    logger.error(`✘ ${movie.title} failed: ${err.message}`);
    state.failed[id] = { title: movie.title, reason: err.message, at: new Date().toISOString() };
    await saveState(state);
    return false;
  } finally {
    await cleanup([rawPath, outPath]);
  }
}

/** Delete a movie's R2 file + revert its DB row, to free budget for a new one. */
async function evictMovie(state, id) {
  const rec = state.done[id];
  if (!rec) return 0;
  if (rec.file) {
    const ok = await deleteObject(rec.file);
    if (!ok) {
      logger.error(`eviction aborted for ${rec.title} — R2 delete failed, keeping row hosted`);
      return 0;
    }
  }
  await dehostMovie(id);
  const freed = rec.bytes || 0;
  delete state.done[id];
  await saveState(state);
  logger.info(`✚ ${rec.title} un-hosted (+${Math.round(freed / MB)} MB freed)`);
  return freed;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const state = await loadState();

  // Optional: before picking, fold duplicate rows (episodes released in later
  // scrapes) into one canonical row so we never carry duplicates around.
  if (args.merge || args.dryMerge) {
    logger.info(`----- duplicate merge (${args.dryMerge ? 'dry-run, --merge to commit' : 'APPLY'}) -----`);
    const res = await mergeDuplicates({ dryRun: args.dryMerge, limit: 200 });
    logger.info(
      `merge: ${res.merges.length} group(s)${args.dryMerge ? ' would be' : ''} merged,` +
        ` ${res.merges.reduce((s, m) => s + (m.rows - 1), 0)} duplicate row(s)` +
        `${args.dryMerge ? '' : ` deleted: ${res.deleted.length}`}`
    );
  }

  const candidates = args.id
    ? [await pickById(args.id)].filter(Boolean)
    : await pickMovies();

  // The sub-set that actually still needs work.
  const todo = candidates.filter(({ movie }) => {
    if (args.force) return true;
    if (state.done[movie.id]) return false;
    if (hasSelfHostedEntry(movie)) {
      logger.info(`skip (already has a Server HD entry): ${movie.title}`);
      return false;
    }
    return true;
  });

  const queued = args.id ? todo : todo.slice(0, args.top);
  logger.info(
    `candidates: ${candidates.length}, need work: ${todo.length}, queued (top ${Math.min(
      args.top,
      todo.length
    )}): ${queued.length}`
  );

  if (args.list) {
    for (const { movie, entry } of queued) {
      logger.info(`todo  ${movie.title} — ${entry.downloadUrl}`);
    }
    return;
  }

  if (args.id && queued.length === 0) {
    logger.info(`id ${args.id} already processed`);
    return;
  }

  // Rolling budget: keep total bytes on R2 inside args.budgetGb.  When a new
  // movie can't fit, evict oldest/lower-priority hosted movies until it does.
  const allowed = args.budgetGb * GB;
  const sliceMb = Math.max(
    config.minTargetMb,
    Math.min(config.maxTargetMb, allowed / Math.max(1, args.top) / MB)
  );
  const used = { v: Object.values(state.done).reduce((s, r) => s + (r.bytes || 0), 0) };
  logger.info(
    `budget ${args.budgetGb} GB, used ~${(used.v / GB).toFixed(2)} GB, per-movie slice ~${Math.round(
      sliceMb
    )} MB`
  );

  for (const { movie, entry } of queued) {
    // Evict until there is room for this movie's planned slice.
    while (used.v + sliceMb * MB > allowed) {
      const evictable = Object.entries(state.done)
        .filter(([id, rec]) => rec?.file && rec.bytes > 0)
        .sort((a, b) => (a[1].score ?? 0) - (b[1].score ?? 0));
      if (evictable.length === 0) {
        logger.error(`skip ${movie.title} — budget full, nothing left to evict`);
        used.overflowed = true;
        break;
      }
      const [evictId] = evictable[0];
      const freed = await evictMovie(state, evictId);
      used.v -= freed;
    }
    if (used.overflowed) break;

    const ok = await processMovie({ movie, entry, args, state });
    if (ok) used.v += state.done[movie.id].bytes;
  }

  const done = Object.keys(state.done).length;
  const failed = Object.keys(state.failed).length;
  const totalBytes = Object.values(state.done).reduce((s, r) => s + (r.bytes || 0), 0);
  logger.info(
    `run finished — hosted: ${done}, used: ${(totalBytes / GB).toFixed(2)} / ${args.budgetGb} GB, failed: ${failed}`
  );
}

main().catch((err) => {
  logger.error(`fatal: ${err.message}`);
  process.exit(1);
});