/**
 * Standalone duplicate-merge runner (also invoked by run.js with --merge).
 *
 * Usage:
 *   node selfhost/merge.js                 # dry-run: report only
 *   node selfhost/merge.js --apply         # actually merge + delete dupes
 *   node selfhost/merge.js --limit 20      # cap how many groups are handled
 */
const { logger, loadState } = require('./lib/state');
const { mergeDuplicates } = require('./lib/merge');

function parseArgs(argv) {
  const args = { apply: false, limit: 50 };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--apply') args.apply = true;
    else if (argv[i] === '--limit') args.limit = parseInt(argv[++i], 10) || 50;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  await loadState(); // ensures logger config is loaded

  logger.info(`merge mode: ${args.apply ? 'APPLY' : 'DRY-RUN'} (--apply to commit)`);
  const res = await mergeDuplicates({ dryRun: !args.apply, limit: args.limit });

  logger.info(
    `scanned ${res.total} rows — ` +
      `${res.merges.length} group(s) to merge, ` +
      `${dryCount(res)} duplicate row(s)` +
      (args.apply ? `, ${res.deleted.length} deleted` : '')
  );
}

function dryCount(res) {
  return res.merges.reduce((s, m) => s + (m.rows - 1), 0);
}

main().catch((err) => {
  logger.error(`merge failed: ${err.message}`);
  process.exit(1);
});