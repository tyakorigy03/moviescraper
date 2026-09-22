/**
 * Self-host pipeline configuration.
 *
 * Every value can be overridden via env vars (see ../.env.example). Defaults
 * are tuned for the Oracle free-tier VM (Ampere A1, 4 OCPU) + Cloudflare R2.
 */
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env'), override: false });

const env = (name, fallback = '') => {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
};

// R2 Endpoint may be pasted with the bucket path appended, e.g.
//   https://<ACCOUNT_ID>.r2.cloudflarestorage.com/filimehome
// The bucket belongs in r2.bucket, so keep only scheme+host here. Defaults to
// the Cloudflare account this project uses; still overridable via R2_ENDPOINT.
const r2Endpoint = env('R2_ENDPOINT', 'https://05378e4f5f3be1bb3e7afa235f590b09.r2.cloudflarestorage.com')
  .replace(/\/+$/, '')
  .replace(/(https?:\/\/[^/]+)\/.*/, '$1');

// Bucket matches the account above. Public base is derived from the S3
// endpoint by default so nothing needs configuring; see the R2_PUBLIC_BASE_URL
// note in ../.env.example (enable R2 "Public access" for real public URLs).
const r2Bucket = env('R2_BUCKET', 'filimehome');
const publicBaseUrl =
  env('R2_PUBLIC_BASE_URL', '').replace(/\/+$/, '') || `${r2Endpoint}/${r2Bucket}`;

if (!env('R2_PUBLIC_BASE_URL')) {
  console.warn(
    '[selfhost] R2_PUBLIC_BASE_URL not set — using derived S3-style URLs. Enable Public access on the R2 bucket (r2.dev subdomain) so these links are reachable by users.'
  );
}

const WORK_DIR = env(
  'SELFHOST_WORK_DIR',
  path.join(require('os').homedir(), '.filimehome-selfhost')
);

module.exports = {
  supabaseUrl: env('SUPABASE_URL'),
  supabaseKey: env('SUPABASE_KEY'),

  // How many movies to process per run / in total, and the storage budget.
  // 9.5 GB default keeps us comfortably inside Cloudflare R2's 10 GB free tier.
  topN: parseInt(env('SELFHOST_TOP_N', '20'), 10),
  budgetGb: parseFloat(env('SELFHOST_BUDGET_GB', '9.5')),

  // Per-movie output size bounds (MB). The bitrate is derived from the budget.
  minTargetMb: parseFloat(env('SELFHOST_MIN_TARGET_MB', '400')),
  maxTargetMb: parseFloat(env('SELFHOST_MAX_TARGET_MB', '1500')),

  // Encode settings. hevc = libx265 (hvc1 tag so Apple devices play it).
  codec: env('SELFHOST_CODEC', 'hevc'),
  preset: env('SELFHOST_PRESET', 'medium'),
  audioBitrate: env('SELFHOST_AUDIO_BITRATE', '128k'),
  audioChannels: parseInt(env('SELFHOST_AUDIO_CHANNELS', '2'), 10),
  maxHeight: parseInt(env('SELFHOST_MAX_HEIGHT', '1080'), 10),

  ffmpegPath: env('FFMPEG_PATH', 'ffmpeg'),
  ffprobePath: env('FFPROBE_PATH', 'ffprobe'),

  // Temporary working area. MUST be outside the git repo so it can never leak
  // into GH Actions' `git add storage/*.json` commit step. Converted files are
  // uploaded to R2 and then deleted here to free the 200GB block volume.
  workDir: WORK_DIR,
  rawDir: path.join(WORK_DIR, 'raw'),
  outDir: path.join(WORK_DIR, 'out'),
  stateFile: path.join(WORK_DIR, 'state.json'),

  // Cloudflare R2 (S3-compatible). endpoint like
  // https://<ACCOUNT_ID>.r2.cloudflarestorage.com
  r2: {
    endpoint: r2Endpoint,
    accessKeyId: env('R2_ACCESS_KEY_ID'),
    secretAccessKey: env('R2_SECRET_ACCESS_KEY'),
    bucket: r2Bucket,
    prefix: env('R2_PREFIX', 'movies'),
    // Public base for served objects, no trailing slash, e.g.
    // https://pub-<HASH>.r2.dev  (free R2 "Public access" subdomain)
    publicBaseUrl,
  },

  logLevel: env('SELFHOST_LOG_LEVEL', 'info'),
};