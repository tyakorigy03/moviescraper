const { execFile } = require('child_process');
const { promisify } = require('util');
const os = require('os');
const path = require('path');
const fs = require('fs-extra');
const config = require('../config');
const { logger } = require('./state');

const execFileP = promisify(execFile);

const GB = 1024 * 1024 * 1024;

/** ffprobe → { durationSeconds, width, height } */
async function probe(inputPath) {
  const args = [
    '-v', 'error',
    '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height,duration:format=duration',
    '-of', 'json',
    inputPath,
  ];
  try {
    const { stdout } = await execFileP(config.ffprobePath, args, { maxBuffer: 16 * 1024 * 1024 });
    const json = JSON.parse(stdout);
    const stream = json.streams?.[0] || {};
    const duration = Number(stream.duration || json.format?.duration || 0);
    return {
      durationSeconds: duration > 0 ? duration : null,
      width: Number(stream.width) || 0,
      height: Number(stream.height) || 0,
    };
  } catch (err) {
    throw new Error(`ffprobe failed: ${err.message}`);
  }
}

/**
 * Compute video bitrate (bits/s) so the final file lands within the per-movie
 * budget. Clamped to sane bounds.
 */
function bitrateForBudget(probeInfo, targetBytes) {
  const audioBits = parseBitrate(config.audioBitrate) * config.audioChannels;
  const duration = probeInfo.durationSeconds;
  if (!duration || duration <= 0) throw new Error('could not determine source duration');

  const maxVideoBits = Math.max(100_000, (targetBytes * 8) / duration - audioBits);
  // clamp: 500 kbps floor, 6 Mbps ceiling (1080p HEVC stays sharp in 900-1500MB)
  return Math.max(500_000, Math.min(6_000_000, Math.round(maxVideoBits)));
}

function parseBitrate(value) {
  const m = String(value).match(/^(\d+(?:\.\d+)?)\s*([kKmM])?$|^(\d+)\s*$/);
  const n = Number(m?.[1] || m?.[3] || 0);
  const unit = m?.[2] || '';
  const mult = unit === 'M' ? 1000 * 1000 : unit === 'm' ? 1000 * 1000 : unit === 'K' || unit === 'k' ? 1000 : 1;
  return n * mult;
}

const videoArgs = (bitrate) => {
  const scale = `scale=w='min(1920,iw)':h=-2:force_original_aspect_ratio=decrease`;
  return [
    '-map', '0:v:0',
    '-map', '0:a?',
    '-vf', scale,
    '-c:v', 'libx265',
    '-preset', config.preset,
    '-tag:v', 'hvc1',
    '-b:v', String(bitrate),
    '-maxrate', String(Math.round(bitrate * 1.2)),
    '-bufsize', String(bitrate * 2),
    '-x265-params', `aq-mode=3:strict-cbr=0`,
    '-c:a', 'aac',
    '-b:a', config.audioBitrate,
    '-ac', String(config.audioChannels),
    '-movflags', '+faststart',
    '-max_muxing_queue_size', '9999',
    '-loglevel', 'error',
    '-y',
  ];
};

/**
 * Two-pass HEVC encode of `inputPath` → `outputPath`, sized to `targetBytes`.
 */
async function convertFile(inputPath, outputPath, targetBytes) {
  const info = await probe(inputPath);
  logger.info(
    `  source ${info.width}x${info.height} · ${Math.round((info.durationSeconds || 0) / 60)}m ` +
      `→ target ${Math.round((targetBytes / 1024 / 1024))} MB`
  );

  const bitrate = bitrateForBudget(info, targetBytes);
  logger.info(`  encode bitrate ≈ ${(bitrate / 1000).toFixed(0)} kbps (2-pass libx265)`);

  const nullDevice = os.platform() === 'win32' ? 'NUL' : '/dev/null';

  await execFileP(config.ffmpegPath, ['-i', inputPath, ...videoArgs(bitrate), '-an', '-pass', '1', nullDevice], {
    maxBuffer: 16 * 1024 * 1024,
  });

  await execFileP(config.ffmpegPath, ['-i', inputPath, ...videoArgs(bitrate), '-pass', '2', outputPath], {
    maxBuffer: 16 * 1024 * 1024,
  });

  const size = (await fs.stat(outputPath)).size;
  logger.info(`  done · ${Math.round(size / 1024 / 1024)} MB`);
  return size;
}

module.exports = { convertFile, probe };