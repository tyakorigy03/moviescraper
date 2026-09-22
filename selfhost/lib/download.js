const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const stream = require('stream');
const { promisify } = require('util');
const { logger } = require('./state');

const pipeline = promisify(stream.pipeline);

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36';

const resolveHost = (url) => {
  try {
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
};

/**
 * mediafire.com/file/<quick_key>/... → its public get_info API returns a JSON
 * file profile with a direct_download link we can fetch without the browser.
 */
async function resolveMediafire(originalUrl) {
  const u = new URL(originalUrl);
  const quickKey = u.pathname.split('/').filter(Boolean)[1];
  if (!quickKey) throw new Error(`mediafire: no quick_key in ${originalUrl}`);

  const api = `https://www.mediafire.com/api/1.5/file/get_info.php?quick_key=${encodeURIComponent(
    quickKey
  )}&response_format=json`;

  const { data } = await axios.get(api, { timeout: 30000, headers: { 'User-Agent': UA } });
  const info =
    data?.response?.file_info ||
    data?.response?.file_download?.file_info ||
    data?.response?.file_download ||
    data?.response ||
    {};
  const links = info.links || {};
  const direct = links.direct_download || links.normal_download;
  if (!direct) throw new Error(`mediafire: no direct link resolved for ${originalUrl}`);
  return direct;
}

/**
 * anonsharing.com/file/<id>/<name>.mp4 — the file page answer whitelists a real
 * media file. We return a URL that streams the file bytes. Tries in order:
 *   1. HEAD on the URL itself (many anonsharing files ARE served directly)
 *   2. /dl/<id> and /download/<id> shortcuts
 *   3. scrape the page for a download href
 */
async function resolveAnonSharing(originalUrl) {
  const u = new URL(originalUrl);
  const segments = u.pathname.split('/').filter(Boolean);
  const fileId = segments[1]; // /file/<id>/<filename>

  const looksVideo = (contentType = '') =>
    /video|octet-stream|binary/i.test(contentType);

  for (const candidate of [
    originalUrl,
    fileId ? `https://anonsharing.com/dl/${fileId}` : '',
    fileId ? `https://anonsharing.com/download/${fileId}` : '',
  ]) {
    if (!candidate) continue;
    try {
      const head = await axios.head(candidate, {
        timeout: 20000,
        maxRedirects: 5,
        headers: { 'User-Agent': UA, Accept: '*/*' },
      });
      const type = String(head.headers['content-type'] || '');
      if (looksVideo(type) || Number(head.headers['content-length'] || 0) > 5_000_000) {
        return candidate;
      }
    } catch {
      // try the next candidate
    }
  }

  // Last resort: parse the HTML page for a download link.
  const { data: html } = await axios.get(originalUrl, {
    timeout: 30000,
    headers: { 'User-Agent': UA },
  });
  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map((m) => m[1]);
  for (const hrefKey of ['/dl/', '/download/', 'direct']) {
    const match = hrefs.find((h) => h.includes(hrefKey));
    if (match) return new URL(match, originalUrl).toString();
  }
  throw new Error(`anonsharing: could not resolve a direct link for ${originalUrl}`);
}

/** Resolve a downloadUrl to a streamable file URL. */
async function resolveDownloadUrl(url) {
  const host = resolveHost(url);
  if (host === 'mediafire.com') return resolveMediafire(url);
  if (host === 'anonsharing.com') return resolveAnonSharing(url);
  throw new Error(`unsupported download host: ${host}`);
}

/** Stream a resolved URL to disk. Returns the on-disk size in bytes. */
async function downloadToFile(url, destPath) {
  await fs.ensureDir(path.dirname(destPath));

  const resp = await axios.get(url, {
    responseType: 'stream',
    timeout: 0,
    maxRedirects: 5,
    headers: {
      'User-Agent': UA,
      Accept: '*/*',
      Referer: url,
    },
    validateStatus: (s) => s >= 200 && s < 300,
    onDownloadProgress: undefined,
  });

  const expected = Number(resp.headers['content-length'] || 0);
  const out = fs.createWriteStream(destPath);
  let bytes = 0;

  resp.data.on('data', (chunk) => {
    bytes += chunk.length;
    if (bytes % (50 * 1024 * 1024) < 100 * 1024) {
      logger.info(
        `  download ${Math.round(bytes / 1024 / 1024)} MB${expected ? ` / ${Math.round(expected / 1024 / 1024)} MB` : ''}`
      );
    }
  });

  await pipeline(resp.data, out);
  const size = (await fs.stat(destPath)).size;
  return size;
}

module.exports = { resolveDownloadUrl, downloadToFile, resolveHost };