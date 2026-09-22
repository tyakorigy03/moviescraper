/**
 * agasobanuyenow CDN URL resolver.
 *
 * The site uses its own media CDN (media.agasobanuyenow.com) for real MP4
 * files. Direct mp4 hrefs are present in the static HTML of movie pages and
 * episode watch pages. Old movies can break the pattern, so every resolution
 * goes through a ladder:
 *
 *   1. scrape the page for a media.agasobanuyenow.com .mp4 href  (authoritative)
 *   2. derive the URL from the {Title} - {Narrator}.mp4 convention and verify it
 *   3. fall back to watch-only (no download link)
 */
const axios = require('axios');
const { CDN_HOST, isCdnHost } = require('./keys');
const { logInfo } = require('../../utils/logger');

const BASE = 'https://agasobanuyenow.com';
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';

async function fetchHtml(url) {
  const res = await axios.get(url, {
    timeout: 30000,
    maxRedirects: 5,
    headers: { 'User-Agent': UA, Accept: 'text/html,*/*', Referer: `${BASE}/` },
    validateStatus: (s) => s >= 200 && s < 400,
  });
  if (res.status >= 400 || !res.headers['content-type']?.includes('html')) {
    throw new Error(`unexpected response ${res.status} from ${url}`);
  }
  return res.data || '';
}

function extractMp4Urls(html = '') {
  const urls = [];
  const re = /https:\/\/media\.agasobanuyenow\.com\/[^"'\s<>]+?\.mp4/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const url = m[0];
    // clean trailing punctuation that regex may have swallowed
    const clean = url.replace(/[\\'")\];,]$/g, '');
    if (!urls.includes(clean)) urls.push(clean);
  }
  // /download/ paths first (those are the friendly names), narrator variants last.
  return urls.sort((a, b) => Number(a.includes('/download/')) > Number(b.includes('/download/')) ? -1 : 0);
}

/** Prefer the variant that includes the narrator's name, else the first. */
function pickBest(urls = [], narrator = '') {
  if (!urls.length) return null;
  const low = String(narrator || '').toLowerCase().split(/\s+/).filter(Boolean);
  const named = urls.find((u) => low.some((tok) => u.toLowerCase().includes(tok) && tok.length > 2));
  return named || urls[0];
}

function extractWatchUrl(html = '', fallback) {
  const src = /<(?:iframe|video)[^>]+src=["']([^"']+)["']/i.exec(html);
  if (src && src[1] && /^https?:\/\//i.test(src[1])) return src[1];
  return fallback;
}

/** Cheaply verify a URL is really a video file (Range GET, 0-0 bytes). */
async function verifyMp4(url) {
  try {
    const res = await axios.get(url, {
      timeout: 20000,
      maxRedirects: 5,
      headers: {
        'User-Agent': UA,
        Referer: `${BASE}/`,
        Accept: '*/*',
        Range: 'bytes=0-0',
      },
      responseType: 'arraybuffer',
      validateStatus: (s) => s >= 200 && s < 300,
    });
    const ct = String(res.headers['content-type'] || '');
    return /video|mp4|octet-stream|binary/i.test(ct);
  } catch {
    return false;
  }
}

const pad = (n) => String(n).padStart(2, '0');

function derivedMovieUrls(title, narrator) {
  const base = `https://${CDN_HOST}/download/`;
  const out = [];
  if (narrator) out.push(`${base}${encodeURIComponent(`${title} - ${narrator}`)}.mp4`);
  out.push(`${base}${encodeURIComponent(title)}.mp4`);
  return out;
}

function derivedEpisodeUrls(showTitle, narrator, s, e) {
  const base = `https://${CDN_HOST}/`;
  const named = `S${pad(s)} - EP${pad(e)} - ${showTitle} - ${narrator}`;
  const bare = `S${pad(s)} - EP${pad(e)} - ${showTitle}`;
  const out = [];
  if (narrator) {
    out.push(`${base}download/${encodeURIComponent(named)}.mp4`);
    out.push(`${base}${encodeURIComponent(named)}.mp4`);
  }
  out.push(`${base}download/${encodeURIComponent(bare)}.mp4`);
  out.push(`${base}${encodeURIComponent(bare)}.mp4`);
  return out;
}

/**
 * Movie page → CDN link. watchUrl is always the movie page so it stays browsable
 * even when there is no direct download.
 */
async function resolveMovie({ slug, title, narrator, deep = false }) {
  const pageUrl = `${BASE}/movies/${slug}`;
  const watchUrl = `${pageUrl}/watch`;

  const pages = [pageUrl];
  if (deep) pages.push(watchUrl);

  for (const page of pages) {
    let html = '';
    try {
      html = await fetchHtml(page);
    } catch {
      continue;
    }
    const urls = extractMp4Urls(html);
    const best = pickBest(urls, narrator);
    if (best) {
      return { downloadUrl: best, watchUrl: pageUrl, ok: true };
    }
  }

  // Pattern fallback: verify a derived URL.
  for (const cand of derivedMovieUrls(title, narrator)) {
    if (await verifyMp4(cand)) {
      return { downloadUrl: cand, watchUrl: pageUrl, ok: true };
    }
  }

  // Watch-only.
  return { downloadUrl: '', watchUrl: pageUrl, ok: false };
}

/**
 * Episode watch page → per-episode CDN link.
 */
async function resolveEpisode({ slug, s, e, showTitle, narrator, deep = false }) {
  const watchUrl = `${BASE}/watch/tv/${slug}/${s}/${e}`;
  const seriesPage = `${BASE}/tv/${slug}`;

  const pages = [watchUrl];
  if (deep) pages.push(seriesPage);

  for (const page of pages) {
    let html = '';
    try {
      html = await fetchHtml(page);
    } catch {
      continue;
    }
    const urls = extractMp4Urls(html);
    if (urls.length) {
      const best = pickBest(urls, narrator);
      if (best) {
        return { downloadUrl: best, watchUrl: extractWatchUrl(html, watchUrl), ok: true };
      }
    }
  }

  // Pattern fallback for old/broken pages.
  for (const cand of derivedEpisodeUrls(showTitle, narrator, s, e)) {
    if (await verifyMp4(cand)) {
      return { downloadUrl: cand, watchUrl, ok: true };
    }
  }

  return { downloadUrl: '', watchUrl, ok: false };
}

module.exports = { resolveMovie, resolveEpisode, fetchHtml, extractMp4Urls, verifyMp4, BASE };