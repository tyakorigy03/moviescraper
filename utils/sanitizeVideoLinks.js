function maybeDecodePercentEncodedUrl(value) {
  if (typeof value !== 'string') return value;

  // Common case: fully percent-encoded URL (e.g. https%3A%2F%2F...)
  if (/^https?%3A%2F%2F/i.test(value) || /%3A%2F%2F/i.test(value)) {
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  }

  return value;
}

function unwrapKnownRedirects(url) {
  const host = (url.hostname || '').toLowerCase();
  const isAgasobanuyeLive = host === 'agasobanuyelive.com' || host === 'www.agasobanuyelive.com';

  if (isAgasobanuyeLive && url.pathname.toLowerCase().startsWith('/welcome')) {
    const target = url.searchParams.get('url');
    if (target) return target;
  }

  return '';
}

function sanitizeUrl(value = '', depth = 0) {
  if (typeof value !== 'string') return '';
  if (depth > 3) return '';

  const trimmed = value
    .trim()
    .replace(/&amp;/g, '&')
    .replace(/[\u0000-\u001F\u007F\s]+/g, '');

  if (!trimmed) return '';

  const maybeDecoded = maybeDecodePercentEncodedUrl(trimmed);
  const normalized = maybeDecoded.startsWith('//') ? `https:${maybeDecoded}` : maybeDecoded;

  try {
    const url = new URL(normalized);
    if (!['http:', 'https:'].includes(url.protocol)) return '';

    const redirected = unwrapKnownRedirects(url);
    if (redirected) {
      return sanitizeUrl(redirected, depth + 1);
    }

    return url.toString();
  } catch {
    return '';
  }
}

function sanitizeVideoEntries(entries = []) {
  if (!Array.isArray(entries)) return [];

  const sanitized = entries
    .map((entry) => {
      const title = typeof entry?.title === 'string' && entry.title.trim()
        ? entry.title.trim()
        : 'Download';
      const watchUrl = sanitizeUrl(entry?.watchUrl);
      const downloadUrl = sanitizeUrl(entry?.downloadUrl);

      if (!watchUrl && !downloadUrl) {
        return null;
      }

      return {
        title,
        watchUrl,
        downloadUrl,
        direct: Boolean(entry?.direct)
      };
    })
    .filter(Boolean);

  const seen = new Set();
  return sanitized.filter((item) => {
    const key = `${item.title}|${item.watchUrl}|${item.downloadUrl}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

module.exports = {
  sanitizeUrl,
  sanitizeVideoEntries
};
