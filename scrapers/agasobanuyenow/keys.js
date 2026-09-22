/**
 * Normalization + locator helpers shared by the matcher and the merger.
 */

// Hosts we consider "slow" — links we actively want to replace/upgrade with
// agasobanuyenow's CDN.
const SLOW_HOSTS = ['anonsharing.com', 'mediafire.com'];
const CDN_HOST = 'media.agasobanuyenow.com';

function hostOf(url = '') {
  try {
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

const isSlowHost = (url = '') => SLOW_HOSTS.includes(hostOf(url));
const isCdnHost = (url = '') => hostOf(url) === CDN_HOST;

/** True when the entry is one of our self-hosted slots (never touch them). */
const isServerEntry = (e = {}) => /\(server hd\)/i.test(String(e.title || ''));

/** "Man on Fire Part 1" → "man on fire" — entity identity (drops parts/episodes/…). */
function coreTitle(raw = '') {
  return String(raw)
    .toLowerCase()
    .replace(/\b(?:19|20)\d{2}\b/g, ' ')
    .replace(/\bs\d{1,2}(?:\s*e\d{1,2})?\b/g, ' ')
    .replace(/\b(?:season|saizonya|saison)\s*\d+\b/g, ' ')
    .replace(/\b(?:episode|igice|ep)\s*\d+\b/g, ' ')
    .replace(/\b(?:part|pt\.?|igice|gbice)\s*(?:one|two|three|four|five|[a-z]|\d+)\b/g, ' ')
    .replace(/\b(?:final|full movie|full)\b/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

/**
 * Locator for an episode/part-like entry title. Returns:
 *  - "s<season>e<ep>"  when the title carries S##E## info
 *  - "e<ep>"           when only "Episode N" / "EP N" is present
 *  - "p<label>"        when it is a Part A / Part 1 style split
 *  - null              otherwise (plain whole-movie entry)
 */
function locator(title = '') {
  const t = String(title).trim();
  const se = /s\s*(\d{1,3})[.\- ]*e\s*(\d{1,3})/i.exec(t);
  if (se) return `s${+se[1]}e${+se[2]}`;
  const ep = /(?:^|[^a-z])(?:ep|episode)\D{0,3}(\d{1,3})/i.exec(t);
  if (ep) return `e${+ep[1]}`;
  const pt = /(?:part|pt\.?)\s*(one|two|three|four|five|six|seven|eight|nine|[a-z]|\d{1,3})/i.exec(t);
  if (pt) return `p${String(pt[1]).toLowerCase()}`;
  return null;
}

/** True when both titles point at the same episode/part split. */
function sameSplit(a = '', b = '') {
  const la = locator(a);
  const lb = locator(b);
  if (la && lb) return la === lb;
  return false;
}

module.exports = { coreTitle, locator, sameSplit, isSlowHost, isCdnHost, isServerEntry, hostOf, CDN_HOST, SLOW_HOSTS };