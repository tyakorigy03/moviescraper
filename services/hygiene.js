/**
 * Row-hygiene helpers: keep genres/country clean when source sites jam extra
 * metadata (countries, narrator names, the word "Movie"/"Serie") into the
 * same Text field.
 *
 * Strategy: a strict genre whitelist (standard film/TV genre vocabulary
 * including common localized alternatives the Kinyarwanda sites use). Any
 * token that isn't a recognized genre is dropped; countries go to `country`,
 * narrator names go to `narrator`, type words are implied by `type`.
 */

const GENRE_WHITELIST = new Set([
  'action', 'adventure', 'animation', 'anime', 'biography', 'biopic',
  'comedy', 'crime', 'documentary', 'drama', 'family', 'fantasy',
  'film noir', 'history', 'historical', 'horror', 'music', 'romance',
  'sci-fi', 'science fiction', 'sport', 'sports', 'thriller', 'war',
  'western', 'mystery', 'musical', 'reality', 'sitcom', 'suspense',
  'crime drama', 'romantic comedy', 'drama series', 'teen', 'superhero',
  'action thriller', 'comedy drama', 'psychological', 'survival',
  'epic', 'period', 'fantasy drama', 'crime mystery', 'family drama',
  'political', 'disaster', 'apocalyptic', 'post-apocalyptic', 'zombie',
]);

// Country names (and fragments) that commonly leak into the genres column.
const COUNTRY_FRAGMENTS = new Set([
  'canada', 'united kingdom', 'uk', 'united states', 'usa', 'us', 'france',
  'nigeria', 'kenya', 'uganda', 'rwanda', 'india', 'china', 'korea', 'south korea',
  'japan', 'mexico', 'brazil', 'germany', 'italy', 'spain', 'tanzania',
  'south africa', 'ghana', 'ethiopia', 'egypt', 'turkey', 'netherlands',
  'sweden', 'norway', 'denmark', 'poland', 'ukraine', 'australia', 'zimbabwe',
  'zambia', 'malawi', 'burundi', 'congo', 'argentina', 'colombia', 'chile',
]);

// Words that describe the row type or process, not a genre.
const NON_GENRE_WORDS = new Set([
  'movie', 'movies', 'film', 'films', 'serie', 'series', 'tv', 'season',
  'seasons', 'episode', 'episodes', 'by', 'with', 'and', 'the', 'auto',
  'watch', 'full', 'hd', 'free', 'download', 'online', 'translated',
  'translation', 'temperature', 'voice', 'narrated',
]);

const normalized = (value) => String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();

// "country" -> "Country" (keep acronyms and hyphenated words Upper-Cased)
function titleCase(value) {
  return String(value || '')
    .trim()
    .split(/\s+/)
    .map((w) => {
      if (w === w.toUpperCase() && w.length > 1) return w;
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(' ');
}

function cleanSiteGenres(raw) {
  const tokens = [];
  const add = (t) => {
    const norm = normalized(t);
    if (!norm) return;
    if (GENRE_WHITELIST.has(norm)) tokens.push(norm === 'sci-fi' ? 'Sci-Fi' : norm === 'film noir' ? 'Film Noir' : titleCase(norm));
  };
  if (Array.isArray(raw)) {
    for (const g of raw) {
      // a cell may itself be a list ("Action|Thriller")
      for (const part of String(g).split(/[|,;]/)) add(part);
    }
  } else if (raw) {
    for (const part of String(raw).split(/[|,;]/)) add(part);
  }
  return [...new Set(tokens)];
}

function cleanSiteCountry(raw) {
  const seen = new Set();
  const parts = [];
  for (const part of String(raw || '').split(/[|,;]/)) {
    const norm = normalized(part);
    if (!norm) continue;
    if (COUNTRY_FRAGMENTS.has(norm) && !seen.has(norm)) {
      seen.add(norm);
      parts.push(titleCase(norm));
    }
  }
  return parts.join(', ') || null;
}

module.exports = { cleanSiteGenres, cleanSiteCountry, GENRE_WHITELIST };