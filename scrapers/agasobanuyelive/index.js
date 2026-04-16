require('dotenv').config();
const puppeteer = require('puppeteer');
const scrapeMovieList = require('./scrapeMovieList');
const scrapeMovieDetails = require('./scrapeMovieDetails');
const { saveMoviesToSupabase } = require('../../services/saveMoviesToSupabase');
const { logInfo, logError } = require('../../utils/logger');

const NAVIGATION_TIMEOUT_MS = 90000;

async function runAgasobanuyeliveScraper(mode = 'all', type = 'notpatch') {
  const browser = await puppeteer.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const pageLimit = mode === 'welcome' ? 2 : 79;

    logInfo(`[${new Date().toLocaleString()}] Starting Agasobanuyelive scraper (mode: ${mode}, type: ${type})...`);

    const movieLinks = await scrapeMovieList(browser, pageLimit, type, NAVIGATION_TIMEOUT_MS);
    logInfo(`Found ${movieLinks.length} movie links.`);

    const detailedMovies = await scrapeMovieDetails(browser, movieLinks, type, NAVIGATION_TIMEOUT_MS);
    logInfo(`Collected ${detailedMovies.length} detailed movie records.`);

    await saveMoviesToSupabase(detailedMovies);
    logInfo('All movies saved to Supabase.');
  } catch (err) {
    logError(`Scraper failed: ${err.message}`);
  } finally {
    await browser.close();
    logInfo('Browser closed.');
  }
}

if (require.main === module) {
  const modeArg = process.argv[2] || 'all';
  const typeArg = process.argv[3] || 'notpatch';
  runAgasobanuyeliveScraper(modeArg, typeArg);
}

module.exports = runAgasobanuyeliveScraper;
