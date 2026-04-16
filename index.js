require('dotenv').config();
const runAgasobanuyeliveScraper = require('./scrapers/agasobanuyelive/index');
const { logInfo, logError } = require('./utils/logger');
const {
  loadRunState,
  saveRunState,
  loadScraperState
} = require('./utils/stateManager');
const updateMovieScores = require('./utils/updateScoresWithSupabase');

const SITE_KEY = 'agasobanuyelive';
const WELCOME_INTERVAL_MINUTES = 30;
const ALL_INTERVAL_DAYS = 7;
const SCORE_UPDATE_INTERVAL_DAYS = 7;

async function hasFailedItems() {
  try {
    const state = await loadScraperState(SITE_KEY);
    const failed = (state.failed || []).filter((item) => !item.saved);
    return failed.length > 0;
  } catch (err) {
    logError(`Failed to check failed items: ${err.message}`);
    return false;
  }
}

(async () => {
  const now = Date.now();
  const welcomeIntervalMs = WELCOME_INTERVAL_MINUTES * 60 * 1000;
  const fullScrapeIntervalMs = ALL_INTERVAL_DAYS * 24 * 60 * 60 * 1000;
  const scoreUpdateIntervalMs = SCORE_UPDATE_INTERVAL_DAYS * 24 * 60 * 60 * 1000;

  const runState = await loadRunState(SITE_KEY);
  const lastWelcome = runState.lastWelcomeRun || 0;
  const lastAll = runState.lastAllRun || 0;
  const lastScoreUpdate = runState.lastScoreUpdate || 0;

  try {
    if (await hasFailedItems()) {
      logInfo('Retrying failed items using the persisted state queue...');
      await runAgasobanuyeliveScraper('all', 'notpatch');
      runState.lastRetryRun = now;
    } else if (now - lastWelcome >= welcomeIntervalMs) {
      logInfo('Running welcome mode...');
      await runAgasobanuyeliveScraper('welcome', 'patch');
      runState.lastWelcomeRun = now;
    } else if (now - lastAll >= fullScrapeIntervalMs) {
      logInfo('Running weekly full scrape...');
      await runAgasobanuyeliveScraper('all', 'patch');
      runState.lastAllRun = now;
    } else if (now - lastScoreUpdate >= scoreUpdateIntervalMs) {
      await updateMovieScores();
      runState.lastScoreUpdate = now;
    } else {
      logInfo('No eligible task to run at this time.');
    }
  } catch (err) {
    logError(`Critical failure in main scheduler: ${err.message}`);
  } finally {
    await saveRunState(SITE_KEY, runState);
  }
})();
