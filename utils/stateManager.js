const fs = require('fs-extra');
const path = require('path');
const { logInfo, logError } = require('./logger');

const STORAGE_DIR = path.join(__dirname, '..', 'storage');

/**
 * Helper to deduplicate array of objects by a key
 */
function dedupe(arr, key = 'link') {
  if (!Array.isArray(arr)) return [];
  const map = new Map();
  arr.forEach(item => {
    if (item[key]) map.set(item[key], item);
  });
  return Array.from(map.values());
}

/**
 * Load scraper state (e.g., progressLink2, failed)
 */
async function loadScraperState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    if (!(await fs.pathExists(file))) return {};
    const state = await fs.readJson(file);

    // Auto-deduplicate on load
    if (state.progressLink2) state.progressLink2 = dedupe(state.progressLink2);
    if (state.failed) state.failed = dedupe(state.failed);

    return state;
  } catch (err) {
    logError(`❌ Failed to load scraper state for ${siteKey}: ${err.message}`);
    return {};
  }
}

/**
 * Save scraper state to disk (with deduplication)
 */
async function saveScraperState(siteKey, state) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    await fs.ensureFile(file);

    // Ensure unique entries before saving
    const sanitizedState = { ...state };
    if (sanitizedState.progressLink2) sanitizedState.progressLink2 = dedupe(sanitizedState.progressLink2);
    if (sanitizedState.failed) sanitizedState.failed = dedupe(sanitizedState.failed);

    await fs.writeJson(file, sanitizedState, { spaces: 2 });
  } catch (err) {
    logInfo(`❌ Failed to save scraper state for ${siteKey}: ${err.message}`);
  }
}

/**
 * Clear scraper state file (reset progress and failures)
 * @param {string} siteKey
 * @returns {Promise<void>}
 */
async function clearScraperState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    if (await fs.pathExists(file)) {
      await fs.writeJson(file, {}, { spaces: 2 });
      logInfo(`🧹 Cleared scraper state for ${siteKey}`);
    }
  } catch (err) {
    logError(`❌ Failed to clear scraper state for ${siteKey}: ${err.message}`);
  }
}

/**
 * Load scheduler run state (e.g., lastWelcomeRun, lastAllRun)
 * @param {string} siteKey
 * @returns {Promise<Object>}
 */
async function loadRunState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}-run-state.json`);
  try {
    if (!(await fs.pathExists(file))) return {};
    return await fs.readJson(file);
  } catch (err) {
    console.error(`❌ Failed to load run state for ${siteKey}: ${err.message}`);
    return {};
  }
}

/**
 * Save scheduler run state to disk
 * @param {string} siteKey
 * @param {Object} state
 * @returns {Promise<void>}
 */
async function saveRunState(siteKey, state) {
  const file = path.join(STORAGE_DIR, `${siteKey}-run-state.json`);
  try {
    await fs.ensureFile(file);
    await fs.writeJson(file, state, { spaces: 2 });
  } catch (err) {
    console.error(`❌ Failed to save run state for ${siteKey}: ${err.message}`);
  }
}


module.exports = {
  loadScraperState,
  saveScraperState,
  clearScraperState, // ✅ Exported here
  loadRunState,
  saveRunState
};
