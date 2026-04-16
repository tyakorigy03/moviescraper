const fs = require('fs-extra');
const path = require('path');
const { logInfo, logError } = require('./logger');

const STORAGE_DIR = path.join(__dirname, '..', 'storage');

function dedupe(arr, key = 'link') {
  if (!Array.isArray(arr)) return [];
  const map = new Map();
  arr.forEach((item) => {
    if (item[key]) map.set(item[key], item);
  });
  return Array.from(map.values());
}

async function loadScraperState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    if (!(await fs.pathExists(file))) return {};
    const state = await fs.readJson(file);

    if (state.progressLink2) state.progressLink2 = dedupe(state.progressLink2);
    if (state.failed) state.failed = dedupe(state.failed);
    if (state.processedLinks) state.processedLinks = dedupe(state.processedLinks, 'page');
    if (state.failedPages) state.failedPages = dedupe(state.failedPages, 'page');

    return state;
  } catch (err) {
    logError(`Failed to load scraper state for ${siteKey}: ${err.message}`);
    return {};
  }
}

async function saveScraperState(siteKey, state) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    await fs.ensureFile(file);

    const sanitizedState = { ...state };
    if (sanitizedState.progressLink2) sanitizedState.progressLink2 = dedupe(sanitizedState.progressLink2);
    if (sanitizedState.failed) sanitizedState.failed = dedupe(sanitizedState.failed);
    if (sanitizedState.processedLinks) sanitizedState.processedLinks = dedupe(sanitizedState.processedLinks, 'page');
    if (sanitizedState.failedPages) sanitizedState.failedPages = dedupe(sanitizedState.failedPages, 'page');

    await fs.writeJson(file, sanitizedState, { spaces: 2 });
  } catch (err) {
    logInfo(`Failed to save scraper state for ${siteKey}: ${err.message}`);
  }
}

async function clearScraperState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}.json`);
  try {
    if (await fs.pathExists(file)) {
      await fs.writeJson(file, {}, { spaces: 2 });
      logInfo(`Cleared scraper state for ${siteKey}`);
    }
  } catch (err) {
    logError(`Failed to clear scraper state for ${siteKey}: ${err.message}`);
  }
}

async function loadRunState(siteKey) {
  const file = path.join(STORAGE_DIR, `${siteKey}-run-state.json`);
  try {
    if (!(await fs.pathExists(file))) return {};
    return await fs.readJson(file);
  } catch (err) {
    console.error(`Failed to load run state for ${siteKey}: ${err.message}`);
    return {};
  }
}

async function saveRunState(siteKey, state) {
  const file = path.join(STORAGE_DIR, `${siteKey}-run-state.json`);
  try {
    await fs.ensureFile(file);
    await fs.writeJson(file, state, { spaces: 2 });
  } catch (err) {
    console.error(`Failed to save run state for ${siteKey}: ${err.message}`);
  }
}

module.exports = {
  loadScraperState,
  saveScraperState,
  clearScraperState,
  loadRunState,
  saveRunState
};
