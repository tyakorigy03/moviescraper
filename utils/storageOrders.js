/**
 * Loading the storage scrapers' catalogue/order JSON.
 *
 * Three sibling scrapers write their listing order into storage/*.json, and the
 * hot-signal scorer reads all three. The shapes differ slightly:
 *
 *   agasobanuyelive.json / oshakurfilms.json  { progressLink2: [...] }  discovery order
 *   agasobanuyenow.json                      [ ... ]                   listing order
 *
 * Earlier index = hotter, so the file order is the ranking.
 */
require('dotenv').config();
const path = require('path');
const fs = require('fs-extra');
const { logError } = require('./logger');

const STORAGE_DIR = path.join(__dirname, '..', 'storage');

/** @returns {Promise<object[]>} entries in listing order; [] when unreadable. */
async function loadStorageJson(name) {
  const file = path.join(STORAGE_DIR, name);
  try {
    if (!(await fs.pathExists(file))) return [];
    const data = await fs.readJson(file);
    if (Array.isArray(data.progressLink2)) return data.progressLink2;
    if (Array.isArray(data)) return data;
    return [];
  } catch (err) {
    logError(`storageOrders: failed reading storage/${name}: ${err.message}`);
    return [];
  }
}

module.exports = { loadStorageJson, STORAGE_DIR };
