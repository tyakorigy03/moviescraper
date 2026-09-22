const fs = require('fs-extra');
const path = require('path');
const config = require('../config');

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

const level = LEVELS[config.logLevel] ?? LEVELS.info;

const ts = () => new Date().toISOString();

function writeLine(line) {
  process.stdout.write(`${line}\n`);
}

const log = (lvl, msg) => {
  if (level <= LEVELS[lvl]) writeLine(`[${ts()}] [${lvl.toUpperCase()}] ${msg}`);
};

const logger = {
  debug: (msg) => log('debug', msg),
  info: (msg) => log('info', msg),
  warn: (msg) => log('warn', msg),
  error: (msg) => log('error', msg),
};

// Persist run progress so interrupted runs can resume with --resume.
async function loadState() {
  try {
    if (await fs.pathExists(config.stateFile)) {
      return await fs.readJson(config.stateFile);
    }
  } catch (err) {
    logger.warn(`Could not read state file: ${err.message}`);
  }
  return { done: {}, failed: {} };
}

async function saveState(state) {
  await fs.ensureDir(path.dirname(config.stateFile));
  await fs.writeJson(config.stateFile, state, { spaces: 2 });
}

module.exports = { logger, loadState, saveState };