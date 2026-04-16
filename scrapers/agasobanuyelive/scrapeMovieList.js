const { getPredefinedLinks } = require('../../utils/linkLoader');
const { loadScraperState, saveScraperState } = require('../../utils/stateManager');
const { logInfo, logError } = require('../../utils/logger');

const SITE_KEY = 'agasobanuyelive';
const DEFAULT_NAVIGATION_TIMEOUT_MS = 90000;

async function discoverCategoryPages(page, categoryUrl, pageLimit, timeoutMs) {
  const discovered = [];
  let nextUrl = categoryUrl;

  while (nextUrl && discovered.length < pageLimit) {
    await page.goto(nextUrl, {
      waitUntil: 'domcontentloaded',
      timeout: timeoutMs
    });
    const currentUrl = page.url();

    if (discovered.includes(currentUrl)) break;
    discovered.push(currentUrl);

    const candidate = await page.evaluate(() => {
      const nextLink = document.querySelector('a.next.page-numbers');
      return nextLink?.href || '';
    });

    nextUrl = candidate && !discovered.includes(candidate) ? candidate : '';
  }

  return discovered;
}

module.exports = async function scrapeMovieList(
  browser,
  pageLimit,
  type,
  timeoutMs = DEFAULT_NAVIGATION_TIMEOUT_MS
) {
  const categoryLinks = await getPredefinedLinks(SITE_KEY);
  const state = await loadScraperState(SITE_KEY);

  const processedLinksArray = state.processedLinks || [];
  const failedPages = state.failedPages || [];

  const alreadyProcessed = new Map(processedLinksArray.map((item) => [item.page, item.subLinks]));
  const alreadyFailed = new Set(failedPages.map((item) => item.page));

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(timeoutMs);
  page.setDefaultTimeout(timeoutMs);

  const results = [];
  const predefinedLinks = [];

  try {
    for (const categoryLink of categoryLinks) {
      const discoveredLinks = await discoverCategoryPages(page, categoryLink, pageLimit, timeoutMs);
      predefinedLinks.push(...discoveredLinks);
    }

    if (predefinedLinks.length === 0) {
      throw new Error('No category pages discovered.');
    }

    if (type !== 'patch') {
      for (const subLinks of alreadyProcessed.values()) {
        results.push(...subLinks);
      }
    }

    for (const link of predefinedLinks) {
      const isAlreadyDone = alreadyProcessed.has(link);
      const shouldSkip = type !== 'patch' && isAlreadyDone;

      if (shouldSkip) {
        logInfo(`Skipping already scraped page: ${link}`);
        continue;
      }

      try {
        await page.goto(link, {
          waitUntil: 'domcontentloaded',
          timeout: timeoutMs
        });
        const movieSubLinks = await page.evaluate(() => {
          return Array.from(document.querySelectorAll('h1.elementor-heading-title.elementor-size-default > a'))
            .map((el) => el?.href || '')
            .filter(Boolean);
        });

        results.push(...movieSubLinks);

        if (type !== 'patch' && pageLimit !== 2) {
          alreadyProcessed.set(link, movieSubLinks);

          const processedLinks = Array.from(alreadyProcessed.entries())
            .map(([pageUrl, subLinks]) => ({ page: pageUrl, subLinks }));

          await saveScraperState(SITE_KEY, {
            ...state,
            processedLinks,
            failedPages
          });
        }

        if (alreadyFailed.has(link)) {
          const updatedFails = failedPages.filter((item) => item.page !== link);
          await saveScraperState(SITE_KEY, {
            ...state,
            processedLinks: Array.from(alreadyProcessed.entries()).map(([pageUrl, subLinks]) => ({
              page: pageUrl,
              subLinks
            })),
            failedPages: updatedFails
          });
        }

        logInfo(`Scraped listing page: ${link}`);
      } catch (err) {
        logError(`Failed to scrape listing page ${link}: ${err.message}`);

        if (!alreadyFailed.has(link)) {
          failedPages.push({ page: link, error: err.message, saved: false });
          await saveScraperState(SITE_KEY, {
            ...state,
            processedLinks: Array.from(alreadyProcessed.entries()).map(([pageUrl, subLinks]) => ({
              page: pageUrl,
              subLinks
            })),
            failedPages
          });
          alreadyFailed.add(link);
        }
      }
    }

    return results;
  } finally {
    await page.close();
  }
};
