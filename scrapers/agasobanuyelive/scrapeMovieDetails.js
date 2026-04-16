const { loadScraperState, saveScraperState } = require('../../utils/stateManager');
const { logInfo, logError } = require('../../utils/logger');
const { enrichWithTMDB } = require('../../services/enrichWithTmdb');
const { decodeHtmlEntities } = require('../../utils/decodeHtml');
const { sanitizeVideoEntries } = require('../../utils/sanitizeVideoLinks');

const SITE_KEY = 'agasobanuyelive';
const DEFAULT_NAVIGATION_TIMEOUT_MS = 90000;

module.exports = async function scrapeMovieDetails(
  browser,
  movieLinks = [],
  type,
  timeoutMs = DEFAULT_NAVIGATION_TIMEOUT_MS
) {
  const state = await loadScraperState(SITE_KEY);
  const progressLink2 = state.progressLink2 || [];
  let failed = state.failed || [];

  const filteredFailed = failed.filter(
    (item) => !progressLink2.some((entry) => entry.link === item.link)
  );

  logInfo('All failed items:', failed.length);
  logInfo('Failed items not already present in progress state:', filteredFailed.length);

  if (filteredFailed.length !== failed.length) {
    const removedCount = failed.length - filteredFailed.length;
    failed = filteredFailed;
    await saveScraperState(SITE_KEY, { ...state, failed });
    logInfo(`Removed ${removedCount} duplicate failed entries from state.`);
  } else {
    logInfo('No duplicate failed items found in progress state.');
  }

  const alreadyScrapedLinks = type === 'patch'
    ? new Set()
    : new Set(progressLink2.map((item) => item.link));
  const alreadyFailedLinks = new Set(failed.map((item) => item.link));

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(timeoutMs);
  page.setDefaultTimeout(timeoutMs);

  const results = type === 'patch'
    ? []
    : progressLink2.filter((item) => !item.saved && !item.ignored);

  try {
    for (const movieLink of [...movieLinks, ...alreadyFailedLinks]) {
      const movieName = movieLink.split('/').filter(Boolean).pop().replace(/[-_]/g, ' ');

      if (alreadyScrapedLinks.has(movieLink)) {
        logInfo(`Already scraped: ${movieName}`);
        const existing = progressLink2.find((item) => item.link === movieLink);
        if (existing && !existing.saved && !existing.ignored) {
          results.push(existing);
        }
        continue;
      }

      try {
        logInfo(`Scraping movie details: ${movieLink}`);
        await page.goto(movieLink, {
          waitUntil: 'domcontentloaded',
          timeout: timeoutMs
        });

        const details = await page.evaluate(() => {
          const jsonLd = document.querySelector('script.aioseo-schema')?.innerText;
          const metadata = { title: '', image: '', publishedAt: '', modifiedAt: '', genres: [], type: '' };

          if (jsonLd) {
            try {
              const data = JSON.parse(jsonLd);
              const post = data['@graph']?.find((item) => item['@type'] === 'BlogPosting');
              if (post) {
                metadata.title = post.headline || '';
                metadata.image = post.image?.url || '';
                metadata.publishedAt = post.datePublished || '';
                metadata.modifiedAt = post.dateModified || '';

                const section = post.articleSection || '';
                metadata.genres = section.split(/[|,]/).map((genre) => genre.trim()).filter(Boolean);

                const isSerie = section.includes('Serie') ||
                  metadata.title.toLowerCase().includes('season') ||
                  /\bs\d{1,2}\b/i.test(metadata.title);
                metadata.type = isSerie ? 'tv' : 'movie';
              }
            } catch (err) {
              return metadata;
            }
          }

          const getTextByLabel = (label) => {
            const elements = Array.from(document.querySelectorAll('.elementor-post-info__item, li, p, span'));
            const target = elements.find((el) => el.textContent.toLowerCase().includes(`${label.toLowerCase()}:`));
            if (target) {
              return target.textContent.split(':').slice(1).join(':').trim();
            }
            return '';
          };

          const downloadUrls = [];
          const downloadNodes = document.querySelectorAll('[id^="tsvg-section-"] > main > figure > ul > li');

          if (downloadNodes.length > 0) {
            downloadNodes.forEach((item) => {
              const downloadUrl = item.getAttribute('data-tsvg-link') || '';
              const watchUrl = item.getAttribute('data-tsvg-href') || '';
              const direct = item.getAttribute('data-tsvg-target') === '_blank';
              const titleEl = item.querySelector('figure > div > figcaption > h2');
              const title = titleEl ? titleEl.innerHTML : 'Download';

              downloadUrls.push({ title, watchUrl, downloadUrl, direct });
            });
          } else {
            const links = Array.from(
              document.querySelectorAll(
                'a[href*="agasobanuyelive.com/welcome"], a[href*="rumble.com"], a[href*="mediafire.com"]'
              )
            );
            const iframes = Array.from(document.querySelectorAll('iframe[src*="rumble.com"]'));
            const urlGroups = new Map();

            const getContextTitle = (el) => {
              let prev = el.previousElementSibling;
              while (prev) {
                if (prev.matches('h2, h3')) return prev.textContent.trim();
                prev = prev.previousElementSibling;
              }
              return 'Default';
            };

            links.forEach((link) => {
              const title = getContextTitle(link);
              const url = new URL(link.href);
              const targetUrl = url.searchParams.get('url') || link.href;

              if (targetUrl.includes('mediafire') || targetUrl.includes('mega.nz') || targetUrl.includes('.mp4')) {
                if (!urlGroups.has(title)) {
                  urlGroups.set(title, { title, watchUrl: '', downloadUrl: '', direct: true });
                }
                urlGroups.get(title).downloadUrl = targetUrl;
              }
            });

            iframes.forEach((iframe) => {
              const src = iframe.getAttribute('src');
              if (src) {
                const title = getContextTitle(iframe);
                if (!urlGroups.has(title)) {
                  urlGroups.set(title, { title, watchUrl: '', downloadUrl: '', direct: true });
                }
                urlGroups.get(title).watchUrl = src;
              }
            });

            downloadUrls.push(...Array.from(urlGroups.values()));
          }

          const narrator = getTextByLabel('Translator') || getTextByLabel('Narrator');

          return {
            ...metadata,
            Downloadurls: downloadUrls,
            narrator: narrator || '',
            release_date: getTextByLabel('Release date'),
            country: getTextByLabel('Country')
          };
        });

        details.title = decodeHtmlEntities(details.title);
        details.narrator = decodeHtmlEntities(details.narrator);
        if (details.Downloadurls) {
          details.Downloadurls = sanitizeVideoEntries(
            details.Downloadurls.map((entry) => ({
              ...entry,
              title: decodeHtmlEntities(entry.title)
            }))
          );
        }

        const enriched = await enrichWithTMDB({
          title: details.title,
          publishedAt: details.release_date,
          type: details.type
        });

        const fullData = {
          link: movieLink,
          ...details,
          ...enriched,
          saved: false
        };

        if (!alreadyScrapedLinks.has(movieLink)) {
          results.push(fullData);
          progressLink2.push(fullData);
          alreadyScrapedLinks.add(movieLink);
        }

        if (alreadyFailedLinks.has(movieLink)) {
          const idx = failed.findIndex((item) => item.link === movieLink);
          if (idx !== -1) failed.splice(idx, 1);
          alreadyFailedLinks.delete(movieLink);
        }

        if (results.length % 20 === 0) {
          await saveScraperState(SITE_KEY, { ...state, progressLink2, failed });
          logInfo(`Batch-saved progress at ${results.length} movies.`);
        }
      } catch (err) {
        logError(`Failed scraping ${movieName}: ${err.message}`);

        if (!alreadyFailedLinks.has(movieLink)) {
          const failEntry = { link: movieLink, error: err.message, saved: false };
          failed.push(failEntry);
          alreadyFailedLinks.add(movieLink);
        }

        if (failed.length % 20 === 0) {
          await saveScraperState(SITE_KEY, { ...state, progressLink2, failed });
        }
      }
    }

    await saveScraperState(SITE_KEY, { ...state, progressLink2, failed });
    logInfo('Final detail scrape state save completed.');
    return results;
  } finally {
    await page.close();
  }
};
