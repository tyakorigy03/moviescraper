const { loadScraperState, saveScraperState } = require('../../utils/stateManager');
const { logInfo, logError } = require('../../utils/logger');
const { enrichWithTMDB } = require('../../services/enrichWithTmdb');

const SITE_KEY = 'agasobanuyelive';

module.exports = async function scrapeMovieDetails(browser, movieLinks = [], type) {
  const state = await loadScraperState(SITE_KEY);
  const progressLink2 = state.progressLink2 || [];
  let failed = state.failed || [];

  // Filter failed items: keep only those whose link is not in progressLink2
  const filteredFailed = failed.filter(
    item => !progressLink2.some(el => el.link === item.link)
  );

  // Log info before and after filtering
  logInfo('All failed items:', failed.length);
  logInfo('Failed items NOT in progressLink2:', filteredFailed.length);

  // Update and save if any duplicates were removed
  if (filteredFailed.length !== failed.length) {
    failed = filteredFailed;
    await saveScraperState(SITE_KEY, { ...state, failed });
    logInfo(`✅ Removed ${failed.length - filteredFailed.length} duplicates from failed list.`);
  } else {
    logInfo('No duplicate failed items found in progressLink2.');
  }

  // Track already scraped and failed links for fast lookup
  const alreadyScrapedLinks = type === 'patch'
    ? new Set()
    : new Set(progressLink2.map(item => item.link));
  const alreadyFailedLinks = new Set(failed.map(item => item.link));


  const page = await browser.newPage();

  // Prepare results: include unsaved progress entries for non-patch
  const results = type === 'patch' ? [] : progressLink2.filter(item => !item.saved);

  for (const movieLink of [...movieLinks, ...alreadyFailedLinks]) {
    const movieName = movieLink.split('/').filter(Boolean).pop().replace(/[-_]/g, ' ');

    if (alreadyScrapedLinks.has(movieLink)) {
      logInfo(`⏭️ Already scraped: ${movieName}`);
      const existing = progressLink2.find(item => item.link === movieLink);
      if (existing && !existing.saved) results.push(existing);
      continue;
    }
    try {
      logInfo(`🎥 Scraping: ${movieLink}`);
      await page.goto(movieLink, { waitUntil: 'domcontentloaded' });

      const details = await page.evaluate(() => {
        const jsonLd = document.querySelector('script.aioseo-schema')?.innerText;
        let metadata = { title: '', image: '', publishedAt: '', modifiedAt: '', genres: [], type: '' };

        if (jsonLd) {
          try {
            const data = JSON.parse(jsonLd);
            const post = data['@graph']?.find(item => item['@type'] === 'BlogPosting');
            if (post) {
              metadata.title = post.headline || '';
              metadata.image = post.image?.url || '';
              metadata.publishedAt = post.datePublished || '';
              metadata.modifiedAt = post.dateModified || '';

              // Handle both | and , as separators
              const section = post.articleSection || '';
              metadata.genres = section.split(/[|,]/).map(g => g.trim()).filter(Boolean);

              // Improved type detection: Check for 'Serie' or 'S0' or 'Season' in title or categories
              const isSerie = section.includes('Serie') ||
                metadata.title.toLowerCase().includes('season') ||
                /\bs\d{1,2}\b/i.test(metadata.title);
              metadata.type = isSerie ? 'tv' : 'movie';
            }
          } catch (err) { }
        }

        const getTextByLabel = (label) => {
          // Look for labels like "Translator:", "Release date:", etc.
          const elements = Array.from(document.querySelectorAll('.elementor-post-info__item, li, p, span'));
          const target = elements.find(el => el.textContent.toLowerCase().includes(label.toLowerCase() + ':'));
          if (target) {
            return target.textContent.split(':').slice(1).join(':').trim();
          }
          return '';
        };

        const Downloadurls = [];
        const DownurlsPaths = document.querySelectorAll('[id^="tsvg-section-"] > main > figure > ul > li');

        if (DownurlsPaths.length > 0) {
          DownurlsPaths.forEach(item => {
            const downloadUrl = item.getAttribute('data-tsvg-link') || '';
            const watchUrl = item.getAttribute('data-tsvg-href') || '';
            const direct = (item.getAttribute('data-tsvg-target') == "_blank");
            const titleEl = item.querySelector("figure > div > figcaption > h2");
            const title = titleEl ? titleEl.innerHTML : 'Download';

            Downloadurls.push({ title, watchUrl, downloadUrl, direct });
          });
        } else {
          // Fallback: search for direct download links and IFRAMES (like Rumble)
          const links = Array.from(document.querySelectorAll('a[href*="agasobanuyelive.com/welcome"], a[href*="rumble.com"], a[href*="mediafire.com"]'));
          const iframes = Array.from(document.querySelectorAll('iframe[src*="rumble.com"]'));

          const urlGroups = new Map();

          const getContextTitle = (el) => {
            // Find nearest preceding H2, or use a default if none
            let prev = el.previousElementSibling;
            while (prev) {
              if (prev.matches('h2, h3')) return prev.textContent.trim();
              prev = prev.previousElementSibling;
            }
            return 'Default';
          };

          // 1. Process Links
          links.forEach(link => {
            const title = getContextTitle(link);
            const url = new URL(link.href);
            const targetUrl = url.searchParams.get('url') || link.href;

            if (targetUrl.includes('mediafire') || targetUrl.includes('mega.nz') || targetUrl.includes('.mp4')) {
              if (!urlGroups.has(title)) urlGroups.set(title, { title, watchUrl: '', downloadUrl: '', direct: true });
              urlGroups.get(title).downloadUrl = targetUrl;
            }
          });

          // 2. Process Iframes
          iframes.forEach(iframe => {
            const src = iframe.getAttribute('src');
            if (src) {
              const title = getContextTitle(iframe);
              if (!urlGroups.has(title)) urlGroups.set(title, { title, watchUrl: '', downloadUrl: '', direct: true });
              urlGroups.get(title).watchUrl = src;
            }
          });

          // Remove 'Default' entries if they don't have both fields or if others exist
          Downloadurls.push(...Array.from(urlGroups.values()));
        }

        let narrator = getTextByLabel('Translator') || getTextByLabel('Narrator');

        return {
          ...metadata,
          Downloadurls,
          narrator: narrator || '',
          release_date: getTextByLabel('Release date'),
          country: getTextByLabel('Country')
        };
      });

      const enriched = await enrichWithTMDB({ title: details.title, publishedAt: details.release_date, type: details.type });
      const fullData = {
        link: movieLink,
        ...details,
        ...enriched,
        saved: false
      };

      // Add new scrape result only if not already saved
      if (!alreadyScrapedLinks.has(movieLink)) {
        results.push(fullData);
        progressLink2.push(fullData);
        alreadyScrapedLinks.add(movieLink);
      }

      // Remove from failed if it was there previously
      if (alreadyFailedLinks.has(movieLink)) {
        const idx = failed.findIndex(item => item.link === movieLink);
        if (idx !== -1) failed.splice(idx, 1);
        alreadyFailedLinks.delete(movieLink);
      }

      // Save updated state in batches to avoid constant disk thrashing
      if (results.length % 20 === 0) {
        await saveScraperState(SITE_KEY, { ...state, progressLink2, failed });
        logInfo(`💾 Batch save: progress saved at ${results.length} movies.`);
      }

    } catch (err) {
      logError(`❌ Failed: ${movieName}: ${err.message}`);

      // Save failure if not already recorded
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

  // Final save after loop finishes
  await saveScraperState(SITE_KEY, { ...state, progressLink2, failed });
  logInfo(`🏁 Final state save completed.`);

  await page.close();
  return results;
};
