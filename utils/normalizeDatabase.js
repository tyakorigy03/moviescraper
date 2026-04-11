const supabase = require('../services/supabaseClient');
const { computeRelevanceScore } = require('./relevanceScore');
const { logInfo, logError } = require('./logger');

async function normalize() {
    logInfo('🚀 Starting Supabase normalization (scoring & metadata)...');

    let allMovies = [];
    const PAGE_SIZE = 1000;
    let from = 0;

    while (true) {
        logInfo(`📡 Fetching movies ${from} to ${from + PAGE_SIZE}...`);
        const { data: chunk, error } = await supabase
            .from('moviesv2')
            .select('*')
            .range(from, from + PAGE_SIZE - 1);

        if (error) {
            logError(`❌ Failed to fetch movies: ${error.message}`);
            break;
        }

        if (!chunk || chunk.length === 0) break;
        allMovies = allMovies.concat(chunk);
        if (chunk.length < PAGE_SIZE) break;
        from += PAGE_SIZE;
    }

    const movies = allMovies;
    logInfo(`📊 Total records fetched: ${movies.length}`);

    const updates = [];
    let updatedCount = 0;

    for (const movie of movies) {
        let changed = false;
        const normalized = { id: movie.id };

        // A. Genre Normalization: Convert "Action|Drama" or "Action,Drama" to ["Action", "Drama"]
        if (typeof movie.genres === 'string') {
            normalized.genres = movie.genres.split(/[|,]/).map(g => g.trim()).filter(Boolean);
            changed = true;
        } else if (Array.isArray(movie.genres)) {
            normalized.genres = movie.genres.map(g => g.trim()).filter(Boolean);
        } else {
            normalized.genres = [];
        }

        // B. Narrator Recovery: If empty, check genres
        let currentNarrator = movie.narrator || '';
        if (!currentNarrator && normalized.genres.length > 0) {
            const narratorRatings = require('../data/narratorRatings.json');
            const knownNarrators = Object.keys(narratorRatings).map(n => n.toLowerCase());
            const found = normalized.genres.find(g => knownNarrators.includes(g.toLowerCase()));
            if (found) {
                currentNarrator = found;
                normalized.narrator = found;
                changed = true;
            }
        }

        // D. Download URLs Normalization: Merge lowercase downloadurls into PascalCase Downloadurls AND Clean redirect URLs
        let downloadUrlsToProcess = normalized.Downloadurls || movie.Downloadurls || [];
        if (!movie.Downloadurls && movie.downloadurls) {
            downloadUrlsToProcess = movie.downloadurls;
            changed = true;
        }

        if (Array.isArray(downloadUrlsToProcess)) {
            const cleanedUrls = downloadUrlsToProcess.map(dl => {
                let cleanWatch = dl.watchUrl || '';
                let cleanDownload = dl.downloadUrl || '';

                // Clean redirect from watchUrl
                if (cleanWatch.includes('agasobanuyelive.com/welcome/?url=')) {
                    try {
                        const urlObj = new URL(cleanWatch);
                        cleanWatch = urlObj.searchParams.get('url') || cleanWatch;
                    } catch (e) { }
                }
                // Clean redirect from downloadUrl
                if (cleanDownload.includes('agasobanuyelive.com/welcome/?url=')) {
                    try {
                        const urlObj = new URL(cleanDownload);
                        cleanDownload = urlObj.searchParams.get('url') || cleanDownload;
                    } catch (e) { }
                }

                if (cleanWatch !== dl.watchUrl || cleanDownload !== dl.downloadUrl) {
                    changed = true;
                    return { ...dl, watchUrl: cleanWatch, downloadUrl: cleanDownload };
                }
                return dl;
            });

            if (changed) {
                normalized.Downloadurls = cleanedUrls;
            }
        }

        // E. Re-Scoring: Apply the fixed logic (narrator boost, etc.)
        const newScore = computeRelevanceScore({
            ...movie,
            genres: normalized.genres || movie.genres,
            narrator: currentNarrator,
            Downloadurls: normalized.Downloadurls || movie.Downloadurls
        });

        if (newScore !== movie.score) {
            normalized.score = newScore;
            changed = true;
        }

        if (changed) {
            updates.push(normalized);
        }
    }

    logInfo(`🔎 Found ${updates.length} records needing updates.`);

    if (updates.length === 0) {
        logInfo('✅ Database is already normalized.');
        return;
    }

    // 2. Perform updates in batches
    const batchSize = 50;
    for (let i = 0; i < updates.length; i += batchSize) {
        const batch = updates.slice(i, i + batchSize);

        // Supabase upsert with ID will update existing rows
        const { error: upError } = await supabase
            .from('moviesv2')
            .upsert(batch, { onConflict: ['id'] });

        if (upError) {
            logError(`❌ Failed to update batch starting at ${i}: ${upError.message}`);
        } else {
            updatedCount += batch.length;
            logInfo(`🔄 Updated ${updatedCount}/${updates.length} records...`);
        }
    }

    logInfo('🎉 Normalization complete.');
}

normalize().catch(console.error);
