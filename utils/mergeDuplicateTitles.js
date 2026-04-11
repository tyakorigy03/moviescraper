const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');

async function mergeDuplicates() {
    logInfo('🚀 Starting Supabase duplicate title merging...');

    let allMovies = [];
    const PAGE_SIZE = 1000;
    let from = 0;

    // 1. Fetch all movies
    while (true) {
        logInfo(`📡 Fetching movies ${from} to ${from + PAGE_SIZE}...`);
        const { data: chunk, error } = await supabase
            .from('moviesv2')
            .select('*')
            .range(from, from + PAGE_SIZE - 1);

        if (error) {
            logError(`❌ Failed to fetch movies chunk at ${from}: ${error.message}`);
            throw error;
        }

        if (!chunk || chunk.length === 0) {
            logInfo('📥 No more chunks.');
            break;
        }
        allMovies = allMovies.concat(chunk);
        logInfo(`✅ Fetched ${chunk.length} records. Total: ${allMovies.length}`);
        if (chunk.length < PAGE_SIZE) break;
        from += PAGE_SIZE;
    }

    logInfo(`📊 Total records fetched: ${allMovies.length}`);

    // 2. Group by normalized title
    const groups = new Map();
    for (const movie of allMovies) {
        if (!movie.title) continue;
        const normalizedTitle = movie.title.toLowerCase().trim();
        if (!groups.has(normalizedTitle)) {
            groups.set(normalizedTitle, []);
        }
        groups.get(normalizedTitle).push(movie);
    }

    const updates = [];
    const deletes = [];

    for (const [title, records] of groups.entries()) {
        if (records.length <= 1) continue;

        logInfo(`🔎 Found duplicate title: "${records[0].title}" (${records.length} records)`);

        // Sort by quality (prefer those with TMDB data and higher score)
        records.sort((a, b) => {
            const scoreA = (a.tmdb_id ? 100 : 0) + (a.score || 0);
            const scoreB = (b.tmdb_id ? 100 : 0) + (b.score || 0);
            return scoreB - scoreA;
        });

        const master = records[0];
        const duplicates = records.slice(1);

        const mergedUrlsMap = new Map();

        // Helper to add URLs to map for deduplication
        const addUrls = (urls) => {
            if (!Array.isArray(urls)) return;
            urls.forEach(u => {
                const key = `${u.title}_${u.watchUrl}_${u.downloadUrl}`;
                if (!mergedUrlsMap.has(key)) {
                    mergedUrlsMap.set(key, u);
                }
            });
        };

        // Add from master
        addUrls(master.Downloadurls);
        addUrls(master.downloadurls);

        // Add from duplicates
        duplicates.forEach(d => {
            addUrls(d.Downloadurls);
            addUrls(d.downloadurls);
        });

        const mergedUrls = Array.from(mergedUrlsMap.values());

        if (mergedUrls.length !== (master.Downloadurls?.length || 0)) {
            updates.push({
                id: master.id,
                Downloadurls: mergedUrls,
                downloadurls: null // Clear deprecated column
            });
        }

        deletes.push(...duplicates.map(d => d.id));
    }

    logInfo(`⚙️  Planning to update ${updates.length} records and delete ${deletes.length} duplicates.`);

    // 3. Execute updates
    if (updates.length > 0) {
        logInfo('🔄 Updating master records...');
        for (let i = 0; i < updates.length; i += 50) {
            const batch = updates.slice(i, i + 50);
            const { error } = await supabase.from('moviesv2').upsert(batch);
            if (error) logError(`❌ Update error at batch ${i}: ${error.message}`);
        }
    }

    // 4. Execute deletes
    if (deletes.length > 0) {
        logInfo('🗑️ Deleting duplicate records...');
        for (let i = 0; i < deletes.length; i += 50) {
            const batch = deletes.slice(i, i + 50);
            const { error } = await supabase.from('moviesv2').delete().in('id', batch);
            if (error) logError(`❌ Delete error at batch ${i}: ${error.message}`);
        }
    }

    logInfo('🎉 Duplicate merging complete.');
}

mergeDuplicates().catch(err => {
    console.error('Fatal error during merge:', err);
    process.exit(1);
});
