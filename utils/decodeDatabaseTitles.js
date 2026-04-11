const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');
const { decodeHtmlEntities } = require('./decodeHtml');

async function cleanupTitles() {
    logInfo('🚀 Starting Supabase HTML entity cleanup...');

    let allMovies = [];
    const PAGE_SIZE = 1000;
    let from = 0;

    // 1. Fetch all movies (we need them all to check for entities)
    while (true) {
        logInfo(`📡 Fetching movies ${from} to ${from + PAGE_SIZE}...`);
        const { data: chunk, error } = await supabase
            .from('moviesv2')
            .select('id, title, narrator, Downloadurls')
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

    logInfo(`📊 Total records fetched: ${allMovies.length}`);

    const updates = [];
    const entityRegex = /(&#\d+;|(&amp;|&quot;|&apos;|&ndash;|&mdash;|&lsquo;|&rsquo;|&ldquo;|&rdquo;|&hellip;))/;

    for (const movie of allMovies) {
        let changed = false;

        const cleanTitle = decodeHtmlEntities(movie.title);
        const cleanNarrator = decodeHtmlEntities(movie.narrator);

        let cleanUrls = movie.Downloadurls;
        if (Array.isArray(movie.Downloadurls)) {
            cleanUrls = movie.Downloadurls.map(u => {
                const ct = decodeHtmlEntities(u.title);
                if (ct !== u.title) changed = true;
                return { ...u, title: ct };
            });
        }

        if (cleanTitle !== movie.title || cleanNarrator !== movie.narrator || changed) {
            updates.push({
                id: movie.id,
                title: cleanTitle,
                narrator: cleanNarrator,
                Downloadurls: cleanUrls
            });
        }
    }

    logInfo(`⚙️  Found ${updates.length} records needing cleanup.`);

    // 2. Execute updates
    if (updates.length > 0) {
        logInfo('🔄 Updating records...');
        for (let i = 0; i < updates.length; i += 50) {
            const batch = updates.slice(i, i + 50);
            const { error } = await supabase.from('moviesv2').upsert(batch);
            if (error) {
                logError(`❌ Update error at batch ${i}: ${error.message}`);
            } else {
                logInfo(`✅ Updated batch ${i / 50 + 1}/${Math.ceil(updates.length / 50)}`);
            }
        }
    }

    logInfo('🎉 Cleanup complete.');
}

cleanupTitles().catch(err => {
    console.error('Fatal error during cleanup:', err);
    process.exit(1);
});
