const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');

async function dedupe() {
    logInfo('🚀 Starting Supabase deduplication...');

    // 1. Fetch all IDs, links, and titles
    const { data: movies, error } = await supabase
        .from('moviesv2')
        .select('id, link, title, inserted_at')
        .order('inserted_at', { ascending: false });

    if (error) {
        logError(`❌ Failed to fetch movies: ${error.message}`);
        return;
    }

    logInfo(`📊 Total records fetched: ${movies.length}`);

    const seenLinks = new Map();
    const toDelete = [];

    for (const movie of movies) {
        if (seenLinks.has(movie.link)) {
            toDelete.push(movie.id);
        } else {
            seenLinks.set(movie.link, movie.id);
        }
    }

    logInfo(`🔎 Found ${toDelete.length} duplicates to remove.`);

    if (toDelete.length === 0) {
        logInfo('✅ No duplicates found.');
        return;
    }

    // 2. Perform deletion in batches
    const batchSize = 100;
    let deletedCount = 0;
    for (let i = 0; i < toDelete.length; i += batchSize) {
        const batch = toDelete.slice(i, i + batchSize);
        const { error: delError } = await supabase
            .from('moviesv2')
            .delete()
            .in('id', batch);

        if (delError) {
            logError(`❌ Failed to delete batch: ${delError.message}`);
        } else {
            deletedCount += batch.length;
            logInfo(`🗑️ Deleted ${deletedCount}/${toDelete.length} duplicates...`);
        }
    }

    logInfo('🎉 Deduplication complete.');
}

dedupe().catch(console.error);
