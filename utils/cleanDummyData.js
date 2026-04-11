const supabase = require('../services/supabaseClient');
const { logInfo, logError } = require('./logger');

async function cleanJunk() {
    logInfo('🚀 Scanning for dummy/junk data in Supabase...');

    // Define junk as records missing title or having empty Downloadurls
    const { data: movies, error } = await supabase
        .from('moviesv2')
        .select('id, title, Downloadurls, link');

    if (error) {
        logError(`❌ Failed to fetch movies: ${error.message}`);
        return;
    }

    const junk = movies.filter(m => {
        const hasNoTitle = !m.title || m.title.toLowerCase().includes('dummy') || m.title.length < 3;
        const hasNoDownloads = !m.Downloadurls || (Array.isArray(m.Downloadurls) && m.Downloadurls.length === 0);
        return hasNoTitle || hasNoDownloads;
    });

    logInfo(`📊 Total records scanned: ${movies.length}`);
    logInfo(`⚠️ Found ${junk.length} potentially invalid/dummy records.`);

    if (junk.length > 0) {
        logInfo('Sample Junk Records:');
        junk.slice(0, 5).forEach(m => console.log(`- [${m.id}] ${m.title || 'NO TITLE'} (${m.link})`));

        logInfo('\n💡 Recommendation: You can delete these manually or run a bulk delete script targeting these IDs.');
        logInfo('To delete them, use: .delete().in("id", [ids])');
    } else {
        logInfo('✅ No junk data found based on current criteria.');
    }
}

cleanJunk().catch(console.error);
