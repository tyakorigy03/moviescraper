/**
 * Decodes HTML entities (both numeric &#123; and named &amp;) in a string.
 * @param {string} text - The text to decode.
 * @returns {string} - The decoded text.
 */
function decodeHtmlEntities(text) {
    if (!text) return text;

    // Handle numeric entities (e.g., &#8211;)
    let decoded = text.replace(/&#(\d+);/g, (match, dec) => {
        return String.fromCharCode(dec);
    });

    // Handle common named entities
    const entities = {
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&apos;': "'",
        '&nbsp;': ' ',
        '&ndash;': '–',
        '&mdash;': '—',
        '&lsquo;': '‘',
        '&rsquo;': '’',
        '&ldquo;': '“',
        '&rdquo;': '”',
        '&hellip;': '…'
    };

    return decoded.replace(/&[a-z]+;/g, (match) => {
        return entities[match] || match;
    });
}

module.exports = { decodeHtmlEntities };
