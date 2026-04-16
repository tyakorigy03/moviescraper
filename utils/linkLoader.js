const links = {
  agasobanuyelive: [
    'https://agasobanuyelive.com/category/serie/',
    'https://agasobanuyelive.com/category/movie/'
  ]
};

async function getPredefinedLinks(key) {
  return links[key] || [];
}

module.exports = { getPredefinedLinks };
