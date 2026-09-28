/**
 * Assertions for utils/hotMatching.js — the hot-signal resolution rules.
 * Run: npm run test:hot
 */
const assert = require('assert');
const {
  showKey,
  familyKey,
  entityKey,
  rankWeight,
  combineHot,
  buildIndex,
  byPreference,
  pickPrimary,
  resolveShow,
  createLedger,
  applyLedger,
} = require('./hotMatching');

let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed++;
    console.log(`  ok   ${name}`);
  } catch (err) {
    console.error(`  FAIL ${name}\n       ${err.message}`);
    process.exitCode = 1;
  }
}
const near = (a, b, eps = 0.051) => assert.ok(Math.abs(a - b) <= eps, `${a} !~= ${b}`);

/* ------------------------------ identity --------------------------------- */

check('showKey is type-, narrator- and season-blind, like the frontend', () => {
  assert.strictEqual(showKey('Beauty In Black  S01'), showKey('Beauty In Black  S03'));
  assert.strictEqual(showKey('Outer Banks Season 03'), 'outer banks');
  assert.strictEqual(showKey('  '), '');
});

check('a trailing (...) qualifier is not part of the work identity', () => {
  // regression: "Outer Banks S05 (NTABWO ISOBANUYE)" used to be a separate show
  assert.strictEqual(showKey('Outer Banks S05 (NTABWO ISOBANUYE)'), 'outer banks');
  assert.strictEqual(familyKey('Outer Banks S05 (NTABWO ISOBANUYE)', 'tv'), 'outer banks|tv');
  assert.strictEqual(familyKey('Betting Raja (Racha)', 'movie'), 'betting raja|movie');
  assert.strictEqual(familyKey('Wild Heart S01 (Yabani) Pt.3', 'tv'), 'wild heart|tv');
  assert.strictEqual(familyKey('Rambo (2025)', 'movie'), 'rambo|movie');
  assert.strictEqual(familyKey('  ', 'tv'), null);
});

check('bracket qualifiers are stripped like the frontend strips them', () => {
  assert.strictEqual(showKey('Elite S01 [Yabani]'), 'elite');
  assert.strictEqual(showKey('Incognito S01 [Pt.4] (2025)'), 'incognito');
  assert.strictEqual(showKey('Wild Heart S01 (Yabani) Pt.3'), 'wild heart');
  assert.strictEqual(showKey('Show S01 Pt.4]'), 'show');
});

check('entityKey still discriminates narrator when it is known', () => {
  assert.strictEqual(entityKey('Vis a vis', 'tv', 'Rocky'), 'vis a vis|tv|rocky');
  assert.notStrictEqual(entityKey('Vis a vis', 'tv', 'Rocky'), entityKey('Vis a vis', 'tv', ''));
});

/* ------------------------------ weighting -------------------------------- */

check('rankWeight is 0.96 at #1 and 0 at the last slot', () => {
  near(rankWeight(0), 0.96);
  near(rankWeight(24), 0);
  assert.strictEqual(rankWeight(25), 0, 'beyond RANKS must be 0');
});

check('no single list can peg the cap', () => {
  near(combineHot([rankWeight(0)]), 9.6);
  assert.ok(combineHot([rankWeight(0)]) < 10, 'one #1 must not reach 10');
});

check('corroboration across lists pushes higher than a single list', () => {
  const one = combineHot([rankWeight(0)]);
  const two = combineHot([rankWeight(0), rankWeight(0)]);
  assert.ok(two > one, `${two} should exceed ${one}`);
  near(two, 9.98);
});

check('deep ranks are worth less than shallow ones', () => {
  const deep = combineHot([rankWeight(20), rankWeight(20)]);
  const shallow = combineHot([rankWeight(1), rankWeight(2)]);
  assert.ok(shallow > deep, `${shallow} should exceed ${deep}`);
});

check('combineHot never exceeds the cap and ignores junk weights', () => {
  assert.ok(combineHot([1, 1, 1, 1, 1]) <= 10);
  assert.strictEqual(combineHot([]), 0);
  assert.strictEqual(combineHot([NaN, null, -3]), 0);
  assert.strictEqual(combineHot([rankWeight(24)]), 0, 'unranked tail contributes nothing');
});

/* ------------------------------ fixtures --------------------------------- */

const ROWS = [
  { link: 'ob-s01', title: 'Outer Banks Season 01', type: 'tv', narrator: 'Rocky', publishedAt: '2022-05-20', modifiedAt: '2022-05-20', base: 49 },
  { link: 'ob-s04', title: 'Outer Banks Season 04', type: 'tv', narrator: 'Rocky', publishedAt: '2025-05-20', modifiedAt: '2025-05-20', base: 49 },
  { link: 'ob-s05', title: 'Outer Banks S05', type: 'tv', narrator: 'Rocky', publishedAt: '2026-05-20', modifiedAt: '2026-05-20', base: 51 },
  { link: 'ob-s05b', title: 'Outer Banks S05 (NTABWO ISOBANUYE)', type: 'tv', narrator: 'Rocky', publishedAt: '2026-08-21', modifiedAt: '2026-08-21', base: 40 },
  { link: 'bik-s01', title: 'Beauty In Black  S01', type: 'tv', narrator: 'Rocky', publishedAt: '2025-09-13', modifiedAt: '2025-09-13', base: 48.3 },
  { link: 'bik-s02', title: 'Beauty In Black  S02', type: 'tv', narrator: 'Rocky', publishedAt: '2026-07-02', modifiedAt: '2026-07-02', base: 50.6 },
  { link: 'bik-s03', title: 'Beauty In Black  S03', type: 'tv', narrator: 'Rocky', publishedAt: '2026-09-23', modifiedAt: '2026-09-23', base: 22 },
  { link: 'vis', title: 'Vis a vis', type: 'tv', narrator: 'Rocky', publishedAt: '2024-01-01', modifiedAt: '2024-01-01', base: 20 },
  { link: 'tehran-tv', title: 'Tehran', type: 'tv', narrator: 'Sankara', publishedAt: '2026-01-01', modifiedAt: '2026-01-01', base: 25 },
  { link: 'tehran-mv', title: 'Tehran', type: 'movie', narrator: 'Mungeli', publishedAt: '2026-01-02', modifiedAt: '2026-01-02', base: 24 },
];
const scoreRow = (r) => r.base;
const INDEX = buildIndex(ROWS);

/* ------------------------------ resolution -------------------------------- */

check('a narrator-less signal resolves a narrated row (the storage bug)', () => {
  const fake = buildIndex([{ link: 'sg', title: 'Sassy Girl S01', type: 'tv', narrator: 'Bingo', publishedAt: '2024-01-01', base: 1 }]);
  const hit = resolveShow(fake, showKey('Sassy Girl S01'), '', scoreRow);
  assert.ok(hit, 'must resolve');
  assert.strictEqual(hit.row.link, 'sg');
});

check('the boost lands on the highest-scoring season, exactly once', () => {
  const hit = resolveShow(INDEX, showKey('Outer Banks Season 01'), '', scoreRow);
  assert.strictEqual(hit.row.link, 'ob-s05', 'S05 outscores the rest');
  assert.strictEqual(hit.rows.length, 4, 'sees every season incl. the (…)-qualified one');
});

check('attribution mirrors the UI, not recency (Beauty S03 regression)', () => {
  // S03 is the newest season but has no tmdb data, so it scores ~22 while S02
  // scores ~50.6. The UI keeps the highest-scoring row, so the boost must too,
  // otherwise the whole hot signal lands on a row nobody sees.
  const hit = resolveShow(INDEX, showKey('Beauty In Black  S01'), '', scoreRow);
  assert.strictEqual(hit.row.link, 'bik-s02', 'must boost S02, not the newer S03');
  assert.strictEqual(hit.reason, 'score');
  const byRecency = byPreference(ROWS.filter((r) => r.link.startsWith('bik')));
  assert.strictEqual(byRecency[0].link, 'bik-s03', 'recency alone would have picked S03');
});

check('a blank type is demoted below a complete row when scores tie', () => {
  const rows = [
    { link: 'good', title: 'Mutiny', type: 'movie', narrator: 'Perfect', publishedAt: '2026-08-20', modifiedAt: '2026-08-20', base: 30 },
    { link: 'bad', title: 'Mutiny', type: '', narrator: 'Sankara Da Premier', publishedAt: '2026-08-21', modifiedAt: '2026-09-24', base: 30 },
  ];
  assert.strictEqual(pickPrimary(rows, '', scoreRow).row.link, 'good');
});

check('a blank narrator is demoted when a narrated twin ties', () => {
  const rows = [
    { link: 'noname', title: 'Show', type: 'tv', narrator: null, publishedAt: '2026-01-01', base: 5 },
    { link: 'named', title: 'Show', type: 'tv', narrator: 'Rocky', publishedAt: '2025-01-01', base: 5 },
  ];
  assert.strictEqual(pickPrimary(rows, '', scoreRow).row.link, 'named');
});

check('a source-supplied narrator wins when it identifies a single row', () => {
  const idx = buildIndex([
    { link: 'dub-sankara', title: 'Elite S01', type: 'tv', narrator: 'Sankara', publishedAt: '2020-01-01', base: 20 },
    { link: 'dub-dope', title: 'Elite S02', type: 'tv', narrator: 'Dope', publishedAt: '2026-01-01', base: 30 },
  ]);
  const hit = resolveShow(idx, showKey('Elite S02'), 'Sankara', scoreRow);
  assert.strictEqual(hit.row.link, 'dub-sankara');
  assert.strictEqual(hit.reason, 'narrator');
});

check('a family split only by a lost narrator is no longer split', () => {
  const idx = buildIndex([
    { link: 'ys-1', title: 'Young Swordsman S01', type: 'tv', narrator: 'B the Great', publishedAt: '2025-01-01', base: 10 },
    { link: 'ys-2', title: 'Young Swordsman S01', type: 'tv', narrator: 'B da Great', publishedAt: '2025-01-01', base: 12 },
  ]);
  const hit = resolveShow(idx, showKey('Young Swordsman S01'), '', scoreRow);
  assert.strictEqual(hit.rows.length, 2);
  assert.strictEqual(hit.row.link, 'ys-2');
});

check('resolveShow returns null for a work we do not carry', () => {
  assert.strictEqual(resolveShow(INDEX, showKey('Jeepers Creepers Reborn'), '', scoreRow), null);
});

/* -------------------------------- ledger ---------------------------------- */

check('ledger corroborates across lists and sites, once, with provenance', () => {
  const ledger = createLedger();
  ledger.add('Beauty In Black  S01', 'tv', 0, 'aglive#0', '', 9);
  ledger.add('Beauty In Black  S02', 'tv', 1, 'oshakur#1', '', 8.55);
  const { boosts, unresolved } = applyLedger(ledger, INDEX, { scoreRow });
  assert.strictEqual(unresolved.length, 0);
  assert.strictEqual(boosts.length, 1, 'both signals collapse onto one row');
  assert.strictEqual(boosts[0].link, 'bik-s02', 'the row the UI will show');
  assert.deepStrictEqual(boosts[0].sources, ['aglive#0', 'oshakur#1']);
  assert.ok(boosts[0].hot > 9.9, `expected near-cap, got ${boosts[0].hot}`);
  assert.strictEqual(boosts[0].rows, 3);
});

check('a movie and a series sharing a title corroborate one signal', () => {
  const ledger = createLedger();
  ledger.add('Tehran', 'tv', 0, 'oshakur#0', '', 9);
  ledger.add('Tehran', 'movie', 3, 'reba-new#3', '', 7.3);
  const { boosts } = applyLedger(ledger, INDEX, { scoreRow });
  assert.strictEqual(boosts.length, 1, 'must not race to overwrite the same row');
  assert.strictEqual(boosts[0].link, 'tehran-tv', 'higher-scoring row, as the UI would keep');
  assert.strictEqual(boosts[0].sources.length, 2);
  assert.ok(boosts[0].hot > combineHot([rankWeight(0)]), 'weights must combine');
});

check('ledger reports work it cannot attach instead of dropping it', () => {
  const ledger = createLedger();
  ledger.add('The Gorge', 'movie', 0, 'agnow#0', '', 9);
  const { boosts, unresolved } = applyLedger(ledger, INDEX, { scoreRow });
  assert.strictEqual(boosts.length, 0);
  assert.strictEqual(unresolved.length, 1);
  assert.strictEqual(unresolved[0].show, 'the gorge');
});

console.log(`\n  ${passed} passed${process.exitCode ? ', some FAILED' : ''}\n`);
