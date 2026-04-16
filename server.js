const express = require('express');
const path = require('path');
const Database = require('better-sqlite3');
const { ensureNamePopularityColumns, initializeSsaPopularity, updateSsaPopularityForNames } = require('./ssa-popularity');
const { ensureNameBtnColumns, initializeBtnMetadata, updateBtnMetadataForNames } = require('./btn-metadata');

const PORT = process.env.PORT || 3000;
const DEFAULT_RATING = 1200;
const K_FACTOR = 24;
const DEFAULT_USER = 'troy';
const SUMMARY_REFRESH_INTERVAL_MS = 1000 * 60 * 60 * 24 * 30;
const SUMMARY_FETCH_DELAY_MS = 250;
const app = express();
const db = new Database(path.join(__dirname, 'baby-names.db'));

const summaryQueue = [];
const queuedSummaryIds = new Set();
let isRefreshingSummaries = false;

initializeDatabase();
queueSummaryRefreshForAllNames();
void initializeSsaPopularity(db);
void initializeBtnMetadata(db);

app.use(express.json());
app.use('/static', express.static(__dirname, { index: false }));

app.get('/', (_req, res) => {
  res.redirect(`/${DEFAULT_USER}`);
});

app.get('/results', (_req, res) => {
  res.sendFile(path.join(__dirname, 'results.html'));
});

app.get('/combined', (_req, res) => {
  res.sendFile(path.join(__dirname, 'combined.html'));
});

app.get('/api/results', (_req, res) => {
  try {
    ensureRatingsForAllUsers();
    res.json(buildState(DEFAULT_USER));
  } catch (error) {
    handleError(res, error);
  }
});

app.get('/api/combined', (req, res) => {
  try {
    ensureRatingsForAllUsers();
    res.json(buildCombinedState(req.query.users));
  } catch (error) {
    handleError(res, error);
  }
});

app.get('/api/state/:userSlug', (req, res) => {
  try {
    const user = ensureUser(req.params.userSlug);
    ensureRatingsForAllUsers();
    res.json(buildState(user.slug));
  } catch (error) {
    handleError(res, error);
  }
});

app.post('/api/users', (req, res) => {
  try {
    const user = ensureUser(req.body.slug);
    ensureRatingsForUser(user.id);
    res.status(201).json({ user });
  } catch (error) {
    handleError(res, error);
  }
});

app.post('/api/names', (req, res) => {
  try {
    const submittedNames = Array.isArray(req.body.names) ? req.body.names : [];
    const addedNames = addNames(submittedNames);
    ensureRatingsForAllUsers();
    queueSummaryRefreshForNames(addedNames);
    updateSsaPopularityForNames(db, addedNames);
    updateBtnMetadataForNames(db, addedNames);
    res.status(201).json({ state: buildState(normalizeSlug(req.body.userSlug) || DEFAULT_USER) });
  } catch (error) {
    handleError(res, error);
  }
});

app.delete('/api/names/:nameId', (req, res) => {
  try {
    const activeUserSlug = normalizeSlug(req.query.userSlug) || DEFAULT_USER;
    deleteName(Number(req.params.nameId));
    res.json({ state: buildState(activeUserSlug) });
  } catch (error) {
    handleError(res, error);
  }
});

app.post('/api/comparisons', (req, res) => {
  try {
    const user = ensureUser(req.body.userSlug);
    const winnerId = Number(req.body.winnerId);
    const loserId = Number(req.body.loserId);

    if (!Number.isInteger(winnerId) || !Number.isInteger(loserId) || winnerId === loserId) {
      res.status(400).json({ error: 'A comparison needs two distinct names.' });
      return;
    }

    recordComparison(user.id, winnerId, loserId);
    res.status(201).json({ state: buildState(user.slug) });
  } catch (error) {
    handleError(res, error);
  }
});

app.get(/^\/api\/.*/, (_req, res) => {
  res.status(404).json({ error: 'Not found.' });
});

app.get(/.*/, (_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Baby Name Pairwise running at http://localhost:${PORT}`);
});

function initializeDatabase() {
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      slug TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS names (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS ratings (
      user_id INTEGER NOT NULL,
      name_id INTEGER NOT NULL,
      rating REAL NOT NULL DEFAULT ${DEFAULT_RATING},
      PRIMARY KEY (user_id, name_id),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (name_id) REFERENCES names(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS comparisons (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      winner_name_id INTEGER NOT NULL,
      loser_name_id INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (winner_name_id) REFERENCES names(id) ON DELETE CASCADE,
      FOREIGN KEY (loser_name_id) REFERENCES names(id) ON DELETE CASCADE
    );
  `);

  ensureNameSummaryColumns();
  ensureNamePopularityColumns(db);
  ensureNameBtnColumns(db);
  ensureUser(DEFAULT_USER);
}

function ensureNameSummaryColumns() {
  const columns = new Set(db.prepare('PRAGMA table_info(names)').all().map((column) => column.name));

  if (!columns.has('wiki_summary')) {
    db.exec("ALTER TABLE names ADD COLUMN wiki_summary TEXT");
  }

  if (!columns.has('wiki_source_url')) {
    db.exec("ALTER TABLE names ADD COLUMN wiki_source_url TEXT");
  }

  if (!columns.has('wiki_status')) {
    db.exec("ALTER TABLE names ADD COLUMN wiki_status TEXT NOT NULL DEFAULT 'pending'");
  }

  if (!columns.has('wiki_updated_at')) {
    db.exec("ALTER TABLE names ADD COLUMN wiki_updated_at TEXT");
  }
}

function ensureUser(rawSlug) {
  const slug = normalizeSlug(rawSlug);

  if (!slug) {
    throw new Error('User slug is required.');
  }

  db.prepare('INSERT OR IGNORE INTO users (slug) VALUES (?)').run(slug);

  const user = db.prepare('SELECT id, slug FROM users WHERE slug = ?').get(slug);
  ensureRatingsForUser(user.id);
  return user;
}

function addNames(rawNames) {
  const insert = db.prepare("INSERT OR IGNORE INTO names (name, wiki_status) VALUES (?, 'pending')");
  const findByName = db.prepare('SELECT id, name FROM names WHERE name = ? COLLATE NOCASE');
  const addedNames = [];

  const transaction = db.transaction((names) => {
    for (const rawName of names) {
      const name = cleanName(rawName);
      if (!name) {
        continue;
      }

      const result = insert.run(name);
      const entry = findByName.get(name);

      if (result.changes > 0 && entry) {
        addedNames.push(entry);
      }
    }
  });

  transaction(rawNames);
  return addedNames;
}

function deleteName(nameId) {
  if (!Number.isInteger(nameId)) {
    throw new Error('A valid name id is required.');
  }

  const name = db.prepare('SELECT id FROM names WHERE id = ?').get(nameId);

  if (!name) {
    throw new Error('Name not found.');
  }

  db.transaction(() => {
    db.prepare('DELETE FROM comparisons WHERE winner_name_id = ? OR loser_name_id = ?').run(nameId, nameId);
    db.prepare('DELETE FROM ratings WHERE name_id = ?').run(nameId);
    db.prepare('DELETE FROM names WHERE id = ?').run(nameId);
  })();
}

function ensureRatingsForAllUsers() {
  const users = db.prepare('SELECT id FROM users').all();
  for (const user of users) {
    ensureRatingsForUser(user.id);
  }
}

function ensureRatingsForUser(userId) {
  db.prepare(`
    INSERT OR IGNORE INTO ratings (user_id, name_id, rating)
    SELECT ?, names.id, ${DEFAULT_RATING}
    FROM names
  `).run(userId);
}

function recordComparison(userId, winnerId, loserId) {
  const winner = db.prepare('SELECT id FROM names WHERE id = ?').get(winnerId);
  const loser = db.prepare('SELECT id FROM names WHERE id = ?').get(loserId);

  if (!winner || !loser) {
    throw new Error('Both names must exist before comparing them.');
  }

  ensureRatingsForUser(userId);

  db.transaction(() => {
    const winnerRow = db.prepare('SELECT rating FROM ratings WHERE user_id = ? AND name_id = ?').get(userId, winnerId);
    const loserRow = db.prepare('SELECT rating FROM ratings WHERE user_id = ? AND name_id = ?').get(userId, loserId);

    const winnerExpected = expectedScore(winnerRow.rating, loserRow.rating);
    const loserExpected = expectedScore(loserRow.rating, winnerRow.rating);
    const nextWinnerRating = winnerRow.rating + K_FACTOR * (1 - winnerExpected);
    const nextLoserRating = loserRow.rating + K_FACTOR * (0 - loserExpected);

    db.prepare('UPDATE ratings SET rating = ? WHERE user_id = ? AND name_id = ?').run(nextWinnerRating, userId, winnerId);
    db.prepare('UPDATE ratings SET rating = ? WHERE user_id = ? AND name_id = ?').run(nextLoserRating, userId, loserId);
    db.prepare('INSERT INTO comparisons (user_id, winner_name_id, loser_name_id) VALUES (?, ?, ?)').run(userId, winnerId, loserId);
  })();
}

function buildState(activeSlug) {
  const activeUser = ensureUser(activeSlug);
  ensureRatingsForAllUsers();

  const users = db.prepare('SELECT id, slug FROM users ORDER BY slug').all();
  const nameComparisonRows = db.prepare(`
    SELECT name_id, COUNT(*) AS count
    FROM (
      SELECT winner_name_id AS name_id
      FROM comparisons
      WHERE user_id = ?
      UNION ALL
      SELECT loser_name_id AS name_id
      FROM comparisons
      WHERE user_id = ?
    )
    GROUP BY name_id
  `).all(activeUser.id, activeUser.id);
  const nameComparisonCounts = new Map(nameComparisonRows.map((row) => [row.name_id, row.count]));
  const names = db.prepare(`
    SELECT id, name, wiki_summary, wiki_source_url, wiki_status, wiki_updated_at, ssa_year, ssa_births, ssa_rank, btn_usage, btn_origin, btn_language_root, btn_status
    FROM names
    ORDER BY name COLLATE NOCASE
  `).all().map((name) => ({
    id: name.id,
    name: name.name,
    summary: name.wiki_summary || '',
    summarySourceUrl: name.wiki_source_url || '',
    summaryStatus: name.wiki_status || 'pending',
    summaryUpdatedAt: name.wiki_updated_at || null,
    ssaYear: name.ssa_year || null,
    ssaBirths: name.ssa_births || null,
    ssaRank: name.ssa_rank || null,
    btnUsage: name.btn_usage || '',
    btnOrigin: name.btn_origin || '',
    btnLanguageRoot: name.btn_language_root || '',
    btnStatus: name.btn_status || 'pending',
    comparisonCount: nameComparisonCounts.get(name.id) || 0,
  }));
  const comparisonRows = db.prepare(`
    SELECT user_id, COUNT(*) AS count
    FROM comparisons
    GROUP BY user_id
  `).all();
  const comparisonCounts = new Map(comparisonRows.map((row) => [row.user_id, row.count]));

  const rankings = users.map((user) => {
    const rankedNames = db.prepare(`
      SELECT names.id, names.name, ratings.rating
      FROM ratings
      JOIN names ON names.id = ratings.name_id
      WHERE ratings.user_id = ?
      ORDER BY ratings.rating DESC, names.name COLLATE NOCASE ASC
    `).all(user.id);

    return {
      slug: user.slug,
      comparisonCount: comparisonCounts.get(user.id) || 0,
      names: rankedNames,
    };
  });

  return {
    activeUser: activeUser.slug,
    users: users.map((user) => ({ slug: user.slug })),
    names,
    rankings,
  };
}

function buildCombinedState(rawSelectedUsers) {
  const users = db.prepare('SELECT id, slug FROM users ORDER BY slug').all();
  const names = db.prepare('SELECT id, name FROM names ORDER BY name COLLATE NOCASE').all();
  const comparisonRows = db.prepare(`
    SELECT user_id, COUNT(*) AS count
    FROM comparisons
    GROUP BY user_id
  `).all();
  const comparisonCounts = new Map(comparisonRows.map((row) => [row.user_id, row.count]));
  const selectedSlugs = resolveSelectedSlugs(rawSelectedUsers, users);
  const selectedUsers = users.filter((user) => selectedSlugs.includes(user.slug));
  const ratingRows = selectedUsers.length
    ? db.prepare(`
        SELECT ratings.name_id, ratings.rating
        FROM ratings
        WHERE ratings.user_id IN (${selectedUsers.map(() => '?').join(', ')})
      `).all(...selectedUsers.map((user) => user.id))
    : [];

  const ratingsByNameId = new Map(names.map((name) => [name.id, []]));
  for (const row of ratingRows) {
    ratingsByNameId.get(row.name_id).push(row.rating);
  }

  const combinedNames = names
    .map((name) => {
      const selectedRatings = ratingsByNameId.get(name.id) || [];
      const combinedRating = selectedRatings.length
        ? selectedRatings.reduce((sum, rating) => sum + rating, 0) / selectedRatings.length
        : DEFAULT_RATING;

      return {
        id: name.id,
        name: name.name,
        rating: combinedRating,
      };
    })
    .sort((left, right) => right.rating - left.rating || left.name.localeCompare(right.name));

  return {
    users: users.map((user) => ({
      slug: user.slug,
      included: selectedSlugs.includes(user.slug),
    })),
    combinedRanking: {
      selectedUsers: selectedSlugs,
      userCount: selectedUsers.length,
      comparisonCount: selectedUsers.reduce((sum, user) => sum + (comparisonCounts.get(user.id) || 0), 0),
      names: combinedNames,
    },
  };
}

function resolveSelectedSlugs(rawSelectedUsers, users) {
  const allSlugs = users.map((user) => user.slug);
  const submittedValues = Array.isArray(rawSelectedUsers) ? rawSelectedUsers : [rawSelectedUsers];
  const submittedSlugs = submittedValues
    .flatMap((value) => String(value || '').split(','))
    .map((value) => normalizeSlug(value))
    .filter(Boolean);

  if (!submittedSlugs.length) {
    return allSlugs;
  }

  return allSlugs.filter((slug) => submittedSlugs.includes(slug));
}

function queueSummaryRefreshForAllNames() {
  const names = db.prepare(`
    SELECT id, wiki_status, wiki_updated_at
    FROM names
    ORDER BY id
  `).all();

  for (const name of names) {
    if (needsSummaryRefresh(name)) {
      enqueueSummaryRefresh(name.id);
    }
  }
}

function queueSummaryRefreshForNames(names) {
  for (const name of names) {
    if (name && Number.isInteger(name.id)) {
      enqueueSummaryRefresh(name.id);
    }
  }
}

function enqueueSummaryRefresh(nameId) {
  if (queuedSummaryIds.has(nameId)) {
    return;
  }

  queuedSummaryIds.add(nameId);
  summaryQueue.push(nameId);
  void processSummaryQueue();
}

async function processSummaryQueue() {
  if (isRefreshingSummaries) {
    return;
  }

  isRefreshingSummaries = true;

  while (summaryQueue.length) {
    const nameId = summaryQueue.shift();
    queuedSummaryIds.delete(nameId);

    try {
      await refreshNameSummary(nameId);
    } catch (error) {
      console.error(`Unable to refresh summary for name ${nameId}:`, error);
    }

    await delay(SUMMARY_FETCH_DELAY_MS);
  }

  isRefreshingSummaries = false;
}

async function refreshNameSummary(nameId) {
  const entry = db.prepare(`
    SELECT id, name, wiki_status, wiki_updated_at
    FROM names
    WHERE id = ?
  `).get(nameId);

  if (!entry || !needsSummaryRefresh(entry)) {
    return;
  }

  const summary = await fetchWikipediaSummary(entry.name);

  db.prepare(`
    UPDATE names
    SET wiki_summary = ?,
        wiki_source_url = ?,
        wiki_status = ?,
        wiki_updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(summary.summary, summary.sourceUrl, summary.status, nameId);
}

function needsSummaryRefresh(entry) {
  if (!entry) {
    return false;
  }

  if (!entry.wiki_updated_at) {
    return true;
  }

  const updatedAt = Date.parse(entry.wiki_updated_at);
  if (Number.isNaN(updatedAt)) {
    return true;
  }

  return Date.now() - updatedAt > SUMMARY_REFRESH_INTERVAL_MS;
}

async function fetchWikipediaSummary(name) {
  const titles = [`${name}_(given_name)`, `${name}_(name)`, name];

  for (const title of titles) {
    const payload = await fetchWikipediaSummaryPayload(title);
    if (!payload || !isUsefulNameSummary(payload, title, name)) {
      continue;
    }

    const extract = cleanSummary(payload.extract);
    if (!extract) {
      continue;
    }

    return {
      summary: extract,
      sourceUrl: payload.content_urls?.desktop?.page || payload.content_urls?.mobile?.page || '',
      status: 'ready',
    };
  }

  return {
    summary: '',
    sourceUrl: '',
    status: 'missing',
  };
}

async function fetchWikipediaSummaryPayload(title) {
  const url = `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`;

  try {
    const response = await fetch(url, {
      headers: {
        Accept: 'application/json',
        'User-Agent': 'pairwise-baby/1.0 (name summary fetch)',
      },
    });

    if (!response.ok) {
      return null;
    }

    return response.json();
  } catch (_error) {
    return null;
  }
}

function isUsefulNameSummary(payload, requestedTitle, name) {
  const canonical = String(payload?.titles?.canonical || payload?.title || '').replace(/ /g, '_');
  const requested = requestedTitle.replace(/ /g, '_');
  const normalizedName = name.toLowerCase();
  const canonicalLower = canonical.toLowerCase();
  const extractLower = String(payload?.extract || '').toLowerCase();

  if (!canonical || payload?.type === 'disambiguation' || extractLower.includes('may refer to')) {
    return false;
  }

  if (requested.endsWith('_(given_name)') || requested.endsWith('_(name)')) {
    return canonicalLower === requested.toLowerCase();
  }

  return canonicalLower === normalizedName || canonicalLower.startsWith(`${normalizedName}_(`);
}

function cleanSummary(value) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) {
    return '';
  }

  const sentences = text.match(/[^.!?]+[.!?]+/g) || [text];
  const shortText = sentences.slice(0, 2).join(' ').trim();
  const clipped = shortText.length > 180 ? `${shortText.slice(0, 177).trimEnd()}...` : shortText;
  return clipped;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSlug(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40);
}

function cleanName(value) {
  return String(value || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, 80);
}

function expectedScore(playerRating, opponentRating) {
  return 1 / (1 + 10 ** ((opponentRating - playerRating) / 400));
}

function handleError(res, error) {
  const message = error instanceof Error ? error.message : 'Unknown error';
  res.status(400).json({ error: message });
}
