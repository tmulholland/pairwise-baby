const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);
const SSA_LIMITS_URL = 'https://www.ssa.gov/oact/babynames/limits.html';
const SSA_ZIP_URL = 'https://www.ssa.gov/oact/babynames/names.zip';
const CACHE_DIR = path.join(__dirname, '.cache');
const CACHE_PATH = path.join(CACHE_DIR, 'ssa-popularity-boys.json');

let cachedYear = null;
let cachedPopularityByName = null;
let refreshPromise = null;

module.exports = {
  ensureNamePopularityColumns,
  initializeSsaPopularity,
  updateSsaPopularityForNames,
};

function ensureNamePopularityColumns(db) {
  const columns = new Set(db.prepare('PRAGMA table_info(names)').all().map((column) => column.name));

  if (!columns.has('ssa_year')) {
    db.exec('ALTER TABLE names ADD COLUMN ssa_year INTEGER');
  }

  if (!columns.has('ssa_births')) {
    db.exec('ALTER TABLE names ADD COLUMN ssa_births INTEGER');
  }

  if (!columns.has('ssa_rank')) {
    db.exec('ALTER TABLE names ADD COLUMN ssa_rank INTEGER');
  }

  if (!columns.has('ssa_updated_at')) {
    db.exec('ALTER TABLE names ADD COLUMN ssa_updated_at TEXT');
  }
}

async function initializeSsaPopularity(db) {
  return refreshSsaPopularityCache(db);
}

function updateSsaPopularityForNames(db, names) {
  if (!cachedPopularityByName || !cachedYear || !Array.isArray(names) || !names.length) {
    return;
  }

  applyPopularityToNames(db, names);
}

async function refreshSsaPopularityCache(db) {
  if (refreshPromise) {
    return refreshPromise;
  }

  refreshPromise = (async () => {
    const latestYear = await fetchLatestFullYear();
    const cached = readCache(latestYear);
    const dataset = cached || await downloadPopularityDataset(latestYear);

    cachedYear = dataset.year;
    cachedPopularityByName = new Map(dataset.names.map((entry) => [entry.name.toLowerCase(), entry]));
    writeCache(dataset);
    applyPopularityToAllNames(db);
  })();

  try {
    await refreshPromise;
  } finally {
    refreshPromise = null;
  }
}

function applyPopularityToAllNames(db) {
  const names = db.prepare('SELECT id, name FROM names ORDER BY id').all();
  applyPopularityToNames(db, names);
}

function applyPopularityToNames(db, names) {
  const update = db.prepare(`
    UPDATE names
    SET ssa_year = ?,
        ssa_births = ?,
        ssa_rank = ?,
        ssa_updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `);

  const transaction = db.transaction((rows) => {
    for (const row of rows) {
      const popularity = cachedPopularityByName.get(String(row.name || '').toLowerCase());
      update.run(
        cachedYear,
        popularity ? popularity.births : null,
        popularity ? popularity.rank : null,
        row.id
      );
    }
  });

  transaction(names);
}

async function fetchLatestFullYear() {
  const response = await fetch(SSA_LIMITS_URL, {
    headers: {
      Accept: 'text/html',
      'User-Agent': 'pairwise-baby/1.0 (ssa popularity cache)',
    },
  });

  if (!response.ok) {
    throw new Error(`SSA limits request failed with ${response.status}`);
  }

  const html = await response.text();
  const matches = [...html.matchAll(/For U\.S\. births in (\d{4})/g)].map((match) => Number(match[1]));

  if (!matches.length) {
    throw new Error('Unable to determine latest SSA full year.');
  }

  return Math.max(...matches);
}

function readCache(expectedYear) {
  try {
    const raw = fs.readFileSync(CACHE_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    if (parsed && parsed.year === expectedYear && Array.isArray(parsed.names)) {
      return parsed;
    }
  } catch (_error) {
    return null;
  }

  return null;
}

function writeCache(dataset) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  fs.writeFileSync(CACHE_PATH, JSON.stringify(dataset));
}

async function downloadPopularityDataset(year) {
  const response = await fetch(SSA_ZIP_URL, {
    headers: {
      Accept: 'application/zip',
      'User-Agent': 'pairwise-baby/1.0 (ssa popularity cache)',
    },
  });

  if (!response.ok) {
    throw new Error(`SSA dataset download failed with ${response.status}`);
  }

  const zipBuffer = Buffer.from(await response.arrayBuffer());
  const zipPath = path.join(os.tmpdir(), `ssa-names-${process.pid}.zip`);
  fs.writeFileSync(zipPath, zipBuffer);

  try {
    const fileName = `yob${year}.txt`;
    const { stdout } = await execFileAsync('unzip', ['-p', zipPath, fileName], {
      encoding: 'utf8',
      maxBuffer: 1024 * 1024 * 12,
    });

    if (!stdout.trim()) {
      throw new Error(`No SSA data found for ${year}`);
    }

    return parsePopularityFile(stdout, year);
  } finally {
    fs.rmSync(zipPath, { force: true });
  }
}

function parsePopularityFile(text, year) {
  const totalsByName = new Map();

  for (const line of text.split(/\r?\n/)) {
    if (!line) {
      continue;
    }

    const [name, sex, countText] = line.split(',');
    const births = Number(countText);

    if (!name || sex !== 'M' || !Number.isFinite(births)) {
      continue;
    }

    const key = name.toLowerCase();
    const current = totalsByName.get(key) || { name, births: 0 };
    current.births += births;
    totalsByName.set(key, current);
  }

  const names = [...totalsByName.values()]
    .sort((left, right) => right.births - left.births || left.name.localeCompare(right.name))
    .map((entry, index) => ({
      name: entry.name,
      births: entry.births,
      rank: index + 1,
    }));

  return {
    year,
    names,
    cachedAt: new Date().toISOString(),
    sourceUrl: SSA_ZIP_URL,
  };
}
