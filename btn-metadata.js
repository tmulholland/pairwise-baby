const fs = require('fs');
const path = require('path');

const CACHE_REFRESH_INTERVAL_MS = 1000 * 60 * 60 * 24 * 180;
const FETCH_DELAY_MS = 600;
const API_BASE_URL = 'https://www.behindthename.com/api/lookup.json';
const API_KEY_PATH = path.join(__dirname, '.btn-api-key');

const metadataQueue = [];
const queuedNameIds = new Set();
let isRefreshingMetadata = false;

module.exports = {
  ensureNameBtnColumns,
  initializeBtnMetadata,
  updateBtnMetadataForNames,
};

function ensureNameBtnColumns(db) {
  const columns = new Set(db.prepare('PRAGMA table_info(names)').all().map((column) => column.name));

  if (!columns.has('btn_usage')) {
    db.exec('ALTER TABLE names ADD COLUMN btn_usage TEXT');
  }

  if (!columns.has('btn_origin')) {
    db.exec('ALTER TABLE names ADD COLUMN btn_origin TEXT');
  }

  if (!columns.has('btn_language_root')) {
    db.exec('ALTER TABLE names ADD COLUMN btn_language_root TEXT');
  }

  if (!columns.has('btn_status')) {
    db.exec("ALTER TABLE names ADD COLUMN btn_status TEXT NOT NULL DEFAULT 'pending'");
  }

  if (!columns.has('btn_updated_at')) {
    db.exec('ALTER TABLE names ADD COLUMN btn_updated_at TEXT');
  }
}

async function initializeBtnMetadata(db) {
  if (!hasApiKey()) {
    markMetadataDisabled(db);
    return;
  }

  queueBtnMetadataRefreshForAllNames(db);
  await processMetadataQueue(db);
}

function updateBtnMetadataForNames(db, names) {
  if (!hasApiKey() || !Array.isArray(names) || !names.length) {
    return;
  }

  for (const name of names) {
    if (name && Number.isInteger(name.id)) {
      enqueueMetadataRefresh(name.id);
    }
  }

  void processMetadataQueue(db);
}

function hasApiKey() {
  try {
    return Boolean(fs.readFileSync(API_KEY_PATH, 'utf8').trim());
  } catch (_error) {
    return false;
  }
}

function readApiKey() {
  return fs.readFileSync(API_KEY_PATH, 'utf8').trim();
}

function markMetadataDisabled(db) {
  db.prepare(`
    UPDATE names
    SET btn_status = 'disabled'
    WHERE btn_status = 'pending'
  `).run();
}

function queueBtnMetadataRefreshForAllNames(db) {
  const names = db.prepare(`
    SELECT id, btn_updated_at
    FROM names
    ORDER BY id
  `).all();

  for (const name of names) {
    if (needsMetadataRefresh(name)) {
      enqueueMetadataRefresh(name.id);
    }
  }
}

function enqueueMetadataRefresh(nameId) {
  if (queuedNameIds.has(nameId)) {
    return;
  }

  queuedNameIds.add(nameId);
  metadataQueue.push(nameId);
}

async function processMetadataQueue(db) {
  if (isRefreshingMetadata || !hasApiKey()) {
    return;
  }

  isRefreshingMetadata = true;

  while (metadataQueue.length) {
    const nameId = metadataQueue.shift();
    queuedNameIds.delete(nameId);

    try {
      await refreshNameMetadata(db, nameId);
    } catch (error) {
      console.error(`Unable to refresh Behind the Name metadata for name ${nameId}:`, error);
    }

    await delay(FETCH_DELAY_MS);
  }

  isRefreshingMetadata = false;
}

async function refreshNameMetadata(db, nameId) {
  const entry = db.prepare(`
    SELECT id, name, btn_updated_at
    FROM names
    WHERE id = ?
  `).get(nameId);

  if (!entry || !needsMetadataRefresh(entry)) {
    return;
  }

  const metadata = await fetchBtnMetadata(entry.name);

  db.prepare(`
    UPDATE names
    SET btn_usage = ?,
        btn_origin = ?,
        btn_language_root = ?,
        btn_status = ?,
        btn_updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(metadata.usage, metadata.origin, metadata.languageRoot, metadata.status, nameId);
}

function needsMetadataRefresh(entry) {
  if (!entry || !entry.btn_updated_at) {
    return true;
  }

  const updatedAt = Date.parse(entry.btn_updated_at);
  if (Number.isNaN(updatedAt)) {
    return true;
  }

  return Date.now() - updatedAt > CACHE_REFRESH_INTERVAL_MS;
}

async function fetchBtnMetadata(name) {
  const url = `${API_BASE_URL}?name=${encodeURIComponent(name)}&key=${encodeURIComponent(readApiKey())}`;
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'pairwise-baby/1.0 (behindthename metadata cache)',
    },
  });

  if (!response.ok) {
    throw new Error(`Behind the Name lookup failed with ${response.status}`);
  }

  const payload = await response.json();
  const entry = Array.isArray(payload) ? payload.find((item) => String(item.name || '').toLowerCase() === String(name).toLowerCase()) || payload[0] : null;

  if (!entry || !Array.isArray(entry.usages) || !entry.usages.length) {
    return {
      usage: '',
      origin: '',
      languageRoot: '',
      status: 'missing',
    };
  }

  const usageLabels = [...new Set(entry.usages.map((item) => item.usage_full).filter(Boolean))];
  const origin = usageLabels[0] || '';
  const languageRoot = usageLabels.length > 1 ? usageLabels[usageLabels.length - 1] : '';

  return {
    usage: usageLabels.join(', '),
    origin,
    languageRoot: languageRoot !== origin ? languageRoot : '',
    status: 'ready',
  };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
