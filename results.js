const LAST_ACTIVE_USER_KEY = 'baby-name-last-active-user';

const elements = {
  rankingColumns: document.querySelector('#ranking-columns'),
  rankingCardTemplate: document.querySelector('#ranking-card-template'),
  resultsStatus: document.querySelector('#results-status'),
  backToComparingLink: document.querySelector('#back-to-comparing-link'),
};

initializePage();
loadResults();

async function loadResults() {
  setStatus('Loading');

  try {
    const payload = await apiFetchJson('/api/results');
    renderRankings(payload.rankings);
    setStatus('Live');
  } catch (error) {
    setStatus('Error');
    renderError(error.message);
  }
}

async function apiFetchJson(url, options) {
  const requestOptions = options || {};
  const response = await fetch(url, {
    ...requestOptions,
    headers: {
      Accept: 'application/json',
      'ngrok-skip-browser-warning': 'true',
      ...(requestOptions.headers || {}),
    },
  });

  const text = await response.text();
  let payload;

  try {
    payload = text ? JSON.parse(text) : {};
  } catch (_error) {
    throw new Error('The server returned a non-JSON response. If you are using the ngrok URL, refresh once and try again.');
  }

  if (!response.ok) {
    throw new Error(payload.error || 'Unable to load rankings.');
  }

  return payload;
}

function renderRankings(rankings) {
  elements.rankingColumns.innerHTML = '';

  for (const ranking of rankings) {
    const fragment = elements.rankingCardTemplate.content.cloneNode(true);
    const personLabel = fragment.querySelector('.ranking-person');
    const title = fragment.querySelector('.ranking-title');
    const summary = fragment.querySelector('.ranking-summary');
    const rankingList = fragment.querySelector('.ranking-list');

    personLabel.textContent = `/${ranking.slug}`;
    title.textContent = 'Current ranking';
    summary.textContent = `${ranking.comparisonCount} picks`;

    if (!ranking.names.length) {
      const empty = document.createElement('li');
      const emptyName = document.createElement('span');
      const emptyScore = document.createElement('span');
      emptyName.className = 'ranking-name';
      emptyScore.className = 'ranking-score';
      emptyName.textContent = 'No names yet';
      emptyScore.textContent = 'Add names to begin.';
      empty.append(emptyName, emptyScore);
      rankingList.append(empty);
    } else {
      for (const entry of ranking.names) {
        const item = document.createElement('li');
        const name = document.createElement('span');
        const score = document.createElement('span');
        name.className = 'ranking-name';
        score.className = 'ranking-score';
        name.textContent = entry.name;
        score.textContent = `${Math.round(entry.rating)} rating`;
        item.append(name, score);
        rankingList.append(item);
      }
    }

    elements.rankingColumns.append(fragment);
  }
}

function renderError(message) {
  elements.rankingColumns.innerHTML = `<p class="muted">${message}</p>`;
}

function setStatus(message) {
  elements.resultsStatus.textContent = message;
}

function initializePage() {
  const activeUser = getLastActiveUser();
  elements.backToComparingLink.href = `/${activeUser}`;
}

function getLastActiveUser() {
  const saved = normalizeSlug(window.localStorage.getItem(LAST_ACTIVE_USER_KEY));
  return saved || 'troy';
}

function normalizeSlug(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40);
}
