const LAST_ACTIVE_USER_KEY = 'baby-name-last-active-user';

const elements = {
  userFilterTabs: document.querySelector('#user-filter-tabs'),
  combinedSelectionSummary: document.querySelector('#combined-selection-summary'),
  combinedStatus: document.querySelector('#combined-status'),
  combinedMeta: document.querySelector('#combined-meta'),
  combinedRankingList: document.querySelector('#combined-ranking-list'),
  backToComparingLink: document.querySelector('#back-to-comparing-link'),
};

const state = {
  users: [],
  selectedUsers: [],
  combinedRanking: null,
};

initializePage();
loadCombinedRanking();

async function loadCombinedRanking() {
  setStatus('Loading');

  try {
    const searchParams = new URLSearchParams();
    if (state.selectedUsers.length) {
      searchParams.set('users', state.selectedUsers.join(','));
    }

    const query = searchParams.toString();
    const payload = await apiFetchJson(`/api/combined${query ? `?${query}` : ''}`);
    applyState(payload);
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
    throw new Error(payload.error || 'Unable to load combined rankings.');
  }

  return payload;
}

function applyState(payload) {
  state.users = payload.users;
  state.selectedUsers = payload.combinedRanking.selectedUsers;
  state.combinedRanking = payload.combinedRanking;
  render();
}

function render() {
  renderUserFilters();
  renderSummary();
  renderRanking();
}

function renderUserFilters() {
  elements.userFilterTabs.innerHTML = '';

  for (const user of state.users) {
    const button = document.createElement('button');
    const included = user.included;
    const status = included ? 'Included' : 'Excluded';

    button.type = 'button';
    button.className = included ? 'person-tab filter-tab included' : 'person-tab filter-tab excluded';
    button.setAttribute('aria-pressed', included ? 'true' : 'false');
    button.innerHTML = `<span class="filter-tab-user">/${user.slug}</span><span class="filter-tab-status">${status}</span>`;
    button.addEventListener('click', () => toggleUser(user.slug));
    elements.userFilterTabs.append(button);
  }
}

function renderSummary() {
  const count = state.combinedRanking.userCount;
  const picks = state.combinedRanking.comparisonCount;

  elements.combinedSelectionSummary.textContent = count
    ? `${count} user${count === 1 ? '' : 's'} selected`
    : 'No users selected';
  elements.combinedMeta.textContent = count
    ? `${picks} total pick${picks === 1 ? '' : 's'} across the selected users.`
    : 'Turn at least one user on to build a combined ranking.';
}

function renderRanking() {
  elements.combinedRankingList.innerHTML = '';

  if (!state.combinedRanking.userCount) {
    renderEmpty('No users selected', 'Click one or more people above.');
    return;
  }

  if (!state.combinedRanking.names.length) {
    renderEmpty('No names yet', 'Add names to begin.');
    return;
  }

  for (const entry of state.combinedRanking.names) {
    const item = document.createElement('li');
    const name = document.createElement('span');
    const score = document.createElement('span');
    name.className = 'ranking-name';
    score.className = 'ranking-score';
    name.textContent = entry.name;
    score.textContent = `${Math.round(entry.rating)} avg rating`;
    item.append(name, score);
    elements.combinedRankingList.append(item);
  }
}

function renderEmpty(nameText, scoreText) {
  const empty = document.createElement('li');
  const emptyName = document.createElement('span');
  const emptyScore = document.createElement('span');
  emptyName.className = 'ranking-name';
  emptyScore.className = 'ranking-score';
  emptyName.textContent = nameText;
  emptyScore.textContent = scoreText;
  empty.append(emptyName, emptyScore);
  elements.combinedRankingList.append(empty);
}

function renderError(message) {
  elements.combinedMeta.textContent = message;
  elements.combinedRankingList.innerHTML = '';
  renderEmpty('Unable to load', message);
}

function setStatus(message) {
  elements.combinedStatus.textContent = message;
}

function toggleUser(slug) {
  if (state.selectedUsers.includes(slug)) {
    state.selectedUsers = state.selectedUsers.filter((entry) => entry !== slug);
  } else {
    state.selectedUsers = [...state.selectedUsers, slug];
  }

  loadCombinedRanking();
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
