const USER_CONFIRM_PREFIX = 'baby-name-confirmed-user:';
const LAST_ACTIVE_USER_KEY = 'baby-name-last-active-user';
const PRIORITIZE_FEWER_KEY = 'baby-name-prioritize-fewer-comparisons';
const SUMMARY_POLL_DELAY_MS = 3000;
const elements = {
  userForm: document.querySelector('#user-form'),
  userSlug: document.querySelector('#user-slug'),
  currentUserHeading: document.querySelector('#current-user-heading'),
  currentUserPill: document.querySelector('#current-user-pill'),
  activeUserInline: document.querySelector('#active-user-inline'),
  userList: document.querySelector('#user-list'),
  bulkAddForm: document.querySelector('#bulk-add-form'),
  bulkNames: document.querySelector('#bulk-names'),
  singleAddForm: document.querySelector('#single-add-form'),
  singleName: document.querySelector('#single-name'),
  matchupState: document.querySelector('#matchup-state'),
  matchup: document.querySelector('#matchup'),
  choiceLeft: document.querySelector('#choice-left'),
  choiceRight: document.querySelector('#choice-right'),
  skipPair: document.querySelector('#skip-pair'),
  comparisonCount: document.querySelector('#comparison-count'),
  prioritizeFewerToggle: document.querySelector('#prioritize-fewer-toggle'),
  manualLeftSelect: document.querySelector('#manual-left-select'),
  manualRightSelect: document.querySelector('#manual-right-select'),
  manualCompareReset: document.querySelector('#manual-compare-reset'),
  manualCompareNote: document.querySelector('#manual-compare-note'),
  nameCount: document.querySelector('#name-count'),
  nameList: document.querySelector('#name-list'),
  personalRankingUser: document.querySelector('#personal-ranking-user'),
  personalRankingSummary: document.querySelector('#personal-ranking-summary'),
  personalRankingList: document.querySelector('#personal-ranking-list'),
  confirmOverlay: document.querySelector('#user-confirm-overlay'),
  confirmUserName: document.querySelector('#confirm-user-name'),
  confirmUserYes: document.querySelector('#confirm-user-yes'),
  confirmUserNo: document.querySelector('#confirm-user-no'),
};

const state = {
  activeUser: getUserSlugFromPath(),
  users: [],
  names: [],
  rankings: [],
  prioritizeFewerComparisons: getSavedPrioritizeFewerPreference(),
  manualLeftId: '',
  manualRightId: '',
};

let currentPair = null;
let summaryPollTimer = null;

bindEvents();
loadState();

function bindEvents() {
  elements.userForm.addEventListener('submit', handleUserSubmit);
  elements.bulkAddForm.addEventListener('submit', handleBulkAdd);
  elements.singleAddForm.addEventListener('submit', handleSingleAdd);
  elements.choiceLeft.addEventListener('click', () => submitComparison(currentPair ? currentPair.leftId : null));
  elements.choiceRight.addEventListener('click', () => submitComparison(currentPair ? currentPair.rightId : null));
  elements.skipPair.addEventListener('click', () => {
    currentPair = null;
    renderMatchup();
  });
  elements.prioritizeFewerToggle.addEventListener('change', handlePrioritizeFewerToggle);
  elements.manualLeftSelect.addEventListener('change', handleManualCompareChange);
  elements.manualRightSelect.addEventListener('change', handleManualCompareChange);
  elements.manualCompareReset.addEventListener('click', resetManualCompare);
  elements.confirmUserYes.addEventListener('click', confirmActiveUser);
  elements.confirmUserNo.addEventListener('click', chooseAnotherUser);
}

async function loadState() {
  clearSummaryPoll();
  setLoading(true);

  try {
    const payload = await apiFetchJson(`/api/state/${encodeURIComponent(state.activeUser)}`);
    applyState(payload);
  } catch (error) {
    showError(error.message);
  } finally {
    setLoading(false);
  }
}

async function handleUserSubmit(event) {
  event.preventDefault();
  const slug = normalizeSlug(elements.userSlug.value);

  if (!slug) {
    return;
  }

  try {
    const payload = await apiFetchJson('/api/users', {
      method: 'POST',
      body: JSON.stringify({ slug }),
    });
    window.location.href = `/${payload.user.slug}`;
  } catch (error) {
    showError(error.message);
  }
}

async function handleBulkAdd(event) {
  event.preventDefault();
  const names = elements.bulkNames.value.split('\n').map(cleanName).filter(Boolean);

  if (!names.length) {
    return;
  }

  await addNames(names);
  elements.bulkNames.value = '';
}

async function handleSingleAdd(event) {
  event.preventDefault();
  const name = cleanName(elements.singleName.value);

  if (!name) {
    return;
  }

  await addNames([name]);
  elements.singleName.value = '';
}

async function addNames(names) {
  try {
    const payload = await apiFetchJson('/api/names', {
      method: 'POST',
      body: JSON.stringify({ names, userSlug: state.activeUser }),
    });
    applyState(payload.state);
  } catch (error) {
    showError(error.message);
  }
}

async function deleteName(nameId, name) {
  const confirmed = window.confirm(`Delete ${name} from the shared name pool? This removes it from every user's rankings and comparison history.`);

  if (!confirmed) {
    return;
  }

  try {
    const payload = await apiFetchJson(`/api/names/${nameId}?userSlug=${encodeURIComponent(state.activeUser)}`, {
      method: 'DELETE',
    });
    applyState(payload.state);
  } catch (error) {
    showError(error.message);
  }
}

async function submitComparison(winnerId) {
  if (!currentPair || !winnerId) {
    return;
  }

  const loserId = winnerId === currentPair.leftId ? currentPair.rightId : currentPair.leftId;

  try {
    const payload = await apiFetchJson('/api/comparisons', {
      method: 'POST',
      body: JSON.stringify({
        userSlug: state.activeUser,
        winnerId,
        loserId,
      }),
    });
    applyState(payload.state);
  } catch (error) {
    showError(error.message);
  }
}

async function apiFetchJson(url, options) {
  const requestOptions = options || {};
  const headers = {
    Accept: 'application/json',
    'ngrok-skip-browser-warning': 'true',
  };

  if (requestOptions.body) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(url, {
    ...requestOptions,
    headers: {
      ...headers,
      ...(requestOptions.headers || {}),
    },
  });

  const text = await response.text();
  let payload;

  try {
    payload = text ? JSON.parse(text) : {};
  } catch (_error) {
    throw new Error('Oops! Please refresh and try again.');
  }

  if (!response.ok) {
    throw new Error(payload.error || 'The request failed.');
  }

  return payload;
}

function applyState(payload) {
  clearSummaryPoll();
  state.activeUser = payload.activeUser;
  window.localStorage.setItem(LAST_ACTIVE_USER_KEY, state.activeUser);
  state.users = payload.users;
  state.names = payload.names;
  state.rankings = payload.rankings;
  currentPair = null;
  render();
}

function render() {
  renderHeader();
  renderUsers();
  renderNames();
  renderManualCompareControls();
  renderMatchup();
  renderPersonalRanking();
  renderUserConfirmation();
}

function renderHeader() {
  const label = `/${state.activeUser}`;
  elements.currentUserHeading.textContent = label;
  elements.currentUserPill.textContent = 'SQLite-backed';
  elements.activeUserInline.textContent = label;
  elements.personalRankingUser.textContent = label;
  elements.prioritizeFewerToggle.checked = state.prioritizeFewerComparisons;
}

function renderUsers() {
  elements.userList.innerHTML = '';

  for (const user of state.users) {
    const item = document.createElement('li');
    const link = document.createElement('a');
    link.href = `/${user.slug}`;
    link.textContent = `/${user.slug}`;
    link.className = user.slug === state.activeUser ? 'user-link active' : 'user-link';
    item.append(link);
    elements.userList.append(item);
  }
}

function renderNames() {
  elements.nameList.innerHTML = '';
  elements.nameCount.textContent = `${state.names.length} ${state.names.length === 1 ? 'name' : 'names'}`;

  for (const entry of state.names) {
    const item = document.createElement('li');
    item.className = 'name-list-item';

    const label = document.createElement('span');
    label.className = 'name-list-label';
    label.textContent = entry.name;

    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'name-delete-button';
    deleteButton.textContent = 'Delete';
    deleteButton.addEventListener('click', () => {
      deleteName(entry.id, entry.name);
    });

    item.append(label, deleteButton);
    elements.nameList.append(item);
  }
}


function renderManualCompareControls() {
  const validIds = new Set(state.names.map((entry) => String(entry.id)));

  if (!validIds.has(state.manualLeftId)) {
    state.manualLeftId = '';
  }

  if (!validIds.has(state.manualRightId)) {
    state.manualRightId = '';
  }

  populateManualSelect(elements.manualLeftSelect, state.manualLeftId, state.manualRightId, 'Choose left name');
  populateManualSelect(elements.manualRightSelect, state.manualRightId, state.manualLeftId, 'Choose right name');

  if (state.names.length < 2) {
    elements.manualLeftSelect.disabled = true;
    elements.manualRightSelect.disabled = true;
    elements.manualCompareReset.disabled = true;
    elements.manualCompareNote.textContent = 'Add at least two names to compare manually.';
    return;
  }

  elements.manualLeftSelect.disabled = false;
  elements.manualRightSelect.disabled = false;
  elements.manualCompareReset.disabled = !state.manualLeftId && !state.manualRightId;

  if (state.manualLeftId && state.manualRightId) {
    elements.manualCompareNote.textContent = state.manualLeftId === state.manualRightId
      ? 'Choose two different names to force a specific comparison.'
      : 'Manual matchup active. Pick the winner above or reset to go back to random.';
    return;
  }

  if (state.manualLeftId || state.manualRightId) {
    elements.manualCompareNote.textContent = 'Pinned name active. The other side stays random until you choose it too.';
    return;
  }

  elements.manualCompareNote.textContent = 'Choose one or two names to control the matchup.';
}

function populateManualSelect(select, selectedId, otherId, placeholder) {
  select.innerHTML = '';

  const placeholderOption = document.createElement('option');
  placeholderOption.value = '';
  placeholderOption.textContent = placeholder;
  select.append(placeholderOption);

  for (const entry of state.names) {
    const option = document.createElement('option');
    option.value = String(entry.id);
    option.textContent = entry.name;
    option.disabled = String(entry.id) === otherId;
    option.selected = String(entry.id) === selectedId;
    select.append(option);
  }

  select.value = selectedId;
}

function renderMatchup() {
  const enoughNames = state.names.length >= 2;
  const activeRanking = getActiveRanking();
  elements.matchup.classList.toggle('hidden', !enoughNames);
  elements.matchupState.classList.toggle('hidden', enoughNames);

  if (!enoughNames) {
    elements.matchupState.innerHTML = '<p>Add at least two names to start comparing.</p>';
    elements.comparisonCount.textContent = '0 comparisons recorded';
    return;
  }

  const manualPair = getManualPair();

  if (manualPair) {
    currentPair = manualPair;
  } else if (!currentPair || !hasPairInNamePool(currentPair)) {
    currentPair = chooseRandomPair();
  }

  const left = findNameById(currentPair.leftId);
  const right = findNameById(currentPair.rightId);

  elements.choiceLeft.innerHTML = renderChoiceCardContent(left);
  elements.choiceRight.innerHTML = renderChoiceCardContent(right);

  if (left.summaryStatus === 'pending' || right.summaryStatus === 'pending') {
    queueSummaryPoll();
  }

  const comparisonCount = activeRanking ? activeRanking.comparisonCount : 0;
  elements.comparisonCount.textContent = `${comparisonCount} comparison${comparisonCount === 1 ? '' : 's'} recorded`;
}

function renderPersonalRanking() {
  const activeRanking = getActiveRanking();
  elements.personalRankingList.innerHTML = '';

  if (!activeRanking) {
    elements.personalRankingSummary.textContent = '0 picks';
    const empty = document.createElement('li');
    const emptyName = document.createElement('span');
    const emptyScore = document.createElement('span');
    emptyName.className = 'ranking-name';
    emptyScore.className = 'ranking-score';
    emptyName.textContent = 'No ranking yet';
    emptyScore.textContent = 'Compare names to build your order.';
    empty.append(emptyName, emptyScore);
    elements.personalRankingList.append(empty);
    return;
  }

  elements.personalRankingSummary.textContent = `${activeRanking.comparisonCount} picks`;

  if (!activeRanking.names.length) {
    const empty = document.createElement('li');
    const emptyName = document.createElement('span');
    const emptyScore = document.createElement('span');
    emptyName.className = 'ranking-name';
    emptyScore.className = 'ranking-score';
    emptyName.textContent = 'No names yet';
    emptyScore.textContent = 'Add names to begin.';
    empty.append(emptyName, emptyScore);
    elements.personalRankingList.append(empty);
    return;
  }

  for (const entry of activeRanking.names) {
    const item = document.createElement('li');
    const name = document.createElement('span');
    const score = document.createElement('span');
    name.className = 'ranking-name';
    score.className = 'ranking-score';
    name.textContent = entry.name;
    score.textContent = `${Math.round(entry.rating)} rating`;
    item.append(name, score);
    elements.personalRankingList.append(item);
  }
}

function renderUserConfirmation() {
  const label = `/${state.activeUser}`;
  elements.confirmUserName.textContent = label;
  elements.confirmOverlay.classList.toggle('hidden', isActiveUserConfirmed());
}

function isActiveUserConfirmed() {
  return window.localStorage.getItem(getUserConfirmKey(state.activeUser)) === 'true';
}

function confirmActiveUser() {
  window.localStorage.setItem(getUserConfirmKey(state.activeUser), 'true');
  renderUserConfirmation();
}

function chooseAnotherUser() {
  elements.confirmOverlay.classList.add('hidden');
  elements.userSlug.value = '';
  elements.userSlug.focus();
  elements.userForm.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function getUserConfirmKey(slug) {
  return `${USER_CONFIRM_PREFIX}${slug}`;
}

function getActiveRanking() {
  return state.rankings.find((ranking) => ranking.slug === state.activeUser);
}


function getManualPair() {
  const leftId = state.manualLeftId ? Number(state.manualLeftId) : null;
  const rightId = state.manualRightId ? Number(state.manualRightId) : null;

  if (leftId && rightId) {
    if (leftId === rightId) {
      return null;
    }

    const left = findNameById(leftId);
    const right = findNameById(rightId);
    return left && right ? { leftId: left.id, rightId: right.id } : null;
  }

  if (leftId) {
    const left = findNameById(leftId);
    if (!left) {
      return null;
    }

    const right = resolvePinnedOpponent('rightId', left.id);
    return right ? { leftId: left.id, rightId: right.id } : null;
  }

  if (rightId) {
    const right = findNameById(rightId);
    if (!right) {
      return null;
    }

    const left = resolvePinnedOpponent('leftId', right.id);
    return left ? { leftId: left.id, rightId: right.id } : null;
  }

  return null;
}

function resolvePinnedOpponent(sideKey, excludedId) {
  const currentOpponentId = currentPair ? currentPair[sideKey] : null;
  const currentOpponent = currentOpponentId ? findNameById(currentOpponentId) : null;

  if (currentOpponent && currentOpponent.id !== excludedId) {
    return currentOpponent;
  }

  const pool = state.names.filter((entry) => entry.id !== excludedId);

  if (!pool.length) {
    return null;
  }

  return pool[Math.floor(Math.random() * pool.length)];
}

function chooseRandomPair() {
  const pool = [...state.names];
  const left = state.prioritizeFewerComparisons
    ? chooseWeightedName(pool)
    : pool[Math.floor(Math.random() * pool.length)];
  const remainingPool = pool.filter((entry) => entry.id !== left.id);
  const right = remainingPool[Math.floor(Math.random() * remainingPool.length)];
  return { leftId: left.id, rightId: right.id };
}

function chooseWeightedName(pool) {
  const weightedPool = pool.map((entry) => ({
    entry,
    weight: 1 / (1 + (entry.comparisonCount || 0)) ** 2,
  }));
  const totalWeight = weightedPool.reduce((sum, item) => sum + item.weight, 0);

  if (!totalWeight) {
    return pool[Math.floor(Math.random() * pool.length)];
  }

  let threshold = Math.random() * totalWeight;

  for (const item of weightedPool) {
    threshold -= item.weight;
    if (threshold <= 0) {
      return item.entry;
    }
  }

  return weightedPool[weightedPool.length - 1].entry;
}

function hasPairInNamePool(pair) {
  return state.names.some((entry) => entry.id === pair.leftId) && state.names.some((entry) => entry.id === pair.rightId);
}

function findNameById(nameId) {
  return state.names.find((entry) => entry.id === nameId);
}

function renderChoiceCardContent(entry) {
  const summary = getChoiceSummary(entry);
  const popularity = getPopularityParts(entry);
  const metadata = getOriginMetadata(entry);
  const popularityInner = popularity
    ? `<strong class="choice-popularity-rank">Popularity ${escapeHtml(popularity.rank)}</strong>`
    : '<span class="choice-popularity-detail">SSA boys popularity loading...</span>';
  const metadataInner = metadata.length
    ? metadata.map((item) => `
        <div class="choice-meta-row">
          <span class="choice-meta-label">${escapeHtml(item.label)}</span>
          <span class="choice-meta-value">${escapeHtml(item.value)}</span>
        </div>
      `).join('')
    : `
        <div class="choice-meta-row">
          <span class="choice-meta-label">Origin</span>
          <span class="choice-meta-value">Loading</span>
        </div>
      `;

  return `
    <span class="choice-name">${escapeHtml(entry.name)}</span>
    <span class="choice-blurb">${escapeHtml(summary)}</span>
    <span class="choice-popularity">${popularityInner}</span>
    <span class="choice-meta-box">${metadataInner}</span>
  `;
}

function getChoiceSummary(entry) {
  if (entry.summary) {
    return entry.summary;
  }

  if (entry.summaryStatus === 'pending') {
    return 'Looking up a quick origin note...';
  }

  return 'No quick origin note found yet.';
}

function getPopularityParts(entry) {
  if (entry.ssaRank) {
    return {
      rank: entry.ssaRank.toLocaleString(),
    };
  }

  if (entry.ssaYear) {
    return {
      rank: 'Unranked',
    };
  }

  return null;
}

function getOriginMetadata(entry) {
  const loadingValue = entry.btnStatus === 'pending' ? 'Loading' : 'Not found';

  return [
    { label: 'Origin', value: entry.btnOrigin || loadingValue },
    { label: 'Usage', value: entry.btnUsage || loadingValue },
  ];
}

function handlePrioritizeFewerToggle() {
  state.prioritizeFewerComparisons = elements.prioritizeFewerToggle.checked;
  window.localStorage.setItem(PRIORITIZE_FEWER_KEY, state.prioritizeFewerComparisons ? 'true' : 'false');
  currentPair = null;
  renderMatchup();
}


function handleManualCompareChange() {
  state.manualLeftId = elements.manualLeftSelect.value;
  state.manualRightId = elements.manualRightSelect.value;

  if (state.manualLeftId && state.manualRightId && state.manualLeftId === state.manualRightId) {
    currentPair = null;
  } else {
    currentPair = getManualPair();
  }

  renderManualCompareControls();
  renderMatchup();
}

function resetManualCompare() {
  state.manualLeftId = '';
  state.manualRightId = '';
  currentPair = null;
  renderManualCompareControls();
  renderMatchup();
}

function queueSummaryPoll() {
  if (summaryPollTimer !== null) {
    return;
  }

  summaryPollTimer = window.setTimeout(() => {
    summaryPollTimer = null;
    loadState();
  }, SUMMARY_POLL_DELAY_MS);
}

function clearSummaryPoll() {
  if (summaryPollTimer === null) {
    return;
  }

  window.clearTimeout(summaryPollTimer);
  summaryPollTimer = null;
}

function getUserSlugFromPath() {
  const slug = normalizeSlug(window.location.pathname.replace(/^\/+/, ''));
  return slug || 'troy';
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
  return String(value || '').trim().replace(/\s+/g, ' ');
}

function getSavedPrioritizeFewerPreference() {
  const saved = window.localStorage.getItem(PRIORITIZE_FEWER_KEY);
  return saved === null ? true : saved === 'true';
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function setLoading(isLoading) {
  elements.currentUserPill.textContent = isLoading ? 'Loading' : 'SQLite-backed';
}

function showError(message) {
  elements.matchup.classList.add('hidden');
  elements.matchupState.classList.remove('hidden');
  elements.matchupState.innerHTML = `<p>${message}</p>`;
}
