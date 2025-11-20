import { getCurrentUser, awaitAuthUser, db } from './auth.js';
import { API_BASE_URL, DEFAULT_REMOTE_API_BASE } from './config.js';
import { ensureTmdbCredentialsLoaded } from './tmdbCredentials.js';

const MOVIE_PREFS_KEY = 'moviePreferences';
const API_KEY_STORAGE = 'moviesApiKey';
const SHARED_API_KEY_STORAGE_KEYS = Object.freeze([API_KEY_STORAGE, 'tvApiKey']);
const DEFAULT_INTEREST = 3;
const INITIAL_DISCOVER_PAGES = 3;
const MAX_DISCOVER_PAGES = 10;
const MAX_DISCOVER_PAGES_LIMIT = 30;
const MAX_CREDIT_REQUESTS = 20;
const PREF_COLLECTION = 'moviePreferences';
const MIN_VOTE_AVERAGE = 7;
const MIN_VOTE_COUNT = 50;
const MIN_PRIORITY_RESULTS = 12;
const MIN_FEED_RESULTS = 10;
const GENRE_SELECTION_ALL = '__all__';
const GENRE_SELECTION_NONE = '__none__';
const MOVIE_RATING_BUCKETS = [
  { label: '9-10', min: 9, max: Infinity },
  { label: '8-8.9', min: 8, max: 9 },
  { label: '7-7.9', min: 7, max: 8 },
  { label: '6-6.9', min: 6, max: 7 },
  { label: '< 6', min: -Infinity, max: 6 }
];

const DEFAULT_TMDB_PROXY_ENDPOINT =
  (typeof process !== 'undefined' && process.env && process.env.TMDB_PROXY_ENDPOINT) ||
  `${API_BASE_URL.replace(/\/$/, '') || DEFAULT_REMOTE_API_BASE}/tmdbProxy`;

let proxyDisabled = false;
const unsupportedProxyEndpoints = new Set();
let loggedProxyCreditsUnsupported = false;

const SUPPRESSED_STATUSES = new Set(['watched', 'notInterested', 'interested']);

const FEED_FILTERS_KEY = 'movieFeedFilters';
const DEFAULT_FEED_FILTER_STATE = Object.freeze({
  minRating: '',
  minVotes: '',
  startYear: '',
  endYear: '',
  selectedGenres: GENRE_SELECTION_ALL
});

let feedFilterState = { ...DEFAULT_FEED_FILTER_STATE };

function getDocument() {
  return typeof document !== 'undefined' ? document : null;
}

const TMDB_DISCOVER_HISTORY_LIMIT = 50;
const TMDB_DISCOVER_STATE_VERSION = 1;
const TMDB_DISCOVER_STATE_FIELD = 'tmdbDiscoverState';
const TMDB_DISCOVER_STATE_STORAGE_KEY = 'movieDiscoverState';
const TMDB_DISCOVER_STATE_PERSIST_DEBOUNCE_MS = 1500;
const tmdbDiscoverHistory = new Map();
let tmdbDiscoverStateDirty = false;
let tmdbDiscoverPersistTimer = null;

const domRefs = {
  list: null,
  interestedList: null,
  interestedFilters: null,
  watchedList: null,
  apiKeyInput: null,
  apiKeyContainer: null,
  tabs: null,
  streamSection: null,
  interestedSection: null,
  watchedSection: null,
  watchedSort: null,
  feedControls: null,
  feedStatus: null,
  feedMinRating: null,
  feedMinVotes: null,
  feedStartYear: null,
  feedEndYear: null,
  feedGenre: null
};

let currentMovies = [];
let currentPrefs = {};
let genreMap = {};
let lastCatalogMetadata = null;
let serverMovieStats = null;
let pendingMovieStatsPromise = null;
let lastFetchedMovieStatsSignature = null;
let activeApiKey = '';
let prefsLoadedFor = null;
let loadingPrefsPromise = null;
let activeUserId = null;
const activeInterestedGenres = new Set();
let refillInProgress = false;
let lastRefillAttempt = 0;
let pendingRefillCooldownTimer = null;
let feedExhausted = false;
let watchedSortMode = 'recent';
let activeInterestedGenre = null;
const handlers = {
  handleKeydown: null,
  handleChange: null
};

const criticScoreStateById = new Map();
const restoredMoviesById = new Map();
const CRITIC_SCORE_TYPE = 'movie';

const STATUS_TONE_CLASSES = Object.freeze({
  info: 'movie-status--info',
  success: 'movie-status--success',
  warning: 'movie-status--warning',
  error: 'movie-status--error'
});

function escapeForAttribute(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

let loadAttemptCounter = 0;

function buildMoviesApiUrl(path = '/api/movies') {
  const trimmedPath = path.startsWith('/') ? path : `/${path}`;
  const base = API_BASE_URL && API_BASE_URL !== 'null'
    ? API_BASE_URL.replace(/\/$/, '')
    : '';
  if (!base) {
    return trimmedPath;
  }
  return `${base}${trimmedPath}`;
}

function formatTimestamp(value) {
  if (!Number.isFinite(value)) return '';
  try {
    return new Date(value).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  } catch (_) {
    return '';
  }
}

function summarizeError(err) {
  if (!err) return 'Unknown error';
  if (typeof err === 'string') return err;
  if (typeof err.message === 'string' && err.message.trim()) {
    return err.message.trim();
  }
  if (typeof err.status === 'number') {
    return `Request failed with status ${err.status}`;
  }
  return 'Unknown error';
}

function getMovieCacheKey(movie) {
  if (!movie || typeof movie !== 'object') return null;
  if (movie.id != null) {
    return `tmdb:${movie.id}`;
  }
  const imdbIdRaw =
    (typeof movie.imdb_id === 'string' && movie.imdb_id) ||
    (typeof movie.imdbId === 'string' && movie.imdbId) ||
    '';
  const imdbId = imdbIdRaw.trim();
  if (imdbId) {
    return `imdb:${imdbId}`;
  }
  const titleSource =
    (typeof movie.title === 'string' && movie.title.trim()) ||
    (typeof movie.name === 'string' && movie.name.trim()) ||
    '';
  if (!titleSource) return null;
  const normalizedTitle = titleSource.toLowerCase();
  let year = '';
  if (typeof movie.release_date === 'string' && movie.release_date.trim()) {
    const parts = movie.release_date.trim().split('-');
    if (parts[0] && /^\d{4}$/.test(parts[0])) {
      year = parts[0];
    }
  }
  return `title:${normalizedTitle}|year:${year}`;
}

function parseCriticPercent(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.endsWith('%') ? raw.slice(0, -1) : raw;
  const num = Number.parseFloat(normalized);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function parseCriticScore(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function parseCriticImdb(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  const clamped = Math.max(0, Math.min(10, num));
  return Math.round(clamped * 10) / 10;
}

function normalizeCriticScoresObject(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const ratings =
    (raw.ratings && typeof raw.ratings === 'object' && raw.ratings) ||
    raw;
  const rottenTomatoes = parseCriticPercent(
    ratings.rottenTomatoes ??
      ratings.rotten_tomatoes ??
      ratings.tomatoMeter ??
      ratings.tomato_meter ??
      ratings.rotten ??
      ratings.tomato
  );
  const metacritic = parseCriticScore(
    ratings.metacritic ?? ratings.Metascore ?? ratings.meta ?? raw.Metascore ?? raw.metacritic
  );
  const imdb = parseCriticImdb(
    ratings.imdb ?? ratings.imdbRating ?? raw.imdbRating ?? ratings.imdb_score ?? ratings.imdbScore
  );

  const fetchedAtSource =
    raw.fetchedAt ??
    raw.fetched_at ??
    (raw.metadata && raw.metadata.fetchedAt) ??
    (raw.metadata && raw.metadata.fetched_at) ??
    ratings.fetchedAt ??
    ratings.fetched_at;
  let fetchedAt = null;
  if (typeof fetchedAtSource === 'string' && fetchedAtSource.trim()) {
    const parsed = new Date(fetchedAtSource);
    if (!Number.isNaN(parsed.getTime())) {
      fetchedAt = parsed.toISOString();
    }
  } else if (typeof fetchedAtSource === 'number' && Number.isFinite(fetchedAtSource)) {
    const parsed = new Date(fetchedAtSource);
    if (!Number.isNaN(parsed.getTime())) {
      fetchedAt = parsed.toISOString();
    }
  }
  if (!fetchedAt) {
    fetchedAt = new Date().toISOString();
  }

  const source =
    (typeof raw.source === 'string' && raw.source.trim()) ||
    (typeof raw.provider === 'string' && raw.provider.trim()) ||
    'omdb';
  const imdbIdValue =
    (typeof raw.imdbId === 'string' && raw.imdbId.trim()) ||
    (typeof raw.imdbID === 'string' && raw.imdbID.trim()) ||
    null;
  const titleValue =
    (typeof raw.title === 'string' && raw.title.trim()) ||
    (typeof raw.Title === 'string' && raw.Title.trim()) ||
    null;
  const yearValue =
    (typeof raw.year === 'string' && raw.year.trim()) ||
    (typeof raw.Year === 'string' && raw.Year.trim()) ||
    null;
  const typeValue =
    (typeof raw.type === 'string' && raw.type.trim()) ||
    CRITIC_SCORE_TYPE;

  return {
    rottenTomatoes: rottenTomatoes ?? null,
    metacritic: metacritic ?? null,
    imdb: imdb ?? null,
    fetchedAt,
    source,
    imdbId: imdbIdValue,
    title: titleValue,
    year: yearValue,
    type: typeValue
  };
}

function normalizeCriticScoresResponse(data) {
  if (!data || typeof data !== 'object') return null;
  return normalizeCriticScoresObject({
    ...data,
    ratings: data.ratings && typeof data.ratings === 'object' ? data.ratings : data
  });
}

function getCriticScoreState(movie) {
  if (!movie) return { status: 'idle', data: null };
  if (movie.criticScoresState && movie.criticScoresState.status) {
    return movie.criticScoresState;
  }
  const key = getMovieCacheKey(movie);
  const existingMovieScores = movie.criticScores
    ? normalizeCriticScoresObject(movie.criticScores)
    : null;
  if (key && criticScoreStateById.has(key)) {
    const state = criticScoreStateById.get(key);
    if (state?.data && !existingMovieScores) {
      movie.criticScores = state.data;
    }
    movie.criticScoresState = state;
    return state;
  }
  if (existingMovieScores) {
    const state = { status: 'loaded', data: existingMovieScores };
    if (key) {
      criticScoreStateById.set(key, state);
    }
    movie.criticScores = existingMovieScores;
    movie.criticScoresState = state;
    return state;
  }
  return { status: 'idle', data: null };
}

function setCriticScoreState(movie, state) {
  if (!movie) return;
  const key = getMovieCacheKey(movie);
  const existing = key ? criticScoreStateById.get(key) : null;
  const normalizedData = state?.data ? normalizeCriticScoresObject(state.data) : null;
  const existingMovieScores = movie.criticScores
    ? normalizeCriticScoresObject(movie.criticScores)
    : null;
  const fallbackData = normalizedData || existing?.data || existingMovieScores || null;
  const nextState = {
    status: state?.status || 'idle',
    data: null
  };

  if (nextState.status === 'loaded') {
    nextState.data = normalizedData || fallbackData;
    if (nextState.data) {
      movie.criticScores = nextState.data;
    }
  } else if (nextState.status === 'loading') {
    nextState.data = normalizedData || fallbackData;
  } else if (nextState.status === 'error') {
    nextState.data = fallbackData;
  } else {
    nextState.data = normalizedData || fallbackData;
  }

  if (state?.error) {
    nextState.error = state.error;
  }

  movie.criticScoresState = nextState;
  if (key) {
    criticScoreStateById.set(key, nextState);
  }
}

function buildCriticLookup(movie) {
  if (!movie) return null;
  const imdbIdRaw =
    (typeof movie.imdb_id === 'string' && movie.imdb_id) ||
    (typeof movie.imdbId === 'string' && movie.imdbId) ||
    '';
  const imdbId = imdbIdRaw.trim();
  const titleSource =
    (typeof movie.title === 'string' && movie.title.trim()) ||
    (typeof movie.name === 'string' && movie.name.trim()) ||
    '';
  let year = '';
  if (typeof movie.release_date === 'string' && movie.release_date.trim()) {
    const match = movie.release_date.trim().match(/^(\d{4})/);
    if (match) {
      year = match[1];
    }
  }
  if (!imdbId && !titleSource) {
    return null;
  }
  return {
    imdbId: imdbId || null,
    title: titleSource || null,
    year: year || null,
    type: CRITIC_SCORE_TYPE
  };
}

function canRequestCriticScores(movie) {
  const lookup = buildCriticLookup(movie);
  return Boolean(lookup && (lookup.imdbId || lookup.title));
}

function describeCriticScoresState(state) {
  if (!state || state.status === 'idle') {
    return 'Not fetched yet';
  }
  if (state.status === 'loading') {
    return 'Fetching critic scores...';
  }
  if (state.status === 'error') {
    return state.error || 'Critic scores unavailable';
  }
  if (state.status === 'loaded') {
    const data = state.data || {};
    const parts = [];
    if (Number.isFinite(data.rottenTomatoes)) {
      parts.push(`Rotten Tomatoes: ${Math.round(data.rottenTomatoes)}%`);
    }
    if (Number.isFinite(data.metacritic)) {
      parts.push(`Metacritic: ${Math.round(data.metacritic)}`);
    }
    if (Number.isFinite(data.imdb)) {
      parts.push(`IMDb: ${data.imdb.toFixed(1)}`);
    }
    if (!parts.length) {
      return 'Critic scores unavailable';
    }
    return parts.join(' · ');
  }
  return 'Critic scores unavailable';
}

function getCriticScoresButtonLabel(state) {
  if (!state || state.status === 'idle') return 'Fetch scores';
  if (state.status === 'loading') return 'Fetching...';
  if (state.status === 'loaded') return 'Refresh scores';
  if (state.status === 'error') return 'Try again';
  return 'Fetch scores';
}

async function requestCriticScores(movie, { force = false } = {}) {
  if (!movie) return null;
  const state = getCriticScoreState(movie);
  if (!force && (state.status === 'loading' || state.status === 'loaded')) {
    return state.data || null;
  }

  const lookup = buildCriticLookup(movie);
  if (!lookup) {
    setCriticScoreState(movie, {
      status: 'error',
      error: 'Not enough information to fetch critic scores.'
    });
    refreshUI();
    return null;
  }

  setCriticScoreState(movie, {
    status: 'loading',
    data: state.data || null,
    error: state.error
  });
  refreshUI();

  try {
    const params = new URLSearchParams();
    if (lookup.imdbId) params.set('imdbId', lookup.imdbId);
    if (lookup.title) params.set('title', lookup.title);
    if (lookup.year) params.set('year', lookup.year);
    if (lookup.type) params.set('type', lookup.type);
    const url = `${buildMoviesApiUrl('/api/movie-ratings')}?${params.toString()}`;
    const response = await fetch(url);
    if (!response.ok) {
      let message = `Request failed with status ${response.status}`;
      try {
        const errorData = await response.json();
        if (errorData?.message) {
          message = errorData.message;
        } else if (errorData?.error) {
          message = errorData.error;
        }
      } catch (_) {
        /* ignore */
      }
      throw new Error(message);
    }
    const data = await response.json();
    const normalized = normalizeCriticScoresResponse(data);
    if (!normalized) {
      setCriticScoreState(movie, {
        status: 'error',
        error: 'Critic scores are unavailable for this title.'
      });
      refreshUI();
      return null;
    }
    setCriticScoreState(movie, { status: 'loaded', data: normalized });
    refreshUI();
    return normalized;
  } catch (err) {
    setCriticScoreState(movie, {
      status: 'error',
      error: summarizeError(err)
    });
    refreshUI();
    return null;
  }
}

function appendCriticScoresMeta(metaList, movie) {
  if (!metaList || !movie) return;
  const item = document.createElement('li');
  item.className = 'movie-meta__item movie-meta__critics';

  const label = document.createElement('span');
  label.className = 'movie-meta__label';
  label.textContent = 'Critic scores:';
  item.appendChild(label);

  const state = getCriticScoreState(movie);
  const value = document.createElement('span');
  value.className = 'movie-meta__value movie-meta__critics-value';
  value.textContent = describeCriticScoresState(state);
  item.appendChild(value);

  if (canRequestCriticScores(movie)) {
    const button = makeActionButton(getCriticScoresButtonLabel(state), () => {
      requestCriticScores(movie, {
        force: state.status === 'loaded' || state.status === 'error'
      });
    });
    button.classList.add('movie-action--inline');
    if (state.status === 'loading') {
      button.disabled = true;
    }
    item.appendChild(button);
  }

  metaList.appendChild(item);
}

function updateFeedStatus(message, { tone = 'info', showSpinner = false } = {}) {
  const statusEl = domRefs.feedStatus;
  if (!statusEl) return;
  const normalizedMessage = typeof message === 'string' ? message : String(message ?? '');
  const isCooldownMessage = /before requesting more movies/i.test(normalizedMessage);
  if (isCooldownMessage) {
    const fallbackLabel = normalizedMessage.trim()
      ? normalizedMessage.trim()
      : 'Preparing more movies shortly';
    const ariaLabel = escapeForAttribute(fallbackLabel);
    statusEl.innerHTML = `
      <div class="movie-status__cooldown" role="status" aria-live="polite"${
        ariaLabel ? ` aria-label="${ariaLabel}"` : ''
      }>
        <span class="movie-status__sr">${ariaLabel || 'Preparing more movies shortly'}</span>
        <div class="movie-status__projector">
          <div class="movie-status__reel"></div>
          <div class="movie-status__filmstrip">
            <span class="movie-status__frame"></span>
            <span class="movie-status__frame"></span>
            <span class="movie-status__frame"></span>
            <span class="movie-status__frame"></span>
          </div>
          <div class="movie-status__reel movie-status__reel--right"></div>
        </div>
        <div class="movie-status__sparkles">
          <span></span>
          <span></span>
          <span></span>
        </div>
      </div>
    `;
    statusEl.setAttribute('aria-busy', 'true');
    if (normalizedMessage.trim()) {
      console.info(normalizedMessage);
    }
  } else if (showSpinner) {
    const ariaLabel = normalizedMessage.trim() ? escapeForAttribute(normalizedMessage) : '';
    statusEl.innerHTML = `
      <div class="movie-status__loader" role="status" aria-live="polite"${
        ariaLabel ? ` aria-label="${ariaLabel}"` : ''
      }>
        <span class="movie-status__sr">${ariaLabel}</span>
        <span class="movie-status__dot"></span>
        <span class="movie-status__dot"></span>
        <span class="movie-status__dot"></span>
      </div>
    `;
    statusEl.setAttribute('aria-busy', 'true');
    if (normalizedMessage.trim()) {
      console.info(normalizedMessage);
    }
  } else {
    statusEl.textContent = normalizedMessage;
    statusEl.removeAttribute('aria-busy');
  }
  Object.values(STATUS_TONE_CLASSES).forEach(cls => {
    statusEl.classList.remove(cls);
  });
  const toneClass = STATUS_TONE_CLASSES[tone] || STATUS_TONE_CLASSES.info;
  statusEl.classList.add(toneClass);
  statusEl.classList.toggle('movie-status--loading', Boolean(showSpinner || isCooldownMessage));
}

function clampUserRating(value) {
  if (!Number.isFinite(value)) return null;
  if (value < 0) return 0;
  if (value > 10) return 10;
  return Math.round(value * 2) / 2;
}
const REFILL_COOLDOWN_MS = 5000;

function getNameList(input) {
  if (!input) return [];

  if (Array.isArray(input)) {
    return input
      .map(entry => {
        if (typeof entry === 'string') {
          return entry.trim();
        }
        if (entry && typeof entry.name === 'string') {
          return entry.name.trim();
        }
        return '';
      })
      .filter(Boolean);
  }

  if (typeof input === 'string') {
    return input
      .split(',')
      .map(name => name.trim())
      .filter(Boolean);
  }

  return [];
}

function meetsQualityThreshold(movie, minAverage = MIN_VOTE_AVERAGE, minVotes = MIN_VOTE_COUNT) {
  if (!movie || typeof movie !== 'object') return false;
  const average = Number(movie.vote_average ?? 0);
  const votes = Number(movie.vote_count ?? 0);
  if (!Number.isFinite(average) || !Number.isFinite(votes)) return false;
  return average >= minAverage && votes >= minVotes;
}

function loadLocalPrefs() {
  if (typeof localStorage === 'undefined') return {};
  try {
    const raw = localStorage.getItem(MOVIE_PREFS_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (_) {
    return {};
  }
}

function saveLocalPrefs(prefs) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(MOVIE_PREFS_KEY, JSON.stringify(prefs));
  } catch (_) {
    /* ignore */
  }
}

function loadLocalDiscoverState() {
  if (typeof localStorage === 'undefined') return null;
  try {
    const raw = localStorage.getItem(TMDB_DISCOVER_STATE_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    return parsed;
  } catch (_) {
    return null;
  }
}

function saveLocalDiscoverState(state) {
  if (typeof localStorage === 'undefined') return;
  try {
    if (
      !state ||
      typeof state !== 'object' ||
      !state.entries ||
      typeof state.entries !== 'object' ||
      !Object.keys(state.entries).length
    ) {
      localStorage.removeItem(TMDB_DISCOVER_STATE_STORAGE_KEY);
      return;
    }
    localStorage.setItem(TMDB_DISCOVER_STATE_STORAGE_KEY, JSON.stringify(state));
  } catch (_) {
    /* ignore */
  }
}

function sanitizeFeedFilterValue(name, rawValue) {
  const value = rawValue == null ? '' : String(rawValue).trim();

  if (name === 'selectedGenres') {
    if (!value) {
      return GENRE_SELECTION_ALL;
    }
    if (value === GENRE_SELECTION_ALL || value === GENRE_SELECTION_NONE) {
      return value;
    }
    const parts = value.split(',');
    const numbers = parts
      .map(entry => Number.parseInt(String(entry).trim(), 10))
      .filter(Number.isFinite);
    if (!numbers.length) {
      return GENRE_SELECTION_NONE;
    }
    const uniqueSorted = Array.from(new Set(numbers)).sort((a, b) => a - b);
    return uniqueSorted.map(entry => entry.toString()).join(',');
  }

  if (!value) return '';

  if (name === 'minRating') {
    const number = Number.parseFloat(value.replace(',', '.'));
    if (!Number.isFinite(number)) return '';
    const clamped = Math.max(0, Math.min(10, number));
    return clamped.toString();
  }

  if (name === 'minVotes') {
    const number = Number.parseInt(value, 10);
    if (!Number.isFinite(number)) return '';
    return Math.max(0, number).toString();
  }

  if (name === 'startYear' || name === 'endYear') {
    const number = Number.parseInt(value, 10);
    if (!Number.isFinite(number)) return '';
    return number.toString();
  }

  return value;
}

function sanitizeFeedFiltersState(state) {
  const base = { ...DEFAULT_FEED_FILTER_STATE };
  if (!state || typeof state !== 'object') {
    return base;
  }

  return {
    ...base,
    minRating: sanitizeFeedFilterValue('minRating', state.minRating),
    minVotes: sanitizeFeedFilterValue('minVotes', state.minVotes),
    startYear: sanitizeFeedFilterValue('startYear', state.startYear),
    endYear: sanitizeFeedFilterValue('endYear', state.endYear),
    selectedGenres: sanitizeFeedFilterValue('selectedGenres', state.selectedGenres)
  };
}

function loadFeedFilterStateFromStorage() {
  if (typeof localStorage === 'undefined') {
    return { ...DEFAULT_FEED_FILTER_STATE };
  }
  try {
    const raw = localStorage.getItem(FEED_FILTERS_KEY);
    if (!raw) return { ...DEFAULT_FEED_FILTER_STATE };
    const parsed = JSON.parse(raw);
    return sanitizeFeedFiltersState(parsed);
  } catch (_) {
    return { ...DEFAULT_FEED_FILTER_STATE };
  }
}

function saveFeedFilters(state) {
  if (typeof localStorage === 'undefined') return;
  try {
    const sanitized = sanitizeFeedFiltersState(state);
    localStorage.setItem(FEED_FILTERS_KEY, JSON.stringify(sanitized));
  } catch (_) {
    /* ignore */
  }
}

function getAvailableGenreIds() {
  return Object.keys(genreMap || {})
    .map(id => Number.parseInt(id, 10))
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
}

function getGenreSelectionValue() {
  const raw =
    typeof feedFilterState.selectedGenres === 'string'
      ? feedFilterState.selectedGenres.trim()
      : '';
  if (!raw) {
    return GENRE_SELECTION_ALL;
  }
  if (raw === GENRE_SELECTION_ALL || raw === GENRE_SELECTION_NONE) {
    return raw;
  }
  return raw;
}

function getGenreSelectionMode() {
  const value = getGenreSelectionValue();
  if (value === GENRE_SELECTION_ALL) return 'all';
  if (value === GENRE_SELECTION_NONE) return 'none';
  return 'custom';
}

function getSelectedGenreIdSet() {
  const mode = getGenreSelectionMode();
  if (mode === 'all') {
    return new Set(getAvailableGenreIds());
  }
  if (mode === 'none') {
    return new Set();
  }
  const value = getGenreSelectionValue();
  const set = new Set();
  value.split(',').forEach(entry => {
    const numeric = Number.parseInt(entry.trim(), 10);
    if (Number.isFinite(numeric)) {
      set.add(numeric);
    }
  });
  return set;
}

function isGenreSelected(id) {
  const numeric = Number.parseInt(id, 10);
  if (!Number.isFinite(numeric)) return false;
  const mode = getGenreSelectionMode();
  if (mode === 'all') return true;
  if (mode === 'none') return false;
  const set = getSelectedGenreIdSet();
  return set.has(numeric);
}

function getDisallowedGenreIdSet() {
  const mode = getGenreSelectionMode();
  if (mode === 'all') {
    return new Set();
  }
  const available = getAvailableGenreIds();
  if (mode === 'none') {
    return new Set(available);
  }
  const selected = getSelectedGenreIdSet();
  const disallowed = new Set();
  available.forEach(id => {
    if (!selected.has(id)) {
      disallowed.add(id);
    }
  });
  return disallowed;
}

function setAllGenresSelected({ persist = true } = {}) {
  setFeedFilter('selectedGenres', GENRE_SELECTION_ALL, { sanitize: true, persist });
}

function setNoGenresSelected({ persist = true } = {}) {
  setFeedFilter('selectedGenres', GENRE_SELECTION_NONE, { sanitize: true, persist });
}

function setSelectedGenresFromSet(values, { persist = true } = {}) {
  const availableIds = getAvailableGenreIds();
  if (!availableIds.length) {
    setAllGenresSelected({ persist });
    return;
  }
  const availableSet = new Set(availableIds);
  const normalized = Array.from(values || [])
    .map(value => Number.parseInt(value, 10))
    .filter(value => Number.isFinite(value) && availableSet.has(value));
  if (!normalized.length) {
    setNoGenresSelected({ persist });
    return;
  }
  if (normalized.length === availableIds.length) {
    setAllGenresSelected({ persist });
    return;
  }
  const sorted = Array.from(new Set(normalized)).sort((a, b) => a - b);
  const serialized = sorted.map(value => value.toString()).join(',');
  setFeedFilter('selectedGenres', serialized, { sanitize: true, persist });
}

function ensureGenreSelectionConsistency() {
  const availableIds = getAvailableGenreIds();
  const availableSet = new Set(availableIds);
  const mode = getGenreSelectionMode();
  if (mode === 'all' || mode === 'none') {
    return;
  }
  const current = getSelectedGenreIdSet();
  const filtered = Array.from(current).filter(id => availableSet.has(id));
  if (!filtered.length) {
    setNoGenresSelected({ persist: true });
    return;
  }
  if (filtered.length === availableIds.length) {
    setAllGenresSelected({ persist: true });
    return;
  }
  if (filtered.length !== current.size) {
    setSelectedGenresFromSet(new Set(filtered), { persist: true });
  }
}

function getGenreSelectionSummaryText() {
  const mode = getGenreSelectionMode();
  if (mode === 'all') {
    return 'All genres selected';
  }
  if (mode === 'none') {
    return 'No genres selected';
  }
  const ids = Array.from(getSelectedGenreIdSet()).sort((a, b) => a - b);
  if (!ids.length) {
    return 'No genres selected';
  }
  const names = ids
    .map(id => genreMap?.[id] || genreMap?.[String(id)] || `Genre ${id}`)
    .filter(Boolean);
  if (!names.length) {
    return `${ids.length} genre${ids.length === 1 ? '' : 's'} selected`;
  }
  if (names.length <= 3) {
    return names.join(', ');
  }
  return `${names.length} genres selected`;
}

function buildGenreQueryParams() {
  const mode = getGenreSelectionMode();
  if (mode === 'all') {
    return { blockAll: false, withGenres: null };
  }
  if (mode === 'none') {
    return { blockAll: true, withGenres: null };
  }
  const ids = Array.from(getSelectedGenreIdSet()).sort((a, b) => a - b);
  if (!ids.length) {
    return { blockAll: true, withGenres: null };
  }
  const serialized = ids.map(id => id.toString()).join('|');
  return { blockAll: false, withGenres: serialized };
}

function buildRatingBucketCounts(movies, buckets) {
  const working = buckets.map(({ label, min, max }) => ({
    label,
    min,
    max,
    count: 0
  }));
  movies.forEach(movie => {
    const value = getVoteAverageValue(movie);
    if (!Number.isFinite(value)) return;
    for (const bucket of working) {
      const meetsMin = bucket.min === -Infinity ? true : value >= bucket.min;
      const belowMax = bucket.max === Infinity ? true : value < bucket.max;
      if (meetsMin && belowMax) {
        bucket.count += 1;
        break;
      }
    }
  });
  return working.map(({ label, count }) => ({ label, count }));
}

function formatStatValue(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toLocaleString() : '0';
}

function renderMediaStats(container, { totals = [], ratings = [] } = {}) {
  if (!container) return;
  const renderItems = items =>
    items
      .map(
        item => `
        <div class="media-stats__item">
          <dt>${item.label}</dt>
          <dd>${formatStatValue(item.value)}</dd>
        </div>`
      )
      .join('');

  const totalsMarkup = renderItems(totals);
  const ratingsMarkup = renderItems(ratings);

  container.innerHTML = `
    <section class="media-stats__section">
      <h4 class="media-stats__heading">Counts</h4>
      <dl class="media-stats__grid">
        ${totalsMarkup}
      </dl>
    </section>
    <section class="media-stats__section">
      <h4 class="media-stats__heading">Rating Distribution</h4>
      <dl class="media-stats__grid">
        ${ratingsMarkup}
      </dl>
    </section>
  `;
}

function getClassifiedMovieIds() {
  return Object.entries(currentPrefs || {})
    .filter(([, pref]) => pref && SUPPRESSED_STATUSES.has(pref.status))
    .map(([id]) => String(id));
}

function computeMovieStatsSignature() {
  const metadataStamp =
    (lastCatalogMetadata &&
      (lastCatalogMetadata.catalogUpdatedAt || lastCatalogMetadata.updatedAt)) ||
    '';
  const ids = getClassifiedMovieIds()
    .map(id => id)
    .sort();
  return `${metadataStamp}|${ids.join(',')}`;
}

function ensureServerMovieStats() {
  const signature = computeMovieStatsSignature();
  if (!signature) return;
  if (serverMovieStats && lastFetchedMovieStatsSignature === signature) return;
  if (pendingMovieStatsPromise && pendingMovieStatsPromise.signature === signature) {
    return;
  }

  const excludeIds = getClassifiedMovieIds()
    .map(id => id)
    .filter(Boolean);
  const params = new URLSearchParams();
  if (excludeIds.length) {
    params.set('excludeIds', excludeIds.join(','));
  }

  const requestContext = {
    signature,
    promise: fetch(`/api/movies/stats${params.toString() ? `?${params.toString()}` : ''}`)
      .then(res => {
        if (!res.ok) {
          throw new Error(`Request failed: ${res.status}`);
        }
        return res.json();
      })
      .then(data => {
        serverMovieStats = data;
        lastFetchedMovieStatsSignature = signature;
        updateMovieStats();
      })
      .catch(err => {
        if (lastFetchedMovieStatsSignature === signature) {
          lastFetchedMovieStatsSignature = null;
        }
        console.warn('Failed to load movie catalog stats', err);
      })
      .finally(() => {
        if (pendingMovieStatsPromise === requestContext) {
          pendingMovieStatsPromise = null;
        }
      })
  };

  pendingMovieStatsPromise = requestContext;
}

function getAllUnclassifiedMovieSummaries() {
  const suppressedIds = new Set(
    Object.entries(currentPrefs || {})
      .filter(([, pref]) => pref && SUPPRESSED_STATUSES.has(pref.status))
      .map(([id]) => String(id))
  );
  const pool = new Map();
  const addMovie = movie => {
    if (!movie) return;
    const summary = captureRestoredMovie(movie) || (movie.id != null ? movie : null);
    if (!summary || summary.id == null) return;
    const key = String(summary.id);
    if (suppressedIds.has(key)) return;
    if (!pool.has(key)) {
      pool.set(key, summary);
    }
  };

  if (Array.isArray(currentMovies)) {
    currentMovies.forEach(addMovie);
  }
  restoredMoviesById.forEach(movie => {
    addMovie(movie);
  });

  return Array.from(pool.values());
}

function buildMovieStats() {
  const serverStats =
    serverMovieStats && typeof serverMovieStats === 'object' ? serverMovieStats : null;
  const metadata = lastCatalogMetadata;
  let catalogTotal = null;
  if (metadata && typeof metadata === 'object') {
    const totals = [
      metadata.curatedCount,
      metadata.totalCatalogSize,
      metadata.totalCatalog,
      metadata.curatedReturnedCount
    ]
      .map(value => Number(value))
      .filter(value => Number.isFinite(value) && value >= 0);
    if (totals.length) {
      catalogTotal = Math.round(totals[0]);
    }
  }

  if (!Number.isFinite(catalogTotal)) {
    catalogTotal = Array.isArray(currentMovies) ? currentMovies.length : 0;
  }

  const classifiedCount = Object.values(currentPrefs || {}).filter(
    pref => pref && SUPPRESSED_STATUSES.has(pref.status)
  ).length;

  if (serverStats && Number.isFinite(Number(serverStats.total))) {
    const bucketMap = new Map(
      Array.isArray(serverStats.buckets)
        ? serverStats.buckets.map(item => [item.label, Number(item.count) || 0])
        : []
    );
    return {
      totals: [{ label: 'Unclassified Movies', value: Number(serverStats.total) || 0 }],
      ratings: MOVIE_RATING_BUCKETS.map(bucket => ({
        label: bucket.label,
        value: bucketMap.get(bucket.label) || 0
      }))
    };
  }

  const unclassifiedMovies = getAllUnclassifiedMovieSummaries();
  let unclassifiedTotal = unclassifiedMovies.length;
  if (Number.isFinite(catalogTotal)) {
    const totalFromCatalog = Math.max(0, catalogTotal - classifiedCount);
    if (totalFromCatalog > unclassifiedTotal) {
      unclassifiedTotal = totalFromCatalog;
    }
  }

  const ratingBuckets = buildRatingBucketCounts(unclassifiedMovies, MOVIE_RATING_BUCKETS);
  return {
    totals: [
      { label: 'Unclassified Movies', value: unclassifiedTotal }
    ],
    ratings: ratingBuckets.map(bucket => ({ label: bucket.label, value: bucket.count }))
  };
}

function updateMovieStats() {
  const doc = getDocument();
  if (!doc) return;
  const container = doc.getElementById('movieStats');
  if (!container) return;
  ensureServerMovieStats();
  const stats = buildMovieStats();
  renderMediaStats(container, stats);
}

function hasActiveFeedFilters() {
  const { minRating, minVotes, startYear, endYear } = feedFilterState;
  if (
    String(minRating ?? '').trim() ||
    String(minVotes ?? '').trim() ||
    String(startYear ?? '').trim() ||
    String(endYear ?? '').trim()
  ) {
    return true;
  }
  return getGenreSelectionMode() !== 'all';
}

function updateFeedGenreUI() {
  const container = domRefs.feedGenre;
  if (!container) return;

  const mode = getGenreSelectionMode();
  const selectedSet = getSelectedGenreIdSet();
  const checkboxes = container.querySelectorAll('input[type="checkbox"][data-genre]');
  checkboxes.forEach(input => {
    const value = Number.parseInt(input.dataset.genre ?? input.value ?? '', 10);
    if (!Number.isFinite(value)) return;
    const isChecked =
      mode === 'all' ? true : mode === 'none' ? false : selectedSet.has(value);
    input.checked = isChecked;
  });

  const summaryEl = container.querySelector('.genre-facet-summary');
  if (summaryEl) {
    summaryEl.textContent = getGenreSelectionSummaryText();
  }

  const selectAllBtn = container.querySelector('[data-genre-action="select-all"]');
  if (selectAllBtn) {
    selectAllBtn.disabled = mode === 'all';
  }
  const selectNoneBtn = container.querySelector('[data-genre-action="select-none"]');
  if (selectNoneBtn) {
    selectNoneBtn.disabled = mode === 'none';
  }
}

function updateFeedFilterInputsFromState() {
  if (domRefs.feedMinRating) {
    domRefs.feedMinRating.value = feedFilterState.minRating ?? '';
  }
  if (domRefs.feedMinVotes) {
    domRefs.feedMinVotes.value = feedFilterState.minVotes ?? '';
  }
  if (domRefs.feedStartYear) {
    domRefs.feedStartYear.value = feedFilterState.startYear ?? '';
  }
  if (domRefs.feedEndYear) {
    domRefs.feedEndYear.value = feedFilterState.endYear ?? '';
  }
  updateFeedGenreUI();
}

function setFeedFilter(name, rawValue, { sanitize = false, persist = true } = {}) {
  if (!Object.prototype.hasOwnProperty.call(feedFilterState, name)) return;

  const rawString = rawValue == null ? '' : String(rawValue);
  const value = sanitize
    ? sanitizeFeedFilterValue(name, rawString)
    : rawString.trim();

  const hasChanged = feedFilterState[name] !== value;
  if (hasChanged) {
    feedFilterState = { ...feedFilterState, [name]: value };
  }

  if (sanitize) {
    updateFeedFilterInputsFromState();
  }

  if (persist) {
    saveFeedFilters(feedFilterState);
  }

  if (hasChanged || sanitize) {
    renderFeed();
  }
}

function populateFeedGenreOptions() {
  const container = domRefs.feedGenre;
  if (!container) return;

  const entries = Object.entries(genreMap || {})
    .map(([id, name]) => ({
      id: Number.parseInt(id, 10),
      name: String(name ?? '').trim() || 'Unknown'
    }))
    .filter(entry => Number.isFinite(entry.id))
    .sort((a, b) => a.name.localeCompare(b.name));

  container.innerHTML = '';

  if (!entries.length) {
    const empty = document.createElement('div');
    empty.className = 'genre-facet-empty';
    empty.textContent = 'No genres available.';
    container.appendChild(empty);
    return;
  }

  ensureGenreSelectionConsistency();

  const wrapper = document.createElement('div');
  wrapper.className = 'genre-facet';

  const controls = document.createElement('div');
  controls.className = 'genre-facet-controls';

  const selectAllBtn = document.createElement('button');
  selectAllBtn.type = 'button';
  selectAllBtn.className = 'genre-facet-action genre-filter-btn';
  selectAllBtn.dataset.genreAction = 'select-all';
  selectAllBtn.textContent = 'Select all';
  selectAllBtn.addEventListener('click', handleGenreSelectionAction);
  controls.appendChild(selectAllBtn);

  const selectNoneBtn = document.createElement('button');
  selectNoneBtn.type = 'button';
  selectNoneBtn.className = 'genre-facet-action genre-filter-btn';
  selectNoneBtn.dataset.genreAction = 'select-none';
  selectNoneBtn.textContent = 'Select none';
  selectNoneBtn.addEventListener('click', handleGenreSelectionAction);
  controls.appendChild(selectNoneBtn);

  wrapper.appendChild(controls);

  const options = document.createElement('div');
  options.className = 'genre-facet-options';

  entries.forEach(entry => {
    const option = document.createElement('label');
    option.className = 'genre-facet-option';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = String(entry.id);
    checkbox.dataset.genre = String(entry.id);
    checkbox.addEventListener('change', handleGenreCheckboxChange);

    const labelText = document.createElement('span');
    labelText.className = 'genre-facet-label';
    labelText.textContent = entry.name;

    option.appendChild(checkbox);
    option.appendChild(labelText);
    options.appendChild(option);
  });

  wrapper.appendChild(options);

  const summary = document.createElement('div');
  summary.className = 'genre-facet-summary';
  wrapper.appendChild(summary);

  container.appendChild(wrapper);

  updateFeedGenreUI();
}

function handleGenreSelectionAction(event) {
  event.preventDefault();
  const button = event.currentTarget;
  if (!button) return;
  const action = button.dataset.genreAction;
  if (action === 'select-all') {
    setAllGenresSelected({ persist: true });
  } else if (action === 'select-none') {
    setNoGenresSelected({ persist: true });
  }
}

function handleGenreCheckboxChange(event) {
  const input = event.currentTarget;
  if (!input) return;
  const value = Number.parseInt(input.dataset.genre ?? input.value ?? '', 10);
  if (!Number.isFinite(value)) return;
  const mode = getGenreSelectionMode();
  let nextValues;
  if (mode === 'all') {
    nextValues = new Set(getAvailableGenreIds());
  } else if (mode === 'none') {
    nextValues = new Set();
  } else {
    nextValues = getSelectedGenreIdSet();
  }
  if (input.checked) {
    nextValues.add(value);
  } else {
    nextValues.delete(value);
  }
  setSelectedGenresFromSet(nextValues, { persist: true });
}

function attachFeedFilterInput(element, name) {
  if (!element) return;

  if (element._feedFilterInputHandler) {
    element.removeEventListener('input', element._feedFilterInputHandler);
  }
  if (element._feedFilterChangeHandler) {
    element.removeEventListener('change', element._feedFilterChangeHandler);
  }

  const inputHandler = event => {
    setFeedFilter(name, event.target.value, { persist: false });
  };

  const changeHandler = event => {
    setFeedFilter(name, event.target.value, { sanitize: true, persist: true });
  };

  element._feedFilterInputHandler = inputHandler;
  element._feedFilterChangeHandler = changeHandler;
  element.addEventListener('input', inputHandler);
  element.addEventListener('change', changeHandler);
}

function attachFeedFilterSelect(element, name) {
  if (!element) return;

  if (element._feedFilterSelectHandler) {
    element.removeEventListener('change', element._feedFilterSelectHandler);
  }

  const handler = event => {
    setFeedFilter(name, event.target.value, { sanitize: true, persist: true });
  };

  element._feedFilterSelectHandler = handler;
  element.addEventListener('change', handler);
}

async function loadPreferences() {
  if (!loadingPrefsPromise) {
    loadingPrefsPromise = (async () => {
      const authed = await awaitAuthUser().catch(() => null);
      const user = getCurrentUser() || authed;
      const key = user?.uid || 'anonymous';
      activeUserId = user?.uid || null;
      if (prefsLoadedFor === key) return currentPrefs;
      let prefs = {};
      if (user) {
        try {
          const snap = await db.collection(PREF_COLLECTION).doc(user.uid).get();
          const data = snap.exists ? snap.data() : null;
          const storedPrefs = data?.prefs;
          prefs = (storedPrefs && typeof storedPrefs === 'object') ? storedPrefs : {};
          hydrateTmdbDiscoverState(data?.[TMDB_DISCOVER_STATE_FIELD] || null);
        } catch (err) {
          console.error('Failed to load movie preferences', err);
          prefs = {};
          hydrateTmdbDiscoverState(null);
        }
      } else {
        prefs = loadLocalPrefs();
        hydrateTmdbDiscoverState(loadLocalDiscoverState());
      }
      prefsLoadedFor = key;
      currentPrefs = prefs || {};
      return currentPrefs;
    })().finally(() => {
      loadingPrefsPromise = null;
    });
  }
  return loadingPrefsPromise;
}

async function savePreferences(prefs) {
  currentPrefs = prefs;
  const authed = await awaitAuthUser().catch(() => null);
  const user = getCurrentUser() || authed || (activeUserId ? { uid: activeUserId } : null);
  const uid = user?.uid || activeUserId;
  if (!uid) {
    saveLocalPrefs(prefs);
    return;
  }
  activeUserId = uid;
  try {
    await db.collection(PREF_COLLECTION).doc(uid).set({ prefs }, { merge: true });
  } catch (err) {
    console.error('Failed to save movie preferences', err);
  }
}

function persistApiKey(key) {
  if (!key) return;
  activeApiKey = key;
  if (typeof window !== 'undefined') {
    window.tmdbApiKey = key;
  }
  if (typeof localStorage !== 'undefined') {
    try {
      SHARED_API_KEY_STORAGE_KEYS.forEach(storageKey => {
        localStorage.setItem(storageKey, key);
      });
    } catch (_) {
      /* ignore */
    }
  }
  if (domRefs.apiKeyContainer) {
    domRefs.apiKeyContainer.style.display = 'none';
  }
}

function resolveApiKey() {
  if (activeApiKey) {
    return activeApiKey;
  }
  const value = domRefs.apiKeyInput?.value;
  return typeof value === 'string' ? value.trim() : '';
}

function getTmdbProxyEndpoint() {
  if (proxyDisabled) return '';
  if (typeof window !== 'undefined' && 'tmdbProxyEndpoint' in window) {
    const value = window.tmdbProxyEndpoint;
    return typeof value === 'string' ? value : '';
  }
  return DEFAULT_TMDB_PROXY_ENDPOINT;
}

function disableTmdbProxy() {
  if (proxyDisabled) return;
  proxyDisabled = true;
  if (domRefs.apiKeyContainer) {
    domRefs.apiKeyContainer.style.display = '';
  }
}

function isProxyEndpointSupported(endpoint) {
  if (!endpoint) return false;
  if (proxyDisabled) return false;
  return !unsupportedProxyEndpoints.has(endpoint);
}

function summarizeProxyError(error) {
  if (!error || typeof error !== 'object') {
    return 'unknown error';
  }

  const parts = [];

  if (typeof error.status === 'number') {
    parts.push(`status ${error.status}`);
  }

  if (error.code) {
    parts.push(`code "${error.code}"`);
  }

  const body = typeof error.body === 'string' ? error.body.trim() : '';
  if (body) {
    parts.push(`body: ${body.slice(0, 120)}${body.length > 120 ? '…' : ''}`);
  }

  if (!parts.length) {
    return 'unknown error';
  }

  return parts.join(', ');
}

function isProxyParameterError(err) {
  if (!err || typeof err !== 'object') {
    return false;
  }
  return err.code === 'unsupported_endpoint' || err.code === 'invalid_endpoint_params';
}

async function callTmdbProxy(endpoint, params = {}) {
  const proxyEndpoint = getTmdbProxyEndpoint();
  if (!proxyEndpoint) {
    throw new Error('TMDB proxy endpoint not configured');
  }

  const url = new URL(proxyEndpoint);
  url.searchParams.set('endpoint', endpoint);
  Object.entries(params).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach(v => url.searchParams.append(key, String(v)));
    } else if (value != null) {
      url.searchParams.set(key, String(value));
    }
  });

  let response;
  try {
    response = await fetch(url.toString());
  } catch (err) {
    disableTmdbProxy();
    throw err;
  }

  if (!response.ok) {
    const error = new Error(`TMDB proxy request failed (status ${response.status})`);
    error.endpoint = endpoint;
    error.status = response.status;
    if (response.statusText) {
      error.statusText = response.statusText;
    }
    try {
      error.body = await response.text();
    } catch (_) {
      error.body = null;
    }

    let parsedBody = null;
    if (typeof error.body === 'string' && error.body.trim()) {
      try {
        parsedBody = JSON.parse(error.body);
      } catch (_) {
        parsedBody = null;
      }
    }

    if (parsedBody && parsedBody.error && !error.code) {
      error.code = parsedBody.error;
    }

    if (parsedBody && parsedBody.message && !error.messageDetail) {
      error.messageDetail = parsedBody.message;
    }

    const shouldDisableProxy = (() => {
      if (response.status >= 500) return true;
      if (response.status === 401 || response.status === 403) return true;
      const bodyText = typeof error.body === 'string' ? error.body : '';
      if (!bodyText) return false;
      if (bodyText.includes('tmdb_key_not_configured')) return true;
      if (response.status === 400) {
        try {
          const parsed = parsedBody || JSON.parse(bodyText);
          const code = parsed?.error;
          if (code === 'unsupported_endpoint') {
            if (endpoint) {
              unsupportedProxyEndpoints.add(endpoint);
            }
            return false;
          }
          if (code === 'invalid_endpoint_params') {
            return false;
          }
          return false;
        } catch (_) {
          return false;
        }
      }
      return false;
    })();

    if (shouldDisableProxy) {
      disableTmdbProxy();
    }

    throw error;
  }
  return response.json();
}

function summarizeMovie(movie) {
  const summary = {
    id: movie.id,
    title: movie.title || movie.name || '',
    release_date: movie.release_date || '',
    poster_path: movie.poster_path || '',
    overview: movie.overview || '',
    vote_average: movie.vote_average ?? null,
    vote_count: movie.vote_count ?? null,
    genre_ids: Array.isArray(movie.genre_ids) ? movie.genre_ids : [],
    topCast: getNameList(movie.topCast).slice(0, 5),
    directors: getNameList(movie.directors).slice(0, 3)
  };

  const criticScores = normalizeCriticScoresObject(movie.criticScores);
  if (criticScores) {
    summary.criticScores = criticScores;
  }

  return summary;
}

function captureRestoredMovie(movie) {
  if (!movie || movie.id == null) return null;
  const summary = summarizeMovie(movie);
  if (!summary || summary.id == null) return null;
  return summary;
}

function makeActionButton(label, handler) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'movie-action';
  btn.textContent = label;
  btn.addEventListener('click', handler);
  return btn;
}

function promptForInterest(initial = DEFAULT_INTEREST) {
  const promptFn =
    (typeof window !== 'undefined' && typeof window.prompt === 'function'
      ? window.prompt.bind(window)
      : null) ||
    (typeof globalThis !== 'undefined' && typeof globalThis.prompt === 'function'
      ? globalThis.prompt.bind(globalThis)
      : null);

  if (!promptFn) return initial;

  const message =
    'How interested are you in this movie? Enter a number from 1 (low) to 5 (high).';

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const response = promptFn(message, String(initial));
    if (response == null) {
      return null;
    }

    const trimmed = String(response).trim();
    if (!trimmed) {
      continue;
    }

    const value = Number(trimmed);
    if (Number.isFinite(value)) {
      const clamped = Math.max(1, Math.min(5, Math.round(value)));
      return clamped;
    }
  }

  return null;
}

function appendMeta(list, label, value) {
  if (!value && value !== 0) return;
  const item = document.createElement('li');
  const strong = document.createElement('strong');
  strong.textContent = `${label}:`;
  item.append(strong, ` ${value}`);
  list.appendChild(item);
}

function getGenreNames(movie) {
  if (!movie) return [];
  const ids = Array.isArray(movie.genre_ids) ? movie.genre_ids : [];
  return ids.map(id => genreMap[id]).filter(Boolean);
}

function appendGenresMeta(list, movie) {
  const genres = getGenreNames(movie);
  if (genres.length) {
    appendMeta(list, 'Genres', genres.join(', '));
  }
}

function hasActiveInterestedGenres() {
  return activeInterestedGenres.size > 0;
}

function toggleInterestedGenre(value) {
  if (!value) {
    if (!hasActiveInterestedGenres()) return;
    activeInterestedGenres.clear();
    renderInterestedList();
    return;
  }

  if (activeInterestedGenres.has(value)) {
    activeInterestedGenres.delete(value);
  } else {
    activeInterestedGenres.add(value);
  }
  renderInterestedList();
}

function removeInterestedGenre(value) {
  if (!value) return;
  if (activeInterestedGenres.delete(value)) {
    renderInterestedList();
  }
}

function renderInterestedFilters(genres) {
  if (!getDocument()) return;
  const container = domRefs.interestedFilters;
  if (!container) return;

  if (!genres.length) {
    container.innerHTML = '';
    container.style.display = 'none';
    activeInterestedGenres.clear();
    return;
  }

  container.style.display = '';
  container.innerHTML = '';

  const sorted = [...new Set(genres)].sort((a, b) => a.localeCompare(b));

  const buttonsWrap = document.createElement('div');
  buttonsWrap.className = 'genre-filter-buttons';

  const createButton = (label, value) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'genre-filter-btn';
    const isActive = value ? activeInterestedGenres.has(value) : !hasActiveInterestedGenres();
    if (isActive) {
      btn.classList.add('active');
    }
    btn.textContent = label;
    btn.dataset.genre = value ?? '';
    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    btn.addEventListener('click', () => {
      toggleInterestedGenre(value ?? null);
    });
    return btn;
  };

  buttonsWrap.appendChild(createButton('All', null));
  sorted.forEach(name => {
    buttonsWrap.appendChild(createButton(name, name));
  });

  const activeWrap = document.createElement('div');
  activeWrap.className = 'genre-filter-active';

  if (hasActiveInterestedGenres()) {
    const label = document.createElement('span');
    label.className = 'genre-filter-active-label';
    label.textContent = 'Filtering by:';
    activeWrap.appendChild(label);

    Array.from(activeInterestedGenres)
      .sort((a, b) => a.localeCompare(b))
      .forEach(name => {
        const chip = document.createElement('span');
        chip.className = 'genre-filter-chip';

        const text = document.createElement('span');
        text.className = 'genre-filter-chip-text';
        text.textContent = name;
        chip.appendChild(text);

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'genre-filter-chip-remove';
        removeBtn.setAttribute('aria-label', `Remove ${name} filter`);
        removeBtn.textContent = '×';
        removeBtn.addEventListener('click', () => removeInterestedGenre(name));
        chip.appendChild(removeBtn);

        activeWrap.appendChild(chip);
      });
  }

  container.append(buttonsWrap, activeWrap);
}

function appendPeopleMeta(list, label, names) {
  const values = getNameList(names);
  if (!values.length) return;
  appendMeta(list, label, values.join(', '));
}

function getVoteAverageValue(movie) {
  if (!movie) return null;
  const value = Number(movie.vote_average);
  return Number.isFinite(value) ? value : null;
}

function getVoteCountValue(movie) {
  if (!movie) return null;
  const value = Number(movie.vote_count);
  return Number.isFinite(value) ? value : null;
}

function getFilterFloat(value, min = -Infinity, max = Infinity) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const number = Number.parseFloat(trimmed.replace(',', '.'));
  if (!Number.isFinite(number)) return null;
  const clamped = Math.min(max, Math.max(min, number));
  return clamped;
}

function getFilterInt(value, min = -Infinity, max = Infinity) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const number = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(number)) return null;
  const clamped = Math.min(max, Math.max(min, number));
  return clamped;
}

function getMovieReleaseYear(movie) {
  if (!movie) return null;
  const raw = String(movie.release_date || movie.first_air_date || '').trim();
  if (!raw) return null;
  const year = Number.parseInt(raw.slice(0, 4), 10);
  return Number.isFinite(year) ? year : null;
}

function getMovieGenreIdSet(movie) {
  const ids = new Set();
  if (movie && Array.isArray(movie.genre_ids)) {
    movie.genre_ids.forEach(id => {
      const num = Number(id);
      if (Number.isFinite(num)) {
        ids.add(num);
      }
    });
  }
  if (movie && Array.isArray(movie.genres)) {
    movie.genres.forEach(entry => {
      const num = Number(entry?.id);
      if (Number.isFinite(num)) {
        ids.add(num);
      }
    });
  }
  return ids;
}

function applyFeedFilters(movies) {
  if (!Array.isArray(movies) || !movies.length) return [];

  const minRating = getFilterFloat(feedFilterState.minRating, 0, 10);
  const minVotes = getFilterInt(feedFilterState.minVotes, 0);
  let startYear = getFilterInt(feedFilterState.startYear, 1800, 3000);
  let endYear = getFilterInt(feedFilterState.endYear, 1800, 3000);

  if (startYear != null && endYear != null && endYear < startYear) {
    const temp = startYear;
    startYear = endYear;
    endYear = temp;
  }

  const genreMode = getGenreSelectionMode();
  const selectedGenres = getSelectedGenreIdSet();
  const disallowedGenres = getDisallowedGenreIdSet();
  const filterByGenres = genreMode === 'custom' && selectedGenres.size > 0;
  const blockAllGenres = genreMode === 'none';
  const enforceDisallowed = disallowedGenres.size > 0;

  return movies.filter(movie => {
    if (blockAllGenres) {
      return false;
    }

    if (minRating != null) {
      const rating = getVoteAverageValue(movie);
      if (rating == null || rating < minRating) {
        return false;
      }
    }

    if (minVotes != null) {
      const votes = getVoteCountValue(movie);
      if (votes == null || votes < minVotes) {
        return false;
      }
    }

    if (startYear != null || endYear != null) {
      const year = getMovieReleaseYear(movie);
      if (startYear != null && (year == null || year < startYear)) {
        return false;
      }
      if (endYear != null && (year == null || year > endYear)) {
        return false;
      }
    }

    if (filterByGenres || enforceDisallowed) {
      const ids = getMovieGenreIdSet(movie);

      if (filterByGenres) {
        let matches = false;
        for (const selectedId of selectedGenres) {
          if (ids.has(selectedId)) {
            matches = true;
            break;
          }
        }
        if (!matches) {
          return false;
        }
      }

      if (enforceDisallowed) {
        for (const disallowedId of disallowedGenres) {
          if (ids.has(disallowedId)) {
            return false;
          }
        }
      }
    }

    return true;
  });
}

function createRatingElement(movie) {
  const rating = getVoteAverageValue(movie);
  const votes = getVoteCountValue(movie);
  if (rating == null && votes == null) return null;
  const ratingEl = document.createElement('p');
  ratingEl.className = 'movie-rating';
  if (rating == null) {
    ratingEl.textContent = 'Rating not available';
  } else {
    const votesText = votes == null ? '' : ` (${votes} votes)`;
    ratingEl.textContent = `Rating: ${rating.toFixed(1)} / 10${votesText}`;
  }
  return ratingEl;
}

function createUserRatingElement(pref) {
  if (!pref || !pref.movie) return null;

  const container = document.createElement('label');
  container.className = 'movie-personal-rating';
  container.textContent = 'Your Rating: ';

  const input = document.createElement('input');
  input.type = 'number';
  input.min = '0';
  input.max = '10';
  input.step = '0.5';
  input.inputMode = 'decimal';
  input.placeholder = '—';

  if (pref.userRating != null && pref.userRating !== '') {
    const rating = clampUserRating(Number(pref.userRating));
    if (rating != null) {
      input.value = rating.toString();
    }
  }

  input.addEventListener('change', event => {
    const value = Number.parseFloat(event.target.value);
    if (Number.isNaN(value)) {
      setUserRating(pref.movie.id, null);
      return;
    }
    setUserRating(pref.movie.id, value);
  });

  container.appendChild(input);

  return container;
}

function applyCreditsToMovie(movie, credits) {
  if (!movie || !credits) return;
  const cast = Array.isArray(credits.cast) ? credits.cast : [];
  const crew = Array.isArray(credits.crew) ? credits.crew : [];

  const topCast = cast
    .filter(person => person && typeof person.name === 'string')
    .slice(0, 5)
    .map(person => person.name.trim())
    .filter(Boolean);

  const directors = crew
    .filter(person => person && person.job === 'Director' && typeof person.name === 'string')
    .map(person => person.name.trim())
    .filter(Boolean);

  if (topCast.length) {
    movie.topCast = Array.from(new Set(topCast));
  }
  if (directors.length) {
    movie.directors = Array.from(new Set(directors));
  }
}

function hasEnrichedCredits(movie) {
  if (!movie) return false;
  const cast = getNameList(movie.topCast);
  const directors = getNameList(movie.directors);
  return cast.length > 0 && directors.length > 0;
}

async function fetchCreditsDirect(movieId, apiKey) {
  if (!apiKey) return null;
  try {
    const url = new URL(`https://api.themoviedb.org/3/movie/${movieId}/credits`);
    url.searchParams.set('api_key', apiKey);
    const res = await fetch(url.toString());
    if (!res.ok) return null;
    return await res.json();
  } catch (err) {
    console.error('Failed to fetch credits directly for movie', movieId, err);
    return null;
  }
}

async function fetchCreditsFromProxy(movieId) {
  if (!movieId && movieId !== 0) return null;

  const paramVariants = [{ movie_id: movieId }, { id: movieId }, { movieId }];
  let lastParamError = null;

  for (const params of paramVariants) {
    try {
      const credits = await callTmdbProxy('credits', params);
      if (credits) {
        return credits;
      }
      return null;
    } catch (err) {
      if (!isProxyParameterError(err)) {
        throw err;
      }
      if (err && err.code === 'unsupported_endpoint') {
        unsupportedProxyEndpoints.add('credits');
        throw err;
      }
      lastParamError = err;
    }
  }

  if (lastParamError) {
    unsupportedProxyEndpoints.add('credits');
    throw lastParamError;
  }

  return null;
}

async function fetchCreditsViaDetailsFromProxy(movieId) {
  if (!movieId && movieId !== 0) return null;

  const baseParams = { append_to_response: 'credits' };
  const paramVariants = [
    { ...baseParams, movie_id: movieId },
    { ...baseParams, id: movieId },
    { ...baseParams, movieId }
  ];

  let lastParamError = null;

  for (const params of paramVariants) {
    try {
      const details = await callTmdbProxy('movie_details', params);
      if (details && typeof details === 'object' && details.credits) {
        return details.credits;
      }
      return null;
    } catch (err) {
      if (err && err.status === 400 && !isProxyParameterError(err)) {
        unsupportedProxyEndpoints.add('movie_details');
        if (!err.code) {
          err.code = 'unsupported_endpoint';
        }
      }
      if (!isProxyParameterError(err)) {
        throw err;
      }
      lastParamError = err;
    }
  }

  if (lastParamError) {
    unsupportedProxyEndpoints.add('movie_details');
    throw lastParamError;
  }

  return null;
}

async function fetchCreditsForMovie(movieId, { usingProxy, apiKey }) {
  if (!movieId) return null;
  const proxyEndpoint = getTmdbProxyEndpoint();
  const proxyAvailable = usingProxy && Boolean(proxyEndpoint);
  let needsDetailsFallback =
    proxyAvailable && !isProxyEndpointSupported('credits') && isProxyEndpointSupported('movie_details');

  if (proxyAvailable && isProxyEndpointSupported('credits')) {
    try {
      const credits = await fetchCreditsFromProxy(movieId);
      if (credits) {
        return credits;
      }
    } catch (err) {
      const summary = summarizeProxyError(err);
      if (isProxyParameterError(err)) {
        needsDetailsFallback =
          proxyAvailable && isProxyEndpointSupported('movie_details');
        if (!loggedProxyCreditsUnsupported) {
          console.info(
            `TMDB proxy credits endpoint unavailable (${summary}), attempting movie_details fallback.`
          );
          loggedProxyCreditsUnsupported = true;
        }
      } else {
        console.warn(
          `TMDB proxy credits request failed (${summary}), attempting direct fallback`,
          err
        );
        disableTmdbProxy();
        const direct = await fetchCreditsDirect(movieId, apiKey);
        if (direct) return direct;
        return null;
      }
    }
  }

  if (proxyAvailable && needsDetailsFallback && isProxyEndpointSupported('movie_details')) {
    try {
      const credits = await fetchCreditsViaDetailsFromProxy(movieId);
      if (credits) {
        return credits;
      }
    } catch (err) {
      const summary = summarizeProxyError(err);
      if (isProxyParameterError(err)) {
        // Swallow and fall back to direct fetching below.
      } else {
        console.warn(
          `TMDB proxy movie details request failed (${summary}), attempting direct fallback`,
          err
        );
        disableTmdbProxy();
        const direct = await fetchCreditsDirect(movieId, apiKey);
        if (direct) return direct;
        return null;
      }
    }
  }

  return fetchCreditsDirect(movieId, apiKey);
}

async function enrichMoviesWithCredits(movies, options = {}) {
  if (!Array.isArray(movies) || !movies.length) return;
  const { prefetchedCredits, ...fetchOptions } = options;
  const byId = new Map();
  movies.forEach(movie => {
    if (!movie || movie.id == null) return;
    byId.set(String(movie.id), movie);
  });

  if (prefetchedCredits && typeof prefetchedCredits === 'object') {
    Object.entries(prefetchedCredits).forEach(([id, credits]) => {
      const movie = byId.get(String(id));
      if (!movie) return;
      applyCreditsToMovie(movie, credits);
    });
  }

  const limit = Math.min(MAX_CREDIT_REQUESTS, movies.length);
  const targets = movies
    .slice(0, limit)
    .filter(movie => movie && movie.id != null && !hasEnrichedCredits(movie));
  if (!targets.length) return;

  const creditsList = await Promise.all(
    targets.map(movie => fetchCreditsForMovie(movie.id, fetchOptions))
  );

  creditsList.forEach((credits, index) => {
    const movie = targets[index];
    if (!movie) return;
    applyCreditsToMovie(movie, credits);
  });
}

async function setStatus(movie, status, options = {}) {
  if (!movie || movie.id == null) return;
  const usingProxy = Boolean(getTmdbProxyEndpoint());
  const apiKey = resolveApiKey();
  if (!getNameList(movie.directors).length || !getNameList(movie.topCast).length) {
    if (usingProxy || apiKey) {
      try {
        const credits = await fetchCreditsForMovie(movie.id, { usingProxy, apiKey });
        applyCreditsToMovie(movie, credits);
      } catch (err) {
        console.warn('Failed to enrich movie credits before saving status', movie.id, err);
      }
    }
  }
  await loadPreferences();
  const id = String(movie.id);
  restoredMoviesById.delete(id);
  const next = { ...currentPrefs };
  const snapshot = summarizeMovie(movie);
  const entry = next[id] ? { ...next[id] } : {};
  const skipRatingPrompt = Boolean(options.skipRatingPrompt);
  entry.status = status;
  entry.updatedAt = Date.now();
  if (status === 'interested') {
    entry.interest = options.interest ?? entry.interest ?? DEFAULT_INTEREST;
    entry.movie = snapshot;
    delete entry.userRating;
  } else if (status === 'watched') {
    entry.movie = snapshot;
    delete entry.interest;
  } else if (status === 'notInterested') {
    delete entry.movie;
    delete entry.interest;
    delete entry.userRating;
  }
  next[id] = entry;
  await savePreferences(next);
  pruneSuppressedMovies();
  refreshUI();
  if (status === 'watched' && !skipRatingPrompt) {
    await promptForUserRating(movie);
  }
  if (!getFeedMovies(currentMovies).length) {
    requestAdditionalMovies();
  }
}

async function promptForUserRating(movie) {
  if (!movie || movie.id == null) return;
  const hasWindow = typeof window !== 'undefined' && window;
  const promptFn = hasWindow && typeof window.prompt === 'function' ? window.prompt : null;
  if (!promptFn) return;

  const title = (movie.title || movie.name || '').trim() || 'this title';
  const message = `Rate "${title}" on a scale of 0-10 (leave blank to skip).`;

  let response;
  try {
    response = promptFn(message, '');
  } catch (err) {
    console.warn('Failed to prompt for movie rating', movie.id, err);
    return;
  }

  if (response == null) return;
  if (typeof response !== 'string') return;

  const trimmed = response.trim();
  if (!trimmed) return;

  const value = Number.parseFloat(trimmed);
  if (Number.isNaN(value)) return;

  await setUserRating(movie.id, value);
}

async function setUserRating(movieId, rating) {
  await loadPreferences();
  const id = String(movieId);
  const pref = currentPrefs[id];
  if (!pref || pref.status !== 'watched') return;

  const next = { ...currentPrefs };
  const entry = { ...pref };

  if (rating == null) {
    delete entry.userRating;
  } else {
    entry.userRating = clampUserRating(rating);
  }
  entry.updatedAt = Date.now();
  next[id] = entry;
  await savePreferences(next);
  refreshUI();
}

async function clearStatus(movieId) {
  await loadPreferences();
  const id = String(movieId);
  const next = { ...currentPrefs };
  const removed = next[id];
  delete next[id];
  await savePreferences(next);
  if (removed && removed.movie) {
    const restored = captureRestoredMovie(removed.movie);
    if (restored && restored.id != null) {
      restoredMoviesById.set(String(restored.id), restored);
      const exists = Array.isArray(currentMovies)
        ? currentMovies.some(movie => String(movie?.id) === String(restored.id))
        : false;
      if (!exists) {
        currentMovies = [restored, ...(Array.isArray(currentMovies) ? currentMovies : [])];
        currentMovies = applyPriorityOrdering(currentMovies);
        feedExhausted = false;
      }
    }
  }
  pruneSuppressedMovies();
  refreshUI();
  if (!getFeedMovies(currentMovies).length) {
    requestAdditionalMovies();
  }
}

function getFeedMovies(movies) {
  if (!Array.isArray(movies) || !movies.length) return [];

  return movies.filter(movie => !isMovieSuppressed(movie?.id));
}

function isMovieSuppressed(movieId) {
  if (movieId == null) return false;
  const pref = currentPrefs[String(movieId)];
  return Boolean(pref && SUPPRESSED_STATUSES.has(pref.status));
}

function pruneSuppressedMovies() {
  if (!Array.isArray(currentMovies) || !currentMovies.length) return;
  currentMovies = currentMovies.filter(movie => !isMovieSuppressed(movie?.id));
  feedExhausted = false;
}

async function requestAdditionalMovies() {
  const now = Date.now();
  if (refillInProgress) {
    const started = formatTimestamp(lastRefillAttempt);
    const label = started ? ` (started at ${started})` : '';
    updateFeedStatus(`Movie request already in progress${label}.`, {
      tone: 'info',
      showSpinner: true
    });
    return;
  }
  if (now - lastRefillAttempt < REFILL_COOLDOWN_MS) {
    const waitMs = Math.max(0, REFILL_COOLDOWN_MS - (now - lastRefillAttempt));
    const waitSeconds = Math.ceil(waitMs / 1000);
    updateFeedStatus(`Waiting ${waitSeconds}s before requesting more movies...`, {
      tone: 'info'
    });
    if (!pendingRefillCooldownTimer) {
      pendingRefillCooldownTimer = setTimeout(() => {
        pendingRefillCooldownTimer = null;
        requestAdditionalMovies();
      }, waitMs);
    }
    return;
  }
  if (pendingRefillCooldownTimer) {
    clearTimeout(pendingRefillCooldownTimer);
    pendingRefillCooldownTimer = null;
  }
  refillInProgress = true;
  lastRefillAttempt = now;
  feedExhausted = false;
  try {
    await loadMovies({ attemptStart: now });
  } finally {
    refillInProgress = false;
    renderFeed();
  }
}

function renderFeed() {
  if (!getDocument()) return;
  const listEl = domRefs.list;
  updateMovieStats();
  if (!listEl) return;

  if (!currentMovies.length) {
    if (refillInProgress) {
      listEl.innerHTML = '<em>Loading more movies...</em>';
      updateFeedStatus('Waiting for movies from TMDB...', {
        tone: 'info',
        showSpinner: true
      });
      return;
    }
    if (feedExhausted) {
      listEl.innerHTML = hasActiveFeedFilters()
        ? '<em>No movies match the current filters.</em>'
        : '<em>No movies found.</em>';
      updateFeedStatus(
        hasActiveFeedFilters()
          ? 'TMDB did not return movies that match your filters.'
          : 'TMDB did not return any movies. Try again later.',
        { tone: 'warning' }
      );
      return;
    }
    listEl.innerHTML = '<em>Loading more movies...</em>';
    updateFeedStatus('Requesting the first batch of movies...', {
      tone: 'info',
      showSpinner: true
    });
    requestAdditionalMovies();
    return;
  }

  const availableMovies = getFeedMovies(currentMovies);

  if (!availableMovies.length) {
    if (refillInProgress) {
      listEl.innerHTML = '<em>Loading more movies...</em>';
      updateFeedStatus('All current results are hidden; waiting for new movies...', {
        tone: 'info',
        showSpinner: true
      });
      return;
    }
    listEl.innerHTML = '<em>Loading more movies...</em>';
    updateFeedStatus(
      'All fetched movies are hidden by saved statuses. Looking for fresh titles...',
      { tone: 'warning', showSpinner: true }
    );
    requestAdditionalMovies();
    return;
  }

  const filteredMovies = applyFeedFilters(availableMovies);

  if (!filteredMovies.length) {
    if (refillInProgress) {
      listEl.innerHTML = '<em>Loading more movies...</em>';
      updateFeedStatus('Filters removed the current batch; waiting for more movies...', {
        tone: 'info',
        showSpinner: true
      });
      return;
    }
    if (feedExhausted) {
      listEl.innerHTML = hasActiveFeedFilters()
        ? '<em>No movies match the current filters.</em>'
        : '<em>No movies found.</em>';
      updateFeedStatus(
        hasActiveFeedFilters()
          ? 'Filters are hiding every movie that is currently available.'
          : 'TMDB did not return any additional movies.',
        { tone: 'warning' }
      );
      return;
    }
    listEl.innerHTML = '<em>Loading more movies...</em>';
    const hiddenByFilters = availableMovies.length;
    updateFeedStatus(
      hiddenByFilters
        ? `Filters are hiding ${hiddenByFilters} movie${hiddenByFilters === 1 ? '' : 's'}; requesting more options...`
        : 'Filters removed the current batch; requesting more movies...',
      { tone: 'warning', showSpinner: true }
    );
    requestAdditionalMovies();
    return;
  }

  const ul = document.createElement('ul');
  filteredMovies.forEach(movie => {
    const li = document.createElement('li');
    li.className = 'movie-card';

    if (movie.poster_path) {
      const img = document.createElement('img');
      img.src = `https://image.tmdb.org/t/p/w200${movie.poster_path}`;
      img.alt = `${movie.title || movie.name || 'Movie'} poster`;
      li.appendChild(img);
    }

    const info = document.createElement('div');
    info.className = 'movie-info';

    const title = (movie.title || movie.name || '').trim();
    const year = (movie.release_date || '').split('-')[0] || 'Unknown';
    const titleEl = document.createElement('h3');
    titleEl.textContent = `${title} (${year})`;
    info.appendChild(titleEl);

    const btnRow = document.createElement('div');
    btnRow.className = 'button-row';
    btnRow.append(
      makeActionButton('Watched Already', () => setStatus(movie, 'watched')),
      makeActionButton('Not Interested', () => setStatus(movie, 'notInterested')),
      makeActionButton(
        'Interested',
        async () => {
          const existing = currentPrefs?.[String(movie.id)];
          const initialInterest =
            Number.isFinite(existing?.interest) && existing?.interest >= 1 && existing?.interest <= 5
              ? Math.round(existing.interest)
              : DEFAULT_INTEREST;
          const interest = promptForInterest(initialInterest);
          if (interest == null) return;
          await setStatus(movie, 'interested', { interest });
        }
      )
    );
    info.appendChild(btnRow);

    const metaList = document.createElement('ul');
    metaList.className = 'movie-meta';

    appendGenresMeta(metaList, movie);
    appendMeta(metaList, 'Average Score', movie.vote_average ?? 'N/A');
    appendMeta(metaList, 'Votes', movie.vote_count ?? 'N/A');
    appendMeta(metaList, 'Release Date', movie.release_date || 'Unknown');
    appendPeopleMeta(metaList, 'Director', movie.directors);
    appendPeopleMeta(metaList, 'Cast', movie.topCast);
    appendCriticScoresMeta(metaList, movie);

    if (metaList.childNodes.length) {
      info.appendChild(metaList);
    }

    if (movie.overview) {
      const overview = document.createElement('p');
      overview.textContent = movie.overview;
      info.appendChild(overview);
    }

    li.appendChild(info);
    ul.appendChild(li);
  });

  listEl.innerHTML = '';
  listEl.appendChild(ul);
  updateFeedStatus(
    `Showing ${filteredMovies.length} movie${filteredMovies.length === 1 ? '' : 's'} (updated ${formatTimestamp(
      Date.now()
    )}).`,
    { tone: 'success' }
  );
}

function renderInterestedList() {
  if (!getDocument()) return;
  const listEl = domRefs.interestedList;
  if (!listEl) return;

  const allEntries = Object.values(currentPrefs)
    .filter(pref => pref.status === 'interested' && pref.movie)
    .sort((a, b) => (b.interest ?? 0) - (a.interest ?? 0) || (b.updatedAt ?? 0) - (a.updatedAt ?? 0));

  const genres = [];
  allEntries.forEach(pref => {
    const names = getGenreNames(pref.movie);
    if (names.length) {
      genres.push(...names);
    }
  });

  let removed = false;
  Array.from(activeInterestedGenres).forEach(name => {
    if (!genres.includes(name)) {
      activeInterestedGenres.delete(name);
      removed = true;
    }
  });

  if (removed && !genres.length) {
    activeInterestedGenres.clear();
  }

  renderInterestedFilters(genres);

  if (!allEntries.length) {
    listEl.innerHTML = '<em>No interested movies yet.</em>';
    return;
  }

  const selectedGenres = Array.from(activeInterestedGenres);
  const entries = selectedGenres.length
    ? allEntries.filter(pref => {
        const names = getGenreNames(pref.movie);
        return names.some(name => activeInterestedGenres.has(name));
      })
    : allEntries;

  if (!entries.length) {
    listEl.innerHTML = '<em>No interested movies for the selected genre.</em>';
    return;
  }

  const ul = document.createElement('ul');
  entries.forEach(pref => {
    const movie = pref.movie;
    const li = document.createElement('li');
    li.className = 'movie-card';

    if (movie.poster_path) {
      const img = document.createElement('img');
      img.src = `https://image.tmdb.org/t/p/w200${movie.poster_path}`;
      img.alt = `${movie.title || 'Movie'} poster`;
      li.appendChild(img);
    }

    const info = document.createElement('div');
    info.className = 'movie-info';

    const year = (movie.release_date || '').split('-')[0] || 'Unknown';
    const titleEl = document.createElement('h3');
    titleEl.textContent = `${movie.title || 'Untitled'} (${year})`;
    info.appendChild(titleEl);

    const interestRow = document.createElement('div');
    interestRow.className = 'interest-row';
    const label = document.createElement('span');
    label.textContent = `Interest: ${pref.interest ?? DEFAULT_INTEREST}`;

    const slider = document.createElement('input');
    slider.type = 'range';
    slider.min = '1';
    slider.max = '5';
    slider.value = String(pref.interest ?? DEFAULT_INTEREST);
    slider.addEventListener('input', () => {
      label.textContent = `Interest: ${slider.value}`;
    });
    slider.addEventListener('change', async () => {
      const updated = { ...currentPrefs };
      const entry = updated[String(movie.id)];
      if (entry) {
        entry.interest = Number(slider.value);
        entry.updatedAt = Date.now();
        await savePreferences(updated);
        renderInterestedList();
      }
    });

    interestRow.append(label, slider);
    info.appendChild(interestRow);

    if (movie.overview) {
      const overview = document.createElement('p');
      overview.textContent = movie.overview;
      info.appendChild(overview);
    }

    const metaList = document.createElement('ul');
    metaList.className = 'movie-meta';
    appendGenresMeta(metaList, movie);
    appendPeopleMeta(metaList, 'Director', movie.directors);
    appendPeopleMeta(metaList, 'Cast', movie.topCast);
    appendCriticScoresMeta(metaList, movie);
    if (metaList.childNodes.length) {
      info.appendChild(metaList);
    }

    const controls = document.createElement('div');
    controls.className = 'button-row';
    controls.append(makeActionButton('Remove', () => clearStatus(movie.id)));
    info.appendChild(controls);

    li.appendChild(info);
    ul.appendChild(li);
  });

  listEl.innerHTML = '';
  listEl.appendChild(ul);
}

function renderWatchedList() {
  if (!getDocument()) return;
  const listEl = domRefs.watchedList;
  if (!listEl) return;

  const entries = Object.values(currentPrefs).filter(
    pref => pref.status === 'watched' && pref.movie
  );

  const sorted = entries.slice();

  const byUpdatedAt = (a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0);
  const getEffectiveRating = pref => {
    if (pref.userRating != null) {
      const rating = clampUserRating(Number(pref.userRating));
      if (rating != null) return rating;
    }
    return getVoteAverageValue(pref.movie);
  };

  const byRatingDesc = (a, b) => {
    const aRating = getEffectiveRating(a);
    const bRating = getEffectiveRating(b);
    if (aRating == null && bRating == null) return byUpdatedAt(a, b);
    if (aRating == null) return 1;
    if (bRating == null) return -1;
    if (bRating !== aRating) return bRating - aRating;
    const aVotes = getVoteCountValue(a.movie);
    const bVotes = getVoteCountValue(b.movie);
    if (aVotes == null && bVotes == null) return byUpdatedAt(a, b);
    if (aVotes == null) return 1;
    if (bVotes == null) return -1;
    if (bVotes !== aVotes) return bVotes - aVotes;
    return byUpdatedAt(a, b);
  };
  const byRatingAsc = (a, b) => {
    const aRating = getEffectiveRating(a);
    const bRating = getEffectiveRating(b);
    if (aRating == null && bRating == null) return byUpdatedAt(a, b);
    if (aRating == null) return 1;
    if (bRating == null) return -1;
    if (aRating !== bRating) return aRating - bRating;
    const aVotes = getVoteCountValue(a.movie);
    const bVotes = getVoteCountValue(b.movie);
    if (aVotes == null && bVotes == null) return byUpdatedAt(a, b);
    if (aVotes == null) return 1;
    if (bVotes == null) return -1;
    if (aVotes !== bVotes) return aVotes - bVotes;
    return byUpdatedAt(a, b);
  };

  if (watchedSortMode === 'ratingDesc') {
    sorted.sort(byRatingDesc);
  } else if (watchedSortMode === 'ratingAsc') {
    sorted.sort(byRatingAsc);
  } else {
    sorted.sort(byUpdatedAt);
  }

  if (domRefs.watchedSort) {
    domRefs.watchedSort.value = watchedSortMode;
  }

  if (!sorted.length) {
    listEl.innerHTML = '<em>No watched movies yet.</em>';
    return;
  }

  const rated = [];
  const unrated = [];

  const hasUserRating = pref => {
    if (pref.userRating == null || pref.userRating === '') return false;
    return clampUserRating(Number(pref.userRating)) != null;
  };

  sorted.forEach(pref => {
    if (hasUserRating(pref)) {
      rated.push(pref);
    } else {
      unrated.push(pref);
    }
  });

  const createCard = pref => {
    const movie = pref.movie;
    const li = document.createElement('li');
    li.className = 'movie-card';

    if (movie.poster_path) {
      const img = document.createElement('img');
      img.src = `https://image.tmdb.org/t/p/w200${movie.poster_path}`;
      img.alt = `${movie.title || 'Movie'} poster`;
      li.appendChild(img);
    }

    const info = document.createElement('div');
    info.className = 'movie-info';

    const year = (movie.release_date || '').split('-')[0] || 'Unknown';
    const titleEl = document.createElement('h3');
    titleEl.textContent = `${movie.title || 'Untitled'} (${year})`;
    info.appendChild(titleEl);

    const ratingEl = createRatingElement(movie);
    if (ratingEl) {
      info.appendChild(ratingEl);
    }

    const personalRatingEl = createUserRatingElement(pref);
    if (personalRatingEl) {
      info.appendChild(personalRatingEl);
    }

    if (movie.overview) {
      const overview = document.createElement('p');
      overview.textContent = movie.overview;
      info.appendChild(overview);
    }

    const metaList = document.createElement('ul');
    metaList.className = 'movie-meta';
    appendMeta(metaList, 'Average Score', movie.vote_average ?? 'N/A');
    appendMeta(metaList, 'Votes', movie.vote_count ?? 'N/A');
    appendMeta(metaList, 'Release Date', movie.release_date || 'Unknown');
    appendGenresMeta(metaList, movie);
    appendPeopleMeta(metaList, 'Director', movie.directors);
    appendPeopleMeta(metaList, 'Cast', movie.topCast);
    appendCriticScoresMeta(metaList, movie);
    if (metaList.childNodes.length) {
      info.appendChild(metaList);
    }

    const controls = document.createElement('div');
    controls.className = 'button-row';
    controls.append(makeActionButton('Remove', () => clearStatus(movie.id)));
    info.appendChild(controls);

    li.appendChild(info);
    return li;
  };

  const createColumn = (title, prefs, emptyMessage) => {
    const column = document.createElement('section');
    column.className = 'watched-column';

    const heading = document.createElement('h4');
    heading.textContent = title;
    column.appendChild(heading);

    if (!prefs.length) {
      const empty = document.createElement('p');
      empty.className = 'watched-empty';
      empty.innerHTML = `<em>${emptyMessage}</em>`;
      column.appendChild(empty);
      return column;
    }

    const columnList = document.createElement('ul');
    prefs.forEach(pref => {
      columnList.appendChild(createCard(pref));
    });
    column.appendChild(columnList);
    return column;
  };

  const container = document.createElement('div');
  container.className = 'watched-columns';
  container.appendChild(createColumn('Rated', rated, 'No rated movies yet.'));
  container.appendChild(createColumn('Unrated', unrated, 'No unrated movies yet.'));

  listEl.innerHTML = '';
  listEl.appendChild(container);
}

function refreshUI() {
  renderFeed();
  renderInterestedList();
  renderWatchedList();
}

function selectPriorityCandidates(movies) {
  if (!Array.isArray(movies) || !movies.length) return [];

  const thresholds = [
    { minAverage: MIN_VOTE_AVERAGE, minVotes: MIN_VOTE_COUNT },
    {
      minAverage: Math.max(6.5, MIN_VOTE_AVERAGE - 0.5),
      minVotes: Math.max(25, Math.floor(MIN_VOTE_COUNT / 2))
    },
    { minAverage: 6, minVotes: 10 }
  ];

  let bestFallback = [];
  for (const { minAverage, minVotes } of thresholds) {
    const filtered = movies.filter(movie => meetsQualityThreshold(movie, minAverage, minVotes));
    if (filtered.length >= MIN_PRIORITY_RESULTS) {
      return filtered;
    }
    if (filtered.length && bestFallback.length === 0) {
      bestFallback = filtered;
    }
  }

  if (bestFallback.length) return bestFallback;

  return movies.filter(movie => {
    const average = Number(movie?.vote_average ?? NaN);
    const votes = Number(movie?.vote_count ?? NaN);
    return Number.isFinite(average) && Number.isFinite(votes);
  });
}

function applyPriorityOrdering(movies) {
  if (!Array.isArray(movies) || !movies.length) return movies || [];

  const candidates = selectPriorityCandidates(movies);
  if (!candidates.length) return [];

  const maxVotes = Math.max(...candidates.map(m => Math.max(0, m.vote_count || 0)), 1);
  const now = Date.now();
  const yearMs = 365 * 24 * 60 * 60 * 1000;

  return candidates
    .map(movie => {
      const rawAverage = Math.max(0, Math.min(10, movie.vote_average ?? 0)) / 10;
      const votes = Math.max(0, movie.vote_count || 0);
      const voteVolume = Math.log10(votes + 1) / Math.log10(maxVotes + 1);

      const confidence = Math.min(1, votes / 150);
      const adjustedAverage = rawAverage * confidence + 0.6 * (1 - confidence);

      let recency = 0.5;
      if (movie.release_date) {
        const release = new Date(movie.release_date).getTime();
        if (!Number.isNaN(release)) {
          const diff = now - release;
          if (diff <= 0) {
            recency = 1;
          } else if (diff >= yearMs) {
            recency = 0;
          } else {
            recency = 1 - diff / yearMs;
          }
        }
      }

      const priority = (adjustedAverage * 0.3) + (Math.sqrt(Math.max(0, voteVolume)) * 0.5) + (recency * 0.2);
      return { ...movie, __priority: priority };
    })
    .sort((a, b) => (b.__priority ?? 0) - (a.__priority ?? 0));
}

function mergeRestoredMovies(baseMovies) {
  const list = Array.isArray(baseMovies) ? baseMovies.slice() : [];
  if (!restoredMoviesById.size) {
    return list;
  }

  const existingById = new Map();
  list.forEach(movie => {
    if (!movie || movie.id == null) return;
    const key = String(movie.id);
    existingById.set(key, movie);
    const normalized = captureRestoredMovie(movie);
    if (normalized) {
      restoredMoviesById.set(key, normalized);
    }
  });

  let modified = false;
  restoredMoviesById.forEach((movie, key) => {
    if (isMovieSuppressed(key)) {
      restoredMoviesById.delete(key);
      return;
    }
    if (existingById.has(key)) {
      return;
    }
    const normalized = captureRestoredMovie(movie);
    if (!normalized) {
      restoredMoviesById.delete(key);
      return;
    }
    list.push(normalized);
    existingById.set(key, normalized);
    modified = true;
  });

  if (!modified) {
    return list;
  }

  const merged = applyPriorityOrdering(list);
  return merged;
}

async function fetchDiscoverPageDirect(apiKey, page) {
  const params = new URLSearchParams({
    api_key: apiKey,
    sort_by: 'popularity.desc',
    include_adult: 'false',
    include_video: 'false',
    language: 'en-US',
    page: String(page)
  });
  const genreQuery = buildGenreQueryParams();
  if (genreQuery.blockAll) {
    return {
      results: [],
      totalPages: 0
    };
  }
  if (genreQuery.withGenres) {
    params.set('with_genres', genreQuery.withGenres);
  }
  const res = await fetch(`https://api.themoviedb.org/3/discover/movie?${params.toString()}`);
  if (!res.ok) throw new Error('Failed to fetch movies');
  const data = await res.json();
  const totalPages = Number(data.total_pages);
  return {
    results: Array.isArray(data.results) ? data.results : [],
    totalPages: Number.isFinite(totalPages) && totalPages > 0 ? totalPages : null
  };
}

async function fetchGenreMapDirect(apiKey) {
  try {
    const res = await fetch(`https://api.themoviedb.org/3/genre/movie/list?api_key=${apiKey}`);
    if (!res.ok) return {};
    const data = await res.json();
    return Object.fromEntries((data.genres || []).map(g => [g.id, g.name]));
  } catch (_) {
    return {};
  }
}

async function fetchDiscoverPageFromProxy(page) {
  const genreQuery = buildGenreQueryParams();
  if (genreQuery.blockAll) {
    return {
      results: [],
      totalPages: 0
    };
  }
  const data = await callTmdbProxy('discover', {
    sort_by: 'popularity.desc',
    include_adult: 'false',
    include_video: 'false',
    language: 'en-US',
    page: String(page),
    ...(genreQuery.withGenres ? { with_genres: genreQuery.withGenres } : {})
  });
  const totalPages = Number(data?.total_pages);
  return {
    results: Array.isArray(data?.results) ? data.results : [],
    totalPages: Number.isFinite(totalPages) && totalPages > 0 ? totalPages : null
  };
}

function normalizeGenreMap(raw) {
  if (!raw) return null;
  if (Array.isArray(raw)) {
    const entries = raw
      .map(entry => {
        const id = Number(entry?.id);
        const name = typeof entry?.name === 'string' ? entry.name.trim() : '';
        if (!Number.isFinite(id) || !name) return null;
        return [id, name];
      })
      .filter(Boolean);
    return entries.length ? Object.fromEntries(entries) : null;
  }
  if (typeof raw === 'object') {
    const entries = Object.entries(raw)
      .map(([id, value]) => {
        const numericId = Number(id);
        const name = typeof value === 'string' ? value.trim() : '';
        if (!Number.isFinite(numericId) || !name) return null;
        return [numericId, name];
      })
      .filter(Boolean);
    return entries.length ? Object.fromEntries(entries) : null;
  }
  return null;
}

function normalizeCreditsMap(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const normalized = {};
  Object.entries(raw).forEach(([id, credits]) => {
    if (!credits || typeof credits !== 'object') return;
    const cast = Array.isArray(credits.cast) ? credits.cast : [];
    const crew = Array.isArray(credits.crew) ? credits.crew : [];
    if (!cast.length && !crew.length) return;
    normalized[String(id)] = { cast, crew };
  });
  return Object.keys(normalized).length ? normalized : null;
}

function buildTmdbDiscoverKey({ usingProxy }) {
  const parts = [
    usingProxy ? 'proxy' : 'direct',
    feedFilterState.minRating ?? '',
    feedFilterState.minVotes ?? '',
    feedFilterState.startYear ?? '',
    feedFilterState.endYear ?? '',
    feedFilterState.selectedGenres ?? GENRE_SELECTION_ALL
  ];
  return parts.map(value => String(value ?? '').trim()).join('|');
}

function normalizeTmdbDiscoverStateEntry(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const now = Date.now();
  const nextPageRaw = Number(raw.nextPage);
  const allowedRaw = Number(raw.allowedPages);
  const totalRaw = Number(raw.totalPages);
  const updatedRaw = Number(raw.updatedAt ?? raw.lastAttempt);
  const lastAttemptRaw = Number(raw.lastAttempt);
  const nextPage = Number.isFinite(nextPageRaw) && nextPageRaw > 0 ? Math.floor(nextPageRaw) : 1;
  const allowedPages = Math.max(
    MAX_DISCOVER_PAGES,
    Number.isFinite(allowedRaw) && allowedRaw > 0 ? Math.floor(allowedRaw) : MAX_DISCOVER_PAGES,
    nextPage
  );
  const totalPages = Number.isFinite(totalRaw) && totalRaw > 0 ? Math.floor(totalRaw) : null;
  const updatedAt = Number.isFinite(updatedRaw) && updatedRaw > 0 ? Math.floor(updatedRaw) : now;
  const lastAttempt = Number.isFinite(lastAttemptRaw) && lastAttemptRaw > 0 ? Math.floor(lastAttemptRaw) : null;
  return {
    nextPage,
    allowedPages,
    totalPages,
    exhausted: Boolean(raw.exhausted),
    updatedAt,
    lastAttempt
  };
}

function hydrateTmdbDiscoverState(raw) {
  tmdbDiscoverHistory.clear();
  const container =
    raw && typeof raw === 'object'
      ? (raw.entries && typeof raw.entries === 'object' ? raw.entries : raw)
      : {};
  Object.entries(container).forEach(([key, value]) => {
    const normalized = normalizeTmdbDiscoverStateEntry(value);
    if (!key || !normalized) return;
    tmdbDiscoverHistory.set(String(key), normalized);
  });
  while (tmdbDiscoverHistory.size > TMDB_DISCOVER_HISTORY_LIMIT) {
    const oldest = tmdbDiscoverHistory.keys().next().value;
    if (oldest == null) break;
    tmdbDiscoverHistory.delete(oldest);
  }
  tmdbDiscoverStateDirty = false;
}

function getSerializableTmdbDiscoverState() {
  const entries = {};
  tmdbDiscoverHistory.forEach((value, key) => {
    entries[key] = {
      nextPage: value.nextPage,
      allowedPages: value.allowedPages,
      totalPages: value.totalPages ?? null,
      exhausted: Boolean(value.exhausted),
      updatedAt: value.updatedAt ?? Date.now(),
      lastAttempt: value.lastAttempt ?? null
    };
  });
  return { version: TMDB_DISCOVER_STATE_VERSION, entries };
}

function scheduleTmdbDiscoverPersist() {
  if (tmdbDiscoverPersistTimer) return;
  tmdbDiscoverPersistTimer = setTimeout(() => {
    tmdbDiscoverPersistTimer = null;
    persistTmdbDiscoverState().catch(err => {
      console.warn('Failed to persist TMDB discover state', err);
    });
  }, TMDB_DISCOVER_STATE_PERSIST_DEBOUNCE_MS);
  if (tmdbDiscoverPersistTimer && typeof tmdbDiscoverPersistTimer.unref === 'function') {
    tmdbDiscoverPersistTimer.unref();
  }
}

async function persistTmdbDiscoverState({ immediate = false } = {}) {
  if (!tmdbDiscoverStateDirty && !immediate) return;
  const serialized = getSerializableTmdbDiscoverState();
  if (activeUserId) {
    if (!db || typeof db.collection !== 'function') {
      saveLocalDiscoverState(serialized);
      tmdbDiscoverStateDirty = false;
      return;
    }
    try {
      await db
        .collection(PREF_COLLECTION)
        .doc(activeUserId)
        .set({ [TMDB_DISCOVER_STATE_FIELD]: serialized }, { merge: true });
      tmdbDiscoverStateDirty = false;
    } catch (err) {
      console.warn('Failed to write TMDB discover state to Firestore', err);
      if (immediate) {
        throw err;
      }
      scheduleTmdbDiscoverPersist();
    }
  } else {
    saveLocalDiscoverState(serialized);
    tmdbDiscoverStateDirty = false;
  }
}

function markTmdbDiscoverStateDirty({ immediate = false } = {}) {
  tmdbDiscoverStateDirty = true;
  if (immediate) {
    persistTmdbDiscoverState({ immediate: true }).catch(err => {
      console.warn('Immediate TMDB discover state persistence failed', err);
    });
    return;
  }
  scheduleTmdbDiscoverPersist();
}

if (typeof window !== 'undefined') {
  window.addEventListener('beforeunload', () => {
    if (!tmdbDiscoverStateDirty) return;
    try {
      persistTmdbDiscoverState({ immediate: true });
    } catch (_) {
      /* ignore */
    }
  });
}

function readTmdbDiscoverState(key) {
  if (!key) return null;
  const entry = tmdbDiscoverHistory.get(key);
  if (!entry) return null;
  return { ...entry };
}

function writeTmdbDiscoverState(key, state) {
  if (!key || !state) return;
  const normalizedTotal = Number.isFinite(state.totalPages) && state.totalPages > 0
    ? Math.floor(state.totalPages)
    : null;
  const normalizedAllowed = Number.isFinite(state.allowedPages) && state.allowedPages > 0
    ? Math.floor(state.allowedPages)
    : MAX_DISCOVER_PAGES;
  const nextPage = Number.isFinite(state.nextPage) && state.nextPage > 0
    ? Math.floor(state.nextPage)
    : 1;
  const payload = normalizeTmdbDiscoverStateEntry({
    nextPage,
    allowedPages: normalizedAllowed,
    totalPages: normalizedTotal,
    exhausted: Boolean(state.exhausted),
    updatedAt: Date.now(),
    lastAttempt: Date.now()
  });
  if (!payload) return;
  const existing = tmdbDiscoverHistory.get(key);
  const isSame =
    existing &&
    existing.nextPage === payload.nextPage &&
    existing.allowedPages === payload.allowedPages &&
    (existing.totalPages ?? null) === (payload.totalPages ?? null) &&
    Boolean(existing.exhausted) === Boolean(payload.exhausted);
  if (isSame) {
    tmdbDiscoverHistory.set(key, {
      ...existing,
      updatedAt: payload.updatedAt,
      lastAttempt: payload.lastAttempt
    });
    return;
  }
  tmdbDiscoverHistory.delete(key);
  tmdbDiscoverHistory.set(key, payload);
  while (tmdbDiscoverHistory.size > TMDB_DISCOVER_HISTORY_LIMIT) {
    const oldest = tmdbDiscoverHistory.keys().next().value;
    if (oldest == null) break;
    tmdbDiscoverHistory.delete(oldest);
  }
  markTmdbDiscoverStateDirty();
}

function normalizeCachedMovie(movie) {
  if (!movie || typeof movie !== 'object') return null;
  const normalized = { ...movie };

  if (normalized.vote_average == null && normalized.score != null) {
    const average = Number(normalized.score);
    if (Number.isFinite(average)) {
      normalized.vote_average = average;
    }
  }

  if (normalized.vote_count == null && normalized.voteCount != null) {
    const votes = Number.parseInt(normalized.voteCount, 10);
    if (Number.isFinite(votes)) {
      normalized.vote_count = votes;
    }
  }

  if (!normalized.release_date && typeof normalized.releaseDate === 'string') {
    normalized.release_date = normalized.releaseDate;
  }

  if (!normalized.title && typeof normalized.name === 'string') {
    normalized.title = normalized.name;
  }

  if (normalized.criticScores) {
    const scores = normalizeCriticScoresObject(normalized.criticScores);
    if (scores) {
      normalized.criticScores = scores;
    } else {
      delete normalized.criticScores;
    }
  }

  return normalized;
}

function collectMoviesFromCache(results, suppressedIds) {
  const seen = new Set();
  const collected = [];
  (Array.isArray(results) ? results : []).forEach(movie => {
    if (!movie || movie.id == null) return;
    const idKey = String(movie.id);
    if (seen.has(idKey)) return;
    seen.add(idKey);
    if (suppressedIds.has(idKey)) return;
    const normalized = normalizeCachedMovie(movie);
    if (!normalized) return;
    collected.push(normalized);
  });
  return applyPriorityOrdering(collected);
}

async function tryFetchCachedMovies({ suppressedIds, minFeedSize }) {
  try {
    const params = new URLSearchParams();
    const minRating = getFilterFloat(feedFilterState.minRating, 0, 10);
    const minVotes = getFilterInt(feedFilterState.minVotes, 0);
    let startYear = getFilterInt(feedFilterState.startYear, 1800, 3000);
    let endYear = getFilterInt(feedFilterState.endYear, 1800, 3000);

    if (startYear != null && endYear != null && endYear < startYear) {
      const temp = startYear;
      startYear = endYear;
      endYear = temp;
    }

    if (Number.isFinite(minRating)) params.set('minRating', String(minRating));
    if (Number.isFinite(minVotes)) params.set('minVotes', String(minVotes));
    if (Number.isFinite(startYear)) params.set('startYear', String(startYear));
    if (Number.isFinite(endYear)) params.set('endYear', String(endYear));
    if (suppressedIds.size) params.set('excludeIds', Array.from(suppressedIds).join(','));
    params.set('limit', String(Math.max(minFeedSize, MIN_PRIORITY_RESULTS)));

    const baseUrl = buildMoviesApiUrl('/api/movies');
    const query = params.toString();
    const url = query ? `${baseUrl}?${query}` : baseUrl;
    const res = await fetch(url);
    if (!res.ok) {
      return null;
    }
    const data = await res.json();
    const metadata =
      data && typeof data === 'object' && data.metadata && typeof data.metadata === 'object'
        ? data.metadata
        : null;
    if (metadata) {
      lastCatalogMetadata = metadata;
    }
    const prioritized = collectMoviesFromCache(data?.results, suppressedIds);
    const filtered = applyFeedFilters(prioritized);
    const satisfied = filtered.length >= minFeedSize;
    return {
      movies: prioritized,
      genres: normalizeGenreMap(data?.genres ?? data?.genreMap),
      credits: normalizeCreditsMap(data?.credits),
      metadata,
      satisfied
    };
  } catch (err) {
    console.warn('Failed to load cached movies', err);
    return null;
  }
}

async function fetchMoviesFromTmdb({
  usingProxy,
  apiKey,
  minFeedSize,
  suppressedIds,
  existingMovies = []
}) {
  const seen = new Set();
  const collected = [];

  (Array.isArray(existingMovies) ? existingMovies : []).forEach(movie => {
    if (!movie || movie.id == null) return;
    const idKey = String(movie.id);
    if (seen.has(idKey)) return;
    seen.add(idKey);
    collected.push(movie);
  });

  let prioritized = applyPriorityOrdering(collected);
  if (applyFeedFilters(prioritized).length >= minFeedSize) {
    return prioritized;
  }

  const requestKey = buildTmdbDiscoverKey({ usingProxy });
  const history = readTmdbDiscoverState(requestKey);
  let page = Math.max(1, history?.nextPage || 1);
  let allowedPages = Math.max(
    MAX_DISCOVER_PAGES,
    Number.isFinite(history?.allowedPages) && history.allowedPages > 0
      ? history.allowedPages
      : MAX_DISCOVER_PAGES,
    page
  );
  let totalPages = Number.isFinite(history?.totalPages) && history.totalPages > 0
    ? history.totalPages
    : Infinity;
  let reachedEnd = false;
  let madeNetworkRequest = false;

  const commitProgress = ({ exhausted } = {}) => {
    if (!madeNetworkRequest) return;
    const normalizedTotal = Number.isFinite(totalPages) && totalPages > 0 ? totalPages : null;
    const payload = {
      nextPage: Math.max(1, page),
      allowedPages: Math.max(allowedPages, MAX_DISCOVER_PAGES, page),
      totalPages: normalizedTotal,
      exhausted:
        exhausted != null
          ? exhausted
          : (reachedEnd && (normalizedTotal == null ? true : page - 1 >= normalizedTotal))
    };
    writeTmdbDiscoverState(requestKey, payload);
  };

  if (
    history &&
    history.exhausted &&
    Number.isFinite(history.totalPages) &&
    history.totalPages > 0 &&
    page > history.totalPages
  ) {
    return prioritized.length ? prioritized : applyPriorityOrdering(collected);
  }

  while (page <= allowedPages && page <= totalPages) {
    const currentPage = page;
    const { results, totalPages: reportedTotal } = usingProxy
      ? await fetchDiscoverPageFromProxy(currentPage)
      : await fetchDiscoverPageDirect(apiKey, currentPage);
    madeNetworkRequest = true;

    if (Number.isFinite(reportedTotal) && reportedTotal > 0) {
      totalPages = reportedTotal;
    }

    const pageResults = Array.isArray(results) ? results : [];
    pageResults.forEach(movie => {
      if (!movie || movie.id == null) return;
      const idKey = String(movie.id);
      if (seen.has(idKey)) return;
      seen.add(idKey);
      if (!suppressedIds.has(idKey)) {
        collected.push(movie);
      }
    });

    prioritized = applyPriorityOrdering(collected);

    const feedMovies = applyFeedFilters(prioritized);
    if (feedMovies.length >= minFeedSize) {
      page = currentPage + 1;
      commitProgress({ exhausted: false });
      return prioritized;
    }

    if (!pageResults.length && (!Number.isFinite(totalPages) || currentPage >= totalPages)) {
      reachedEnd = true;
      page = currentPage + 1;
      break;
    }

    page = currentPage + 1;

    if (page > allowedPages && allowedPages < MAX_DISCOVER_PAGES_LIMIT) {
      allowedPages = Math.min(
        MAX_DISCOVER_PAGES_LIMIT,
        Math.max(allowedPages + INITIAL_DISCOVER_PAGES, page)
      );
    }
  }

  if (!reachedEnd && madeNetworkRequest && Number.isFinite(totalPages) && totalPages > 0 && page > totalPages) {
    reachedEnd = true;
  }

  commitProgress({});

  return prioritized.length ? prioritized : applyPriorityOrdering(collected);
}

async function fetchMovies({ usingProxy, apiKey, minFeedSize = MIN_FEED_RESULTS }) {
  const suppressedIds = new Set(
    Object.entries(currentPrefs)
      .filter(([, pref]) => pref && SUPPRESSED_STATUSES.has(pref.status))
      .map(([id]) => id)
  );

  const cacheResult = await tryFetchCachedMovies({ suppressedIds, minFeedSize });
  let movies = Array.isArray(cacheResult?.movies) ? cacheResult.movies : [];
  let metadata = cacheResult?.metadata || null;
  let usedTmdbFallback = false;

  if (!cacheResult || !cacheResult.satisfied) {
    try {
      movies = await fetchMoviesFromTmdb({
        usingProxy,
        apiKey,
        minFeedSize,
        suppressedIds,
        existingMovies: movies
      });
      usedTmdbFallback = true;
    } catch (err) {
      if (!movies.length) {
        throw err;
      }
      console.warn('TMDB fallback failed but cached movies are available', err);
      usedTmdbFallback = false;
    }
  }

  return {
    movies,
    genres: cacheResult?.genres || null,
    credits: cacheResult?.credits || null,
    metadata,
    usedTmdbFallback,
    fromCache: Boolean(cacheResult?.movies?.length)
  };
}

async function fetchGenreMapFromProxy() {
  try {
    const data = await callTmdbProxy('genres', { language: 'en-US' });
    return Object.fromEntries((data.genres || []).map(g => [g.id, g.name]));
  } catch (_) {
    return {};
  }
}

async function loadMovies({ attemptStart } = {}) {
  const listEl = domRefs.list;
  if (!listEl) return;

  const proxyEndpoint = getTmdbProxyEndpoint();
  const usingProxy = Boolean(proxyEndpoint);

  const inputKey = domRefs.apiKeyInput?.value.trim();
  let apiKey = activeApiKey || inputKey;
  let usingTestFallback = false;

  if (!apiKey && typeof window !== 'undefined' && window.tmdbApiKey) {
    apiKey = window.tmdbApiKey;
  }

  // Allow automated tests to exercise the flow without a real TMDB key.
  const inVitest =
    typeof process !== 'undefined' &&
    process.env &&
    (process.env.VITEST === 'true' || process.env.NODE_ENV === 'test');

  if (!apiKey && inVitest && !usingProxy) {
    apiKey = '__TEST_FALLBACK_API_KEY__';
    usingTestFallback = true;
  }

  if (!usingProxy && !apiKey) {
    listEl.innerHTML = '<em>TMDB API key unavailable.</em>';
    updateFeedStatus(
      'TMDB API key unavailable. Configure the server secret or enable the proxy to load movies.',
      {
        tone: 'warning'
      }
    );
    return;
  }

  if (!usingProxy && !activeApiKey) {
    if (usingTestFallback) {
      activeApiKey = apiKey;
    } else if (apiKey) {
      persistApiKey(apiKey);
    }
  }

  const attemptNumber = ++loadAttemptCounter;
  const startedLabel = formatTimestamp(
    Number.isFinite(attemptStart) ? attemptStart : Date.now()
  );
  const sourceLabel = usingProxy ? 'TMDB proxy service' : 'direct TMDB API';
  const attemptIntro = usingProxy
    ? 'Checking the movie cache before reaching out to the TMDB proxy service with your saved preferences.'
    : 'Checking the movie cache before contacting TMDB directly using the server TMDB API key.';
  const fallbackNote = usingProxy
    ? ' If this route fails we will automatically switch to the server TMDB API key.'
    : '';
  updateFeedStatus(
    `Loading movies (attempt ${attemptNumber})${
      startedLabel ? ` started at ${startedLabel}` : ''
    }. ${attemptIntro}${fallbackNote}`,
    { tone: 'info', showSpinner: true }
  );

  listEl.innerHTML = '<em>Loading...</em>';
  try {
    const {
      movies,
      genres: cachedGenreMap,
      credits: prefetchedCredits,
      metadata: catalogMetadata,
      usedTmdbFallback,
      fromCache
    } = await fetchMovies({ usingProxy, apiKey, minFeedSize: MIN_FEED_RESULTS });
    await enrichMoviesWithCredits(movies, {
      usingProxy,
      apiKey,
      prefetchedCredits
    });
    let genres = cachedGenreMap;
    const needsGenreFetch =
      !genres || !Object.keys(genres).length || usedTmdbFallback;
    if (needsGenreFetch) {
      genres = usingProxy ? await fetchGenreMapFromProxy() : await fetchGenreMapDirect(apiKey);
    }
    const fetchedMovies = Array.isArray(movies) ? movies : [];
    currentMovies = mergeRestoredMovies(fetchedMovies);
    genreMap = genres || {};
    if (catalogMetadata && typeof catalogMetadata === 'object') {
      lastCatalogMetadata = catalogMetadata;
    }
    populateFeedGenreOptions();
    updateFeedFilterInputsFromState();
    feedExhausted = !currentMovies.length;
    refreshUI();
    const availableCount = getFeedMovies(currentMovies).length;
    const completedLabel = formatTimestamp(Date.now());
    const finalSourceLabel = !usedTmdbFallback && fromCache
      ? 'the movie cache'
      : `the ${sourceLabel}`;
    updateFeedStatus(
      `Loaded ${movies.length} movie${movies.length === 1 ? '' : 's'} on attempt ${attemptNumber}${
        completedLabel ? ` at ${completedLabel}` : ''
      } using ${finalSourceLabel}. ${availableCount} ${
        availableCount === 1 ? 'match' : 'matches'
      } your current filters.`,
      { tone: availableCount ? 'success' : 'warning' }
    );
  } catch (err) {
    if (usingProxy) {
      console.warn('TMDB proxy unavailable, falling back to direct API', err);
      updateFeedStatus(
        `Attempt ${attemptNumber} using the TMDB proxy service failed (${summarizeProxyError(
          err
        )}). Switching to the server TMDB API key.`,
        { tone: 'warning' }
      );
      disableTmdbProxy();
      if (!apiKey) {
        listEl.innerHTML =
          '<em>TMDB proxy is unavailable and no server TMDB API key is configured.</em>';
        updateFeedStatus(
          'TMDB proxy is unavailable and no TMDB API key is configured on the server. Contact an administrator to restore access.',
          { tone: 'error' }
        );
        return;
      }
      await loadMovies();
      return;
    }
    console.error('Failed to load movies', err);
    listEl.textContent = 'Failed to load movies.';
    updateFeedStatus(
      `Attempt ${attemptNumber} using the ${sourceLabel} failed (${summarizeError(
        err
      )}). No movies were loaded. Check the TMDB API configuration and try again.`,
      { tone: 'error' }
    );
  }
}

export async function initMoviesPanel() {
  const doc = getDocument();
  if (!doc) return;

  domRefs.list = doc.getElementById('movieList');
  if (!domRefs.list) return;

  await ensureTmdbCredentialsLoaded().catch(err => {
    if (typeof console !== 'undefined' && console.warn) {
      console.warn('Unable to preload TMDB credentials for movies panel', err);
    }
  });

  domRefs.interestedList = doc.getElementById('savedMoviesList');
  domRefs.interestedFilters = doc.getElementById('savedMoviesFilters');
  domRefs.watchedList = doc.getElementById('watchedMoviesList');
  domRefs.apiKeyInput = doc.getElementById('moviesApiKey');
  domRefs.apiKeyContainer = doc.getElementById('moviesApiKeyContainer');
  domRefs.tabs = doc.getElementById('movieTabs');
  domRefs.streamSection = doc.getElementById('movieStreamSection');
  domRefs.interestedSection = doc.getElementById('savedMoviesSection');
  domRefs.watchedSection = doc.getElementById('watchedMoviesSection');
  domRefs.watchedSort = doc.getElementById('watchedMoviesSort');
  domRefs.feedControls = doc.getElementById('movieFeedControls');
  domRefs.feedStatus = doc.getElementById('movieStatus');
  domRefs.feedMinRating = doc.getElementById('movieFilterMinRating');
  domRefs.feedMinVotes = doc.getElementById('movieFilterMinVotes');
  domRefs.feedStartYear = doc.getElementById('movieFilterStartYear');
  domRefs.feedEndYear = doc.getElementById('movieFilterEndYear');
  domRefs.feedGenre = doc.getElementById('movieFilterGenre');

  updateMovieStats();

  loadAttemptCounter = 0;

  currentPrefs = await loadPreferences();

  feedFilterState = loadFeedFilterStateFromStorage();
  updateFeedFilterInputsFromState();

  attachFeedFilterInput(domRefs.feedMinRating, 'minRating');
  attachFeedFilterInput(domRefs.feedMinVotes, 'minVotes');
  attachFeedFilterInput(domRefs.feedStartYear, 'startYear');
  attachFeedFilterInput(domRefs.feedEndYear, 'endYear');

  const storedKey =
    (typeof window !== 'undefined' && window.tmdbApiKey) ||
    (typeof localStorage !== 'undefined' && localStorage.getItem(API_KEY_STORAGE)) ||
    '';
  activeApiKey = storedKey || '';
  if (domRefs.apiKeyInput && storedKey) {
    domRefs.apiKeyInput.value = storedKey;
    if (domRefs.apiKeyContainer) domRefs.apiKeyContainer.style.display = 'none';
  }

  if (domRefs.apiKeyInput && !getTmdbProxyEndpoint()) {
    if (!handlers.handleKeydown) {
      handlers.handleKeydown = e => {
        if (e.key === 'Enter') {
          e.preventDefault();
          persistApiKey(domRefs.apiKeyInput.value.trim());
          loadMovies();
        }
      };
    }
    if (!handlers.handleChange) {
      handlers.handleChange = () => {
        persistApiKey(domRefs.apiKeyInput.value.trim());
        loadMovies();
      };
    }
    domRefs.apiKeyInput.removeEventListener('keydown', handlers.handleKeydown);
    domRefs.apiKeyInput.removeEventListener('change', handlers.handleChange);
    domRefs.apiKeyInput.addEventListener('keydown', handlers.handleKeydown);
    domRefs.apiKeyInput.addEventListener('change', handlers.handleChange);
  }

  if (domRefs.apiKeyContainer && getTmdbProxyEndpoint()) {
    domRefs.apiKeyContainer.style.display = 'none';
  }

  if (domRefs.tabs) {
    domRefs.tabs.setAttribute('role', 'tablist');
    const buttons = Array.from(domRefs.tabs.querySelectorAll('.movie-tab'));
    buttons.forEach(btn => {
      btn.setAttribute('role', 'tab');
      btn.setAttribute('aria-selected', btn.classList.contains('active') ? 'true' : 'false');
      if (btn._movieTabHandler) {
        btn.removeEventListener('click', btn._movieTabHandler);
      }
      const handler = () => {
        buttons.forEach(b => {
          b.classList.remove('active');
          b.setAttribute('aria-selected', 'false');
        });
        btn.classList.add('active');
        btn.setAttribute('aria-selected', 'true');
        const target = btn.dataset.target;
        if (domRefs.streamSection) {
          domRefs.streamSection.style.display =
            target === 'movieStreamSection' ? '' : 'none';
        }
        if (domRefs.interestedSection) {
          domRefs.interestedSection.style.display =
            target === 'savedMoviesSection' ? '' : 'none';
          if (target === 'savedMoviesSection') renderInterestedList();
        }
        if (domRefs.watchedSection) {
          domRefs.watchedSection.style.display =
            target === 'watchedMoviesSection' ? '' : 'none';
          if (target === 'watchedMoviesSection') renderWatchedList();
        }
      };
      btn._movieTabHandler = handler;
      btn.addEventListener('click', handler);
    });
  }

  if (domRefs.watchedSort) {
    if (domRefs.watchedSort._moviesSortHandler) {
      domRefs.watchedSort.removeEventListener(
        'change',
        domRefs.watchedSort._moviesSortHandler
      );
    }
    const handler = () => {
      const value = domRefs.watchedSort?.value || 'recent';
      watchedSortMode = value;
      renderWatchedList();
    };
    domRefs.watchedSort._moviesSortHandler = handler;
    domRefs.watchedSort.addEventListener('change', handler);
    domRefs.watchedSort.value = watchedSortMode;
  }

  await loadMovies();
}

if (typeof window !== 'undefined') {
  window.initMoviesPanel = initMoviesPanel;
}
