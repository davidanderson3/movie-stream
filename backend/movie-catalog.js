const fs = require('fs');
const path = require('path');
const { readCachedResponse, writeCachedResponse } = require('../shared/cache');

const MIN_SCORE = 6;
const DEFAULT_LIMIT = 20;
const MOVIE_CACHE_COLLECTION = 'movieCatalog';
const MOVIE_CACHE_KEY = ['curated', 'v1'];
const MOVIE_RANGE_CACHE_COLLECTION = 'movieCatalogRanges';
const MOVIE_RANGE_CACHE_VERSION = 'v1';
const MOVIE_RANGE_SPAN_YEARS = Math.max(
  1,
  Number(process.env.MOVIE_CATALOG_RANGE_SPAN_YEARS) || 5
);
const MOVIE_RANGE_MIN_YEAR = (() => {
  const configured = Number(process.env.MOVIE_CATALOG_MIN_YEAR);
  if (Number.isFinite(configured)) {
    return Math.max(1900, Math.floor(configured));
  }
  return 1970;
})();
const MOVIE_RANGE_MAX_YEAR = (() => {
  const currentYear = new Date().getFullYear();
  const configured = Number(process.env.MOVIE_CATALOG_MAX_YEAR);
  if (Number.isFinite(configured)) {
    return Math.max(MOVIE_RANGE_MIN_YEAR, Math.floor(configured));
  }
  return Math.max(MOVIE_RANGE_MIN_YEAR, currentYear);
})();
const MOVIE_RANGE_CACHE_TTL_MS = Math.max(
  24 * 60 * 60 * 1000,
  Number(process.env.MOVIE_CATALOG_RANGE_TTL_MS) || 90 * 24 * 60 * 60 * 1000
);
const MOVIE_RANGE_FETCH_LIMIT = Math.max(
  1,
  Number(process.env.MOVIE_CATALOG_RANGE_FETCH_LIMIT) || 2
);
const MOVIE_RANGE_REQUEST_DELAY_MS = Math.max(
  0,
  Number(process.env.MOVIE_CATALOG_RANGE_REQUEST_DELAY_MS) || 120
);
const NEW_RELEASE_CACHE_COLLECTION = 'movieNewReleases';
const NEW_RELEASE_CACHE_VERSION = 'v1';
const NEW_RELEASE_CACHE_TTL_MS = Math.max(
  60 * 60 * 1000,
  Number(process.env.MOVIE_NEW_RELEASE_CACHE_TTL_MS) || 24 * 60 * 60 * 1000
);
const NEW_RELEASE_CACHE_LIMIT = Math.max(
  10,
  Number(process.env.MOVIE_NEW_RELEASE_CACHE_LIMIT) || 50
);
const REFRESH_INTERVAL_MS = Math.max(
  60_000,
  Number(process.env.MOVIE_CATALOG_REFRESH_INTERVAL_MS) || 12 * 60 * 60 * 1000
);
const STALE_AFTER_MS = Math.max(
  5 * 60_000,
  Number(process.env.MOVIE_CATALOG_STALE_MS) || 24 * 60 * 60 * 1000
);
const NEW_RELEASE_LOOKBACK_DAYS = Math.max(
  7,
  Number(process.env.MOVIE_NEW_RELEASE_DAYS) || 45
);
const MAX_DISCOVER_PAGES = Math.max(
  1,
  Number(process.env.MOVIE_CATALOG_MAX_PAGES) || 10
);

let state = {
  movies: [],
  updatedAt: 0,
  metadata: { source: 'empty', total: 0, updatedAt: null },
  byId: new Map()
};

let hydratePromise = null;
let refreshPromise = null;
let refreshTimer = null;

function getTmdbCredentials() {
  const bearer =
    process.env.TMDB_BEARER_TOKEN ||
    process.env.TMDB_READ_ACCESS_TOKEN ||
    process.env.TMDB_ACCESS_TOKEN;
  const apiKey = process.env.TMDB_API_KEY || process.env.TMDB_KEY || process.env.TMDB_TOKEN;
  if (!bearer && !apiKey) {
    return null;
  }
  return { bearer, apiKey };
}

function hasTmdbCredentials() {
  return Boolean(getTmdbCredentials());
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  const ms = Date.parse(dateStr);
  if (!Number.isFinite(ms)) return null;
  return ms;
}

function wait(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.resolve();
  }
  return new Promise(resolve => setTimeout(resolve, ms));
}

function computeRanking(score, voteCount, releaseDate, popularity) {
  const normalizedScore = Math.max(0, Math.min(Number(score) || 0, 10)) / 10;
  const voteWeight = Math.log10(Math.max(1, Number(voteCount) || 0) + 1);
  const popWeight = Math.log10(Math.max(1, Number(popularity) || 0) + 1);
  let recencyWeight = 0.25;
  const releaseMs = parseDate(releaseDate);
  if (Number.isFinite(releaseMs)) {
    const diffDays = (Date.now() - releaseMs) / (1000 * 60 * 60 * 24);
    const windowDays = 365 * 5;
    if (diffDays <= 0) {
      recencyWeight = 1;
    } else if (diffDays >= windowDays) {
      recencyWeight = 0;
    } else {
      recencyWeight = 1 - diffDays / windowDays;
    }
  }
  return normalizedScore * 5 + voteWeight * 0.75 + popWeight * 0.5 + recencyWeight * 0.75;
}

function normalizeGenreIds(movie) {
  const ids = new Set();
  const rawIds = movie?.genre_ids ?? movie?.genreIds ?? null;
  if (Array.isArray(rawIds)) {
    rawIds.forEach(value => {
      const numeric = Number(value);
      if (Number.isFinite(numeric)) {
        ids.add(numeric);
      }
    });
  }
  if (Array.isArray(movie?.genres)) {
    movie.genres.forEach(entry => {
      if (entry == null) return;
      const numeric = Number(typeof entry === 'number' ? entry : entry.id);
      if (Number.isFinite(numeric)) {
        ids.add(numeric);
      }
    });
  }
  return ids.size ? Array.from(ids).sort((a, b) => a - b) : [];
}

function normalizeMovie(movie, { allowLowScore = false } = {}) {
  if (!movie) return null;
  const title = typeof movie.title === 'string' ? movie.title.trim() : '';
  if (!title) return null;
  const rawScore = movie.score ?? movie.vote_average ?? movie.voteAverage ?? null;
  const score = Number(rawScore);
  if (!Number.isFinite(score)) return null;
  if (!allowLowScore && score < MIN_SCORE) return null;
  const idValue = movie.id ?? movie.movieID ?? movie.movieId ?? null;
  const id = idValue == null ? null : String(idValue);
  const poster_path =
    typeof movie.poster_path === 'string'
      ? movie.poster_path
      : typeof movie.posterPath === 'string'
      ? movie.posterPath
      : typeof movie.poster === 'string'
      ? movie.poster
      : null;
  const backdrop_path =
    typeof movie.backdrop_path === 'string'
      ? movie.backdrop_path
      : typeof movie.backdropPath === 'string'
      ? movie.backdropPath
      : typeof movie.backdrop === 'string'
      ? movie.backdrop
      : null;
  const releaseDate =
    typeof movie.releaseDate === 'string'
      ? movie.releaseDate
      : typeof movie.release_date === 'string'
      ? movie.release_date
      : typeof movie.year === 'number'
      ? `${movie.year}-01-01`
      : null;
  const genre_ids = normalizeGenreIds(movie);
  const voteCount = Number(
    movie.vote_count ?? movie.voteCount ?? (Array.isArray(movie.ratings) ? movie.ratings.length : 0) ?? 0
  );
  const popularity = Number(movie.popularity ?? movie.popularityScore ?? 0);
  const searchTitle = title.toLowerCase();
  const ranking = computeRanking(score, voteCount, releaseDate, popularity);
  return {
    id,
    title,
    score,
    poster_path,
    backdrop_path,
    releaseDate,
    genre_ids,
    voteCount,
    popularity,
    searchTitle,
    ranking
  };
}

function formatMovieForResponse(movie, source = 'catalog') {
  if (!movie) return null;
  const roundedScore = Math.round(movie.score * 10) / 10;
  return {
    id: movie.id,
    title: movie.title,
    score: Number.isFinite(roundedScore) ? roundedScore : movie.score,
    poster_path: movie.poster_path || null,
    backdrop_path: movie.backdrop_path || null,
    releaseDate: movie.releaseDate || null,
    genre_ids: Array.isArray(movie.genre_ids) ? movie.genre_ids : [],
    voteCount: Number.isFinite(movie.voteCount) ? movie.voteCount : null,
    popularity: Number.isFinite(movie.popularity) ? movie.popularity : null,
    source
  };
}

function stripDerivedMovieFields(movie) {
  if (!movie) return null;
  const id = movie.id == null ? null : String(movie.id);
  const title = typeof movie.title === 'string' ? movie.title : '';
  if (!id || !title) return null;
  const score = Number(movie.score);
  if (!Number.isFinite(score)) return null;
  const poster_path =
    typeof movie.poster_path === 'string'
      ? movie.poster_path
      : typeof movie.posterPath === 'string'
      ? movie.posterPath
      : null;
  const backdrop_path =
    typeof movie.backdrop_path === 'string'
      ? movie.backdrop_path
      : typeof movie.backdropPath === 'string'
      ? movie.backdropPath
      : null;
  const releaseDate = typeof movie.releaseDate === 'string' && movie.releaseDate ? movie.releaseDate : null;
  const genre_ids = normalizeGenreIds(movie);
  const voteCount = Number.isFinite(Number(movie.voteCount)) ? Number(movie.voteCount) : null;
  const popularity = Number.isFinite(Number(movie.popularity)) ? Number(movie.popularity) : null;
  return {
    id,
    title,
    score,
    poster_path,
    backdrop_path,
    releaseDate,
    genre_ids,
    voteCount,
    popularity
  };
}

function prepareMoviesForCache(movies) {
  return (Array.isArray(movies) ? movies : []).map(stripDerivedMovieFields).filter(Boolean);
}

function applyState(movies, metadata = {}) {
  const prepared = Array.isArray(movies)
    ? movies
        .map(movie => normalizeMovie(movie, { allowLowScore: Boolean(metadata.allowLowScore) }))
        .filter(Boolean)
        .sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0))
    : [];
  const updatedAtMs = (() => {
    if (metadata.updatedAt) {
      const parsed = parseDate(metadata.updatedAt);
      if (Number.isFinite(parsed)) return parsed;
    }
    return Date.now();
  })();
  state = {
    movies: prepared,
    updatedAt: updatedAtMs,
    metadata: {
      ...metadata,
      total: prepared.length,
      updatedAt: new Date(updatedAtMs).toISOString()
    },
    byId: new Map(prepared.map(movie => [movie.id, movie]))
  };
  return state;
}

async function loadCatalogFromFirestore() {
  const cached = await readCachedResponse(MOVIE_CACHE_COLLECTION, MOVIE_CACHE_KEY, 0);
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    return null;
  }
  try {
    return JSON.parse(cached.body);
  } catch (err) {
    console.error('Failed to parse cached movie catalog', err);
    return null;
  }
}

async function loadCatalogFromRangeCache() {
  const ranges = buildReleaseRanges();
  if (!ranges.length) return null;
  const entries = [];
  for (const range of ranges) {
    // eslint-disable-next-line no-await-in-loop
    const cached = await loadRangeFromCache(range, 0);
    if (cached) {
      entries.push(cached);
    }
  }
  if (!entries.length) return null;
  const movies = mergeRangeMovies(entries);
  if (!movies.length) return null;
  const metadata = summarizeRangeMetadata(entries, movies, ranges.length, 0);
  return { movies, metadata };
}

async function persistCatalog() {
  if (!state.movies.length) return;
  const payload = {
    updatedAt: new Date(state.updatedAt).toISOString(),
    metadata: { ...state.metadata, total: state.movies.length },
    movies: state.movies.map(stripDerivedMovieFields).filter(Boolean)
  };
  const body = JSON.stringify(payload);
  await writeCachedResponse(MOVIE_CACHE_COLLECTION, MOVIE_CACHE_KEY, {
    status: 200,
    contentType: 'application/json',
    body,
    metadata: {
      source: state.metadata?.source || null,
      total: state.movies.length
    }
  });
}

function buildReleaseRanges() {
  const ranges = [];
  const span = Math.max(1, Number(MOVIE_RANGE_SPAN_YEARS) || 1);
  const minYear = Math.max(1900, Number(MOVIE_RANGE_MIN_YEAR) || 1900);
  const maxYear = Math.max(minYear, Number(MOVIE_RANGE_MAX_YEAR) || minYear);
  for (let endYear = maxYear; endYear >= minYear; endYear -= span) {
    const startYear = Math.max(minYear, endYear - span + 1);
    const startDate = `${String(startYear).padStart(4, '0')}-01-01`;
    const endDate = `${String(endYear).padStart(4, '0')}-12-31`;
    ranges.push({
      startYear,
      endYear,
      startDate,
      endDate,
      label: `${startYear}-${endYear}`
    });
  }
  return ranges;
}

function rangeCacheParts(range) {
  return ['range', MOVIE_RANGE_CACHE_VERSION, range.startDate, range.endDate];
}

async function loadRangeFromCache(range, ttlMs = MOVIE_RANGE_CACHE_TTL_MS) {
  const cached = await readCachedResponse(
    MOVIE_RANGE_CACHE_COLLECTION,
    rangeCacheParts(range),
    ttlMs
  );
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    return null;
  }
  try {
    const parsed = JSON.parse(cached.body);
    const rawMovies = Array.isArray(parsed?.movies) ? parsed.movies : [];
    const movies = rawMovies.map(movie => normalizeMovie(movie)).filter(Boolean);
    if (!movies.length) return null;
    const fetchedAt = (() => {
      if (parsed?.range?.fetchedAt && typeof parsed.range.fetchedAt === 'string') {
        return parsed.range.fetchedAt;
      }
      if (cached.metadata && typeof cached.metadata.fetchedAt === 'string') {
        return cached.metadata.fetchedAt;
      }
      return null;
    })();
    const total = Number(parsed?.range?.total || cached.metadata?.total || movies.length) || movies.length;
    const metadata = {
      source: 'cache',
      fetchedAt,
      total,
      label: range.label,
      startDate: range.startDate,
      endDate: range.endDate,
      tmdbTotalResults:
        Number(parsed?.range?.totalResults ?? cached.metadata?.tmdbTotalResults) || null,
      tmdbTotalPages:
        Number(parsed?.range?.totalPages ?? cached.metadata?.tmdbTotalPages) || null
    };
    return { range, movies, metadata };
  } catch (err) {
    console.error(`Failed to parse cached TMDB range ${range.label}`, err);
    return null;
  }
}

async function cacheRangeMovies(range, movies, metadata = {}) {
  const sanitizedMovies = prepareMoviesForCache(movies);
  if (!sanitizedMovies.length) return;
  const fetchedAt = new Date().toISOString();
  const body = JSON.stringify({
    range: {
      label: range.label,
      startDate: range.startDate,
      endDate: range.endDate,
      fetchedAt,
      total: sanitizedMovies.length,
      totalResults:
        Number.isFinite(Number(metadata.totalResults)) && Number(metadata.totalResults) > 0
          ? Number(metadata.totalResults)
          : undefined,
      totalPages:
        Number.isFinite(Number(metadata.totalPages)) && Number(metadata.totalPages) > 0
          ? Number(metadata.totalPages)
          : undefined
    },
    movies: sanitizedMovies
  });
  const cacheMetadata = {
    label: range.label,
    startDate: range.startDate,
    endDate: range.endDate,
    total: sanitizedMovies.length,
    fetchedAt,
    tmdbTotalResults:
      Number.isFinite(Number(metadata.totalResults)) && Number(metadata.totalResults) > 0
        ? Number(metadata.totalResults)
        : null,
    tmdbTotalPages:
      Number.isFinite(Number(metadata.totalPages)) && Number(metadata.totalPages) > 0
        ? Number(metadata.totalPages)
        : null,
    version: MOVIE_RANGE_CACHE_VERSION
  };
  try {
    await writeCachedResponse(MOVIE_RANGE_CACHE_COLLECTION, rangeCacheParts(range), {
      status: 200,
      contentType: 'application/json',
      body,
      metadata: cacheMetadata
    });
  } catch (err) {
    console.error(`Failed to cache TMDB range ${range.label}`, err);
  }
}

function mergeRangeMovies(rangeEntries) {
  const merged = new Map();
  for (const entry of rangeEntries) {
    if (!entry || !Array.isArray(entry.movies)) continue;
    for (const movie of entry.movies) {
      if (!movie || movie.id == null) continue;
      const key = String(movie.id);
      if (!merged.has(key)) {
        merged.set(key, movie);
      } else {
        const existing = merged.get(key);
        if ((movie.ranking ?? 0) > (existing.ranking ?? 0)) {
          merged.set(key, movie);
        }
      }
    }
  }
  return Array.from(merged.values()).sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0));
}

function summarizeRangeMetadata(rangeEntries, aggregatedMovies, totalConfiguredRanges, fetchedThisRun) {
  let cachedCount = 0;
  let fetchedCount = 0;
  const coverageYears = [];
  const fetchedTimestamps = [];
  for (const entry of rangeEntries) {
    if (!entry) continue;
    if (entry.metadata?.source === 'tmdb') {
      fetchedCount += 1;
    } else {
      cachedCount += 1;
    }
    if (entry.range) {
      if (Number.isFinite(entry.range.startYear)) coverageYears.push(entry.range.startYear);
      if (Number.isFinite(entry.range.endYear)) coverageYears.push(entry.range.endYear);
    }
    const fetchedAtMs = parseDate(entry.metadata?.fetchedAt);
    if (Number.isFinite(fetchedAtMs)) {
      fetchedTimestamps.push(fetchedAtMs);
    }
  }
  const latestFetchedMs = fetchedTimestamps.length ? Math.max(...fetchedTimestamps) : null;
  const minYear = coverageYears.length ? Math.min(...coverageYears) : null;
  const maxYear = coverageYears.length ? Math.max(...coverageYears) : null;
  return {
    source: 'tmdb-range-cache',
    totalCollected: aggregatedMovies.length,
    rangeVersion: MOVIE_RANGE_CACHE_VERSION,
    rangeSpanYears: MOVIE_RANGE_SPAN_YEARS,
    configuredRanges: totalConfiguredRanges,
    coveredRanges: rangeEntries.length,
    missingRanges: Math.max(0, totalConfiguredRanges - rangeEntries.length),
    fetchedRanges: fetchedCount,
    cachedRanges: cachedCount,
    fetchedThisRun,
    minYear,
    maxYear,
    rangeCacheTtlMs: MOVIE_RANGE_CACHE_TTL_MS,
    fetchedAt: latestFetchedMs ? new Date(latestFetchedMs).toISOString() : null
  };
}

async function tmdbRequest(pathname, params, credentials) {
  const creds = credentials || getTmdbCredentials();
  if (!creds) throw new Error('TMDB credentials not configured');
  const url = new URL(`https://api.themoviedb.org/3/${pathname}`);
  if (params instanceof URLSearchParams) {
    params.forEach((value, key) => {
      url.searchParams.set(key, value);
    });
  }
  if (!creds.bearer && creds.apiKey && !url.searchParams.has('api_key')) {
    url.searchParams.set('api_key', creds.apiKey);
  }
  const headers = {};
  if (creds.bearer) {
    headers.Authorization = `Bearer ${creds.bearer}`;
  }
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`TMDB request failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function fetchCatalogFromTmdb(credentials) {
  const params = new URLSearchParams({
    sort_by: 'vote_average.desc',
    vote_average: `gte:${MIN_SCORE}`,
    'vote_average.gte': String(MIN_SCORE),
    'vote_count.gte': '100',
    include_adult: 'false',
    include_video: 'false',
    language: 'en-US'
  });
  const collected = [];
  let pagesFetched = 0;
  let totalPages = Infinity;
  for (let page = 1; page <= MAX_DISCOVER_PAGES && page <= totalPages; page += 1) {
    params.set('page', String(page));
    const data = await tmdbRequest('discover/movie', params, credentials);
    const results = Array.isArray(data?.results) ? data.results : [];
    collected.push(...results);
    pagesFetched = page;
    const reportedTotal = Number(data?.total_pages);
    if (Number.isFinite(reportedTotal) && reportedTotal > 0) {
      totalPages = reportedTotal;
    }
    if (!results.length && (!Number.isFinite(totalPages) || page >= totalPages)) {
      break;
    }
  }
  return {
    movies: collected,
    metadata: {
      source: 'tmdb',
      fetchedPages: pagesFetched,
      totalCollected: collected.length
    }
  };
}

async function fetchRangeCatalog(range, credentials) {
  const params = new URLSearchParams({
    sort_by: 'vote_average.desc',
    vote_average: `gte:${MIN_SCORE}`,
    'vote_average.gte': String(MIN_SCORE),
    'vote_count.gte': '100',
    include_adult: 'false',
    include_video: 'false',
    language: 'en-US',
    'primary_release_date.gte': range.startDate,
    'primary_release_date.lte': range.endDate
  });
  const seen = new Map();
  let pagesFetched = 0;
  let totalPages = Infinity;
  let totalResults = null;
  for (let page = 1; page <= MAX_DISCOVER_PAGES && page <= totalPages; page += 1) {
    params.set('page', String(page));
    const data = await tmdbRequest('discover/movie', params, credentials);
    const results = Array.isArray(data?.results) ? data.results : [];
    for (const item of results) {
      const movie = normalizeMovie(item);
      if (!movie || movie.id == null) continue;
      const key = String(movie.id);
      const existing = seen.get(key);
      if (!existing || (movie.ranking ?? 0) > (existing.ranking ?? 0)) {
        seen.set(key, movie);
      }
    }
    pagesFetched = page;
    const reportedTotalPages = Number(data?.total_pages);
    if (Number.isFinite(reportedTotalPages) && reportedTotalPages > 0) {
      totalPages = reportedTotalPages;
    }
    const reportedTotalResults = Number(data?.total_results);
    if (Number.isFinite(reportedTotalResults) && reportedTotalResults >= 0) {
      totalResults = reportedTotalResults;
    }
    if (!results.length && (!Number.isFinite(totalPages) || page >= totalPages)) {
      break;
    }
  }
  const movies = Array.from(seen.values()).sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0));
  return {
    range,
    movies,
    metadata: {
      source: 'tmdb',
      label: range.label,
      totalCollected: movies.length,
      fetchedPages: pagesFetched,
      totalPages: Number.isFinite(totalPages) ? totalPages : null,
      totalResults: Number.isFinite(totalResults) ? totalResults : null
    }
  };
}

const LOCAL_LEGACY_DATA_PATH = path.join(__dirname, 'data', 'legacy-movies.json');

async function loadLocalLegacyCatalog() {
  try {
    const contents = await fs.promises.readFile(LOCAL_LEGACY_DATA_PATH, 'utf8');
    const parsed = JSON.parse(contents);
    const rawMovies = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.movies) ? parsed.movies : [];
    const movies = rawMovies
      .map(item => ({
        id: item?.id ?? item?.movieID ?? item?.movieId ?? item?.imdbID ?? null,
        title: item?.title || '',
        score: Number(item?.score ?? item?.rating ?? 0),
        releaseDate: item?.releaseDate || item?.release_date || null,
        voteCount: Number(item?.voteCount ?? item?.vote_count ?? 0),
        popularity: Number(item?.popularity ?? 0)
      }))
      .filter(movie => movie.id && movie.title && Number.isFinite(movie.score));
    if (!movies.length) {
      return null;
    }
    return {
      movies,
      metadata: {
        source: 'local',
        totalCollected: movies.length,
        path: LOCAL_LEGACY_DATA_PATH
      }
    };
  } catch (err) {
    if (err && err.code !== 'ENOENT') {
      console.error('Failed to load local legacy movie dataset', err);
    }
    return null;
  }
}

async function fetchLegacyCatalog() {
  const local = await loadLocalLegacyCatalog();
  if (local) {
    return local;
  }

  const url =
    'https://raw.githubusercontent.com/FEND16/movie-json-data/master/json/top-rated-movies-01.json';
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Legacy dataset request failed: ${res.status}`);
    const data = await res.json();
    const movies = Array.isArray(data)
      ? data
          .map(item => {
            const ratings = Array.isArray(item?.ratings) ? item.ratings : [];
            const score = ratings.length
              ? ratings.reduce((sum, value) => sum + Number(value || 0), 0) / ratings.length
              : Number(item?.rating || 0);
            return {
              id: item?.id ?? item?.movieID ?? item?.movieId ?? item?.imdbID ?? null,
              title: item?.title || '',
              score,
              releaseDate: item?.releaseDate || null,
              voteCount: ratings.length,
              popularity: item?.popularity || 0
            };
          })
          .filter(Boolean)
      : [];
    if (!movies.length) {
      const fallback = await loadLocalLegacyCatalog();
      if (fallback) {
        return fallback;
      }
    }
    return {
      movies,
      metadata: { source: 'legacy', totalCollected: movies.length, url }
    };
  } catch (err) {
    console.error('Failed to load legacy movie dataset', err);
    const fallback = await loadLocalLegacyCatalog();
    if (fallback) {
      return fallback;
    }
    return { movies: [], metadata: { source: 'legacy', error: err?.message || 'failed' } };
  }
}

async function fetchCuratedCatalog(options = {}) {
  const isTestEnvironment =
    process.env.VITEST === 'true' ||
    process.env.NODE_ENV === 'test' ||
    process.env.JEST_WORKER_ID !== undefined;
  if (isTestEnvironment) {
    const localCatalog = await loadLocalLegacyCatalog();
    if (localCatalog) {
      return localCatalog;
    }
  }

  const credentials = getTmdbCredentials();
  if (!credentials) {
    return fetchLegacyCatalog();
  }

  const ranges = buildReleaseRanges();
  if (!ranges.length) {
    try {
      return await fetchCatalogFromTmdb(credentials);
    } catch (err) {
      console.error('TMDB catalog fetch failed', err);
      return fetchLegacyCatalog();
    }
  }

  const preparedRanges = [];
  const missingRanges = [];

  const bypassRangeCache = Boolean(options.bypassRangeCache);

  for (const range of ranges) {
    // eslint-disable-next-line no-await-in-loop
    const cached = bypassRangeCache ? null : await loadRangeFromCache(range);
    if (cached) {
      preparedRanges.push(cached);
    } else {
      missingRanges.push(range);
    }
  }

  let fetchedThisRun = 0;
  const fetchAllRanges = options.fetchAllRanges !== false;
  const fetchBudget = fetchAllRanges
    ? missingRanges.length
    : (preparedRanges.length ? MOVIE_RANGE_FETCH_LIMIT : missingRanges.length);
  const maxFetches = Math.min(missingRanges.length, fetchBudget);
  if (missingRanges.length && maxFetches > 0) {
    for (const range of missingRanges) {
      if (fetchedThisRun >= maxFetches) break;
      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await fetchRangeCatalog(range, credentials);
        if (Array.isArray(result.movies) && result.movies.length) {
          preparedRanges.push({
            range: result.range,
            movies: result.movies,
            metadata: { ...result.metadata, source: 'tmdb' }
          });
          fetchedThisRun += 1;
          // eslint-disable-next-line no-await-in-loop
          await cacheRangeMovies(range, result.movies, result.metadata);
          if (fetchedThisRun < maxFetches) {
            // eslint-disable-next-line no-await-in-loop
            await wait(MOVIE_RANGE_REQUEST_DELAY_MS);
          }
        }
      } catch (err) {
        console.error(`TMDB range fetch failed for ${range.label}`, err);
      }
    }
  }

  if (!preparedRanges.length) {
    try {
      const catalog = await fetchCatalogFromTmdb(credentials);
      if (Array.isArray(catalog.movies) && catalog.movies.length) {
        return catalog;
      }
    } catch (err) {
      console.error('TMDB catalog fetch fallback failed', err);
    }
    return fetchLegacyCatalog();
  }

  const aggregatedMovies = mergeRangeMovies(preparedRanges);
  const metadata = summarizeRangeMetadata(
    preparedRanges,
    aggregatedMovies,
    ranges.length,
    fetchedThisRun
  );

  return { movies: aggregatedMovies, metadata };
}

async function hydrateFromStorage() {
  const firestoreCatalog = await loadCatalogFromFirestore();
  if (firestoreCatalog && Array.isArray(firestoreCatalog.movies) && firestoreCatalog.movies.length) {
    applyState(firestoreCatalog.movies, {
      ...firestoreCatalog.metadata,
      updatedAt: firestoreCatalog.updatedAt,
      loadedFrom: 'firestore'
    });
    return state;
  }
  const rangeCatalog = await loadCatalogFromRangeCache();
  if (rangeCatalog && Array.isArray(rangeCatalog.movies) && rangeCatalog.movies.length) {
    applyState(rangeCatalog.movies, {
      ...rangeCatalog.metadata,
      updatedAt: rangeCatalog.metadata?.fetchedAt || new Date().toISOString(),
      loadedFrom: 'range-cache'
    });
    return state;
  }
  return state;
}

function shouldRefresh() {
  if (!state.movies.length) return true;
  if (!Number.isFinite(state.updatedAt) || !state.updatedAt) return true;
  return Date.now() - state.updatedAt > STALE_AFTER_MS;
}

async function refreshCatalog({ force = false, bypassRangeCache = false } = {}) {
  if (!force && !shouldRefresh()) {
    return state;
  }
  if (refreshPromise) {
    return refreshPromise;
  }
  refreshPromise = (async () => {
    const catalog = await fetchCuratedCatalog({ bypassRangeCache });
    applyState(catalog.movies, { ...catalog.metadata, updatedAt: new Date().toISOString() });
    await persistCatalog();
    return state;
  })()
    .catch(err => {
      console.error('Movie catalog refresh failed', err);
      return state;
    })
    .finally(() => {
      refreshPromise = null;
    });
  return refreshPromise;
}

function startRefreshTimer() {
  if (refreshTimer) return;
  refreshTimer = setInterval(() => {
    refreshCatalog().catch(err => {
      console.error('Scheduled movie catalog refresh failed', err);
    });
  }, REFRESH_INTERVAL_MS);
  if (typeof refreshTimer.unref === 'function') {
    refreshTimer.unref();
  }
}

async function init() {
  if (!hydratePromise) {
    hydratePromise = (async () => {
      await hydrateFromStorage();
      startRefreshTimer();
      return state;
    })();
  }
  return hydratePromise;
}

async function ensureCatalog(options = {}) {
  await init();
  const cacheOnly = Boolean(options.cacheOnly);
  if (!cacheOnly) {
    if (options.forceRefresh) {
      await refreshCatalog({
        force: true,
        bypassRangeCache: Boolean(options.bypassRangeCache)
      });
    } else if (shouldRefresh()) {
      if (options.allowStale && state.movies.length) {
        refreshCatalog().catch(err => {
          console.error('Movie catalog background refresh failed', err);
        });
      } else {
        await refreshCatalog();
      }
    }
  }
  return state;
}

function buildSearchMatches(query, { limit, minScore, excludeIds } = {}) {
  const normalizedQuery = String(query || '').trim().toLowerCase();
  const targetMinScore = Number.isFinite(minScore) ? Number(minScore) : MIN_SCORE;
  const matches = [];
  const isEmptyQuery = !normalizedQuery.length;
  const tokens = normalizedQuery.split(/\s+/).filter(Boolean);
  const excludeSet = excludeIds instanceof Set
    ? excludeIds
    : new Set(Array.isArray(excludeIds) ? excludeIds.map(id => String(id)) : []);
  for (const movie of state.movies) {
    if (excludeSet.size && movie?.id != null && excludeSet.has(String(movie.id))) {
      continue;
    }
    if (movie.score < targetMinScore) continue;
    if (isEmptyQuery) {
      matches.push({ movie, score: movie.ranking });
    } else {
      let matched = true;
      let earliestIndex = Infinity;
      for (const token of tokens) {
        const idx = movie.searchTitle.indexOf(token);
        if (idx === -1) {
          matched = false;
          break;
        }
        earliestIndex = Math.min(earliestIndex, idx);
      }
      if (!matched) continue;
      let score = movie.ranking;
      if (movie.searchTitle === normalizedQuery) {
        score += 5;
      } else if (earliestIndex === 0) {
        score += 1.5;
      } else if (Number.isFinite(earliestIndex)) {
        score += Math.max(0, 1 - earliestIndex / 50);
      }
      matches.push({ movie, score });
    }
  }
  matches.sort((a, b) => b.score - a.score);
  const limited = matches.slice(0, limit);
  return {
    totalMatches: matches.length,
    results: limited.map(entry => formatMovieForResponse(entry.movie, 'catalog'))
  };
}

function searchCatalog(query, options = {}) {
  if (!state.movies.length) return [];
  const limit = Math.max(1, Number(options.limit) || DEFAULT_LIMIT);
  const minScore = options.minScore;
  const excludeIds = options.excludeIds;
  return buildSearchMatches(query, { limit, minScore, excludeIds }).results;
}

function searchCatalogWithStats(query, options = {}) {
  if (!state.movies.length) {
    return { results: [], totalMatches: 0 };
  }
  const limit = Math.max(1, Number(options.limit) || DEFAULT_LIMIT);
  const minScore = options.minScore;
  const excludeIds = options.excludeIds;
  return buildSearchMatches(query, { limit, minScore, excludeIds });
}

function recentThresholdDate() {
  const thresholdMs = Date.now() - NEW_RELEASE_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
  return new Date(thresholdMs).toISOString().slice(0, 10);
}

function newReleaseCacheParts(query) {
  const normalized = String(query || '').trim().toLowerCase();
  return ['releases', NEW_RELEASE_CACHE_VERSION, normalized || 'default'];
}

async function loadCachedNewReleases(query) {
  const cached = await readCachedResponse(
    NEW_RELEASE_CACHE_COLLECTION,
    newReleaseCacheParts(query),
    NEW_RELEASE_CACHE_TTL_MS
  );
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    return null;
  }
  try {
    const parsed = JSON.parse(cached.body);
    const rawMovies = Array.isArray(parsed?.movies) ? parsed.movies : [];
    const movies = rawMovies
      .map(item => normalizeMovie(item, { allowLowScore: true }))
      .filter(Boolean)
      .sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0));
    if (!movies.length) return null;
    return {
      movies,
      metadata: {
        ...(parsed?.metadata && typeof parsed.metadata === 'object' ? parsed.metadata : {}),
        source: 'cache'
      }
    };
  } catch (err) {
    console.error('Failed to parse cached new release movies', err);
    return null;
  }
}

async function cacheNewReleases(query, movies, metadata = {}) {
  const sanitizedMovies = prepareMoviesForCache(movies).slice(0, NEW_RELEASE_CACHE_LIMIT);
  if (!sanitizedMovies.length) return;
  const fetchedAt = new Date().toISOString();
  const payload = {
    metadata: {
      source: 'tmdb',
      query: query || null,
      fetchedAt,
      total: sanitizedMovies.length,
      ...(metadata && typeof metadata === 'object' ? metadata : {})
    },
    movies: sanitizedMovies
  };
  const cacheMetadata = {
    fetchedAt,
    query: query || null,
    total: sanitizedMovies.length,
    version: NEW_RELEASE_CACHE_VERSION
  };
  try {
    await writeCachedResponse(NEW_RELEASE_CACHE_COLLECTION, newReleaseCacheParts(query), {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(payload),
      metadata: cacheMetadata
    });
  } catch (err) {
    console.error('Failed to cache new release movies', err);
  }
}

function filterFreshResults(results, { allowLowScore = true, excludeIds }) {
  const exclude = new Set((excludeIds || []).map(id => String(id)));
  const threshold = recentThresholdDate();
  const thresholdMs = parseDate(threshold);
  return (Array.isArray(results) ? results : [])
    .map(item => normalizeMovie(item, { allowLowScore }))
    .filter(movie => {
      if (!movie) return false;
      if (exclude.size && exclude.has(movie.id)) return false;
      if (state.byId.has(movie.id)) return false;
      const releaseMs = parseDate(movie.releaseDate);
      if (Number.isFinite(thresholdMs) && Number.isFinite(releaseMs)) {
        return releaseMs >= thresholdMs;
      }
      return true;
    });
}

async function fetchNewReleases({ query = '', limit = 10, excludeIds = [] } = {}) {
  const credentials = getTmdbCredentials();
  if (!credentials) return [];
  const sanitizedLimit = Math.max(1, Number(limit) || 10);
  const normalizedQuery = String(query || '').trim();
  const excludeList = Array.isArray(excludeIds) ? excludeIds.map(id => String(id)) : [];

  let prepared = null;
  try {
    prepared = await loadCachedNewReleases(normalizedQuery);
    if (prepared && (!Array.isArray(prepared.movies) || !prepared.movies.length)) {
      prepared = null;
    }
  } catch (err) {
    console.error('Failed to load cached new releases', err);
    prepared = null;
  }

  let normalizedResults = Array.isArray(prepared?.movies) ? prepared.movies : null;
  if (!normalizedResults) {
    const seen = new Map();
    const pushMovies = movies => {
      for (const movie of movies) {
        if (!movie || movie.id == null) continue;
        const key = String(movie.id);
        const existing = seen.get(key);
        if (!existing || (movie.ranking ?? 0) > (existing.ranking ?? 0)) {
          seen.set(key, movie);
        }
      }
    };

    let usedSearch = false;
    if (normalizedQuery) {
      try {
        const params = new URLSearchParams({
          query: normalizedQuery,
          include_adult: 'false',
          language: 'en-US',
          page: '1'
        });
        const data = await tmdbRequest('search/movie', params, credentials);
        const movies = filterFreshResults(data?.results, { excludeIds: excludeList });
        pushMovies(movies);
        usedSearch = true;
      } catch (err) {
        console.error('TMDB search for new releases failed', err);
      }
    }

    let usedDiscover = false;
    if (seen.size < NEW_RELEASE_CACHE_LIMIT) {
      try {
        const params = new URLSearchParams({
          sort_by: 'primary_release_date.desc',
          include_adult: 'false',
          include_video: 'false',
          language: 'en-US',
          page: '1',
          'primary_release_date.gte': recentThresholdDate(),
          with_release_type: '2|3'
        });
        const data = await tmdbRequest('discover/movie', params, credentials);
        const movies = filterFreshResults(data?.results, { excludeIds: excludeList });
        pushMovies(movies);
        usedDiscover = true;
      } catch (err) {
        console.error('TMDB new release fetch failed', err);
      }
    }

    normalizedResults = Array.from(seen.values()).sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0));

    if (normalizedResults.length) {
      try {
        await cacheNewReleases(normalizedQuery, normalizedResults, {
          searchUsed: usedSearch,
          discoverUsed: usedDiscover
        });
      } catch (err) {
        console.error('Failed to cache new release results', err);
      }
    }
  }

  if (!normalizedResults || !normalizedResults.length) {
    return [];
  }

  const filtered = filterFreshResults(normalizedResults, {
    allowLowScore: true,
    excludeIds: excludeList
  });
  const results = [];
  const seenIds = new Set();
  for (const movie of filtered) {
    if (!movie || movie.id == null) continue;
    const key = String(movie.id);
    if (seenIds.has(key)) continue;
    seenIds.add(key);
    results.push(formatMovieForResponse(movie, 'fresh'));
    if (results.length >= sanitizedLimit) break;
  }

  return results;
}

function stop() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

module.exports = {
  MIN_SCORE,
  init,
  stop,
  ensureCatalog,
  searchCatalog,
  searchCatalogWithStats,
  fetchNewReleases,
  hasTmdbCredentials
};
