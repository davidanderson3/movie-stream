const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');

dotenv.config();

const backendEnvPath = path.resolve(__dirname, '.env');
if (fs.existsSync(backendEnvPath)) {
  dotenv.config({ path: backendEnvPath, override: false });
}

const express = require('express');
const { Configuration, PlaidApi, PlaidEnvironments } = require('plaid');
const cors = require('cors');
const {
  readCachedResponse,
  writeCachedResponse,
  getCacheBackendStatus,
  probeFirestoreWrite
} = require('../shared/cache');
const movieCatalog = require('./movie-catalog');
let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch {
  nodemailer = null;
}

const app = express();

movieCatalog
  .init()
  .catch(err => {
    console.error('Initial movie catalog load failed', err);
  });
const PORT = Number(process.env.PORT) || 3003;
const HOST = process.env.HOST || (process.env.VITEST ? '127.0.0.1' : '0.0.0.0');
const MOVIE_STATS_BUCKETS = [
  { label: '9-10', min: 9, max: Infinity },
  { label: '8-8.9', min: 8, max: 9 },
  { label: '7-7.9', min: 7, max: 8 },
  { label: '6-6.9', min: 6, max: 7 },
  { label: '< 6', min: -Infinity, max: 6 }
];
const SPOONACULAR_CACHE_COLLECTION = 'recipeCache';
const SPOONACULAR_CACHE_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours
const DEFAULT_MOVIE_LIMIT = 20;
const OMDB_BASE_URL = 'https://www.omdbapi.com/';
const OMDB_API_KEY =
  process.env.OMDB_API_KEY ||
  process.env.OMDB_KEY ||
  process.env.OMDB_TOKEN ||
  '';
const OMDB_CACHE_COLLECTION = 'omdbRatings';
const OMDB_CACHE_TTL_MS = 1000 * 60 * 60 * 12; // 12 hours
const OMDB_PREFETCH_STATE_COLLECTION = 'omdbRatingsPrefetch';
const OMDB_PREFETCH_STATE_KEY = ['state', 'v1'];
const OMDB_PREFETCH_DEFAULT_DELAY_MS = Math.max(
  600,
  Number(process.env.OMDB_PREFETCH_DELAY_MS) || 1500
);
const OMDB_PREFETCH_DEFAULT_JITTER_MS = Math.max(
  0,
  Number(process.env.OMDB_PREFETCH_JITTER_MS) || 350
);
const OMDB_PREFETCH_DEFAULT_CHECKPOINT_EVERY = Math.max(
  1,
  Number(process.env.OMDB_PREFETCH_CHECKPOINT_EVERY) || 10
);
const OMDB_PREFETCH_DEFAULT_MAX_FETCHES_PER_RUN = Math.max(
  0,
  Number(process.env.OMDB_PREFETCH_MAX_FETCHES_PER_RUN) || 0
);
const OMDB_PREFETCH_DEFAULT_RETRY_AFTER_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.OMDB_PREFETCH_RETRY_AFTER_MS) || 60 * 60 * 1000
);
const YOUTUBE_SEARCH_BASE_URL = 'https://www.googleapis.com/youtube/v3/search';
const YOUTUBE_API_KEY =
  process.env.YOUTUBE_API_KEY ||
  process.env.YOUTUBE_KEY ||
  process.env.GOOGLE_API_KEY ||
  '';
const YOUTUBE_SEARCH_CACHE_COLLECTION = 'youtubeSearchCache';
const YOUTUBE_SEARCH_CACHE_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours

const DEFAULT_REMOTE_API_BASE = 'https://narrow-down.web.app/api';
const DEFAULT_REMOTE_TMDB_PROXY_URL = `${DEFAULT_REMOTE_API_BASE}/tmdbProxy`;
const TMDB_BASE_URL = 'https://api.themoviedb.org';
const TMDB_IMAGE_BASE_URL = 'https://image.tmdb.org/t/p/';
const TMDB_IMAGE_CACHE_COLLECTION = 'tmdbImageCache';
const TMDB_IMAGE_CACHE_TTL_MS = Math.max(
  24 * 60 * 60 * 1000,
  Number(process.env.TMDB_IMAGE_CACHE_TTL_MS) || 30 * 24 * 60 * 60 * 1000
);
const TMDB_IMAGE_ALLOWED_SIZES = new Set([
  'w92',
  'w154',
  'w185',
  'w200',
  'w300',
  'w342',
  'w400',
  'w500',
  'w780',
  'original'
]);
const omdbPrefetchRuntime = {
  running: false,
  stopRequested: false,
  startedAt: null,
  lastFinishedAt: null,
  options: null,
  progress: null,
  lastError: null
};

function buildRatingPrecisionDistribution(movies, precision, limit = 5) {
  const normalizedPrecision = Number(precision);
  if (!Number.isFinite(normalizedPrecision) || normalizedPrecision <= 0) {
    return [];
  }

  const decimalPlaces = (() => {
    const str = String(normalizedPrecision);
    const dotIndex = str.indexOf('.');
    if (dotIndex === -1) return 0;
    return str.length - dotIndex - 1;
  })();

  const clamp = value => Math.min(10, Math.max(0, value));
  const bucketCounts = new Map();

  const safeFloor = value => {
    return Math.floor((value + normalizedPrecision * 1e-8) / normalizedPrecision);
  };

  (Array.isArray(movies) ? movies : []).forEach(movie => {
    const score = Number(movie?.score);
    if (!Number.isFinite(score)) return;
    const clamped = clamp(score);
    const bucketIndex = safeFloor(clamped);
    let bucketValue = bucketIndex * normalizedPrecision;
    if (bucketValue > 10) bucketValue = 10;
    const bucketKey = Number(bucketValue.toFixed(decimalPlaces));
    bucketCounts.set(bucketKey, (bucketCounts.get(bucketKey) || 0) + 1);
  });

  if (!bucketCounts.size) return [];

  const limited = Array.from(bucketCounts.entries())
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.value - a.value)
    .slice(0, Math.max(1, Number(limit) || 1));

  return limited.map(({ value, count }) => ({
    label: value.toFixed(decimalPlaces),
    count
  }));
}

async function safeReadCachedResponse(collection, keyParts, ttlMs) {
  try {
    return await readCachedResponse(collection, keyParts, ttlMs);
  } catch (err) {
    console.warn('Cache read failed', err?.message || err);
    return null;
  }
}

async function safeWriteCachedResponse(collection, keyParts, payload) {
  try {
    await writeCachedResponse(collection, keyParts, payload);
  } catch (err) {
    console.warn('Cache write failed', err?.message || err);
  }
}

function resolveTmdbApiKey() {
  return (
    process.env.TMDB_API_KEY ||
    process.env.TMDB_KEY ||
    process.env.TMDB_TOKEN ||
    ''
  );
}

function resolveSelfOrigin(req) {
  if (!req) return null;
  const host = req.get('host');
  if (!host) return null;
  const protocolHeader = req.headers['x-forwarded-proto'];
  const forwardedProtocol = Array.isArray(protocolHeader)
    ? protocolHeader[0]
    : protocolHeader;
  const protocol = req.protocol || (forwardedProtocol ? String(forwardedProtocol).split(',')[0].trim() : null) || 'http';
  return `${protocol}://${host}`;
}

function resolveTmdbProxyEndpoint(req) {
  const explicit = process.env.TMDB_PROXY_ENDPOINT;
  if (explicit) {
    return explicit;
  }
  const origin = resolveSelfOrigin(req);
  if (origin) {
    return `${origin.replace(/\/+$/, '')}/tmdbProxy`;
  }
  const base =
    (process.env.API_BASE_URL && process.env.API_BASE_URL.replace(/\/+$/, '')) || '';
  if (base) {
    return `${base}/tmdbProxy`;
  }
  return '';
}

function resolveTmdbProxyUpstreamUrl() {
  const explicit = process.env.TMDB_PROXY_UPSTREAM || process.env.TMDB_REMOTE_PROXY_URL;
  if (explicit) {
    return explicit;
  }
  const endpoint = process.env.TMDB_PROXY_ENDPOINT;
  if (endpoint && /^https?:\/\//i.test(endpoint)) {
    const lowered = endpoint.toLowerCase();
    if (
      !lowered.includes('localhost') &&
      !lowered.includes('127.0.0.1') &&
      !lowered.includes('::1')
    ) {
      return endpoint;
    }
  }
  return DEFAULT_REMOTE_TMDB_PROXY_URL;
}

const TMDB_ALLOWED_ENDPOINTS = {
  discover: { path: '/3/discover/movie' },
  genres: { path: '/3/genre/movie/list' },
  credits: {
    path: query => {
      const rawId = query?.movie_id ?? query?.id ?? query?.movieId;
      const value = Array.isArray(rawId) ? rawId[0] : rawId;
      if (!value && value !== 0) return null;
      const trimmed = String(value).trim();
      if (!trimmed) return null;
      return `/3/movie/${encodeURIComponent(trimmed)}/credits`;
    },
    omitParams: ['movie_id', 'movieId', 'id']
  },
  movie_details: {
    path: query => {
      const rawId = query?.movie_id ?? query?.id ?? query?.movieId;
      const value = Array.isArray(rawId) ? rawId[0] : rawId;
      if (!value && value !== 0) return null;
      const trimmed = String(value).trim();
      if (!trimmed) return null;
      return `/3/movie/${encodeURIComponent(trimmed)}`;
    },
    omitParams: ['movie_id', 'movieId', 'id']
  },
  person_details: {
    path: query => {
      const rawId = query?.person_id ?? query?.id;
      const value = Array.isArray(rawId) ? rawId[0] : rawId;
      if (!value && value !== 0) return null;
      const trimmed = String(value).trim();
      if (!trimmed) return null;
      return `/3/person/${encodeURIComponent(trimmed)}`;
    },
    omitParams: ['person_id', 'id']
  },
  search_multi: { path: '/3/search/multi' },
  search_movie: { path: '/3/search/movie' },
  trending_all: { path: '/3/trending/all/day' },
  trending_movies: { path: '/3/trending/movie/day' },
  popular_movies: { path: '/3/movie/popular' },
  upcoming_movies: { path: '/3/movie/upcoming' }
};

function buildTmdbPath(endpointKey, query) {
  const config = TMDB_ALLOWED_ENDPOINTS[endpointKey];
  if (!config) {
    const error = new Error('unsupported_endpoint');
    error.status = 400;
    throw error;
  }
  if (typeof config.path === 'function') {
    const resolved = config.path(query);
    if (!resolved) {
      const error = new Error('invalid_endpoint_params');
      error.status = 400;
      throw error;
    }
    return resolved;
  }
  return config.path;
}

function buildTmdbSearchParams(query, omit = []) {
  const params = new URLSearchParams();
  const omitSet = new Set(omit);
  Object.entries(query).forEach(([key, value]) => {
    if (omitSet.has(key)) return;
    if (value === undefined || value === null) return;
    if (Array.isArray(value)) {
      value.forEach(item => {
        if (item === undefined || item === null) return;
        params.append(key, String(item));
      });
      return;
    }
    params.append(key, String(value));
  });
  return params;
}

async function fetchTmdbDirect(endpointKey, query, apiKey) {
  const path = buildTmdbPath(endpointKey, query);
  const config = TMDB_ALLOWED_ENDPOINTS[endpointKey] || {};
  const params = buildTmdbSearchParams(query, config.omitParams || []);
  params.set('api_key', apiKey);
  const url = new URL(path, TMDB_BASE_URL);
  url.search = params.toString();
  const response = await fetch(url.toString(), {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'narrow-down-local-proxy'
    }
  });
  const text = await response.text();
  if (!response.ok) {
    const error = new Error(`TMDB request failed (${response.status})`);
    error.status = response.status;
    error.body = text;
    throw error;
  }
  return text ? JSON.parse(text) : {};
}

async function forwardTmdbProxy(endpointKey, query) {
  const upstream = resolveTmdbProxyUpstreamUrl();
  if (!upstream) {
    const error = new Error('tmdb_proxy_upstream_unavailable');
    error.status = 502;
    throw error;
  }
  const url = new URL(upstream);
  url.searchParams.set('endpoint', endpointKey);
  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    if (Array.isArray(value)) {
      value.forEach(item => {
        if (item === undefined || item === null) return;
        url.searchParams.append(key, String(item));
      });
      return;
    }
    url.searchParams.append(key, String(value));
  });
  const response = await fetch(url.toString(), {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'narrow-down-local-proxy'
    }
  });
  const body = await response.text();
  return {
    status: response.status,
    contentType: response.headers.get('content-type'),
    body
  };
}

async function requestTmdbData(endpointKey, query = {}) {
  const apiKey = resolveTmdbApiKey();
  if (apiKey) {
    try {
      return await fetchTmdbDirect(endpointKey, query, apiKey);
    } catch (err) {
      console.warn(`Direct TMDB request failed for ${endpointKey}`, err?.message || err);
    }
  }
  const forwarded = await forwardTmdbProxy(endpointKey, query);
  if (forwarded.status >= 400) {
    const error = new Error('tmdb_proxy_forward_failed');
    error.status = forwarded.status;
    error.body = forwarded.body;
    throw error;
  }
  if (!forwarded.body) {
    return {};
  }
  try {
    return JSON.parse(forwarded.body);
  } catch (err) {
    const parseError = new Error('invalid_tmdb_proxy_response');
    parseError.status = 502;
    throw parseError;
  }
}

// Enable CORS for all routes so the frontend can reach the API
app.use(cors());

const CONTACT_EMAIL = Buffer.from('ZHZkbmRyc25AZ21haWwuY29t', 'base64').toString('utf8');
const ADMIN_REFRESH_TOKEN =
  process.env.ADMIN_REFRESH_TOKEN || process.env.MOVIE_CACHE_REFRESH_TOKEN || '';
const mailer = (() => {
  if (!nodemailer || !process.env.SMTP_HOST || !process.env.SMTP_USER || !process.env.SMTP_PASS) return null;
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS
    }
  });
})();

app.use(express.json());

async function handleTmdbProxyRequest(req, res) {
  const endpointKey = String(req.query.endpoint || 'discover');
  const query = { ...req.query };
  delete query.endpoint;

  const apiKey = resolveTmdbApiKey();
  if (apiKey) {
    try {
      const data = await fetchTmdbDirect(endpointKey, query, apiKey);
      res.type('application/json').send(JSON.stringify(data));
      return;
    } catch (err) {
      console.warn('Direct TMDB request failed, attempting upstream proxy', err);
    }
  }

  try {
    const forwarded = await forwardTmdbProxy(endpointKey, query);
    res.status(forwarded.status);
    if (forwarded.contentType) {
      res.set('content-type', forwarded.contentType);
    } else {
      res.type('application/json');
    }
    if (forwarded.body) {
      res.send(forwarded.body);
    } else {
      res.send('');
    }
  } catch (err) {
    console.error('TMDB proxy request failed', err);
    const status =
      err && typeof err.status === 'number' && err.status >= 400 ? err.status : 502;
    res.status(status).json({
      error: 'tmdb_proxy_failed',
      message: (err && err.message) || 'TMDB proxy request failed'
    });
  }
}

app.get('/tmdbProxy', handleTmdbProxyRequest);
app.get('/api/tmdbProxy', handleTmdbProxyRequest);

app.get('/api/movie-image', async (req, res) => {
  const imagePath = normalizeTmdbImagePath(req.query.path);
  if (!imagePath) {
    res.status(400).json({ error: 'invalid_image_path' });
    return;
  }

  const size = normalizeTmdbImageSize(req.query.size);
  const cacheKey = ['tmdb-image', size, imagePath];
  const cached = await safeReadCachedResponse(
    TMDB_IMAGE_CACHE_COLLECTION,
    cacheKey,
    TMDB_IMAGE_CACHE_TTL_MS
  );
  if (cached && typeof cached.body === 'string' && cached.body) {
    const contentType =
      typeof cached.contentType === 'string' && cached.contentType
        ? cached.contentType
        : 'image/jpeg';
    try {
      const decoded = Buffer.from(cached.body, 'base64');
      if (decoded.length) {
        res.set('Cache-Control', 'public, max-age=604800, stale-while-revalidate=86400');
        res.type(contentType);
        res.send(decoded);
        return;
      }
    } catch (err) {
      console.warn('Failed to decode cached movie image', err?.message || err);
    }
  }

  const imageUrl = `${TMDB_IMAGE_BASE_URL}${size}${imagePath}`;
  try {
    const upstream = await fetch(imageUrl, {
      headers: {
        Accept: 'image/*',
        'User-Agent': 'narrow-down-local-proxy'
      }
    });
    if (!upstream.ok) {
      res.status(upstream.status).json({ error: 'tmdb_image_fetch_failed' });
      return;
    }

    const arrayBuffer = await upstream.arrayBuffer();
    const imageBuffer = Buffer.from(arrayBuffer);
    const contentType = upstream.headers.get('content-type') || 'image/jpeg';

    if (imageBuffer.length) {
      await safeWriteCachedResponse(TMDB_IMAGE_CACHE_COLLECTION, cacheKey, {
        status: 200,
        contentType,
        body: imageBuffer.toString('base64'),
        metadata: {
          encoding: 'base64',
          size,
          path: imagePath,
          source: imageUrl
        }
      });
    }

    res.set('Cache-Control', 'public, max-age=604800, stale-while-revalidate=86400');
    res.type(contentType);
    res.send(imageBuffer);
  } catch (err) {
    console.error('Failed to proxy movie image', err);
    res.status(502).json({ error: 'movie_image_proxy_failed' });
  }
});

app.get('/api/cache-status', async (req, res) => {
  const status = getCacheBackendStatus();
  let probe = null;
  if (parseBooleanQuery(req.query.probe)) {
    probe = await probeFirestoreWrite();
  }
  res.json({
    cache: status,
    probe,
    timestamp: new Date().toISOString()
  });
});

function sendCachedResponse(res, cached) {
  if (!cached || typeof cached.body !== 'string') return false;
  res.status(typeof cached.status === 'number' ? cached.status : 200);
  res.type(cached.contentType || 'application/json');
  res.send(cached.body);
  return true;
}

function parseBooleanQuery(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return false;
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return Boolean(normalized);
}

function parseNumberQuery(value) {
  if (value === undefined || value === null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeTmdbImagePath(value) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';
  const normalized = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
  if (normalized.includes('..')) return '';
  if (!/^\/[A-Za-z0-9/_.-]+$/.test(normalized)) return '';
  return normalized;
}

function normalizeTmdbImageSize(value) {
  const fallback = 'w200';
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  if (TMDB_IMAGE_ALLOWED_SIZES.has(trimmed)) return trimmed;
  return fallback;
}

function readAdminToken(req) {
  const authHeader = req.get('authorization') || '';
  if (authHeader.toLowerCase().startsWith('bearer ')) {
    return authHeader.slice(7).trim();
  }
  const headerToken = req.get('x-admin-token');
  if (headerToken) return String(headerToken).trim();
  const queryToken = req.query.token;
  return typeof queryToken === 'string' ? queryToken.trim() : '';
}

function normalizePositiveInteger(value, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  const clamped = Math.min(Math.max(parsed, min), max);
  return clamped;
}

function normalizeYouTubeQuery(value) {
  if (typeof value !== 'string') return '';
  return value.replace(/\s+/g, ' ').trim();
}

function normalizeYouTubeThumbnails(thumbnails) {
  if (!thumbnails || typeof thumbnails !== 'object') return undefined;
  const normalized = {};
  Object.entries(thumbnails).forEach(([key, value]) => {
    if (!value || typeof value !== 'object') return;
    const url = typeof value.url === 'string' ? value.url : null;
    if (!url) return;
    const width = Number.isFinite(value.width) ? Number(value.width) : null;
    const height = Number.isFinite(value.height) ? Number(value.height) : null;
    normalized[key] = {
      url,
      width: width === null ? undefined : width,
      height: height === null ? undefined : height
    };
  });
  return Object.keys(normalized).length ? normalized : undefined;
}

function youtubeSearchCacheKey(query) {
  const normalized = normalizeYouTubeQuery(query).toLowerCase();
  return ['youtubeSearch', normalized];
}

function parseOmdbPercent(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.endsWith('%') ? raw.slice(0, -1) : raw;
  const num = Number.parseFloat(normalized);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function extractYear(value) {
  if (typeof value !== 'string') return null;
  const match = value.trim().match(/^(\d{4})/);
  if (!match) return null;
  const year = Number(match[1]);
  return Number.isFinite(year) ? year : null;
}

function parseIdSet(raw) {
  const set = new Set();
  const addParts = value => {
    if (!value && value !== 0) return;
    String(value)
      .split(/[,|\s]+/)
      .map(part => part.trim())
      .filter(Boolean)
      .forEach(part => set.add(part));
  };
  if (Array.isArray(raw)) {
    raw.forEach(addParts);
  } else if (typeof raw === 'string') {
    addParts(raw);
  }
  return set;
}

function parseOmdbScore(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function parseOmdbImdbRating(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  const clamped = Math.max(0, Math.min(10, num));
  return Math.round(clamped * 10) / 10;
}

function sanitizeOmdbString(value) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  return trimmed;
}

function buildOmdbCacheKeyParts({ imdbId, title, year, type }) {
  const parts = ['omdb'];
  const normalizedType = typeof type === 'string' && type ? type.toLowerCase() : 'any';
  parts.push(`type:${normalizedType}`);
  if (imdbId) {
    parts.push(`imdb:${imdbId.toLowerCase()}`);
  } else if (title) {
    parts.push(`title:${title.toLowerCase()}`);
  } else {
    parts.push('title:');
  }
  if (year) {
    parts.push(`year:${year}`);
  } else {
    parts.push('year:');
  }
  return parts;
}

function normalizeOmdbPayload(data, { type, requestedTitle, requestedYear }) {
  if (!data || typeof data !== 'object') return null;
  const ratingsArray = Array.isArray(data.Ratings) ? data.Ratings : [];
  const ratingMap = new Map();
  ratingsArray.forEach(entry => {
    if (!entry || typeof entry.Source !== 'string') return;
    const key = entry.Source.trim().toLowerCase();
    if (!key) return;
    ratingMap.set(key, entry.Value);
  });

  const rottenTomatoes = parseOmdbPercent(
    ratingMap.get('rotten tomatoes') ?? ratingMap.get('rottentomatoes')
  );
  const metacritic = parseOmdbScore(data.Metascore ?? ratingMap.get('metacritic'));
  const imdb = parseOmdbImdbRating(
    data.imdbRating ?? ratingMap.get('internet movie database') ?? ratingMap.get('imdb')
  );

  const imdbId = sanitizeOmdbString(data.imdbID);
  const title = sanitizeOmdbString(data.Title) || sanitizeOmdbString(requestedTitle);
  const year = sanitizeOmdbString(data.Year) || sanitizeOmdbString(requestedYear);

  const payload = {
    source: 'omdb',
    ratings: {
      rottenTomatoes: rottenTomatoes ?? null,
      metacritic: metacritic ?? null,
      imdb: imdb ?? null
    },
    imdbId: imdbId || null,
    title: title || null,
    year: year || null,
    type: typeof type === 'string' && type ? type : null,
    fetchedAt: new Date().toISOString()
  };

  return payload;
}

function wait(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.resolve();
  }
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

function normalizePrefetchState(raw) {
  const base = {
    cursor: 0,
    completedPasses: 0,
    totalMovies: 0,
    processed: 0,
    fetched: 0,
    cacheHits: 0,
    notFound: 0,
    skipped: 0,
    failed: 0,
    rateLimited: 0,
    haltedReason: null,
    nextEligibleAt: null,
    updatedAt: null
  };
  if (!raw || typeof raw !== 'object') {
    return base;
  }
  const normalized = { ...base };
  Object.keys(base).forEach(key => {
    if (!(key in raw)) return;
    if (key === 'haltedReason' || key === 'nextEligibleAt' || key === 'updatedAt') {
      normalized[key] = raw[key] == null ? null : String(raw[key]);
      return;
    }
    const value = Number(raw[key]);
    normalized[key] = Number.isFinite(value) && value >= 0 ? Math.floor(value) : base[key];
  });
  return normalized;
}

async function loadOmdbPrefetchState() {
  const cached = await safeReadCachedResponse(
    OMDB_PREFETCH_STATE_COLLECTION,
    OMDB_PREFETCH_STATE_KEY,
    0
  );
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    return normalizePrefetchState(null);
  }
  try {
    return normalizePrefetchState(JSON.parse(cached.body));
  } catch (err) {
    console.warn('Failed to parse OMDb prefetch state cache', err?.message || err);
    return normalizePrefetchState(null);
  }
}

async function saveOmdbPrefetchState(state) {
  const normalized = normalizePrefetchState(state);
  await safeWriteCachedResponse(OMDB_PREFETCH_STATE_COLLECTION, OMDB_PREFETCH_STATE_KEY, {
    body: JSON.stringify(normalized),
    metadata: {
      updatedAt: normalized.updatedAt || new Date().toISOString()
    }
  });
}

function getOmdbLookupFromMovie(movie) {
  if (!movie || typeof movie !== 'object') return null;
  const imdbId = sanitizeOmdbString(movie.imdbId || movie.imdb_id || movie.imdbID || '');
  const title = sanitizeOmdbString(movie.title || movie.name || '');
  const yearFromDate = extractYear(
    sanitizeOmdbString(movie.releaseDate || movie.release_date || movie.first_air_date || '')
  );
  const year =
    sanitizeOmdbString(String(yearFromDate || '')) ||
    sanitizeOmdbString(String(movie.year || ''));
  if (!imdbId && !title) return null;
  return {
    imdbId,
    title,
    year,
    type: 'movie'
  };
}

async function lookupAndCacheOmdbRatings({
  imdbId,
  title,
  year,
  type = 'movie',
  forceRefresh = false,
  apiKey
}) {
  const effectiveApiKey = sanitizeOmdbString(apiKey || OMDB_API_KEY);
  if (!effectiveApiKey) {
    return {
      outcome: 'invalid_key',
      message: 'OMDb API key is missing.',
      madeNetworkRequest: false
    };
  }

  const cacheParts = buildOmdbCacheKeyParts({
    imdbId,
    title,
    year,
    type: type || 'any'
  });

  if (!forceRefresh) {
    const cached = await safeReadCachedResponse(
      OMDB_CACHE_COLLECTION,
      cacheParts,
      OMDB_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string' && cached.body.length) {
      return {
        outcome: 'cache_hit',
        cacheParts,
        madeNetworkRequest: false
      };
    }
  }

  const params = new URLSearchParams();
  params.set('apikey', effectiveApiKey);
  if (imdbId) {
    params.set('i', imdbId);
  } else if (title) {
    params.set('t', title);
  }
  if (year) params.set('y', year);
  if (type) params.set('type', type);
  params.set('plot', 'short');
  params.set('r', 'json');

  try {
    const response = await fetch(`${OMDB_BASE_URL}?${params.toString()}`);
    if (!response.ok) {
      return {
        outcome: 'request_failed',
        status: response.status || 502,
        message: `OMDb request failed with status ${response.status || 502}.`,
        madeNetworkRequest: true
      };
    }

    const data = await response.json();
    if (!data || data.Response === 'False') {
      const message = typeof data?.Error === 'string' ? data.Error : 'OMDb returned no results';
      const normalized = message.toLowerCase();
      if (normalized.includes('api key')) {
        return {
          outcome: 'invalid_key',
          message,
          madeNetworkRequest: true
        };
      }
      if (
        normalized.includes('limit') ||
        normalized.includes('too many') ||
        normalized.includes('request limit')
      ) {
        return {
          outcome: 'rate_limited',
          message,
          madeNetworkRequest: true
        };
      }
      return {
        outcome: 'not_found',
        message,
        madeNetworkRequest: true
      };
    }

    const payload = normalizeOmdbPayload(data, {
      type: type || null,
      requestedTitle: title,
      requestedYear: year
    });

    if (!payload) {
      return {
        outcome: 'not_found',
        message: 'OMDb did not return critic scores for this title.',
        madeNetworkRequest: true
      };
    }

    await safeWriteCachedResponse(OMDB_CACHE_COLLECTION, cacheParts, {
      body: JSON.stringify(payload),
      metadata: {
        imdbId: payload.imdbId || imdbId || null,
        title: payload.title || title || null,
        year: payload.year || year || null,
        type: payload.type || type || null
      }
    });

    return {
      outcome: 'fetched',
      payload,
      madeNetworkRequest: true
    };
  } catch (err) {
    return {
      outcome: 'request_failed',
      message: String(err?.message || err),
      madeNetworkRequest: true
    };
  }
}

async function getOmdbPrefetchStatus() {
  const persisted = await loadOmdbPrefetchState();
  return {
    persisted,
    runtime: {
      running: Boolean(omdbPrefetchRuntime.running),
      stopRequested: Boolean(omdbPrefetchRuntime.stopRequested),
      startedAt: omdbPrefetchRuntime.startedAt,
      lastFinishedAt: omdbPrefetchRuntime.lastFinishedAt,
      options: omdbPrefetchRuntime.options,
      progress: omdbPrefetchRuntime.progress,
      lastError: omdbPrefetchRuntime.lastError
    }
  };
}

function scheduleOmdbPrefetch(options = {}) {
  if (omdbPrefetchRuntime.running) {
    return false;
  }

  omdbPrefetchRuntime.running = true;
  omdbPrefetchRuntime.stopRequested = false;
  omdbPrefetchRuntime.startedAt = new Date().toISOString();
  omdbPrefetchRuntime.options = { ...options };
  omdbPrefetchRuntime.progress = null;
  omdbPrefetchRuntime.lastError = null;

  (async () => {
    const persisted = await loadOmdbPrefetchState();
    const catalogState = await movieCatalog.ensureCatalog({
      allowStale: true,
      cacheOnly: true
    });
    const movies = Array.isArray(catalogState?.movies) ? catalogState.movies : [];
    const totalMovies = movies.length;
    const restarted = Boolean(options.restart);
    let cursor = restarted
      ? 0
      : Math.max(0, Math.min(Number(persisted.cursor) || 0, totalMovies));
    let completedPasses = Number(persisted.completedPasses) || 0;

    const counters = {
      processed: 0,
      fetched: 0,
      cacheHits: 0,
      notFound: 0,
      skipped: 0,
      failed: 0,
      rateLimited: 0,
      networkRequests: 0
    };

    const checkpointEvery = Math.max(
      1,
      Number(options.checkpointEvery) || OMDB_PREFETCH_DEFAULT_CHECKPOINT_EVERY
    );
    const maxFetches = Math.max(
      0,
      Number(options.maxFetches) || OMDB_PREFETCH_DEFAULT_MAX_FETCHES_PER_RUN
    );
    const delayMs = Math.max(
      600,
      Number(options.delayMs) || OMDB_PREFETCH_DEFAULT_DELAY_MS
    );
    const jitterMs = Math.max(
      0,
      Number(options.jitterMs) || OMDB_PREFETCH_DEFAULT_JITTER_MS
    );
    const retryAfterMs = Math.max(
      5 * 60 * 1000,
      Number(options.retryAfterMs) || OMDB_PREFETCH_DEFAULT_RETRY_AFTER_MS
    );
    const forceRefresh = Boolean(options.forceRefresh);

    let haltedReason = null;
    let checkpointCounter = 0;

    const persistProgress = async () => {
      const nextState = {
        cursor,
        completedPasses,
        totalMovies,
        processed: counters.processed,
        fetched: counters.fetched,
        cacheHits: counters.cacheHits,
        notFound: counters.notFound,
        skipped: counters.skipped,
        failed: counters.failed,
        rateLimited: counters.rateLimited,
        haltedReason,
        nextEligibleAt:
          haltedReason === 'rate_limited'
            ? new Date(Date.now() + retryAfterMs).toISOString()
            : null,
        updatedAt: new Date().toISOString()
      };
      omdbPrefetchRuntime.progress = {
        ...nextState,
        networkRequests: counters.networkRequests
      };
      await saveOmdbPrefetchState(nextState);
    };

    if (!OMDB_API_KEY) {
      haltedReason = 'missing_omdb_key';
      await persistProgress();
      return;
    }

    if (!totalMovies) {
      haltedReason = 'empty_catalog';
      await persistProgress();
      return;
    }

    while (cursor < totalMovies) {
      if (omdbPrefetchRuntime.stopRequested) {
        haltedReason = 'stop_requested';
        break;
      }
      if (maxFetches > 0 && counters.networkRequests >= maxFetches) {
        haltedReason = 'max_fetches_reached';
        break;
      }

      const movie = movies[cursor];
      cursor += 1;
      counters.processed += 1;
      checkpointCounter += 1;

      const lookup = getOmdbLookupFromMovie(movie);
      if (!lookup) {
        counters.skipped += 1;
      } else {
        const result = await lookupAndCacheOmdbRatings({
          ...lookup,
          apiKey: OMDB_API_KEY,
          forceRefresh
        });
        if (result.madeNetworkRequest) {
          counters.networkRequests += 1;
        }
        if (result.outcome === 'cache_hit') {
          counters.cacheHits += 1;
        } else if (result.outcome === 'fetched') {
          counters.fetched += 1;
        } else if (result.outcome === 'not_found') {
          counters.notFound += 1;
        } else if (result.outcome === 'rate_limited') {
          counters.rateLimited += 1;
          haltedReason = 'rate_limited';
        } else if (result.outcome === 'invalid_key') {
          haltedReason = 'invalid_omdb_key';
        } else {
          counters.failed += 1;
        }

        if (result.madeNetworkRequest && !haltedReason) {
          const jitter = jitterMs > 0 ? Math.floor(Math.random() * (jitterMs + 1)) : 0;
          await wait(delayMs + jitter);
        }
      }

      if (haltedReason) {
        break;
      }

      if (checkpointCounter >= checkpointEvery) {
        checkpointCounter = 0;
        await persistProgress();
      }
    }

    if (!haltedReason && cursor >= totalMovies) {
      haltedReason = 'completed_pass';
      completedPasses += 1;
      cursor = 0;
    }

    await persistProgress();
  })()
    .catch(err => {
      omdbPrefetchRuntime.lastError = String(err?.message || err);
      console.error('OMDb ratings prefetch failed', err);
    })
    .finally(() => {
      omdbPrefetchRuntime.running = false;
      omdbPrefetchRuntime.stopRequested = false;
      omdbPrefetchRuntime.lastFinishedAt = new Date().toISOString();
    });

  return true;
}

const plaidClient = (() => {
  const clientID = process.env.PLAID_CLIENT_ID;
  const secret = process.env.PLAID_SECRET;
  const env = process.env.PLAID_ENV || 'sandbox';
  if (!clientID || !secret) return null;
  const config = new Configuration({
    basePath: PlaidEnvironments[env],
    baseOptions: {
      headers: {
        'PLAID-CLIENT-ID': clientID,
        'PLAID-SECRET': secret
      }
    }
  });
  return new PlaidApi(config);
})();

// Serve static files (like index.html, style.css, script.js)
// Allow API routes to continue past the static middleware
// when no matching asset is found. Express 5 changes the default `fallthrough`
// behavior, so we explicitly enable it to avoid returning a 404 before our API
// handlers get a chance to run.
app.use(
  express.static(path.resolve(__dirname, '../'), {
    fallthrough: true
  })
);

app.post('/contact', async (req, res) => {
  const { name, from, message } = req.body || {};
  if (!from || !message) {
    return res.status(400).json({ error: 'invalid' });
  }
  if (!mailer) {
    return res.status(500).json({ error: 'mail disabled' });
  }
  try {
    await mailer.sendMail({
      to: CONTACT_EMAIL,
      from: process.env.SMTP_USER,
      replyTo: from,
      subject: `Dashboard contact from ${name || 'Anonymous'}`,
      text: message
    });
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Contact email failed', err);
    res.status(500).json({ error: 'failed' });
  }
});

// --- Description persistence ---
const descFile = path.join(__dirname, 'descriptions.json');

function readDescriptions() {
  try {
    const text = fs.readFileSync(descFile, 'utf8');
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function writeDescriptions(data) {
  fs.writeFileSync(descFile, JSON.stringify(data, null, 2));
}

app.get('/api/descriptions', (req, res) => {
  res.json(readDescriptions());
});

app.post('/api/description', (req, res) => {
  const { panelId, position, text } = req.body || {};
  if (!panelId || !['top', 'bottom'].includes(position) || typeof text !== 'string') {
    return res.status(400).json({ error: 'invalid' });
  }
  const data = readDescriptions();
  data[panelId] = data[panelId] || {};
  data[panelId][position] = text;
  writeDescriptions(data);
  res.json({ status: 'ok' });
});

// --- Saved movies persistence ---
const savedFile = path.join(__dirname, 'saved-movies.json');

function readSavedMovies() {
  try {
    const txt = fs.readFileSync(savedFile, 'utf8');
    return JSON.parse(txt);
  } catch {
    return [];
  }
}

function writeSavedMovies(data) {
  fs.writeFileSync(savedFile, JSON.stringify(data, null, 2));
}

app.get('/api/saved-movies', (req, res) => {
  res.json(readSavedMovies());
});

app.post('/api/saved-movies', (req, res) => {
  const movie = req.body || {};
  if (!movie || !movie.id) {
    return res.status(400).json({ error: 'invalid' });
  }
  const data = readSavedMovies();
  if (!data.some(m => String(m.id) === String(movie.id))) {
    data.push(movie);
    writeSavedMovies(data);
  }
  res.json({ status: 'ok' });
});

// --- Spotify client ID ---
app.get('/api/spotify-client-id', (req, res) => {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  if (!clientId) {
    return res.status(500).json({ error: 'missing' });
  }
  res.json({ clientId });
});

app.get('/api/tmdb-config', (req, res) => {
  const apiKey = resolveTmdbApiKey();
  const proxyEndpoint = resolveTmdbProxyEndpoint(req);

  if (!apiKey && !proxyEndpoint) {
    return res.status(404).json({ error: 'tmdb_config_unavailable' });
  }

  const payload = {
    hasKey: Boolean(apiKey),
    hasProxy: Boolean(proxyEndpoint)
  };

  if (apiKey) {
    payload.apiKey = apiKey;
  }
  if (proxyEndpoint) {
    payload.proxyEndpoint = proxyEndpoint;
  }

  res.json(payload);
});

app.get('/api/youtube/search', async (req, res) => {
  const rawQuery =
    req.query.q ?? req.query.query ?? req.query.term ?? req.query.artist ?? req.query.name ?? '';
  const query = normalizeYouTubeQuery(rawQuery);

  if (!query) {
    return res.status(400).json({ error: 'missing_query' });
  }

  if (!YOUTUBE_API_KEY) {
    return res.status(501).json({ error: 'youtube_api_key_missing' });
  }

  const cacheKey = youtubeSearchCacheKey(query);
  const cached = await safeReadCachedResponse(
    YOUTUBE_SEARCH_CACHE_COLLECTION,
    cacheKey,
    YOUTUBE_SEARCH_CACHE_TTL_MS
  );
  if (sendCachedResponse(res, cached)) {
    return;
  }

  const params = new URLSearchParams({
    key: YOUTUBE_API_KEY,
    part: 'snippet',
    type: 'video',
    maxResults: '1',
    videoEmbeddable: 'true',
    videoSyndicated: 'true',
    safeSearch: 'moderate',
    q: query
  });

  const url = `${YOUTUBE_SEARCH_BASE_URL}?${params.toString()}`;

  let response;
  let text;

  try {
    response = await fetch(url);
    text = await response.text();
  } catch (err) {
    console.error('YouTube search request failed', { query, err });
    return res.status(502).json({ error: 'youtube_search_failed' });
  }

  if (!response.ok) {
    console.error(
      'YouTube search responded with error',
      response.status,
      text ? text.slice(0, 200) : ''
    );
    return res.status(response.status).json({ error: 'youtube_search_error' });
  }

  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (err) {
    console.error('Failed to parse YouTube search response as JSON', err);
    return res.status(502).json({ error: 'youtube_response_invalid' });
  }

  const items = Array.isArray(data?.items) ? data.items : [];
  const bestItem = items.find(item => item?.id?.videoId);

  const snippet = bestItem?.snippet && typeof bestItem.snippet === 'object' ? bestItem.snippet : {};
  const videoId = typeof bestItem?.id?.videoId === 'string' ? bestItem.id.videoId.trim() : '';

  const payload = {
    query,
    video: videoId
      ? {
          id: videoId,
          title: typeof snippet.title === 'string' ? snippet.title : '',
          description: typeof snippet.description === 'string' ? snippet.description : '',
          channel: {
            id: typeof snippet.channelId === 'string' ? snippet.channelId : '',
            title: typeof snippet.channelTitle === 'string' ? snippet.channelTitle : ''
          },
          publishedAt: typeof snippet.publishedAt === 'string' ? snippet.publishedAt : '',
          thumbnails: normalizeYouTubeThumbnails(snippet.thumbnails),
          url: `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`,
          embedUrl: `https://www.youtube.com/embed/${encodeURIComponent(videoId)}`
        }
      : null
  };

  const body = JSON.stringify(payload);

  await safeWriteCachedResponse(YOUTUBE_SEARCH_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body,
    metadata: { query, fetchedAt: new Date().toISOString() }
  });

  res.set('Cache-Control', 'public, max-age=1800');
  res.type('application/json').send(body);
});

// --- GeoLayers game endpoints ---
const layerOrder = ['rivers','lakes','elevation','roads','outline','cities','label'];
const countriesPath = path.join(__dirname, '../geolayers-game/public/countries.json');
let countryData = [];
try {
  countryData = JSON.parse(fs.readFileSync(countriesPath, 'utf8'));
} catch {
  countryData = [];
}
const locations = countryData.map(c => c.code);
const leaderboard = [];
const countryNames = Object.fromEntries(countryData.map(c => [c.code, c.name]));

async function fetchCitiesForCountry(iso3) {
  const endpoint = 'https://query.wikidata.org/sparql';
  const query = `
SELECT ?city ?cityLabel ?population ?coord WHERE {
  ?country wdt:P298 "${iso3}".
  ?city (wdt:P31/wdt:P279*) wd:Q515;
        wdt:P17 ?country;
        wdt:P625 ?coord.
  OPTIONAL { ?city wdt:P1082 ?population. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY DESC(?population)
LIMIT 10`;
  const url = endpoint + '?format=json&query=' + encodeURIComponent(query);
  const res = await fetch(url, {
    headers: {
      'Accept': 'application/sparql-results+json',
      'User-Agent': 'dashboard-app/1.0'
    }
  });
  if (!res.ok) throw new Error('SPARQL query failed');
  const data = await res.json();
  const features = data.results.bindings
    .map(b => {
      const m = /Point\(([-\d\.eE]+)\s+([-\d\.eE]+)\)/.exec(b.coord.value);
      if (!m) return null;
      const lon = Number(m[1]);
      const lat = Number(m[2]);
      if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
      return {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [lon, lat] },
        properties: {
          name: b.cityLabel?.value || '',
          population: b.population ? Number(b.population.value) : null
        }
      };
    })
    .filter(Boolean);
  return { type: 'FeatureCollection', features };
}

async function ensureCitiesForCountry(code) {
  const dir = path.join(__dirname, '../geolayers-game/public/data', code);
  const file = path.join(dir, 'cities.geojson');
  if (!fs.existsSync(file)) {
    const geo = await fetchCitiesForCountry(code);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(file, JSON.stringify(geo));
    console.log('Fetched cities for', code);
  }
  return file;
}

async function ensureAllCities() {
  for (const code of locations) {
    try {
      await ensureCitiesForCountry(code);
    } catch (err) {
      console.error('Failed to fetch cities for', code, err);
    }
  }
}

function dailySeed() {
  const today = new Date().toISOString().slice(0,10);
  let seed = 0;
  for (const c of today) {
    seed = (seed * 31 + c.charCodeAt(0)) >>> 0;
  }
  return seed;
}

function pickLocation() {
  const seed = dailySeed();
  return locations[seed % locations.length];
}

app.get('/daily', (req, res) => {
  const loc = pickLocation();
  res.json({
    locationId: loc,
    layers: layerOrder.map(l => `/layer/${loc}/${l}`)
  });
});

app.get('/countries', (req, res) => {
  const list = Object.entries(countryNames).map(([code, name]) => ({ code, name }));
  res.json(list);
});

app.get('/layer/:loc/:name', async (req, res) => {
  const { loc, name } = req.params;
  const file = path.join(__dirname, '../geolayers-game/public/data', loc, `${name}.geojson`);
  if (name === 'cities' && !fs.existsSync(file)) {
    try {
      await ensureCitiesForCountry(loc);
    } catch (err) {
      console.error('ensureCitiesForCountry failed', err);
    }
  }
  fs.readFile(file, 'utf8', (err, data) => {
    if (err) return res.status(404).send('Layer not found');
    res.type('application/json').send(data);
  });
});

app.post('/score', (req, res) => {
  const { playerName, score } = req.body || {};
  if (typeof playerName === 'string' && typeof score === 'number') {
    leaderboard.push({ playerName, score });
    leaderboard.sort((a, b) => b.score - a.score);
    res.json({ status: 'ok' });
  } else {
    res.status(400).json({ error: 'invalid' });
  }
});

app.get('/leaderboard', (req, res) => {
  res.json(leaderboard.slice(0, 10));
});

app.get('/api/movies', async (req, res) => {
  try {
    const query = typeof req.query.q === 'string' ? req.query.q : '';
    const limit = parseNumberQuery(req.query.limit) ?? DEFAULT_MOVIE_LIMIT;
    const freshLimit = parseNumberQuery(req.query.freshLimit);
    const minScore = parseNumberQuery(req.query.minScore);
    const excludeRaw = req.query.excludeIds;
    const excludeSet = new Set();

    const addExclusions = value => {
      if (!value) return;
      const parts = String(value)
        .split(/[,|\s]+/)
        .map(part => part.trim())
        .filter(Boolean);
      parts.forEach(part => excludeSet.add(part));
    };

    if (Array.isArray(excludeRaw)) {
      excludeRaw.forEach(addExclusions);
    } else if (typeof excludeRaw === 'string') {
      addExclusions(excludeRaw);
    }
    const cacheOnly = parseBooleanQuery(
      req.query.cacheOnly ?? req.query.cache ?? req.query.cache_only
    );
    const includeFresh = cacheOnly
      ? false
      : parseBooleanQuery(req.query.includeFresh ?? req.query.fresh ?? req.query.includeNew);
    const freshOnly = cacheOnly
      ? false
      : parseBooleanQuery(req.query.freshOnly ?? req.query.onlyFresh ?? req.query.newOnly) ||
        (typeof req.query.scope === 'string' && req.query.scope.toLowerCase() === 'new');
    const forceRefresh = parseBooleanQuery(req.query.refresh);

    const curatedLimit = Math.max(1, Number(limit) || 20);
    const fallbackFreshLimit = Math.max(1, Math.min(curatedLimit, 10));
    const effectiveFreshLimit = Math.max(1, Number(freshLimit) || fallbackFreshLimit);

    const catalogState = await movieCatalog.ensureCatalog({
      forceRefresh: cacheOnly ? false : forceRefresh,
      allowStale: true,
      cacheOnly
    });
    const hasCredentials = movieCatalog.hasTmdbCredentials();
    const curatedSearch = movieCatalog.searchCatalogWithStats(query, {
      limit: curatedLimit,
      minScore: minScore == null ? undefined : minScore,
      excludeIds: excludeSet
    });
    const curatedResults = freshOnly ? [] : curatedSearch.results;
    const curatedTotalMatches = Math.max(
      0,
      Number.isFinite(curatedSearch?.totalMatches)
        ? Number(curatedSearch.totalMatches)
        : Array.isArray(curatedSearch?.results)
        ? curatedSearch.results.length
        : 0
    );
    const curatedReturnedCount = freshOnly
      ? 0
      : Array.isArray(curatedResults)
      ? curatedResults.length
      : 0;

    let freshResults = [];
    let freshError = null;
    const shouldFetchFresh =
      !cacheOnly &&
      (freshOnly ||
        includeFresh ||
        (!curatedResults.length && Boolean(query)));

    if (shouldFetchFresh) {
      if (hasCredentials) {
        try {
          const freshExcludeIds = curatedResults.map(movie => movie.id);
          if (excludeSet.size) {
            freshExcludeIds.push(...excludeSet);
          }
          freshResults = await movieCatalog.fetchNewReleases({
            query,
            limit: freshOnly ? curatedLimit : effectiveFreshLimit,
            excludeIds: freshExcludeIds
          });
        } catch (err) {
          console.error('Failed to fetch new release movies', err);
          freshError = 'failed';
        }
      } else {
        freshError = 'credentials missing';
      }
    }

    const response = {
      results: freshOnly ? freshResults : curatedResults,
      curated: curatedResults,
      fresh: freshResults,
      metadata: {
        query: query || null,
        curatedCount: curatedTotalMatches,
        curatedReturnedCount,
        freshCount: freshResults.length,
        totalCatalogSize:
          catalogState?.metadata?.total ?? catalogState?.movies?.length ?? 0,
        catalogUpdatedAt:
          catalogState?.metadata?.updatedAt ||
          (catalogState?.updatedAt
            ? new Date(catalogState.updatedAt).toISOString()
            : null),
        minScore: minScore == null ? movieCatalog.MIN_SCORE : minScore,
        includeFresh: Boolean(shouldFetchFresh && hasCredentials),
        freshOnly: Boolean(freshOnly),
        cacheOnly: Boolean(cacheOnly),
        curatedLimit,
        source: catalogState?.metadata?.source || null,
        freshRequested: Boolean(shouldFetchFresh)
      }
    };

    if (freshOnly) {
      response.curated = curatedResults;
      response.metadata.curatedCount = curatedResults.length;
    }

    if (freshError) {
      response.metadata.freshError = freshError;
    }

    res.json(response);
  } catch (err) {
    console.error('Failed to fetch movies', err);
    res.status(500).json({ error: 'Failed to fetch movies' });
  }
});

app.post('/api/admin/refresh-movie-cache', async (req, res) => {
  if (!ADMIN_REFRESH_TOKEN) {
    return res.status(503).json({ error: 'admin_refresh_unconfigured' });
  }
  const token = readAdminToken(req);
  if (!token || token !== ADMIN_REFRESH_TOKEN) {
    return res.status(401).json({ error: 'admin_refresh_unauthorized' });
  }
  const startedAt = Date.now();
  try {
    const catalogState = await movieCatalog.ensureCatalog({
      forceRefresh: true,
      allowStale: false,
      cacheOnly: false,
      bypassRangeCache: true
    });
    const total = Array.isArray(catalogState?.movies) ? catalogState.movies.length : 0;
    res.json({
      ok: true,
      refreshedAt: new Date().toISOString(),
      durationMs: Date.now() - startedAt,
      catalogTotal: total,
      catalogUpdatedAt:
        catalogState?.metadata?.updatedAt ||
        (catalogState?.updatedAt
          ? new Date(catalogState.updatedAt).toISOString()
          : null),
      source: catalogState?.metadata?.source || null
    });
  } catch (err) {
    console.error('Admin movie cache refresh failed', err);
    res.status(500).json({ error: 'admin_refresh_failed' });
  }
});

app.get('/api/admin/prefetch-movie-ratings', async (req, res) => {
  if (!ADMIN_REFRESH_TOKEN) {
    return res.status(503).json({ error: 'admin_refresh_unconfigured' });
  }
  const token = readAdminToken(req);
  if (!token || token !== ADMIN_REFRESH_TOKEN) {
    return res.status(401).json({ error: 'admin_refresh_unauthorized' });
  }

  const status = await getOmdbPrefetchStatus();
  res.json({
    ok: true,
    ...status
  });
});

app.post('/api/admin/prefetch-movie-ratings', async (req, res) => {
  if (!ADMIN_REFRESH_TOKEN) {
    return res.status(503).json({ error: 'admin_refresh_unconfigured' });
  }
  const token = readAdminToken(req);
  if (!token || token !== ADMIN_REFRESH_TOKEN) {
    return res.status(401).json({ error: 'admin_refresh_unauthorized' });
  }

  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const stop = parseBooleanQuery(payload.stop ?? req.query.stop);
  if (stop) {
    if (omdbPrefetchRuntime.running) {
      omdbPrefetchRuntime.stopRequested = true;
    }
    const status = await getOmdbPrefetchStatus();
    return res.json({
      ok: true,
      started: false,
      stopping: Boolean(omdbPrefetchRuntime.running),
      ...status
    });
  }

  if (omdbPrefetchRuntime.running) {
    const status = await getOmdbPrefetchStatus();
    return res.status(409).json({
      error: 'omdb_prefetch_in_progress',
      ...status
    });
  }

  const parsedJitter = parseNumberQuery(payload.jitterMs ?? req.query.jitterMs);
  const parsedMaxFetches = parseNumberQuery(payload.maxFetches ?? req.query.maxFetches);

  const options = {
    restart: parseBooleanQuery(payload.restart ?? req.query.restart),
    forceRefresh: parseBooleanQuery(payload.forceRefresh ?? req.query.forceRefresh),
    delayMs:
      normalizePositiveInteger(payload.delayMs ?? req.query.delayMs, { min: 600, max: 60000 }) ||
      OMDB_PREFETCH_DEFAULT_DELAY_MS,
    jitterMs:
      parsedJitter == null
        ? OMDB_PREFETCH_DEFAULT_JITTER_MS
        : Math.max(0, Math.min(10000, Math.round(parsedJitter))),
    checkpointEvery:
      normalizePositiveInteger(
        payload.checkpointEvery ?? req.query.checkpointEvery,
        { min: 1, max: 500 }
      ) || OMDB_PREFETCH_DEFAULT_CHECKPOINT_EVERY,
    maxFetches:
      parsedMaxFetches == null
        ? OMDB_PREFETCH_DEFAULT_MAX_FETCHES_PER_RUN
        : Math.max(0, Math.min(100000, Math.round(parsedMaxFetches))),
    retryAfterMs:
      normalizePositiveInteger(payload.retryAfterMs ?? req.query.retryAfterMs, {
        min: 5 * 60 * 1000,
        max: 24 * 60 * 60 * 1000
      }) || OMDB_PREFETCH_DEFAULT_RETRY_AFTER_MS
  };

  const started = scheduleOmdbPrefetch(options);
  const status = await getOmdbPrefetchStatus();
  return res.status(started ? 202 : 409).json({
    ok: started,
    started,
    ...status
  });
});

app.get('/api/movies/stats', async (req, res) => {
  try {
    const cacheOnly = parseBooleanQuery(
      req.query.cacheOnly ?? req.query.cache ?? req.query.cache_only
    );
    const catalogState = await movieCatalog.ensureCatalog({
      allowStale: true,
      cacheOnly
    });
    const movies = Array.isArray(catalogState?.movies) ? catalogState.movies : [];
    const excludeRaw = req.query.excludeIds;
    const excludeSet = new Set();

    const addExclusions = value => {
      if (!value) return;
      const parts = String(value)
        .split(/[,|\s]+/)
        .map(part => part.trim())
        .filter(Boolean);
      parts.forEach(part => excludeSet.add(part));
    };

    if (Array.isArray(excludeRaw)) {
      excludeRaw.forEach(addExclusions);
    } else if (typeof excludeRaw === 'string') {
      addExclusions(excludeRaw);
    }

    const bucketStats = MOVIE_STATS_BUCKETS.map(bucket => ({
      label: bucket.label,
      min: bucket.min,
      max: bucket.max,
      count: 0
    }));

    let total = 0;
    movies.forEach(movie => {
      if (!movie || movie.id == null) return;
      const id = String(movie.id);
      if (excludeSet.has(id)) return;
      total += 1;
      const score = Number(movie.score);
      if (!Number.isFinite(score)) return;
      for (const bucket of bucketStats) {
        const meetsMin = bucket.min === -Infinity ? true : score >= bucket.min;
        const belowMax = bucket.max === Infinity ? true : score < bucket.max;
        if (meetsMin && belowMax) {
          bucket.count += 1;
          break;
        }
      }
    });

    const ratingPrecision =
      parseNumberQuery(req.query.ratingPrecision ?? req.query.precision) ?? null;
    const ratingTop =
      parseNumberQuery(req.query.ratingTop ?? req.query.top) ?? 5;

    const ratingDistribution =
      ratingPrecision && ratingTop
        ? buildRatingPrecisionDistribution(movies, ratingPrecision, ratingTop)
        : [];

    res.json({
      total,
      catalogTotal: movies.length,
      catalogUpdatedAt:
        catalogState?.metadata?.updatedAt ||
        (catalogState?.updatedAt
          ? new Date(catalogState.updatedAt).toISOString()
          : null),
      buckets: bucketStats.map(({ label, count }) => ({ label, count })),
      ratingDistribution
    });
  } catch (err) {
    console.error('Failed to compute movie stats', err);
    res.status(500).json({ error: 'failed_to_compute_movie_stats' });
  }
});

app.get('/api/movie-ratings', async (req, res) => {
  const imdbId = sanitizeOmdbString(req.query.imdbId || req.query.imdbID);
  const title = sanitizeOmdbString(req.query.title);
  const year = sanitizeOmdbString(req.query.year);
  const typeParam = sanitizeOmdbString(req.query.type).toLowerCase();
  const allowedTypes = new Set(['movie', 'series', 'episode']);
  const type = allowedTypes.has(typeParam) ? typeParam : '';
  const forceRefresh = parseBooleanQuery(req.query.refresh);
  const queryApiKey = sanitizeOmdbString(req.query.apiKey);
  const apiKey = queryApiKey || OMDB_API_KEY;

  if (!apiKey) {
    return res.status(400).json({
      error: 'omdb_key_missing',
      message: 'OMDb API key is not configured on the server.'
    });
  }

  if (!imdbId && !title) {
    return res.status(400).json({
      error: 'missing_lookup',
      message: 'Provide an imdbId or title to look up critic scores.'
    });
  }

  const cacheParts = buildOmdbCacheKeyParts({
    imdbId,
    title,
    year,
    type: type || 'any'
  });

  if (!forceRefresh) {
    const cached = await safeReadCachedResponse(
      OMDB_CACHE_COLLECTION,
      cacheParts,
      OMDB_CACHE_TTL_MS
    );
    if (sendCachedResponse(res, cached)) {
      return;
    }
  }

  const params = new URLSearchParams();
  params.set('apikey', apiKey);
  if (imdbId) {
    params.set('i', imdbId);
  } else if (title) {
    params.set('t', title);
  }
  if (year) params.set('y', year);
  if (type) params.set('type', type);
  params.set('plot', 'short');
  params.set('r', 'json');

  try {
    const response = await fetch(`${OMDB_BASE_URL}?${params.toString()}`);
    if (!response.ok) {
      const status = response.status || 502;
      return res.status(status).json({
        error: 'omdb_request_failed',
        message: `OMDb request failed with status ${status}`
      });
    }

    const data = await response.json();
    if (!data || data.Response === 'False') {
      const message = typeof data?.Error === 'string' ? data.Error : 'OMDb returned no results';
      const normalized = message.toLowerCase();
      if (normalized.includes('api key')) {
        return res.status(401).json({ error: 'omdb_invalid_key', message });
      }
      return res.status(404).json({ error: 'omdb_not_found', message });
    }

    const payload = normalizeOmdbPayload(data, {
      type: type || null,
      requestedTitle: title,
      requestedYear: year
    });

    if (!payload) {
      return res.status(404).json({
        error: 'omdb_not_found',
        message: 'OMDb did not return critic scores for this title.'
      });
    }

    const body = JSON.stringify(payload);
    await safeWriteCachedResponse(OMDB_CACHE_COLLECTION, cacheParts, {
      body,
      metadata: {
        imdbId: payload.imdbId || imdbId || null,
        title: payload.title || title || null,
        year: payload.year || year || null,
        type: payload.type || type || null
      }
    });

    res.json(payload);
  } catch (err) {
    console.error('Failed to fetch critic scores from OMDb', err);
    res.status(500).json({
      error: 'omdb_request_failed',
      message: 'Failed to fetch critic scores.'
    });
  }
});

app.get('/api/transactions', async (req, res) => {
  if (!plaidClient || !process.env.PLAID_ACCESS_TOKEN) {
    res.status(500).json({ error: 'Plaid not configured' });
    return;
  }
  try {
    const start = new Date();
    start.setMonth(start.getMonth() - 1);
    const end = new Date();
    const response = await plaidClient.transactionsGet({
      access_token: process.env.PLAID_ACCESS_TOKEN,
      start_date: start.toISOString().slice(0, 10),
      end_date: end.toISOString().slice(0, 10)
    });
    res.json(response.data);
  } catch (err) {
    console.error('Plaid error', err);
    res.status(500).json({ error: 'Failed to fetch transactions' });
  }
});

if (require.main === module) {
  let server = null;
  server = app
    .listen(PORT, HOST, () => {
      console.log(
        `✅ Serving static files at http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${PORT}`
      );
    })
    .on('error', err => {
      console.error('Failed to start server', err);
      process.exit(1);
    });
  module.exports = server;
  module.exports.app = app;
} else {
  module.exports = app;
}
