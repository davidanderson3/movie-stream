const functions = require('firebase-functions');

const { readCachedResponse, writeCachedResponse } = require('../shared/cache');

const DEFAULT_REGION = 'us-central1';
const TMDB_BASE_URL = 'https://api.themoviedb.org';
const TMDB_CACHE_COLLECTION = 'tmdbCache';
const TMDB_CACHE_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours

function normalizeTmdbParams(params) {
  const normalized = [];
  if (!params) return normalized;
  for (const [key, value] of params.entries()) {
    if (key === 'api_key') continue;
    normalized.push([key, value]);
  }
  normalized.sort(([keyA, valueA], [keyB, valueB]) => {
    if (keyA === keyB) {
      return valueA.localeCompare(valueB);
    }
    return keyA.localeCompare(keyB);
  });
  return normalized.map(([key, value]) => ({ key, value }));
}

function tmdbCacheKeyParts(path, params) {
  const normalizedPath = String(path || '').trim();
  const normalizedParams = normalizeTmdbParams(params);
  const serializedParams = normalizedParams.map(entry => `${entry.key}=${entry.value}`).join('&');
  return {
    parts: ['tmdb', normalizedPath, serializedParams],
    normalizedPath,
    normalizedParams
  };
}

async function readTmdbCache(path, params) {
  const { parts } = tmdbCacheKeyParts(path, params);
  return readCachedResponse(TMDB_CACHE_COLLECTION, parts, TMDB_CACHE_TTL_MS);
}

async function writeTmdbCache(path, params, status, contentType, body, metadata = {}) {
  const { parts, normalizedPath, normalizedParams } = tmdbCacheKeyParts(path, params);
  await writeCachedResponse(TMDB_CACHE_COLLECTION, parts, {
    status,
    contentType,
    body,
    metadata: {
      path: normalizedPath,
      params: normalizedParams,
      ...metadata
    }
  });
}

const ALLOWED_ENDPOINTS = {
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

function getTmdbApiKey() {
  const fromEnv = process.env.TMDB_API_KEY;
  if (fromEnv) return fromEnv;
  const fromConfig = functions.config()?.tmdb?.key;
  if (fromConfig) return fromConfig;
  return null;
}

function getTmdbProxyEndpoint() {
  const fromEnv = process.env.TMDB_PROXY_ENDPOINT;
  if (fromEnv) return fromEnv;
  const fromConfig = functions.config()?.tmdb?.proxy_endpoint;
  if (fromConfig) return fromConfig;
  return '';
}

function withCors(res) {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set(
    'Access-Control-Allow-Headers',
    'Content-Type, Authorization, X-Api-Key'
  );
}

exports.tmdbProxy = functions
  .region(DEFAULT_REGION)
  .https.onRequest(async (req, res) => {
    withCors(res);

    if (req.method === 'OPTIONS') {
      res.status(204).send('');
      return;
    }

    if (req.method !== 'GET') {
      res.status(405).json({ error: 'method_not_allowed' });
      return;
    }

    const endpointKey = String(req.query.endpoint || 'discover');
    const endpointConfig = ALLOWED_ENDPOINTS[endpointKey];

    if (!endpointConfig) {
      res.status(400).json({ error: 'unsupported_endpoint' });
      return;
    }

    let targetPath = null;
    if (typeof endpointConfig === 'string') {
      targetPath = endpointConfig;
    } else if (endpointConfig && typeof endpointConfig.path === 'function') {
      targetPath = endpointConfig.path(req.query || {});
    } else if (endpointConfig && endpointConfig.path) {
      targetPath = endpointConfig.path;
    }

    if (!targetPath) {
      res.status(400).json({ error: 'invalid_endpoint_params' });
      return;
    }

    const apiKey = getTmdbApiKey();
    if (!apiKey) {
      console.error('TMDB API key missing for proxy request');
      res.status(500).json({ error: 'tmdb_key_not_configured' });
      return;
    }

    const params = new URLSearchParams();
    const omitParams = new Set(['endpoint', 'api_key']);
    if (endpointConfig && Array.isArray(endpointConfig.omitParams)) {
      endpointConfig.omitParams.forEach(param => omitParams.add(param));
    }

    for (const [key, value] of Object.entries(req.query || {})) {
      if (omitParams.has(key)) continue;
      if (Array.isArray(value)) {
        value.forEach(v => params.append(key, String(v)));
      } else if (value !== undefined) {
        params.append(key, String(value));
      }
    }
    params.set('api_key', apiKey);

    const targetUrl = `${TMDB_BASE_URL}${targetPath}?${params.toString()}`;

    const cached = await readTmdbCache(targetPath, params);
    if (cached) {
      res.status(cached.status);
      res.type(cached.contentType);
      res.send(cached.body);
      return;
    }

    try {
      const tmdbResponse = await fetch(targetUrl, {
        headers: {
          'Accept': 'application/json'
        }
      });

      const payload = await tmdbResponse.text();
      const contentType = tmdbResponse.headers.get('content-type') || 'application/json';
      if (tmdbResponse.ok) {
        await writeTmdbCache(targetPath, params, tmdbResponse.status, contentType, payload, {
          endpoint: endpointKey
        });
      }
      res.status(tmdbResponse.status);
      res.type(contentType);
      res.send(payload);
    } catch (err) {
      console.error('TMDB proxy failed', err);
      res.status(500).json({ error: 'tmdb_proxy_failed' });
    }
  });

exports.tmdbConfig = functions
  .region(DEFAULT_REGION)
  .https.onRequest((req, res) => {
    withCors(res);

    if (req.method === 'OPTIONS') {
      res.status(204).send('');
      return;
    }

    if (req.method !== 'GET') {
      res.status(405).json({ error: 'method_not_allowed' });
      return;
    }

    const apiKey = getTmdbApiKey();
    const proxyEndpoint = getTmdbProxyEndpoint();

    if (!apiKey && !proxyEndpoint) {
      res.status(404).json({ error: 'tmdb_config_unavailable' });
      return;
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
