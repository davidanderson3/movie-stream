const { getFirestore, serverTimestamp, getFirestoreStatus } = require('./firestore');

const MAX_IN_MEMORY_CACHE_ENTRIES = 500;
const inMemoryCache = new Map();
const REQUIRE_FIRESTORE = (() => {
  const raw = process.env.CACHE_REQUIRE_FIRESTORE ?? process.env.FIRESTORE_REQUIRED;
  if (raw === undefined || raw === null) return false;
  const normalized = String(raw).trim().toLowerCase();
  return ['1', 'true', 'yes', 'on'].includes(normalized);
})();

function serializePart(part) {
  if (part === null || part === undefined) return '';
  if (typeof part === 'string') return part;
  if (typeof part === 'number' || typeof part === 'boolean') return String(part);
  if (typeof URLSearchParams !== 'undefined' && part instanceof URLSearchParams) {
    return Array.from(part.entries())
      .map(([key, value]) => `${key}=${value}`)
      .join('&');
  }
  if (Array.isArray(part)) {
    return part.map(serializePart).join('|');
  }
  if (typeof part === 'object') {
    return Object.entries(part)
      .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
      .map(([key, value]) => `${key}:${serializePart(value)}`)
      .join(',');
  }
  return String(part);
}

function buildCacheId(parts) {
  const normalized = Array.isArray(parts) ? parts.map(serializePart) : [serializePart(parts)];
  const raw = normalized.join('||');
  return Buffer.from(raw, 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function rememberInMemory(docId, payload, fetchedAt = Date.now()) {
  if (REQUIRE_FIRESTORE) return;
  if (!docId || !payload || typeof payload.body !== 'string' || !payload.body.length) {
    return;
  }
  const normalizedPayload = {
    status: typeof payload.status === 'number' ? payload.status : 200,
    contentType:
      typeof payload.contentType === 'string' && payload.contentType
        ? payload.contentType
        : 'application/json',
    body: payload.body,
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : null,
    fetchedAt
  };
  inMemoryCache.set(docId, normalizedPayload);
  if (inMemoryCache.size > MAX_IN_MEMORY_CACHE_ENTRIES) {
    const firstKey = inMemoryCache.keys().next().value;
    if (firstKey !== undefined) {
      inMemoryCache.delete(firstKey);
    }
  }
}

function readInMemoryCache(docId, ttlMs) {
  if (REQUIRE_FIRESTORE) return null;
  if (!docId) return null;
  const entry = inMemoryCache.get(docId);
  if (!entry) return null;
  if (typeof ttlMs === 'number' && ttlMs > 0 && Date.now() - entry.fetchedAt > ttlMs) {
    inMemoryCache.delete(docId);
    return null;
  }
  return {
    status: entry.status,
    contentType: entry.contentType,
    body: entry.body,
    metadata: entry.metadata || null
  };
}

async function readCachedResponse(collection, parts, ttlMs) {
  const db = getFirestore();
  const docId = buildCacheId(parts);
  if (!db) {
    if (REQUIRE_FIRESTORE) {
      return null;
    }
    return readInMemoryCache(docId, ttlMs);
  }
  try {
    const snap = await db.collection(collection).doc(docId).get();
    if (!snap.exists) {
      if (REQUIRE_FIRESTORE) {
        return null;
      }
      return readInMemoryCache(docId, ttlMs);
    }
    const data = snap.data() || {};
    if (typeof data.body !== 'string' || !data.body) {
      if (REQUIRE_FIRESTORE) {
        return null;
      }
      return readInMemoryCache(docId, ttlMs);
    }
    const payload = {
      status: typeof data.status === 'number' ? data.status : 200,
      contentType:
        typeof data.contentType === 'string' && data.contentType
          ? data.contentType
          : 'application/json',
      body: data.body,
      metadata: data.metadata || null
    };
    const fetchedAt = data.fetchedAt;
    const fetchedMs =
      fetchedAt && typeof fetchedAt.toMillis === 'function' ? fetchedAt.toMillis() : null;
    if (!fetchedMs) {
      if (typeof ttlMs === 'number' && ttlMs > 0) {
        if (REQUIRE_FIRESTORE) {
          return null;
        }
        return readInMemoryCache(docId, ttlMs);
      }
      rememberInMemory(docId, payload);
      return payload;
    }
    if (typeof ttlMs === 'number' && ttlMs > 0 && Date.now() - fetchedMs > ttlMs) {
      inMemoryCache.delete(docId);
      return null;
    }
    rememberInMemory(docId, payload, fetchedMs);
    return payload;
  } catch (err) {
    console.error(`Failed to read cache entry ${collection}/${docId}`, err);
    if (REQUIRE_FIRESTORE) {
      return null;
    }
    return readInMemoryCache(docId, ttlMs);
  }
}

async function writeCachedResponse(collection, parts, payload = {}) {
  if (typeof payload.body !== 'string' || !payload.body.length) {
    return;
  }
  const docId = buildCacheId(parts);
  const normalizedParts = Array.isArray(parts)
    ? parts.map(serializePart)
    : [serializePart(parts)];
  const normalizedPayload = {
    status: typeof payload.status === 'number' ? payload.status : 200,
    contentType:
      typeof payload.contentType === 'string' && payload.contentType
        ? payload.contentType
        : 'application/json',
    body: payload.body,
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : null
  };
  rememberInMemory(docId, normalizedPayload);
  const db = getFirestore();
  if (!db) {
    if (REQUIRE_FIRESTORE) {
      console.error(
        `Cache write skipped for ${collection}/${docId}: Firestore is required but unavailable.`,
        getFirestoreStatus()
      );
    }
    return;
  }
  try {
    await db
      .collection(collection)
      .doc(docId)
      .set({
        keyParts: normalizedParts,
        status: normalizedPayload.status,
        contentType: normalizedPayload.contentType,
        body: normalizedPayload.body,
        metadata: normalizedPayload.metadata,
        fetchedAt: serverTimestamp()
      });
  } catch (err) {
    console.error(`Failed to write cache entry ${collection}/${docId}`, err);
  }
}

async function probeFirestoreWrite() {
  const db = getFirestore();
  if (!db) {
    return {
      ok: false,
      reason: 'firestore_unavailable'
    };
  }

  const probeTimeoutMs = Math.max(
    1000,
    Number(process.env.FIRESTORE_PROBE_TIMEOUT_MS) || 5000
  );
  const withTimeout = promise =>
    Promise.race([
      promise,
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error('firestore_probe_timeout')), probeTimeoutMs);
      })
    ]);

  const id = `cache-probe-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const ref = db.collection('_cacheHealth').doc(id);
  try {
    await withTimeout(ref.set({
      source: 'cache-probe',
      createdAt: serverTimestamp()
    }));
    await withTimeout(ref.delete());
    return { ok: true };
  } catch (err) {
    try {
      await withTimeout(ref.delete());
    } catch (_) {
      // Ignore cleanup failures for probe docs.
    }
    return {
      ok: false,
      reason: 'firestore_write_failed',
      error: String(err?.message || err)
    };
  }
}

function getCacheBackendStatus() {
  return {
    requireFirestore: REQUIRE_FIRESTORE,
    firestore: getFirestoreStatus()
  };
}

module.exports = {
  buildCacheId,
  readCachedResponse,
  writeCachedResponse,
  probeFirestoreWrite,
  serializePart,
  getCacheBackendStatus
};
