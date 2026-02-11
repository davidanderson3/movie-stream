if (!('FIRESTORE_ENABLE_TRACING' in process.env)) {
  process.env.FIRESTORE_ENABLE_TRACING = 'false';
}

const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');

let firestore = null;
let firestoreInitFailed = false;
let firestoreInitError = null;
let firestoreInitMode = 'uninitialized';

const isTestEnvironment =
  process.env.VITEST === 'true' ||
  process.env.NODE_ENV === 'test' ||
  process.env.JEST_WORKER_ID !== undefined;

function parseBooleanEnv(name, fallback = false) {
  const value = process.env[name];
  if (value === undefined || value === null) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function resolveProjectId() {
  const direct =
    process.env.GOOGLE_CLOUD_PROJECT ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    process.env.FIREBASE_PROJECT_ID ||
    '';
  if (direct) return String(direct).trim();

  const firebaseConfig = process.env.FIREBASE_CONFIG;
  if (!firebaseConfig) return '';
  try {
    const parsed = firebaseConfig.trim().startsWith('{')
      ? JSON.parse(firebaseConfig)
      : null;
    const projectId = parsed && parsed.projectId ? String(parsed.projectId).trim() : '';
    return projectId;
  } catch (_) {
    return '';
  }
}

function normalizeServiceAccount(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const clientEmail = raw.client_email ? String(raw.client_email).trim() : '';
  const privateKeyRaw = raw.private_key ? String(raw.private_key) : '';
  if (!clientEmail || !privateKeyRaw) return null;
  return {
    ...raw,
    client_email: clientEmail,
    private_key: privateKeyRaw.replace(/\\n/g, '\n')
  };
}

function parseServiceAccountJsonFromEnv() {
  const directRaw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || process.env.SERVICE_ACCOUNT_JSON;
  if (directRaw && String(directRaw).trim()) {
    try {
      return normalizeServiceAccount(JSON.parse(String(directRaw)));
    } catch (err) {
      console.warn('Invalid FIREBASE_SERVICE_ACCOUNT_JSON', err?.message || err);
    }
  }

  const base64Raw =
    process.env.FIREBASE_SERVICE_ACCOUNT_BASE64 || process.env.SERVICE_ACCOUNT_BASE64;
  if (base64Raw && String(base64Raw).trim()) {
    try {
      const decoded = Buffer.from(String(base64Raw), 'base64').toString('utf8');
      return normalizeServiceAccount(JSON.parse(decoded));
    } catch (err) {
      console.warn('Invalid FIREBASE_SERVICE_ACCOUNT_BASE64', err?.message || err);
    }
  }

  return null;
}

function readServiceAccountFromFile() {
  const configuredPath =
    process.env.FIREBASE_SERVICE_ACCOUNT_PATH || process.env.GOOGLE_APPLICATION_CREDENTIALS || '';
  const candidates = [];
  if (configuredPath) {
    candidates.push(configuredPath);
  }
  candidates.push(path.resolve(process.cwd(), 'serviceAccountKey.json'));

  for (const rawPath of candidates) {
    const absolute = path.isAbsolute(rawPath)
      ? rawPath
      : path.resolve(process.cwd(), rawPath);
    if (!fs.existsSync(absolute)) continue;
    try {
      const contents = fs.readFileSync(absolute, 'utf8');
      const parsed = JSON.parse(contents);
      const normalized = normalizeServiceAccount(parsed);
      if (normalized) {
        return normalized;
      }
      console.warn(`Service account file missing required fields: ${absolute}`);
    } catch (err) {
      console.warn(`Failed to read service account file: ${absolute}`, err?.message || err);
    }
  }

  return null;
}

function buildFirebaseAppOptions() {
  const projectId = resolveProjectId();
  const fromEnv = parseServiceAccountJsonFromEnv();
  const fromFile = fromEnv ? null : readServiceAccountFromFile();
  const serviceAccount = fromEnv || fromFile;

  const options = {};
  if (projectId) {
    options.projectId = projectId;
  }

  if (serviceAccount) {
    options.credential = admin.credential.cert(serviceAccount);
    return {
      options,
      mode: fromEnv ? 'service-account-env' : 'service-account-file'
    };
  }

  // Fall back to ADC when no explicit service account was provided.
  options.credential = admin.credential.applicationDefault();
  return {
    options,
    mode: 'application-default'
  };
}

function getFirestore() {
  if (firestore || firestoreInitFailed) {
    return firestore;
  }

  const enableInTests = parseBooleanEnv('FIRESTORE_ENABLE_IN_TESTS', false);
  if (isTestEnvironment && !enableInTests) {
    firestoreInitFailed = true;
    firestoreInitMode = 'disabled-test';
    return null;
  }

  try {
    if (!admin.apps.length) {
      const initConfig = buildFirebaseAppOptions();
      admin.initializeApp(initConfig.options);
      firestoreInitMode = initConfig.mode;
    } else if (firestoreInitMode === 'uninitialized') {
      firestoreInitMode = 'reuse-existing-app';
    }

    firestore = admin.firestore();
  } catch (err) {
    firestoreInitFailed = true;
    firestoreInitError = err;
    firestore = null;
    console.error('Failed to initialize Firestore', err);
  }
  return firestore;
}

function getFirestoreStatus() {
  const db = getFirestore();
  const app = admin.apps.length ? admin.app() : null;
  const appProjectId =
    app && app.options && app.options.projectId
      ? String(app.options.projectId)
      : '';
  const resolvedProjectId = resolveProjectId() || appProjectId || null;
  return {
    available: Boolean(db),
    initFailed: firestoreInitFailed,
    mode: firestoreInitMode,
    projectId: resolvedProjectId,
    error: firestoreInitError ? String(firestoreInitError.message || firestoreInitError) : null
  };
}

function serverTimestamp() {
  return admin.firestore.FieldValue.serverTimestamp();
}

module.exports = {
  getFirestore,
  getFirestoreStatus,
  serverTimestamp,
  firestoreAdmin: admin
};
