import { getCurrentUser, auth, db, awaitAuthUser } from './auth.js';
import {
  getDecisionsCache,
  setDecisionsCache,
  clearDecisionsCache,
  getGoalOrderCache,
  setGoalOrderCache,
  clearGoalOrderCache
} from './cache.js';

export { clearDecisionsCache, clearGoalOrderCache } from './cache.js';





const DECISIONS_ENABLED = true;



export function dedupeById(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  return list.filter(it => {
    if (!it?.id || seen.has(it.id)) return false;
    seen.add(it.id);
    return true;
  });
}

function dedupeByTextAndType(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  return list.filter(it => {
    const text = it?.text?.trim().toLowerCase();
    const type = it?.type;
    const parent = it?.parentGoalId || '';
    const key = text && type ? `${type}|${text}|${parent}` : null;
    if (!key) return true;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function dedupeDecisions(list) {
  return dedupeById(list);
}


// Cache decisions and goal order in memory only
let saveTimer = null;
let pendingDecisions = null;

// Clear any lingering pending edits when a user logs in so they never sync to
// the authenticated account. Anonymous edits are stored separately and should
// not be merged after authentication.
auth.onAuthStateChanged(user => {
  if (user) pendingDecisions = null;
});

function notifyDecisionsUpdated() {
  if (typeof window !== 'undefined' && typeof window.dispatchEvent === 'function') {
    window.dispatchEvent(new Event('decisionsUpdated'));
  }
}

function scheduleSave(user, items) {
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(async () => {
    try {
      let authUser = getCurrentUser();
      if (!authUser) authUser = await awaitAuthUser();
      if (!authUser) {
        console.warn('🛑 Cannot save decisions: no authenticated user');
        return;
      }
      let toSave = items;
      if (containsDemoItems(toSave)) {
        toSave = stripDemoItems(toSave);
      }
      if (!toSave.length || isSampleDataset(toSave)) return;
      await db
        .collection('decisions')
        .doc(authUser.uid)
        .set({ items: toSave }, { merge: true });
    } catch (err) {
      if (err?.code === 'permission-denied') {
        console.error('Save rejected by security rules:', err);
      } else {
        console.error('Failed to save decisions:', err);
      }
      alert('⚠️ Failed to save changes.');
    }
  }, 200);
}

export function generateId() {
  return '_' + Math.random().toString(36).substr(2, 9);
}

export function getSampleSessionId() {
  if (typeof localStorage === 'undefined') {
    return generateId();
  }
  const key = 'sampleSessionId';
  let id = localStorage.getItem(key);
  if (!id) {
    id = generateId();
    localStorage.setItem(key, id);
  }
  return id;
}

export async function loadDecisions(forceRefresh = false) {
  if (!DECISIONS_ENABLED) {
    const cached = getDecisionsCache();
    if (cached && !forceRefresh) return cached;
    const empty = [];
    setDecisionsCache(empty);
    notifyDecisionsUpdated();
    return empty;
  }
  await awaitAuthUser();
  const currentUser = getCurrentUser();

  const cached = getDecisionsCache();
  if (cached && !forceRefresh) {
    return cached;
  }

  if (!currentUser) {
    console.warn('🚫 No current user — no sample data to return');
    return [];
  }

  const snap = await db.collection('decisions').doc(currentUser.uid).get();
  const data = snap.data();
  const rawItems = data && Array.isArray(data.items) ? data.items : [];

  // detect duplicate IDs before we dedupe so we can log them
  const seenIds = new Set();
  const dupes = [];
  for (const it of rawItems) {
    const id = it?.id;
    if (id) {
      if (seenIds.has(id)) dupes.push(it);
      else seenIds.add(id);
    }
  }

  let items = rawItems.map(it => {
    if (it && it.hiddenUntil && typeof it.hiddenUntil.toDate === 'function') {
      return { ...it, hiddenUntil: it.hiddenUntil.toDate().toISOString() };
    }
    return it;
  });

  // Remove any duplicate decisions by id
  items = dedupeById(items);

  if (dupes.length) {
    console.warn(`🗑️ Removing ${dupes.length} duplicate decisions from Firestore`, dupes);
    try {
      await db.collection('decisions').doc(currentUser.uid).set({ items }, { merge: true });
    } catch (err) {
      console.error('Failed to purge duplicates', err);
    }
  }



  setDecisionsCache(items);
  notifyDecisionsUpdated();
  return getDecisionsCache();
}

export async function removeDuplicateDecisionsFromDb() {
  if (!DECISIONS_ENABLED) return [];
  await awaitAuthUser();
  const currentUser = getCurrentUser();
  if (!currentUser) {
    console.warn('🛑 Cannot remove duplicates: no authenticated user');
    return [];
  }
  const docRef = db.collection('decisions').doc(currentUser.uid);
  const snap = await docRef.get();
  const data = snap.data();
  const items = data && Array.isArray(data.items) ? data.items : [];
  const seenKeys = new Set();
  const dupes = [];
  for (const it of items) {
    const text = it?.text?.trim().toLowerCase();
    const type = it?.type;
    const parent = it?.parentGoalId || '';
    const key = text && type ? `${type}|${text}|${parent}` : null;
    if (key) {
      if (seenKeys.has(key)) dupes.push(it);
      else seenKeys.add(key);
    }
  }
  const deduped = dedupeByTextAndType(dedupeById(items));
  if (deduped.length !== items.length) {
    console.warn(`🗑️ Removed ${dupes.length} duplicate decisions`, dupes);
    await docRef.set({ items: deduped }, { merge: true });
    setDecisionsCache(deduped);
    notifyDecisionsUpdated();
  }
  return deduped;
}

export async function saveDecisions(items, { skipNotify = false } = {}) {
  if (!DECISIONS_ENABLED) return;
  if (!Array.isArray(items)) return;
  // Remove duplicate IDs before caching/saving
  items = dedupeDecisions(items);
  // ensure at least one valid decision exists
  if (!items.some(i => i.id && i.text)) {
    console.warn('⚠️ Refusing to save empty or invalid decisions');
    return;
  }


  setDecisionsCache(items);
  if (!skipNotify) notifyDecisionsUpdated();
  const user = getCurrentUser();
  if (!user) {
    console.warn('🛑 Cannot save decisions: no authenticated user');
    alert('⚠️ Please sign in to save your changes.');
    return;
  }
  scheduleSave(user, items);
}

export async function loadGoalOrder(forceRefresh = false) {
  const currentUser = getCurrentUser();
  if (!currentUser) {
    return [];
  }

  const cached = getGoalOrderCache();
  if (cached && !forceRefresh) {
    return cached;
  }

  const snap = await db.collection('decisions').doc(currentUser.uid).get();
  const order = Array.isArray(snap.data()?.goalOrder)
    ? snap.data().goalOrder
    : [];
  setGoalOrderCache(order);
  return order;
}

export async function flushPendingDecisions() {
  if (!saveTimer) return;
  let user = getCurrentUser();
  if (!user) user = await awaitAuthUser();
  if (!user) {
    console.warn('🛑 Cannot flush decisions: no authenticated user');
    return;
  }
  let items = getDecisionsCache();

  clearTimeout(saveTimer);
  saveTimer = null;
  try {
    await db
      .collection('decisions')
      .doc(user.uid)
      .set({ items }, { merge: true });
  } catch (err) {
    if (err?.code === 'permission-denied') {
      console.error('Save rejected by security rules:', err);
    } else {
      console.error('Failed to save decisions:', err);
    }
    alert('⚠️ Failed to save changes.');
  }
}

export async function restoreBackup(selectFn) {
  const backups = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith('backup-')) backups.push(key);
  }
  if (backups.length === 0) {
    alert('No backups found');
    return null;
  }
  const choose = selectFn || ((keys) => {
    const msg = `Choose backup:\n${keys.join('\n')}`;
    return window.prompt(msg);
  });
  const chosen = choose(backups);
  if (!chosen || !backups.includes(chosen)) return null;
  let items;
  try {
    items = JSON.parse(localStorage.getItem(chosen));
  } catch {
    alert('Invalid backup data');
    return null;
  }
  if (!Array.isArray(items) || items.length === 0) {
    alert('⚠️ Refusing to restore empty backup.');
    return null;
  }

  const dateStr = chosen.replace('backup-', '');
  const sizeKb = (JSON.stringify(items).length / 1024).toFixed(1);
  const msg = `Restore backup from ${dateStr} containing ${items.length} item${
    items.length === 1 ? '' : 's'
  } (~${sizeKb} KB)?`;
  const confirmFn = typeof globalThis.confirm === 'function' ? globalThis.confirm : () => true;
  if (!confirmFn(msg)) return null;
  let currentUser = getCurrentUser();
  if (!currentUser) {
    currentUser = await awaitAuthUser();
    if (!currentUser) {
      alert('⚠️ Please sign in to restore backup.');
      return null;
    }
  }
  try {
    const docRef = db.collection('decisions').doc(currentUser.uid);
    const snap = await docRef.get();
    const prevItems = (snap.data && typeof snap.data === 'function' ? snap.data() : snap.data)?.items || [];
    await db
      .collection('restoreBackups')
      .doc(currentUser.uid)
      .set({ items: prevItems });
    await docRef.set({ items }, { merge: true });
    setDecisionsCache(items);
    return chosen;
  } catch (err) {
    if (err?.code === 'permission-denied') {
      console.error('Restore rejected by security rules:', err);
    } else {
      console.error('Failed to restore backup:', err);
    }
    alert('⚠️ Failed to restore backup.');
    return null;
  }
}

export async function saveGoalOrder(order) {
  const currentUser = getCurrentUser();
  if (!currentUser || !Array.isArray(order) || order.length === 0) {
    console.warn('⚠️ Refusing to save empty goalOrder');
    return;
  }

  try {
    await db
      .collection('decisions')
      .doc(currentUser.uid)
      .set({ goalOrder: order }, { merge: true });
    setGoalOrderCache(order);
  } catch (err) {
    if (err?.code === 'permission-denied') {
      console.error('Save goal order rejected by security rules:', err);
    } else {
      console.error('Failed to save goal order:', err);
    }
    alert('⚠️ Failed to save changes.');
  }
}

export function parseNaturalDate(input) {
  const today = new Date();
  const weekdays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  const norm = input.trim().toLowerCase();
  if (norm === 'today') {
    return today.toISOString().split('T')[0];
  }
  const dow = weekdays.find(d => norm.startsWith(d));
  if (dow) {
    const target = weekdays.indexOf(dow);
    let delta = target - today.getDay();
    if (delta <= 0) delta += 7;
    const next = new Date(today);
    next.setDate(today.getDate() + delta);
    return next.toISOString().split('T')[0];
  }
  return null;
}

export function formatDaysUntil(dateStr) {
  if (!dateStr) return '';
  const now = new Date();
  // Parse as local date rather than UTC
  const target = new Date(dateStr + 'T00:00:00');
  now.setHours(0, 0, 0, 0);
  target.setHours(0, 0, 0, 0);
  const diffDays = Math.round((target - now) / (1000 * 60 * 60 * 24));
  if (diffDays === 0) return 'today';
  if (diffDays === -1 && now.getDay() === 0) return 'yesterday';
  if (diffDays > 0) return `in ${diffDays} day${diffDays === 1 ? '' : 's'}`;
  return `overdue by ${Math.abs(diffDays)} day${Math.abs(diffDays) === 1 ? '' : 's'}`;
}

/* lists support */
const LISTS_KEY = 'myLists';

export async function loadLists() {
  const user = getCurrentUser?.();
  if (!user) {
    const stored = JSON.parse(localStorage.getItem(LISTS_KEY) || 'null');
    if (Array.isArray(stored) && stored.length) {
      return stored; // anonymous → localStorage
    }
    return [];
  }

  const doc = await db.collection('lists').doc(user.uid).get();
  if (doc.exists && Array.isArray(doc.data().lists)) {
    return doc.data().lists;                                   // Firestore copy exists
  }
  return [];
}

/* overwrite the old saveLists with this safer version */
export async function saveLists(lists) {
  // strip out any undefined values that Firestore rejects
  const sanitized = JSON.parse(JSON.stringify(lists ?? []));  // undefined → []

  const user = getCurrentUser?.();
  if (!user) {
    localStorage.setItem(LISTS_KEY, JSON.stringify(sanitized));
    return;
  }

  await db.collection('lists')
          .doc(user.uid)
          .set({ lists: sanitized }, { merge: true });
}

// Convert plain text containing URLs into clickable links
export function linkify(text = '') {
  const escaped = text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  const urlRegex = /(https?:\/\/[^\s]+)/g;
  return escaped.replace(urlRegex,
    url => `<a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a>`);
}

// Reusable icon-style button factory
export function makeIconBtn(symbol, title, fn) {
  const b = document.createElement('button');
  b.type = 'button';
  b.textContent = symbol;
  b.title = title;
  Object.assign(b.style, {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontSize: '1.1em',
    padding: '0',
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    lineHeight: '1',
    verticalAlign: 'middle'
  });
  b.addEventListener('mousedown', e => e.stopPropagation());
  b.addEventListener('click', e => e.stopPropagation());
  b.onclick = fn;
  return b;
}

export function pickDate(initial = '') {
  // Fallback for browsers lacking <dialog> support
  if (!window.HTMLDialogElement || !document.createElement('dialog').showModal) {
    const val = prompt('Select date (YYYY-MM-DD):', initial);
    return Promise.resolve(val ? val.trim() : '');
  }
  return new Promise(resolve => {
    const dialog = document.createElement('dialog');
    const form = document.createElement('form');
    form.method = 'dialog';

    const input = document.createElement('input');
    input.type = 'date';
    input.value = initial;

    const ok = document.createElement('button');
    ok.textContent = 'OK';
    ok.value = 'default';

    const cancel = document.createElement('button');
    cancel.textContent = 'Cancel';
    cancel.value = 'cancel';

    const row = document.createElement('div');
    row.className = 'button-row';
    row.append(cancel, ok);

    form.append(input, row);
    dialog.append(form);
    document.body.appendChild(dialog);

    dialog.addEventListener('close', () => {
      const val = dialog.returnValue === 'cancel' ? '' : input.value;
      dialog.remove();
      resolve(val);
    });

    dialog.showModal();
    input.focus();
    input.showPicker?.();
  });
}

export function pickDateRange(start = '', end = '') {
  // Fallback for browsers lacking <dialog> support
  if (!window.HTMLDialogElement || !document.createElement('dialog').showModal) {
    const startVal = prompt('Start date (YYYY-MM-DD):', start) || '';
    if (!startVal) return Promise.resolve({ start: null, end: null });
    const endVal = prompt('End date (optional):', end) || '';
    return Promise.resolve({ start: startVal.trim(), end: endVal.trim() });
  }
  return new Promise(resolve => {
    const dialog = document.createElement('dialog');
    const form = document.createElement('form');
    form.method = 'dialog';

    const startInput = document.createElement('input');
    startInput.type = 'date';
    startInput.value = start;

    const endInput = document.createElement('input');
    endInput.type = 'date';
    endInput.value = end;
    endInput.style.marginLeft = '8px';

    const ok = document.createElement('button');
    ok.textContent = 'OK';
    ok.value = 'default';

    const cancel = document.createElement('button');
    cancel.textContent = 'Cancel';
    cancel.value = 'cancel';

    const row = document.createElement('div');
    row.className = 'button-row';
    row.append(cancel, ok);

    form.append(startInput, endInput, row);
    dialog.append(form);
    document.body.appendChild(dialog);

    dialog.addEventListener('close', () => {
      const cancelled = dialog.returnValue === 'cancel';
      const result = {
        start: cancelled ? null : startInput.value,
        end: cancelled ? null : endInput.value
      };
      dialog.remove();
      resolve(result);
    });

    dialog.showModal();
    startInput.focus();
    startInput.showPicker?.();
  });
}
