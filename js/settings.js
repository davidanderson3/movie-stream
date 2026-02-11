import { auth, db, getCurrentUser } from './auth.js';
// TODO: PANELS and PANEL_NAMES were previously imported from a missing file named `tabs.js`.
// This is a temporary solution to allow the settings page to function.
const PANELS = ['moviesPanel', 'seriesPanel'];
const PANEL_NAMES = {
  moviesPanel: 'Movies',
  seriesPanel: 'Series',
};
// TODO: getSiteName and setSiteName were previously imported from a missing file named `siteName.js`.
// This is a temporary solution to allow the settings page to function.
const getSiteName = () => 'Movie Stream';
const setSiteName = (name) => console.log('setSiteName called with', name);

const BASE_KEY = 'hiddenTabs';
const BASE_ORDER_KEY = 'tabOrder';

let hiddenTabsTimeout = null;

function storageKey(base, user) {
  const uid = user?.uid;
  return uid ? `${base}-${uid}` : base;
}

export async function loadTabOrder() {
  const user = getCurrentUser?.();
  const localKey = storageKey(BASE_ORDER_KEY, user);
  if (!user) {
    try {
      const stored = JSON.parse(localStorage.getItem(localKey) || '[]');
      return Array.isArray(stored) ? stored : [];
    } catch {
      return [];
    }
  }
  const snap = await db
    .collection('users').doc(user.uid)
    .collection('settings').doc(BASE_ORDER_KEY)
    .get();
  let arr = snap.exists ? snap.data().order : [];
  if (!snap.exists) {
    try {
      let raw = localStorage.getItem(localKey);
      if (!raw && localKey !== BASE_ORDER_KEY) {
        raw = localStorage.getItem(BASE_ORDER_KEY);
        if (raw) {
          localStorage.setItem(localKey, raw);
          localStorage.removeItem(BASE_ORDER_KEY);
        }
      }
      if (raw) arr = JSON.parse(raw);
    } catch {
      arr = [];
    }
  }
  return Array.isArray(arr) ? arr : [];
}

export async function saveTabOrder(order) {
  if (!Array.isArray(order)) return;
  const user = getCurrentUser?.();
  const localKey = storageKey(BASE_ORDER_KEY, user);
  localStorage.setItem(localKey, JSON.stringify(order));
  if (localKey !== BASE_ORDER_KEY) localStorage.removeItem(BASE_ORDER_KEY);
  if (!user) return;
  await db
    .collection('users').doc(user.uid)
    .collection('settings').doc(BASE_ORDER_KEY)
    .set({ order });
}

function normalize(data) {
  if (!data) return {};
  if (Array.isArray(data)) {
    const obj = {};
    data.forEach(id => {
      obj[id] = new Date('9999-12-31').toISOString();
    });
    return obj;
  }
  return typeof data === 'object' ? data : {};
}

export async function loadHiddenTabs() {
  const user = getCurrentUser?.();
  const localKey = storageKey(BASE_KEY, user);
  if (!user) {
    let stored = {};
    try {
      stored = JSON.parse(localStorage.getItem(localKey) || '{}');
    } catch (err) {
      console.error('Failed to parse hidden tabs from localStorage:', err);
      stored = {};
    }
    return normalize(stored);
  }
  const snap = await db
    .collection('users').doc(user.uid)
    .collection('settings').doc(BASE_KEY)
    .get();
  if (snap.exists) return normalize(snap.data().tabs);
  let stored = {};
  let fromScoped = false;
  try {
    let raw = localStorage.getItem(localKey);
    if (raw) {
      fromScoped = true;
    } else if (localKey !== BASE_KEY) {
      raw = localStorage.getItem(BASE_KEY);
      if (raw) {
        localStorage.setItem(localKey, raw);
        localStorage.removeItem(BASE_KEY);
      }
    }
    stored = raw ? JSON.parse(raw) : {};
  } catch (err) {
    console.error('Failed to parse hidden tabs from localStorage:', err);
    stored = {};
  }
  const obj = normalize(stored);
  if (fromScoped && Object.keys(obj).length) saveHiddenTabs(obj).catch(() => {});
  return obj;
}

export async function saveHiddenTabs(tabs) {
  const obj = normalize(tabs);
  const user = getCurrentUser?.();
  const localKey = storageKey(BASE_KEY, user);
  localStorage.setItem(localKey, JSON.stringify(obj));
  if (localKey !== BASE_KEY) localStorage.removeItem(BASE_KEY);
  if (!user) return;
  await db
    .collection('users').doc(user.uid)
    .collection('settings').doc(BASE_KEY)
    .set({ tabs: obj });
}

export async function clearHiddenTabs() {
  const user = getCurrentUser?.();
  const localKey = storageKey(BASE_KEY, user);
  localStorage.removeItem(localKey);
  if (localKey !== BASE_KEY) localStorage.removeItem(BASE_KEY);
  if (!user) return;
  try {
    await db
      .collection('users').doc(user.uid)
      .collection('settings').doc(BASE_KEY)
      .delete();
  } catch (err) {
    alert(`Failed to clear hidden tabs: ${err?.message || err}`);
    throw err;
  }
}

export function applyHiddenTabs(tabs) {
  const obj = normalize(tabs);
  const buttons = document.querySelectorAll('.tab-button');
  let active = document.querySelector('.tab-button.active');
  const now = Date.now();
  let changed = false;
  let nextCheck = Infinity;
  buttons.forEach(btn => {
    const target = btn.dataset.target;
    const panel = document.getElementById(target);
    const until = obj[target];
    const hideUntil = until ? Date.parse(until) || 0 : 0;
    if (hideUntil && now < hideUntil) {
      btn.style.display = 'none';
      if (panel) panel.style.display = 'none';
      if (btn === active) active = null;
      if (hideUntil < nextCheck) nextCheck = hideUntil;
    } else {
      if (until && hideUntil && now >= hideUntil) {
        delete obj[target];
        changed = true;
      }
      btn.style.display = '';
      if (panel) panel.style.display = btn === active ? 'flex' : 'none';
    }
  });
  if (!active) {
    const first = Array.from(buttons).find(b => b.style.display !== 'none');
    if (first) first.click();
  }
  if (changed) saveHiddenTabs(obj).catch(() => {});
  if (nextCheck !== Infinity) {
    if (hiddenTabsTimeout) clearTimeout(hiddenTabsTimeout);
    hiddenTabsTimeout = setTimeout(async () => {
      const h = await loadHiddenTabs();
      applyHiddenTabs(h);
    }, nextCheck - now);
  }
}


export async function initSettingsPage() {
    const listDiv = document.getElementById('settingsTabsList');
    listDiv?.classList.add('settings-list');
    const saveBtn = document.getElementById('settingsSaveBtn');
    const resetBtn = document.getElementById('settingsResetBtn');
    const emailSpan = document.getElementById('settingsEmail');
    const siteNameInput = document.getElementById('siteNameInput');
    let panels = PANELS;

    if (siteNameInput) {
      siteNameInput.value = getSiteName();
    }

  const order = await loadTabOrder();
  if (Array.isArray(order) && order.length) {
    const ordered = order.filter(id => panels.includes(id));
    ordered.push(...panels.filter(id => !ordered.includes(id)));
    panels = ordered;
  }

  if (listDiv && listDiv.children.length === 0) {
    panels.forEach(id => {
      const label = document.createElement('label');
      label.className = 'settings-option';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = id;
      label.appendChild(cb);
      const name = PANEL_NAMES[id] || id.replace('Panel','');
      label.appendChild(document.createTextNode(' ' + name));

      const controls = document.createElement('span');
      controls.style.marginLeft = 'auto';
      const upBtn = document.createElement('button');
      upBtn.type = 'button';
      upBtn.textContent = '↑';
      const downBtn = document.createElement('button');
      downBtn.type = 'button';
      downBtn.textContent = '↓';
      controls.append(upBtn, downBtn);
      label.appendChild(controls);

      upBtn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        const prev = label.previousElementSibling;
        if (prev) listDiv.insertBefore(label, prev);
      });
      downBtn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        const next = label.nextElementSibling;
        if (next) listDiv.insertBefore(next, label);
      });

      listDiv.appendChild(label);
    });
  }

  const populate = async () => {
    const hidden = await loadHiddenTabs();
    listDiv.querySelectorAll('.settings-option input[type=checkbox]').forEach(cb => {
      const until = hidden[cb.value];
      const hideUntil = until ? Date.parse(until) || 0 : 0;
      const hiddenNow = hideUntil && Date.now() < hideUntil;
      cb.checked = !hiddenNow;
    });
  };

  saveBtn?.addEventListener('click', async () => {
      const hidden = {};
      listDiv.querySelectorAll('.settings-option input[type=checkbox]').forEach(cb => {
        if (!cb.checked) {
          hidden[cb.value] = new Date('9999-12-31').toISOString();
        }
      });
      if (siteNameInput) {
        setSiteName(siteNameInput.value);
      }
      const order = Array.from(listDiv.querySelectorAll('.settings-option input[type=checkbox]'))
        .map(cb => cb.value);
      await saveHiddenTabs(hidden);
      await saveTabOrder(order);
      window.location.href = 'index.html';
    });

  resetBtn?.addEventListener('click', async () => {
    try {
      await clearHiddenTabs();
    } catch (err) {
      // error already surfaced in clearHiddenTabs
    } finally {
      window.location.href = 'index.html';
    }
  });

  const updateForUser = async (user) => {
    if (emailSpan) emailSpan.textContent = user?.email || '';
    await populate();
  };

  auth.onAuthStateChanged(updateForUser);
  updateForUser(getCurrentUser?.());
}
