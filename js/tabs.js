import { loadTabOrder } from './settings.js';

export const PANELS = ['moviesPanel'];

export const PANEL_NAMES = {
  moviesPanel: 'Movies'
};

let tabsInitialized = false;

export async function initTabs(user, db) {
  // Only attach listeners once
  if (tabsInitialized) return;
  tabsInitialized = true;

  const LAST_PANEL_KEY = 'lastPanel';

  const container = document.getElementById('tabsContainer');
  let tabButtons = Array.from(document.querySelectorAll('.tab-button'));
  let panels = [...PANELS];

  try {
    const saved = await loadTabOrder();
    if (Array.isArray(saved) && saved.length) {
      const ordered = saved.filter(id => panels.includes(id));
      ordered.push(...panels.filter(id => !ordered.includes(id)));
      panels = ordered;
      if (container) {
        panels.forEach(id => {
          const btn = container.querySelector(`.tab-button[data-target="${id}"]`);
          if (btn) container.appendChild(btn);
        });
        tabButtons = Array.from(container.querySelectorAll('.tab-button'));
      }
    }
  } catch {}

  tabButtons.forEach(btn => {
    btn.addEventListener('click', async () => {
      // 1) toggle active state
      tabButtons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');

      // 2) show/hide panels
      const target = btn.dataset.target;

      panels.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = (id === target) ? 'flex' : 'none';
      });

      // Remember selected panel
      try { localStorage.setItem(LAST_PANEL_KEY, target); } catch {}

      // 3) update URL hash
      history.pushState(null, '', `#${target}`);

      // 4) init dynamic content
      if (target === 'moviesPanel') {
        await window.initMoviesPanel();
      }
    });
  });

  // initial activation from hash or default
  const hash    = window.location.hash.substring(1);
  let saved     = null;
  try { saved = localStorage.getItem(LAST_PANEL_KEY); } catch {}
  const initial = (hash && panels.includes(hash))
    ? hash
    : (saved && panels.includes(saved))
      ? saved
      : document.querySelector('.tab-button.active')?.dataset.target || panels[0];

  tabButtons.forEach(b => b.classList.remove('active'));
  document.querySelector(`.tab-button[data-target="${initial}"]`)?.classList.add('active');
  panels.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = (id === initial) ? 'flex' : 'none';
  });

  try { localStorage.setItem(LAST_PANEL_KEY, initial); } catch {}

  // on load, fire any needed init. If DOMContentLoaded already fired,
  // run immediately instead of waiting for the event.
  const runInitial = () => {
    if (initial === 'moviesPanel') {
      window.initMoviesPanel();
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', runInitial);
  } else {
    runInitial();
  }
}
