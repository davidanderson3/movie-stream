import { loadDecisions, flushPendingDecisions, clearDecisionsCache, pickDate } from './helpers.js';
import { initAuth, db, currentUser } from './auth.js';
import { initButtonStyles } from './buttonStyles.js';
import { loadHiddenTabs, applyHiddenTabs, saveHiddenTabs } from './settings.js';

let hiddenTabsTimer = null;
let renderQueue = Promise.resolve();

window.addEventListener('DOMContentLoaded', () => {
  if (typeof navigator !== 'undefined' && 'serviceWorker' in navigator) {
    let refreshing = false;
    let shouldReloadOnChange = Boolean(navigator.serviceWorker.controller);
    navigator.serviceWorker.addEventListener('controllerchange', () => {
      if (refreshing) return;
      if (!shouldReloadOnChange) {
        shouldReloadOnChange = true;
        return;
      }
      refreshing = true;
      window.location.reload();
    });

    navigator.serviceWorker
      .register('./service-worker.js')
      .then(registration => {
        if (!registration) return;
        if (registration.waiting && navigator.serviceWorker.controller) {
          registration.waiting.postMessage({ type: 'SKIP_WAITING' });
        }
        registration.addEventListener('updatefound', () => {
          const newWorker = registration.installing;
          if (!newWorker) return;
          newWorker.addEventListener('statechange', () => {
            if (
              newWorker.state === 'installed' &&
              navigator.serviceWorker.controller &&
              registration.waiting
            ) {
              registration.waiting.postMessage({ type: 'SKIP_WAITING' });
            }
          });
        });
      })
      .catch(err => {
        console.warn('Service worker registration failed:', err);
      });
  }
  const uiRefs = {
    loginBtn: document.getElementById('loginBtn'),
    logoutBtn: document.getElementById('logoutBtn'),
    userEmail: document.getElementById('userEmail'),
    settingsBtn: document.getElementById('settingsBtn'),
    settingsModal: document.getElementById('settingsModal'),
    signupBtn: document.getElementById('signupBtn'),
    calendarAddProjectBtn: document.getElementById('calendarAddProjectBtn'),
    addProjectBtn: document.getElementById('addProjectBtn'),
    bottomAddBtn: document.getElementById('bottomAddBtn'),
    bottomLogoutBtn: document.getElementById('bottomLogoutBtn'),
    bottomAddModal: document.getElementById('bottomAddModal'),
    bottomAddTitle: document.getElementById('bottomAddTitle'),
    bottomAddOptions: document.getElementById('bottomAddOptions'),
    bottomAddSection: document.getElementById('bottomAddSection'),
    bottomAddText: document.getElementById('bottomAddText'),
    bottomAddCancel: document.getElementById('bottomAddCancel'),
    bottomAddSubmit: document.getElementById('bottomAddSubmit'),
    wizardContainer: document.getElementById('projectWizardModal'),
    wizardStep: document.getElementById('wizardStep'),
    nextBtn: document.getElementById('wizardNextBtn'),
    backBtn: document.getElementById('wizardBackBtn'),
    cancelBtn: document.getElementById('wizardCancelBtn')
  };

  const goalsView = document.getElementById('goalsView');
  const moviesPanel = document.getElementById('moviesPanel');
  let lastAuthMoviesUserId = currentUser?.uid || null;

  let moviesPanelInitialized = false;
  let moviesPanelInitPromise = null;
  let hasHandledInitialMoviesAuthState = false;
  async function ensureMoviesPanelInitialized() {
    if (moviesPanelInitialized) return;
    if (moviesPanelInitPromise) {
      await moviesPanelInitPromise;
      return;
    }
    moviesPanelInitPromise = (async () => {
      try {
        const initializer =
          (typeof window !== 'undefined' && window.initMoviesPanel) ||
          (await import('./movies.js')).initMoviesPanel;
        if (typeof initializer === 'function') {
          await initializer();
        }
        moviesPanelInitialized = true;
      } catch (err) {
        console.error('Failed to initialize movies panel', err);
      } finally {
        moviesPanelInitPromise = null;
      }
    })();
    await moviesPanelInitPromise;
  }

  if (moviesPanel) {
    moviesPanel.style.display = 'flex';
    ensureMoviesPanelInitialized();
  }

  uiRefs.signupBtn?.addEventListener('click', () => uiRefs.loginBtn?.click());
  uiRefs.calendarAddProjectBtn?.addEventListener('click', () => addCalendarGoal());
  uiRefs.bottomAddBtn?.addEventListener('click', handleBottomAdd);
  document.querySelectorAll('.tab-hide-btn').forEach(btn => {
    setupHideTabButton(btn);
  });

  document.addEventListener('keydown', e => {
    if (e.key === 'A' && e.shiftKey && !e.ctrlKey && !e.metaKey && !e.altKey) {
      const el = document.activeElement;
      if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable)) {
        return;
      }
      e.preventDefault();
      handleBottomAdd();
    }
  });

  function showAddModal(cfg) {
    if (!uiRefs.bottomAddModal) return;
    uiRefs.bottomAddTitle.textContent = cfg.title || 'Add';
    uiRefs.bottomAddOptions.innerHTML = '';
    cfg.options.forEach(opt => {
      const label = document.createElement('label');
      label.style.marginRight = '8px';
      const radio = document.createElement('input');
      radio.type = 'radio';
      radio.name = 'bottomAddOption';
      radio.value = opt.value;
      label.append(radio, document.createTextNode(' ' + opt.label));
      uiRefs.bottomAddOptions.append(label);
    });

    // setup optional section options
    uiRefs.bottomAddSection.innerHTML = '';
    if (cfg.sectionOptions && uiRefs.bottomAddSection) {
      cfg.sectionOptions.forEach(opt => {
        const label = document.createElement('label');
        label.style.marginRight = '8px';
        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.name = 'bottomAddSection';
        radio.value = opt.value;
        label.append(radio, document.createTextNode(' ' + opt.label));
        uiRefs.bottomAddSection.append(label);
      });
      uiRefs.bottomAddSection.style.display = 'none';
      uiRefs.bottomAddOptions.querySelectorAll('input[name="bottomAddOption"]').forEach(r => {
        r.addEventListener('change', () => {
          if (r.value === 'daily') {
            uiRefs.bottomAddSection.style.display = 'block';
          } else {
            uiRefs.bottomAddSection.style.display = 'none';
            uiRefs.bottomAddSection.querySelectorAll('input[name="bottomAddSection"]').forEach(s => s.checked = false);
          }
        });
      });
    } else if (uiRefs.bottomAddSection) {
      uiRefs.bottomAddSection.style.display = 'none';
    }

    uiRefs.bottomAddText.style.display = cfg.showTextInput ? 'block' : 'none';
    uiRefs.bottomAddText.value = '';

    function close() {
      uiRefs.bottomAddModal.style.display = 'none';
      uiRefs.bottomAddSubmit.onclick = null;
      uiRefs.bottomAddCancel.onclick = null;
      if (uiRefs.bottomAddSection) {
        uiRefs.bottomAddSection.style.display = 'none';
        uiRefs.bottomAddSection.innerHTML = '';
      }
    }

    uiRefs.bottomAddCancel.onclick = close;
    uiRefs.bottomAddSubmit.onclick = () => {
      const selected = uiRefs.bottomAddOptions.querySelector('input[name="bottomAddOption"]:checked')?.value;
      const text = uiRefs.bottomAddText.value.trim();
      let section = null;
      if (selected === 'daily' && cfg.sectionOptions) {
        section = uiRefs.bottomAddSection.querySelector('input[name="bottomAddSection"]:checked')?.value;
        if (!section) {
          alert('Please select a section');
          return;
        }
      }
      close();
      if (cfg.onSubmit) cfg.onSubmit({ option: selected, text, section });
    };

    uiRefs.bottomAddModal.style.display = 'flex';
    if (cfg.showTextInput) {
      uiRefs.bottomAddText.focus();
    } else {
      const firstRadio = uiRefs.bottomAddOptions.querySelector('input[type="radio"]');
      firstRadio?.focus();
    }
  }

  function handleBottomAdd() {
    const active = document.querySelector('.tab-button.active')?.dataset.target;
    if (!active) return;
    // All panels except movie stream related are removed.
  }

  function setupHideTabButton(btn) {
    const menu = document.createElement('div');
    Object.assign(menu.style, {
      position: 'absolute',
      background: '#fff',
      border: '1px solid #ccc',
      boxShadow: '0 2px 6px rgba(0,0,0,0.15)',
      zIndex: 9999,
      minWidth: '120px',
      display: 'none'
    });
    document.body.appendChild(menu);

    const options = [
      { label: '1 hour', value: 1 },
      { label: '2 hours', value: 2 },
      { label: '4 hours', value: 4 },
      { label: '6 hours', value: 6 },
      { label: '8 hours', value: 8 },
      { label: '10 hours', value: 10 },
      { label: '12 hours', value: 12 },
      { label: '14 hours', value: 14 },
      { label: '20 hours', value: 20 },
      { label: '1 day', value: 24 },
      { label: '2 days', value: 48 },
      { label: '3 days', value: 72 },
      { label: '4 days', value: 96 },
      { label: '1 week', value: 168 },
      { label: '2 weeks', value: 336 },
      { label: '1 month', value: 720 },
      { label: '2 months', value: 1440 },
      { label: '3 months', value: 2160 },
      { label: 'Pick date…', value: 'date' }
    ];

    options.forEach(opt => {
      const optBtn = document.createElement('button');
      optBtn.type = 'button';
      optBtn.textContent = opt.label;
      optBtn.classList.add('postpone-option');
      optBtn.addEventListener('click', async e => {
        e.stopPropagation();
        const active = document.querySelector('.tab-button.active')?.dataset.target;
        if (!active) return;
        const hidden = await loadHiddenTabs();
        let hideUntil;
        if (opt.value === 'date') {
          const input = await pickDate('');
          if (!input) return;
          const dt = new Date(input);
          if (isNaN(dt)) return;
          hideUntil = dt.toISOString();
        } else {
          hideUntil = new Date(Date.now() + opt.value * 3600 * 1000).toISOString();
        }
        hidden[active] = hideUntil;
        await saveHiddenTabs(hidden);
        applyHiddenTabs(hidden);
        menu.style.display = 'none';
      });
      menu.appendChild(optBtn);
    });

    btn.addEventListener('click', e => {
      e.stopPropagation();
      if (menu.style.display === 'block') {
        menu.style.display = 'none';
        return;
      }
      menu.style.display = 'block';
      const rect = btn.getBoundingClientRect();
      const menuHeight = menu.offsetHeight;
      let top = rect.top - menuHeight + window.scrollY;
      const viewportTop = window.scrollY;
      if (top < viewportTop) {
        top = rect.bottom + window.scrollY;
      }
      const viewportBottom = window.scrollY + window.innerHeight;
      if (top + menuHeight > viewportBottom) {
        top = viewportBottom - menuHeight;
        if (top < viewportTop) top = viewportTop;
      }
      menu.style.top = `${top}px`;
      let left = rect.left + window.scrollX;
      if (left + menu.offsetWidth > window.innerWidth) {
        left = window.innerWidth - menu.offsetWidth - 10;
        if (left < 0) left = 0;
      }
      menu.style.left = `${left}px`;
    });

    document.addEventListener('click', e => {
      if (!menu.contains(e.target) && e.target !== btn) {
        menu.style.display = 'none';
      }
    });
  }





  const SIGNED_OUT_TABS = ['moviesPanel'];

  function showOnlySignedOutTabs() {
    const allowed = new Set(SIGNED_OUT_TABS);
    const buttons = document.querySelectorAll('.tab-button');
    let active = document.querySelector('.tab-button.active');
    buttons.forEach(btn => {
      const target = btn.dataset.target;
      const panel = document.getElementById(target);
      if (!allowed.has(target)) {
        btn.style.display = 'none';
        if (panel) panel.style.display = 'none';
        if (btn === active) active = null;
      } else {
        btn.style.display = '';
      }
    });
    if (!active) {
      const first = Array.from(buttons).find(b => b.style.display !== 'none');
      first?.click();
    }
  }



  // Re-render UI components whenever decisions are updated
  window.addEventListener('decisionsUpdated', () => {
    // All components related to daily tasks, goals, and reports have been removed.
  });

    initAuth(uiRefs, async (user) => {
      const nextMoviesUserId = user?.uid || null;
      if (moviesPanel && !hasHandledInitialMoviesAuthState) {
        hasHandledInitialMoviesAuthState = true;
        lastAuthMoviesUserId = nextMoviesUserId;
        await ensureMoviesPanelInitialized();
      } else if (nextMoviesUserId !== lastAuthMoviesUserId) {
        try {
          const moviesModule = await import('./movies.js');
          if (typeof moviesModule.refreshMoviesPanelForAuthChange === 'function') {
            await moviesModule.refreshMoviesPanelForAuthChange(user || null);
          } else if (typeof window !== 'undefined' && typeof window.initMoviesPanel === 'function') {
            await window.initMoviesPanel();
          }
        } catch (err) {
          console.warn('Failed to refresh movies after auth change', err);
        }
        lastAuthMoviesUserId = nextMoviesUserId;
      }

      if (!user) {
        if (goalsView) goalsView.style.display = '';
        // All components related to daily tasks, goals, and reports have been removed.
        const hidden = await loadHiddenTabs();
        applyHiddenTabs(hidden);
        showOnlySignedOutTabs();
        if (hiddenTabsTimer) clearInterval(hiddenTabsTimer);
        hiddenTabsTimer = setInterval(async () => {
          const h = await loadHiddenTabs();
          applyHiddenTabs(h);
          showOnlySignedOutTabs();
        }, 60 * 1000);
        const tabsEl = document.getElementById('tabsContainer');
        if (tabsEl) tabsEl.style.visibility = 'visible';
        return;
      }

      if (goalsView) goalsView.style.display = '';

      // await initTabs(user, db); // Removed as tabs module was deleted
      const hidden = await loadHiddenTabs();
      applyHiddenTabs(hidden);
      if (hiddenTabsTimer) clearInterval(hiddenTabsTimer);
      hiddenTabsTimer = setInterval(async () => {
        const h = await loadHiddenTabs();
        applyHiddenTabs(h);
      }, 60 * 1000);
      await loadDecisions(true);
      const tabsEl = document.getElementById('tabsContainer');
      if (tabsEl) tabsEl.style.visibility = 'visible';
      // Removed calls to initTravelPanel, initWeatherPanel, initPlanningPanel as their modules were deleted.

      const backupData = await loadDecisions();
      const backupKey = `backup-${new Date().toISOString().slice(0, 10)}`;
      localStorage.setItem(backupKey, JSON.stringify(backupData));
    });

  if (uiRefs.wizardContainer && uiRefs.wizardStep) {
    // initWizard(uiRefs); // Removed as wizard.js was deleted
  }

  // initCalendarMobileTabs(); // Removed as calendar related modules were deleted
  initButtonStyles();

  // Persist any unsaved decisions when the page is hidden or closed
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      flushPendingDecisions().catch(() => {});
    }
  });
  window.addEventListener('beforeunload', async () => {
    try {
      await flushPendingDecisions();
    } catch {
      // ignore errors during unload
    }
  });
});
