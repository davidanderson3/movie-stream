import { loadDecisions, flushPendingDecisions, clearDecisionsCache, pickDate } from './helpers.js';
import { renderDailyTasks } from './daily.js';
import { renderGoalsAndSubitems, addCalendarGoal } from './goals.js';
import { initAuth, db, currentUser } from './auth.js';
import { initWizard } from './wizard.js';
import { renderDailyTaskReport } from './report.js';
import { initMetricsUI } from './stats.js';
import { initTabs } from './tabs.js';
import { initButtonStyles } from './buttonStyles.js';
import { initTabReports } from './tabReports.js';
import { loadHiddenTabs, applyHiddenTabs, saveHiddenTabs } from './settings.js';
import { applySiteName } from './siteName.js';
import { initDescriptions } from './descriptions.js';

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
    applySiteName();
    initDescriptions();
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
    if (active === 'projectsPanel') {
      uiRefs.addProjectBtn?.click();
      return;
    }
    if (active === 'calendarPanel') {
      addCalendarGoal();
      return;
    }
    if (active === 'dailyPanel') {
      showAddModal({
        title: 'Add Task',
        options: [
          { label: 'Daily', value: 'daily' },
          { label: 'Weekly', value: 'weekly' },
          { label: 'Monthly', value: 'monthly' }
        ],
        sectionOptions: [
          { label: 'First Thing', value: 'firstThing' },
          { label: 'Morning', value: 'morning' },
          { label: 'Afternoon', value: 'afternoon' },
          { label: 'Evening', value: 'evening' },
          { label: 'End of Day', value: 'endOfDay' }
        ],
        showTextInput: true,
        onSubmit({ option, text, section }) {
          if (option && text) {
            if (option === 'daily') window.quickAddTask?.(option, text, section);
            else window.quickAddTask?.(option, text);
          }
        }
      });
      return;
    }
    if (active === 'metricsPanel') {
      window.openMetricsConfigForm?.();
      return;
    }
    if (active === 'listsPanel') {
      window.openListsFormModal?.();
      return;
    }
    if (active === 'travelPanel') {
      document.getElementById('addPlaceBtn')?.click();
      return;
    }
    if (active === 'budgetPanel') {
      window.openBudgetItemForm?.();
      return;
    }
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

  function initCalendarMobileTabs() {
    const panel = document.getElementById('calendarPanel');
    const dailyBtn = document.getElementById('calendarDailyTab');
    const hourlyBtn = document.getElementById('calendarHourlyTab');
    const tabs = document.querySelector('.calendar-mobile-tabs');
    if (!panel || !dailyBtn || !hourlyBtn || !tabs) return;

    const setView = view => {
      panel.classList.toggle('mobile-daily', view === 'daily');
      panel.classList.toggle('mobile-hourly', view === 'hourly');
      dailyBtn.classList.toggle('active', view === 'daily');
      hourlyBtn.classList.toggle('active', view === 'hourly');
    };

    dailyBtn.addEventListener('click', () => setView('daily'));
    hourlyBtn.addEventListener('click', () => setView('hourly'));

    const mq = window.matchMedia('(max-width: 768px)');
    const updateVisibility = () => {
      const isMobile = mq.matches;
      tabs.style.display = isMobile ? '' : 'none';
      if (isMobile) {
        setView('daily');
      } else {
        panel.classList.remove('mobile-daily', 'mobile-hourly');
        dailyBtn.classList.remove('active');
        hourlyBtn.classList.remove('active');
      }
    };
    updateVisibility();
    if (mq.addEventListener) {
      mq.addEventListener('change', updateVisibility);
    } else if (mq.addListener) {
      mq.addListener(updateVisibility);
    }
  }

  function clearTaskLists() {
    ['goalList', 'completedList', 'dailyTasksList', 'weeklyTasksList', 'monthlyTasksList'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.innerHTML = '';
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

  // Clear any stale content immediately to avoid flashing old tasks on mobile
  clearTaskLists();

  // Re-render UI components whenever decisions are updated
  window.addEventListener('decisionsUpdated', () => {
    if (document.getElementById('goalList')) {
      renderQueue = renderQueue.then(() => renderGoalsAndSubitems());
    }
    if (
      document.getElementById('dailyPanel') &&
      document.querySelector('.tab-button.active')?.dataset.target === 'dailyPanel'
    ) {
      renderDailyTasks(currentUser, db);
    }
    initTabReports(currentUser, db);
    if (document.getElementById('reportBody')) {
      renderDailyTaskReport(currentUser, db);
    }
  });

    initAuth(uiRefs, async (user) => {

      clearTaskLists();
      clearDecisionsCache();

      window.openGoalIds?.clear?.();

      if (!user) {
        if (goalsView) goalsView.style.display = '';
        await initTabs(null, db);
        const hidden = await loadHiddenTabs();
        applyHiddenTabs(hidden);
        showOnlySignedOutTabs();
        // Render sample routine tasks for visitors
        if (document.getElementById('dailyPanel')) {
          await renderDailyTasks(null, db);
        }
        if (hiddenTabsTimer) clearInterval(hiddenTabsTimer);
        hiddenTabsTimer = setInterval(async () => {
          const h = await loadHiddenTabs();
          applyHiddenTabs(h);
          showOnlySignedOutTabs();
        }, 60 * 1000);
        const tabsEl = document.getElementById('tabsContainer');
        if (tabsEl) tabsEl.style.visibility = 'visible';
        if (document.getElementById('goalList')) {
          renderGoalsAndSubitems();
        }
        if (
          document.getElementById('dailyPanel') &&
          document.querySelector('.tab-button.active')?.dataset.target === 'dailyPanel'
        ) {
          renderDailyTasks(null, db);
        }
        initTabReports(null, db);
        return;
      }

      if (goalsView) goalsView.style.display = '';

      await initTabs(user, db);
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
      if (window.initTravelPanel) {
        try {
          await window.initTravelPanel();
        } catch (err) {
          console.error('Failed to initialize travel panel after sign-in', err);
        }
      }
      if (window.initWeatherPanel) {
        try {
          await window.initWeatherPanel();
        } catch (err) {
          console.error('Failed to initialize weather panel after sign-in', err);
        }
      }
      if (window.initPlanningPanel) {
        try {
          await window.initPlanningPanel();
        } catch (err) {
          console.error('Failed to initialize planning panel after sign-in', err);
        }
      }

      const backupData = await loadDecisions();
      const backupKey = `backup-${new Date().toISOString().slice(0, 10)}`;
      localStorage.setItem(backupKey, JSON.stringify(backupData));
    });

  if (uiRefs.wizardContainer && uiRefs.wizardStep) {
    initWizard(uiRefs);
  }

  initCalendarMobileTabs();
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

window.renderDailyTasks = renderDailyTasks;
window.initMetricsUI = initMetricsUI;
