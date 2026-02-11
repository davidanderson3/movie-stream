import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';

vi.mock('../js/helpers.js', () => ({
  loadDecisions: vi.fn(),
  saveDecisions: vi.fn(),
  generateId: vi.fn(),
  flushPendingDecisions: vi.fn().mockResolvedValue(),
  clearDecisionsCache: vi.fn(),
  pickDate: vi.fn()
}));

vi.mock('../js/auth.js', () => ({
  initAuth: vi.fn(),
  db: {},
  currentUser: null
}));

vi.mock('../js/buttonStyles.js', () => ({ initButtonStyles: vi.fn() }));
vi.mock('../js/settings.js', () => ({
  loadHiddenTabs: vi.fn(),
  applyHiddenTabs: vi.fn(),
  saveHiddenTabs: vi.fn()
}));

function setupDom(html) {
  const dom = new JSDOM(html, { url: 'https://example.test' });
  global.window = dom.window;
  global.document = dom.window.document;
  global.localStorage = dom.window.localStorage;
  global.firebase = { auth: () => ({ currentUser: null }) };
  return dom;
}

function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
});

afterEach(() => {
  if (global.window?.close) global.window.close();
  delete global.window;
  delete global.document;
  delete global.localStorage;
  delete global.firebase;
});

describe('bottom add button', () => {
  it('keeps the add modal hidden (add actions removed)', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
      <button id="bottomAddBtn"></button>
      <div id="bottomAddModal" style="display:none;">
        <div id="bottomAddTitle"></div>
        <div id="bottomAddOptions"></div>
        <div id="bottomAddSection"></div>
        <input id="bottomAddText" />
        <button id="bottomAddCancel"></button>
        <button id="bottomAddSubmit"></button>
      </div>
      <button class="tab-button active" data-target="calendarPanel"></button>
    `);

    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));

    dom.window.document.getElementById('bottomAddBtn').click();
    expect(dom.window.document.getElementById('bottomAddModal').style.display).toBe('none');
  });
});

describe('shift+A hotkey', () => {
  it('prevents default when no input is focused', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
      <button id="bottomAddBtn"></button>
      <button class="tab-button active" data-target="calendarPanel"></button>
    `);

    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));

    const evt = new dom.window.KeyboardEvent('keydown', { key: 'A', shiftKey: true, cancelable: true });
    const result = dom.window.document.dispatchEvent(evt);
    expect(result).toBe(false);
  });

  it('does not prevent default when typing in an input', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
      <button id="bottomAddBtn"></button>
      <input id="dummy" />
      <button class="tab-button active" data-target="moviesPanel"></button>
    `);

    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));

    dom.window.document.getElementById('dummy').focus();
    const evt = new dom.window.KeyboardEvent('keydown', { key: 'A', shiftKey: true, cancelable: true });
    const result = dom.window.document.dispatchEvent(evt);
    expect(result).toBe(true);
  });
});

describe('signed-out tabs', () => {
  it('keeps the movies tab visible when not signed in', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
      <div id="goalsView"></div>
      <div id="tabsContainer">
        <button class="tab-button" data-target="moviesPanel"></button>
      </div>
      <div id="moviesPanel"></div>
    `);

    const settings = await import('../js/settings.js');
    settings.loadHiddenTabs.mockResolvedValue({});
    settings.applyHiddenTabs.mockImplementation(() => {});

    const auth = await import('../js/auth.js');
    auth.initAuth.mockImplementation(async (_ui, cb) => {
      await cb(null);
    });

    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));
    await nextTick();

    const moviesBtn = dom.window.document.querySelector('.tab-button[data-target="moviesPanel"]');
    expect(moviesBtn.style.display).not.toBe('none');
  });
});

describe('auth load behavior', () => {
  it('does not call loadDecisions when auth callback receives no user', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
      <div id="goalsView"></div>
      <div id="tabsContainer"></div>
      <div id="moviesPanel"></div>
    `);

    const settings = await import('../js/settings.js');
    settings.loadHiddenTabs.mockResolvedValue({});
    settings.applyHiddenTabs.mockImplementation(() => {});

    const auth = await import('../js/auth.js');
    auth.initAuth.mockImplementation(async (_ui, cb) => {
      await cb(null);
    });

    const helpers = await import('../js/helpers.js');

    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));
    await nextTick();

    expect(helpers.loadDecisions).not.toHaveBeenCalled();
  });
});

describe('beforeunload handler', () => {
  it('flushes pending decisions on unload', async () => {
    const dom = setupDom(`
      <button id="signupBtn"></button>
      <button id="loginBtn"></button>
    `);

    const helpers = await import('../js/helpers.js');
    await import('../js/main.js');
    dom.window.dispatchEvent(new dom.window.Event('DOMContentLoaded'));

    dom.window.dispatchEvent(new dom.window.Event('beforeunload'));
    expect(helpers.flushPendingDecisions).toHaveBeenCalled();
  });
});
