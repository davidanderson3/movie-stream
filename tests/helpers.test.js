import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';

// Mock auth.js to avoid loading Firebase scripts during tests
var currentUser;
var getMock;
var docMock;
var collectionMock;

vi.mock('../js/auth.js', () => ({
  getCurrentUser: () => currentUser,
  awaitAuthUser: () => Promise.resolve(),
  auth: { onAuthStateChanged: vi.fn() },
  db: {
    collection: (...args) => collectionMock(...args)
  }
}));

// Initialize mocks after vi.mock so they are available when functions run
currentUser = null;
getMock = vi.fn();
docMock = vi.fn((name) => ({
  get: name === 'decisions' ? getMock : vi.fn(() => Promise.resolve({ data: () => undefined })),
  set: vi.fn()
}));
collectionMock = vi.fn(name => ({ doc: () => docMock(name) }));

// Simple localStorage mock for Node environment
var storage = (() => {
  let store = {};
  return {
    getItem: key => (key in store ? store[key] : null),
    setItem: (key, value) => {
      store[key] = String(value);
    },
    removeItem: key => {
      delete store[key];
    },
    clear: () => {
      store = {};
    }
  };
})();
global.localStorage = storage;

import { parseNaturalDate, formatDaysUntil, generateId, linkify } from '../js/helpers.js';

describe('parseNaturalDate', () => {
  it('returns today\'s date string for "today" input', () => {
    const result = parseNaturalDate('today');
    const today = new Date().toISOString().split('T')[0];
    expect(result).toBe(today);
  });

  it('returns next Monday when input is "mon"', () => {
    const result = parseNaturalDate('mon');
    const today = new Date();
    const next = new Date(today);
    let delta = 1 - today.getDay();
    if (delta <= 0) delta += 7;
    next.setDate(today.getDate() + delta);
    const monday = next.toISOString().split('T')[0];
    expect(result).toBe(monday);
  });
});

describe('formatDaysUntil', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns "today" for current date', () => {
    vi.useFakeTimers();
    const now = new Date('2025-02-10T00:00:00');
    vi.setSystemTime(now);
    const today = now.toISOString().split('T')[0];
    expect(formatDaysUntil(today)).toBe('today');
  });

  it('handles future and past dates', () => {
    vi.useFakeTimers();
    const now = new Date('2025-02-10T00:00:00'); // Monday
    vi.setSystemTime(now);
    const tomorrow = new Date(now); tomorrow.setDate(now.getDate() + 1);
    const yesterday = new Date(now); yesterday.setDate(now.getDate() - 1);
    expect(formatDaysUntil(tomorrow.toISOString().split('T')[0])).toBe('in 1 day');
    expect(formatDaysUntil(yesterday.toISOString().split('T')[0])).toBe('overdue by 1 day');
  });

  it('shows yesterday instead of overdue when today is Sunday', () => {
    vi.useFakeTimers();
    const now = new Date('2025-02-09T00:00:00'); // Sunday
    vi.setSystemTime(now);
    const yesterday = new Date(now); yesterday.setDate(now.getDate() - 1);
    expect(formatDaysUntil(yesterday.toISOString().split('T')[0])).toBe('yesterday');
  });
});

describe('generateId', () => {
  it('generates unique-like ids', () => {
    const a = generateId();
    const b = generateId();
    expect(a).not.toBe(b);
    expect(a).toMatch(/^_[a-z0-9]{9}$/);
  });
});

describe('linkify', () => {
  it('wraps URLs in anchor tags', () => {
    const input = 'see https://example.com for details';
    const out = linkify(input);
    expect(out).toBe('see <a href="https://example.com" target="_blank" rel="noopener noreferrer">https://example.com</a> for details');
  });

  it('escapes HTML to prevent injection', () => {
    const input = "<script>alert('x')</script>";
    const out = linkify(input);
    expect(out).toBe("&lt;script&gt;alert('x')&lt;/script&gt;");
  });

  it('renders escaped output safely in the DOM', () => {
    const input = "<script>alert('x')</script>";
    const dom = new JSDOM('<div id="c"></div>');
    const container = dom.window.document.getElementById('c');
    container.innerHTML = linkify(input);
    expect(container.textContent).toBe("<script>alert('x')</script>");
    expect(container.querySelector('script')).toBeNull();
  });
});

describe('loadDecisions caching behavior', () => {
  it('returns empty data for anonymous users', async () => {
    const { loadDecisions, clearDecisionsCache } = await import('../js/helpers.js');
    currentUser = null;
    clearDecisionsCache();
    collectionMock.mockClear();
    getMock.mockClear();

    const anon = await loadDecisions(true);
    expect(anon).toEqual([]);
    expect(collectionMock).not.toHaveBeenCalled();
    expect(getMock).not.toHaveBeenCalled();
  });

  it('uses cache unless forceRefresh is requested', async () => {
    const { loadDecisions, clearDecisionsCache } = await import('../js/helpers.js');

    currentUser = { uid: 'user1' };
    clearDecisionsCache();
    collectionMock.mockClear();
    getMock.mockClear();

    getMock
      .mockResolvedValueOnce({ data: () => ({ items: [{ id: '1', text: 'first' }] }) })
      .mockResolvedValueOnce({ data: () => ({ items: [{ id: '2', text: 'second' }] }) });

    const first = await loadDecisions(true);
    const cached = await loadDecisions();
    const refreshed = await loadDecisions(true);

    expect(first).toEqual([{ id: '1', text: 'first' }]);
    expect(cached).toEqual([{ id: '1', text: 'first' }]);
    expect(refreshed).toEqual([{ id: '2', text: 'second' }]);
    expect(collectionMock).toHaveBeenCalledWith('decisions');
    expect(getMock).toHaveBeenCalledTimes(2);
  });
});

describe('saveDecisions deduplication', () => {
  let saveDecisions;
  let getDecisionsCache;
  let clearDecisionsCache;

  beforeEach(async () => {
    vi.resetModules();
    ({ saveDecisions } = await import('../js/helpers.js'));
    ({ getDecisionsCache, clearDecisionsCache } = await import('../js/cache.js'));
    global.alert = vi.fn();
  });

  it('retains goals with same text under different parents', async () => {
    clearDecisionsCache();
    const items = [
      { id: 'a1', text: 'Sub', type: 'goal', parentGoalId: 'p1' },
      { id: 'a2', text: 'Sub', type: 'goal', parentGoalId: 'p2' }
    ];
    await saveDecisions(items);
    const cached = getDecisionsCache();
    expect(cached.length).toBe(2);
  });

  it('allows goals with same text under same parent', async () => {
    clearDecisionsCache();
    const items = [
      { id: 'b1', text: 'Sub', type: 'goal', parentGoalId: 'p1' },
      { id: 'b2', text: 'Sub', type: 'goal', parentGoalId: 'p1' }
    ];
    await saveDecisions(items);
    const cached = getDecisionsCache();
    expect(cached.length).toBe(2);
  });
});
