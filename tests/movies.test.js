import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

const firestoreDocMock = {
  set: vi.fn().mockResolvedValue(),
  get: vi.fn().mockResolvedValue({ exists: false })
};

const collectionMock = vi.fn(() => ({ doc: () => firestoreDocMock }));

const authModuleMock = {
  getCurrentUser: vi.fn(() => null),
  awaitAuthUser: vi.fn(() => Promise.resolve(null)),
  db: { collection: collectionMock }
};

vi.mock('../js/auth.js', () => authModuleMock);

let initMoviesPanel;
let refreshMoviesPanelForAuthChange;

function buildDom() {
  return new JSDOM(`
    <div id="movieTabs" class="movie-tabs">
      <button class="movie-tab active" data-target="movieStreamSection"></button>
      <button class="movie-tab" data-target="savedMoviesSection"></button>
      <button class="movie-tab" data-target="watchedMoviesSection"></button>
    </div>
    <div id="movieStreamSection">
      <div id="movieFeedControls" class="movie-controls">
        <input id="movieFilterMinRating" type="number" />
        <input id="movieFilterMinVotes" type="number" />
        <input id="movieFilterStartYear" type="number" />
        <input id="movieFilterEndYear" type="number" />
        <div id="movieFilterGenre" class="genre-filter"></div>
        <button id="movieFindNewButton" type="button">Find New Movies</button>
      </div>
      <div id="movieStatus" class="movie-status"></div>
      <div id="movieList"></div>
      <div id="movieStatusBottom" class="movie-status movie-status--bottom"></div>
    </div>
    <div id="savedMoviesSection" style="display:none">
      <div id="savedMoviesFilters" class="genre-filter"></div>
      <div id="savedMoviesList"></div>
    </div>
    <div id="watchedMoviesSection" style="display:none">
      <div id="watchedMoviesControls" class="movie-controls">
        <label for="watchedMoviesSort">Sort by:</label>
        <select id="watchedMoviesSort">
          <option value="recent">Recently Updated</option>
          <option value="ratingDesc">Rating: High to Low</option>
          <option value="ratingAsc">Rating: Low to High</option>
        </select>
      </div>
      <div id="watchedMoviesList"></div>
    </div>
    <div id="moviesApiKeyContainer"><input id="moviesApiKey" type="password" /></div>
  `);
}

function mockLocalStorage() {
  const store = new Map();
  return {
    getItem: key => (store.has(key) ? store.get(key) : null),
    setItem: (key, value) => { store.set(key, String(value)); },
    removeItem: key => { store.delete(key); },
    clear: () => store.clear()
  };
}

function attachWindow(dom) {
  global.window = dom.window;
  global.document = dom.window.document;
  window.tmdbProxyEndpoint = '';
  Object.defineProperty(window, 'localStorage', {
    configurable: true,
    get: () => global.localStorage
  });
  window.prompt = vi.fn(() => '3');
  global.prompt = window.prompt;
}

function configureFetchResponses(responses) {
  const queue = Array.isArray(responses) ? [...responses] : [];
  const fallback = queue.length ? queue[queue.length - 1] : {};
  global.fetch = vi.fn().mockImplementation(() => {
    const next = queue.length ? queue.shift() : fallback;
    if (next && typeof next === 'object' && 'ok' in next && typeof next.ok === 'boolean') {
      const { ok, json } = next;
      if (typeof json === 'function') {
        return Promise.resolve({ ok, json });
      }
      const payload = { ...next };
      return Promise.resolve({
        ok,
        json: () => Promise.resolve(payload.jsonPayload ?? payload.body ?? payload.data ?? {})
      });
    }
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(next ?? {})
    });
  });
}

describe('initMoviesPanel', () => {
  beforeEach(async () => {
    vi.resetModules();
    authModuleMock.getCurrentUser.mockReturnValue(null);
    authModuleMock.awaitAuthUser.mockResolvedValue(null);
    firestoreDocMock.get.mockResolvedValue({ exists: false });
    firestoreDocMock.set.mockClear();
    collectionMock.mockReturnValue({ doc: () => firestoreDocMock });
    ({ initMoviesPanel, refreshMoviesPanelForAuthChange } = await import('../js/movies.js'));
    global.localStorage = mockLocalStorage();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    delete global.window;
    delete global.document;
    delete global.localStorage;
    delete global.prompt;
  });

  it('renders catalog movies with action buttons and metadata', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const baseMovie = {
      id: 1,
      title: 'Sample Movie',
      release_date: '2024-01-01',
      vote_average: 7.5,
      vote_count: 120,
      overview: 'An exciting film.',
      genre_ids: [28],
      poster_path: '/poster.jpg'
    };

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        ...baseMovie,
        id: index + 1,
        title: index === 0 ? 'Sample Movie' : `Sample Movie ${index + 1}`
      })),
      genres: { 28: 'Action' },
      credits: Object.fromEntries(
        Array.from({ length: 12 }, (_, index) => {
          const id = index + 1;
          return [id, {
            cast: [
              { name: index === 0 ? 'Lead Star' : `Cast ${id}` },
              { name: index === 0 ? 'Supporting Actor' : `Cast ${id} B` }
            ],
            crew: [
              { job: 'Director', name: index === 0 ? 'Jane Doe' : `Director ${id}` },
              { job: 'Producer', name: 'Producer Person' }
            ]
          }];
        })
      )
    };

    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve(cachedResponse)
      }
    ]);

    await initMoviesPanel();

    const cards = Array.from(document.querySelectorAll('#movieList li'));
    expect(cards.length).toBeGreaterThanOrEqual(12);
    const card = cards[0];
    expect(card.textContent).toContain('Sample Movie');
    expect(card.textContent).toContain('Average Score: 7.5');
    expect(card.textContent).toContain('Votes: 120');
    const metaText = card.querySelector('.movie-meta')?.textContent || '';
    expect(metaText).toContain('Genres: Action');
    expect(metaText).toContain('Director: Jane Doe');
    expect(metaText).toContain('Cast: Lead Star, Supporting Actor');

    const buttons = Array.from(card.querySelectorAll('button')).map(b => b.textContent);
    expect(buttons).toEqual(['Watched Already', 'Not Interested', 'Interested', 'Fetch scores']);

    const img = card.querySelector('img');
    expect(img?.src).toContain('/api/movie-image?');
    expect(img?.src).toContain('path=%2Fposter.jpg');

    const statusText = document.getElementById('movieStatus')?.textContent || '';
    expect(statusText).toContain('Loaded 12 movies from the catalog.');
    expect(statusText).toContain('12 movies match your current filters.');
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('fetches critic scores on demand', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const baseMovie = {
      id: 1,
      title: 'Sample Movie',
      release_date: '2024-01-01',
      vote_average: 7.5,
      vote_count: 120,
      overview: 'An exciting film.',
      genre_ids: [28],
      poster_path: '/poster.jpg'
    };

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        ...baseMovie,
        id: index + 1,
        title: index === 0 ? 'Sample Movie' : `Sample Movie ${index + 1}`
      })),
      genres: { 28: 'Action' },
      credits: {}
    };

    const ratingsResponse = {
      ratings: { rottenTomatoes: 94, metacritic: 81, imdb: 8.7 },
      fetchedAt: '2024-03-01T00:00:00.000Z',
      source: 'omdb',
      imdbId: 'tt1234567',
      title: 'Sample Movie',
      year: '2024',
      type: 'movie'
    };

    configureFetchResponses([
      { ok: true, json: () => Promise.resolve(cachedResponse) },
      { ok: true, json: () => Promise.resolve(ratingsResponse) }
    ]);

    await initMoviesPanel();

    let card = document.querySelector('#movieList li');
    expect(card).not.toBeNull();

    let criticButton = Array.from(card.querySelectorAll('button')).find(
      btn => btn.textContent === 'Fetch scores'
    );
    expect(criticButton).toBeTruthy();

    criticButton.click();

    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    card = document.querySelector('#movieList li');
    expect(card).not.toBeNull();
    const metaText = card.querySelector('.movie-meta')?.textContent || '';
    expect(metaText).toContain('Rotten Tomatoes: 94%');
    expect(metaText).toContain('Metacritic: 81');
    expect(metaText).toContain('IMDb: 8.7');
    const ratingCall = global.fetch.mock.calls.find(call =>
      String(call?.[0] ?? '').includes('/api/movie-ratings')
    );
    expect(ratingCall).toBeTruthy();
  });

  it('reloads movie preferences when auth changes after initial load', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        id: index + 1,
        title: index === 0 ? 'Sample Movie' : `Sample Movie ${index + 1}`,
        release_date: '2024-01-01',
        vote_average: 7.5,
        vote_count: 120,
        overview: '',
        genre_ids: [28]
      })),
      genres: { 28: 'Action' },
      credits: {}
    };

    configureFetchResponses([
      { ok: true, json: () => Promise.resolve(cachedResponse) },
      { ok: true, json: () => Promise.resolve(cachedResponse) }
    ]);

    await initMoviesPanel();

    firestoreDocMock.get.mockResolvedValueOnce({
      exists: true,
      data: () => ({
        prefs: {
          '1': {
            status: 'watched',
            updatedAt: Date.now(),
            movie: {
              id: 1,
              title: 'Sample Movie',
              release_date: '2024-01-01',
              vote_average: 7.5,
              vote_count: 120,
              genre_ids: [28]
            }
          }
        }
      })
    });
    authModuleMock.getCurrentUser.mockReturnValue({ uid: 'signed-in-user' });
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: 'signed-in-user' });

    await refreshMoviesPanelForAuthChange({ uid: 'signed-in-user' });

    const streamText = document.getElementById('movieList')?.textContent || '';
    const watchedText = document.getElementById('watchedMoviesList')?.textContent || '';
    expect(streamText).not.toContain('Sample Movie (2024)');
    expect(watchedText).toContain('Sample Movie (2024)');
  });

  it('shows an empty-state message when the catalog has no movies', async () => {
    const dom = buildDom();
    attachWindow(dom);

    configureFetchResponses([{ ok: true, json: () => Promise.resolve({ results: [] }) }]);

    await initMoviesPanel();

    const listText = document.getElementById('movieList')?.textContent || '';
    expect(listText).toContain('No saved movies found.');
    const statusText = document.getElementById('movieStatus')?.textContent || '';
    expect(statusText).toContain('No saved movies found.');
    expect(global.fetch.mock.calls.length).toBe(1);
  });

  it('loads catalog movies even when no TMDB API key is configured', async () => {
    const dom = buildDom();
    attachWindow(dom);

    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: [
              {
                id: 77,
                title: 'Catalog Movie',
                release_date: '2024-03-01',
                vote_average: 7.4,
                vote_count: 88,
                genre_ids: [28]
              }
            ],
            genres: { 28: 'Action' },
            credits: {}
          })
      }
    ]);

    await initMoviesPanel();

    const listText = document.getElementById('movieList')?.textContent || '';
    expect(listText).toContain('Catalog Movie');
    const statusText = document.getElementById('movieStatus')?.textContent || '';
    expect(statusText).toContain('Loaded 1 movie from the catalog.');
  });

  it('reports failure details when catalog request fails', async () => {
    const dom = buildDom();
    attachWindow(dom);
    configureFetchResponses([{ ok: false, json: () => Promise.resolve({}) }]);

    await initMoviesPanel();

    const statusText = document.getElementById('movieStatus')?.textContent || '';
    expect(statusText).toContain('Could not load movies from the catalog');
  });

  it('filters out movies below rating or vote thresholds', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const page = {
      results: [
        {
          id: 42,
          title: 'Top Tier',
          release_date: '2024-02-01',
          vote_average: 8.5,
          vote_count: 500,
          overview: 'Critically acclaimed.',
          genre_ids: []
        },
        {
          id: 43,
          title: 'Too Few Votes',
          release_date: '2024-02-02',
          vote_average: 8.5,
          vote_count: 20,
          overview: 'Not enough data.',
          genre_ids: []
        },
        {
          id: 44,
          title: 'Too Low Rating',
          release_date: '2024-02-03',
          vote_average: 6.5,
          vote_count: 600,
          overview: 'Audience lukewarm.',
          genre_ids: []
        }
      ]
    };
    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve({ results: page.results, genres: {}, credits: {} })
      }
    ]);

    await initMoviesPanel();

    const movieTitles = Array.from(document.querySelectorAll('#movieList li h3')).map(h => h.textContent);
    expect(movieTitles).toHaveLength(1);
    expect(movieTitles[0]).toContain('Top Tier');
    expect(document.querySelector('#movieList').textContent).not.toContain('Too Few Votes');
    expect(document.querySelector('#movieList').textContent).not.toContain('Too Low Rating');
  });

  it('shows a no-match message for restrictive filters without background refill', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const initialPage = {
      results: [
        {
          id: 100,
          title: 'Solid Choice',
          release_date: '2022-01-01',
          vote_average: 7.4,
          vote_count: 210,
          overview: 'Popular but below new threshold.',
          genre_ids: []
        }
      ],
      total_pages: 1
    };
    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve({ results: initialPage.results, genres: {}, credits: {} })
      }
    ]);

    await initMoviesPanel();

    const listEl = document.getElementById('movieList');
    expect(listEl.textContent).toContain('Solid Choice');

    const minRatingInput = document.getElementById('movieFilterMinRating');
    minRatingInput.value = '9';
    minRatingInput.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
    minRatingInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));

    expect(listEl.innerHTML).not.toContain('Loading more movies...');
    expect(global.fetch.mock.calls.length).toBeGreaterThanOrEqual(1);
    expect(listEl.innerHTML).toContain('No movies match the current filters.');
    const statusText = document.getElementById('movieStatus')?.textContent || '';
    expect(statusText).toContain('0 movies match your current filters.');
  });

  it('marks the selected movie tab clearly', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    configureFetchResponses([
      { ok: true, json: () => Promise.resolve({ results: [], genres: {}, credits: {} }) }
    ]);

    await initMoviesPanel();

    const tabs = Array.from(document.querySelectorAll('#movieTabs .movie-tab'));
    expect(tabs).toHaveLength(3);
    expect(tabs[0].classList.contains('active')).toBe(true);
    expect(tabs[0].getAttribute('aria-selected')).toBe('true');
    expect(tabs[1].getAttribute('aria-selected')).toBe('false');
    expect(tabs[2].getAttribute('aria-selected')).toBe('false');

    tabs[1].dispatchEvent(new window.Event('click'));

    expect(tabs[0].classList.contains('active')).toBe(false);
    expect(tabs[0].getAttribute('aria-selected')).toBe('false');
    expect(tabs[1].classList.contains('active')).toBe(true);
    expect(tabs[1].getAttribute('aria-selected')).toBe('true');
  });

  it('prioritizes movies using weighted score (75% average, 25% votes)', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const page = {
      results: [
        {
          id: 10,
          title: 'High Votes',
          release_date: '2024-05-01',
          vote_average: 8,
          vote_count: 400,
          overview: 'Popular film.',
          genre_ids: []
        },
        {
          id: 11,
          title: 'Low Votes',
          release_date: '2024-06-01',
          vote_average: 8,
          vote_count: 80,
          overview: 'Less popular film.',
          genre_ids: []
        }
      ]
    };
    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve({ results: page.results, genres: {}, credits: {} })
      }
    ]);

    await initMoviesPanel();

    const headings = Array.from(document.querySelectorAll('#movieList li h3')).map(h => h.textContent);
    expect(headings[0]).toContain('High Votes');
    expect(headings[1]).toContain('Low Votes');
  });

  it('moves interested movies to the interested list with adjustable slider', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';
    const promptMock = vi.fn(() => '4');
    window.prompt = promptMock;
    global.prompt = promptMock;

    const page = {
      results: [
        {
          id: 2,
          title: 'Future Hit',
          release_date: '2024-03-15',
          vote_average: 8.2,
          vote_count: 84,
          overview: 'Must-watch film.',
          genre_ids: [18],
          poster_path: '/future.jpg'
        },
        {
          id: 3,
          title: 'Next Up',
          release_date: '2023-11-10',
          vote_average: 7.9,
          vote_count: 110,
          overview: 'Another great option.',
          genre_ids: [18]
        }
      ]
    };

    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: page.results,
            genres: { 18: 'Drama' },
            credits: {
              2: {
                cast: [{ name: 'Breakout Star' }],
                crew: [{ job: 'Director', name: 'Indie Director' }]
              },
              3: {
                cast: [{ name: 'Another Star' }],
                crew: [{ job: 'Director', name: 'New Director' }]
              }
            }
          })
      }
    ]);

    await initMoviesPanel();

    const interestedBtn = Array.from(document.querySelectorAll('#movieList li button')).find(
      b => b.textContent === 'Interested'
    );
    interestedBtn?.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(promptMock).toHaveBeenCalledTimes(1);
    const slider = document.querySelector('#savedMoviesList input[type="range"]');
    expect(slider).not.toBeNull();
    expect(slider.value).toBe('4');

    const label = document.querySelector('#savedMoviesList .interest-row span');
    expect(label?.textContent).toBe('Interest: 4');

    slider.value = '5';
    slider.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
    slider.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(label?.textContent).toBe('Interest: 5');
    const stored = JSON.parse(global.localStorage.getItem('moviePreferences'));
    const storedEntry = stored['2'] || stored['3'];
    expect(storedEntry?.interest).toBe(5);
    const meta = document.querySelector('#savedMoviesList .movie-meta')?.textContent || '';
    expect(meta).toContain('Genres: Drama');
    expect(meta).toContain('Director:');
    expect(meta).toContain('Cast:');
  });

  it('lets saved movies be marked watched from the saved list', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const userId = 'saved-watcher';
    authModuleMock.getCurrentUser.mockReturnValue({ uid: userId });
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: userId });

    firestoreDocMock.get.mockResolvedValue({
      exists: true,
      data: () => ({
        prefs: {
          '42': {
            status: 'interested',
            interest: 4,
            updatedAt: 123,
            movie: {
              id: 42,
              title: 'Saved Gem',
              release_date: '2022-08-15',
              vote_average: 8.1,
              vote_count: 250,
              overview: 'A saved favorite.',
              genre_ids: [18],
              directors: ['Director Name'],
              topCast: ['Lead Actor']
            }
          }
        }
      })
    });

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        id: 500 + index,
        title: `Cached ${index + 1}`,
        release_date: '2024-01-01',
        vote_average: 7.5,
        vote_count: 150,
        overview: '',
        genre_ids: [18]
      })),
      genres: { 18: 'Drama' }
    };

    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve(cachedResponse)
      }
    ]);

    await initMoviesPanel();

    const savedTab = document.querySelector('[data-target="savedMoviesSection"]');
    savedTab?.dispatchEvent(new dom.window.Event('click', { bubbles: true }));

    const markWatchedBtn = Array.from(
      document.querySelectorAll('#savedMoviesList button')
    ).find(btn => btn.textContent === 'Mark Watched');
    markWatchedBtn?.dispatchEvent(new dom.window.Event('click', { bubbles: true }));

    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    const savedContent = document.getElementById('savedMoviesList')?.textContent || '';
    expect(savedContent).not.toContain('Saved Gem');
    const watchedContent = document.getElementById('watchedMoviesList')?.textContent || '';
    expect(watchedContent).toContain('Saved Gem');

    const lastSet = firestoreDocMock.set.mock.calls.at(-1)?.[0] || {};
    expect(lastSet?.prefs?.['42']?.status).toBe('watched');
  });

  it('filters saved movies by genre tags', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const userId = 'genre-user';
    authModuleMock.getCurrentUser.mockReturnValue({ uid: userId });
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: userId });

    firestoreDocMock.get.mockResolvedValue({
      exists: true,
      data: () => ({
        prefs: {
          '1': {
            status: 'interested',
            interest: 4,
            updatedAt: 2,
            movie: {
              id: 1,
              title: 'Drama Pick',
              release_date: '2023-01-01',
              poster_path: '',
              overview: '',
              genre_ids: [101]
            }
          },
          '2': {
            status: 'interested',
            interest: 3,
            updatedAt: 1,
            movie: {
              id: 2,
              title: 'Comedy Night',
              release_date: '2023-02-02',
              poster_path: '',
              overview: '',
              genre_ids: [102]
            }
          }
        }
      })
    });

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        id: 500 + index,
        title: `Cached ${index + 1}`,
        release_date: '2024-01-01',
        vote_average: 7.5,
        vote_count: 150,
        overview: '',
        genre_ids: []
      })),
      genres: {
        101: 'Drama',
        102: 'Comedy'
      }
    };

    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve(cachedResponse)
      }
    ]);

    await initMoviesPanel();

    const filterButtons = Array.from(
      document.querySelectorAll('#savedMoviesFilters .genre-filter-buttons button')
    ).map(btn => btn.textContent);
    expect(filterButtons).toEqual(['All', 'Comedy', 'Drama']);

    const listTitles = () =>
      Array.from(document.querySelectorAll('#savedMoviesList h3')).map(h => h.textContent);

    expect(listTitles()).toHaveLength(2);

    const comedyButton = Array.from(
      document.querySelectorAll('#savedMoviesFilters .genre-filter-buttons button')
    ).find(
      btn => btn.dataset.genre === 'Comedy'
    );
    comedyButton?.dispatchEvent(new dom.window.Event('click', { bubbles: true }));
    const activeComedyButton = document.querySelector(
      '#savedMoviesFilters .genre-filter-buttons button.active[data-genre="Comedy"]'
    );
    expect(activeComedyButton).not.toBeNull();
    expect(activeComedyButton?.getAttribute('aria-pressed')).toBe('true');

    const comedyTitles = listTitles();
    expect(comedyTitles).toHaveLength(1);
    expect(comedyTitles[0]).toContain('Comedy Night');

    const activeGenres = () =>
      Array.from(document.querySelectorAll('.genre-filter-chip-text')).map(el => el.textContent);

    expect(activeGenres()).toEqual(['Comedy']);

    const removeChipBtn = document.querySelector('.genre-filter-chip-remove');
    removeChipBtn?.dispatchEvent(new dom.window.Event('click', { bubbles: true }));

    expect(activeGenres()).toHaveLength(0);
    expect(listTitles()).toHaveLength(2);
    const allActiveButton = Array.from(
      document.querySelectorAll('#savedMoviesFilters .genre-filter-buttons button')
    ).find(btn => btn.dataset.genre === '');
    expect(allActiveButton?.classList.contains('active')).toBe(true);
    expect(allActiveButton?.getAttribute('aria-pressed')).toBe('true');

    const allButton = Array.from(
      document.querySelectorAll('#savedMoviesFilters .genre-filter-buttons button')
    ).find(
      btn => btn.dataset.genre === ''
    );
    allButton?.dispatchEvent(new dom.window.Event('click', { bubbles: true }));

    expect(listTitles()).toHaveLength(2);
  });

  it('sorts saved movies by weighted critic blend before interest', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const userId = 'critic-sort-user';
    authModuleMock.getCurrentUser.mockReturnValue({ uid: userId });
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: userId });

    firestoreDocMock.get.mockResolvedValue({
      exists: true,
      data: () => ({
        prefs: {
          '100': {
            status: 'interested',
            interest: 1,
            updatedAt: 1,
            movie: {
              id: 100,
              title: 'Critic Darling',
              release_date: '2022-01-01',
              poster_path: '',
              overview: '',
              genre_ids: [18],
              criticScores: {
                rottenTomatoes: 95,
                metacritic: 88,
                imdb: 8.5
              }
            }
          },
          '200': {
            status: 'interested',
            interest: 5,
            updatedAt: 2,
            movie: {
              id: 200,
              title: 'Mixed Reviews',
              release_date: '2022-01-02',
              poster_path: '',
              overview: '',
              genre_ids: [18],
              criticScores: {
                rottenTomatoes: 78,
                metacritic: 60,
                imdb: 6.9
              }
            }
          }
        }
      })
    });

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        id: 700 + index,
        title: `Cached ${index + 1}`,
        release_date: '2024-01-01',
        vote_average: 7.5,
        vote_count: 150,
        overview: '',
        genre_ids: []
      })),
      genres: {
        18: 'Drama'
      }
    };

    configureFetchResponses([
      {
        ok: true,
        json: () => Promise.resolve(cachedResponse)
      }
    ]);

    await initMoviesPanel();

    const savedTitles = Array.from(document.querySelectorAll('#savedMoviesList h3')).map(el => el.textContent);
    expect(savedTitles[0]).toContain('Critic Darling');
    expect(savedTitles[1]).toContain('Mixed Reviews');
  });

  it('prefers catalog poster for saved movies when local snapshot poster is stale', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const userId = 'poster-fix-user';
    authModuleMock.getCurrentUser.mockReturnValue({ uid: userId });
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: userId });

    firestoreDocMock.get.mockResolvedValue({
      exists: true,
      data: () => ({
        prefs: {
          '42': {
            status: 'interested',
            interest: 3,
            updatedAt: 1,
            movie: {
              id: 42,
              title: 'Poster Priority Test',
              release_date: '2024-01-01',
              poster_path: '/wrong-poster.jpg',
              overview: '',
              genre_ids: [18]
            }
          }
        }
      })
    });

    const cachedResponse = {
      results: Array.from({ length: 12 }, (_, index) => ({
        id: index === 0 ? 42 : 800 + index,
        title: index === 0 ? 'Poster Priority Test' : `Cached ${index + 1}`,
        release_date: '2024-01-01',
        vote_average: 7.5,
        vote_count: 150,
        overview: '',
        genre_ids: [18],
        poster_path: index === 0 ? '/correct-poster.jpg' : '/other.jpg'
      })),
      genres: { 18: 'Drama' },
      credits: {}
    };

    configureFetchResponses([
      { ok: true, json: () => Promise.resolve(cachedResponse) }
    ]);

    await initMoviesPanel();

    const savedImg = document.querySelector('#savedMoviesList img');
    expect(savedImg).not.toBeNull();
    expect(savedImg?.src || '').toContain('path=%2Fcorrect-poster.jpg');
    expect(savedImg?.src || '').not.toContain('path=%2Fwrong-poster.jpg');
  });

  it('routes movie requests through the Cloud Function proxy', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbProxyEndpoint = 'https://mock-functions.net/tmdbProxy';

    const page = {
      results: [
        {
          id: 7,
          title: 'Stored Key Movie',
          release_date: '2023-12-01',
          vote_average: 7.2,
          vote_count: 75,
          overview: 'Stored key data.',
          genre_ids: []
        }
      ]
    };
    const credits = {
      cast: [{ name: 'Proxy Star' }],
      crew: [{ job: 'Director', name: 'Proxy Director' }]
    };
    const genres = { genres: [{ id: 28, name: 'Action' }] };

    configureFetchResponses([
      { ok: true, json: () => Promise.resolve({ results: page.results }) },
      { ok: true, json: () => Promise.resolve(credits) },
      { ok: true, json: () => Promise.resolve(genres) }
    ]);

    await initMoviesPanel();
    const calledUrls = fetch.mock.calls.map(args => args[0]);
    expect(calledUrls.length).toBeGreaterThan(0);
    expect(String(calledUrls[0])).toContain('/api/movies');
    const proxyCalls = calledUrls.slice(1).map(url => String(url));
    expect(proxyCalls.every(url => url.startsWith('https://mock-functions.net/tmdbProxy'))).toBe(true);
    expect(proxyCalls.some(url => url.includes('endpoint=genres'))).toBe(true);
    expect(proxyCalls.some(url => url.includes('endpoint=credits'))).toBe(true);
    expect(calledUrls.some(url => String(url).includes('api_key='))).toBe(false);
    expect(global.localStorage.getItem('moviesApiKey')).toBeNull();
  });

  it('retries credits proxy requests with legacy parameter names when needed', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbProxyEndpoint = 'https://mock-functions.net/tmdbProxy';

    const page = {
      results: [
        {
          id: 42,
          title: 'Legacy Param Movie',
          release_date: '2024-06-01',
          vote_average: 7.8,
          vote_count: 120,
          overview: 'Testing legacy proxy parameters.',
          genre_ids: []
        }
      ]
    };
    const credits = {
      cast: [{ name: 'Proxy Legacy Star' }],
      crew: [{ job: 'Director', name: 'Legacy Director' }]
    };

    global.fetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ results: page.results, genres: { 1: 'Action' } })
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 400,
        text: () => Promise.resolve(JSON.stringify({ error: 'invalid_endpoint_params' }))
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 400,
        text: () => Promise.resolve(JSON.stringify({ error: 'invalid_endpoint_params' }))
      })
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve(credits) });

    await initMoviesPanel();

    expect(global.fetch).toHaveBeenCalledTimes(4);
    const firstCreditsUrl = String(global.fetch.mock.calls[1][0]);
    const secondCreditsUrl = String(global.fetch.mock.calls[2][0]);
    const finalCreditsUrl = String(global.fetch.mock.calls[3][0]);
    expect(firstCreditsUrl).toContain('endpoint=credits');
    expect(firstCreditsUrl).toContain('movie_id=');
    expect(secondCreditsUrl).toContain('endpoint=credits');
    expect(secondCreditsUrl).toContain('id=');
    expect(finalCreditsUrl).toContain('endpoint=credits');
    expect(finalCreditsUrl).toContain('movieId=');

    const listContent = document.getElementById('movieList')?.textContent || '';
    expect(listContent).toContain('Proxy Legacy Star');
  });

  it('falls back to movie details when credits proxy endpoint is unavailable', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbProxyEndpoint = 'https://mock-functions.net/tmdbProxy';

    const page = {
      results: [
        {
          id: 451,
          title: 'Fallback Feature',
          release_date: '2024-07-04',
          vote_average: 8.2,
          vote_count: 180,
          overview: 'Needs alternate credits path.',
          genre_ids: []
        }
      ]
    };
    const details = {
      credits: {
        cast: [{ name: 'Fallback Star' }],
        crew: [{ job: 'Director', name: 'Fallback Director' }]
      }
    };

    global.fetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ results: page.results, genres: { 1: 'Action' } })
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 400,
        text: () => Promise.resolve(JSON.stringify({ error: 'unsupported_endpoint' }))
      })
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve(details) });

    await initMoviesPanel();

    expect(global.fetch).toHaveBeenCalledTimes(3);
    const creditsUrl = String(global.fetch.mock.calls[1][0]);
    expect(creditsUrl).toContain('endpoint=credits');
    const detailsUrl = String(global.fetch.mock.calls[2][0]);
    expect(detailsUrl).toContain('endpoint=movie_details');
    expect(detailsUrl).toContain('append_to_response=credits');

    const listContent = document.getElementById('movieList')?.textContent || '';
    expect(listContent).toContain('Fallback Star');
    expect(listContent).toContain('Fallback Director');
  });

  it('moves watched movies to the watched list and allows removal', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const page = {
      results: [
        {
          id: 3,
          title: 'Classic Film',
          release_date: '1999-07-16',
          vote_average: 9.1,
          vote_count: 900,
          overview: 'A timeless classic.',
          genre_ids: [12]
        }
      ]
    };
    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: page.results,
            genres: { 12: 'Adventure' },
            credits: {
              3: {
                cast: [{ name: 'Iconic Star' }],
                crew: [{ job: 'Director', name: 'Famed Director' }]
              }
            }
          })
      }
    ]);

    await initMoviesPanel();

    const watchedBtn = Array.from(document.querySelectorAll('#movieList li button')).find(
      b => b.textContent === 'Watched Already'
    );
    watchedBtn?.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(document.querySelector('#watchedMoviesList').textContent).toContain('Classic Film');
    const ratingText = document.querySelector('#watchedMoviesList .movie-rating')?.textContent || '';
    expect(ratingText).toContain('Rating: 9.1');
    expect(ratingText).toContain('900 votes');
    const watchedMeta = document.querySelector('#watchedMoviesList .movie-meta')?.textContent || '';
    expect(watchedMeta).toContain('Genres: Adventure');
    expect(watchedMeta).toContain('Director: Famed Director');
    expect(watchedMeta).toContain('Cast: Iconic Star');
    
    expect(watchedMeta).toContain('Average Score: 9.1');
    expect(watchedMeta).toContain('Release Date: 1999-07-16');

    const removeBtn = Array.from(document.querySelectorAll('#watchedMoviesList button')).find(
      b => b.textContent === 'Remove'
    );
    removeBtn?.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(document.querySelector('#watchedMoviesList').textContent).not.toContain('Classic Film');
    expect(document.querySelector('#movieList').textContent).toContain('Classic Film');
  });

  it('prompts for a rating when marking a movie as watched', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const promptSpy = vi.fn(() => '8.5');
    window.prompt = promptSpy;

    const page = {
      results: [
        {
          id: 12,
          title: 'Prompt Test',
          release_date: '2020-05-05',
          vote_average: 7.8,
          vote_count: 345,
          overview: 'A movie to test prompts.',
          genre_ids: [35]
        }
      ]
    };
    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: page.results,
            genres: { 35: 'Comedy' },
            credits: {
              12: {
                cast: [{ name: 'Comedic Star' }],
                crew: [{ job: 'Director', name: 'Funny Director' }]
              }
            }
          })
      }
    ]);

    await initMoviesPanel();

    const watchedBtn = Array.from(document.querySelectorAll('#movieList li button')).find(
      b => b.textContent === 'Watched Already'
    );
    watchedBtn?.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(promptSpy).toHaveBeenCalledTimes(1);
    const promptMessage = promptSpy.mock.calls[0][0] || '';
    expect(promptMessage).toContain('Rate "Prompt Test"');

    const personalRatingInput = document.querySelector(
      '#watchedMoviesList .movie-personal-rating input'
    );
    expect(personalRatingInput?.value).toBe('8.5');
  });

  it('sorts watched movies by rating when selected', async () => {
    const dom = buildDom();
    attachWindow(dom);
    window.tmdbApiKey = 'TEST_KEY';

    const page = {
      results: [
        {
          id: 20,
          title: 'Low Rated',
          release_date: '2020-01-01',
          vote_average: 7.4,
          vote_count: 200,
          overview: 'Solid enough.',
          genre_ids: []
        },
        {
          id: 21,
          title: 'High Rated',
          release_date: '2021-05-10',
          vote_average: 9.4,
          vote_count: 850,
          overview: 'Critically acclaimed.',
          genre_ids: []
        }
      ],
      total_pages: 1
    };
    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: page.results,
            genres: {},
            credits: {
              20: { cast: [], crew: [] },
              21: { cast: [], crew: [] }
            }
          })
      }
    ]);

    await initMoviesPanel();

    const cards = Array.from(document.querySelectorAll('#movieList li'));
    const lowCard = cards.find(card => card.textContent.includes('Low Rated'));
    const highCard = cards.find(card => card.textContent.includes('High Rated'));
    const lowWatch = lowCard
      ? Array.from(lowCard.querySelectorAll('button')).find(b => b.textContent === 'Watched Already')
      : null;
    const highWatch = highCard
      ? Array.from(highCard.querySelectorAll('button')).find(b => b.textContent === 'Watched Already')
      : null;

    expect(lowWatch).toBeTruthy();
    expect(highWatch).toBeTruthy();

    lowWatch?.click();
    highWatch?.click();
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(document.querySelectorAll('#watchedMoviesList .movie-card')).toHaveLength(2);

    const sortSelect = document.getElementById('watchedMoviesSort');
    sortSelect.value = 'ratingDesc';
    sortSelect.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await new Promise(resolve => setTimeout(resolve, 0));

    const titlesDesc = Array.from(
      document.querySelectorAll('#watchedMoviesList .movie-card h3')
    ).map(el => el.textContent);
    expect(titlesDesc[0]).toContain('High Rated');
    expect(titlesDesc[1]).toContain('Low Rated');

    sortSelect.value = 'ratingAsc';
    sortSelect.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await new Promise(resolve => setTimeout(resolve, 0));

    const titlesAsc = Array.from(
      document.querySelectorAll('#watchedMoviesList .movie-card h3')
    ).map(el => el.textContent);
    expect(titlesAsc[0]).toContain('Low Rated');
    expect(titlesAsc[1]).toContain('High Rated');
  });

  it('saves preferences to Firestore for authenticated users', async () => {
    const dom = buildDom();
    attachWindow(dom);

    const userId = 'user123';
    authModuleMock.awaitAuthUser.mockResolvedValue({ uid: userId });
    authModuleMock.getCurrentUser.mockReturnValue({ uid: userId });
    firestoreDocMock.get.mockResolvedValue({ exists: true, data: () => ({ prefs: {} }) });

    const page = {
      results: [
        {
          id: 9,
          title: 'Firestore Movie',
          release_date: '2024-04-01',
          vote_average: 7,
          vote_count: 120,
          overview: 'Stored remotely.',
          genre_ids: []
        }
      ]
    };
    configureFetchResponses([
      {
        ok: true,
        json: () =>
          Promise.resolve({
            results: page.results,
            genres: {},
            credits: {
              9: {
                cast: [{ name: 'Remote Star' }],
                crew: [{ job: 'Director', name: 'Remote Director' }]
              }
            }
          })
      }
    ]);

    await initMoviesPanel();

    const interestedBtn = Array.from(document.querySelectorAll('#movieList li button')).find(
      b => b.textContent === 'Interested'
    );
    interestedBtn?.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(firestoreDocMock.set).toHaveBeenCalled();
  });
});
