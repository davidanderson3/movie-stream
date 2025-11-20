# Dashboard

Dashboard is a personal decision-making and entertainment hub focused on movie discovery. The front end is a vanilla JavaScript single-page experience backed by Firebase for auth/persistence and a lightweight Express server for API proxying, caching, and scheduled scripts.

## Table of Contents
- [Feature Tour](#feature-tour)
  - [Movies](#movies)
  - [Backups, Restore, and Settings Utilities](#backups-restore-and-settings-utilities)
- [Architecture Overview](#architecture-overview)
- [Configuration & Required Secrets](#configuration--required-secrets)
- [Local Development](#local-development)
- [Testing](#testing)
- [Troubleshooting Checklist](#troubleshooting-checklist)

## Feature Tour

### Movies
The Movies tab is a curated discovery feed for film night:
- **Three collections** – a live "Movie Stream" feed, a "Saved" list you can curate, and a "Watched" archive with ratings.
- **Quality filters** – filter the stream by minimum TMDB rating, vote count, release year window, and genre before requesting more titles.
- **Genre controls** – toggle a single focus genre or exclude any number of genres with pill chips; selections persist between visits and flow into TMDB `without_genres` requests.
- **Progressive discovery** – the client keeps paging through TMDB Discover results until it finds enough titles that meet the quality threshold (`vote_average ≥ 7` and `vote_count ≥ 50` by default).
- **Personal ratings** – mark any movie as Interested, Watched, or Not Interested. Ratings are clamped to 0–10 with half-point granularity.
- **Saved list persistence** – lists and ratings are stored both locally and in Firestore so they follow the authenticated user.
- **TMDB integration** – the UI accepts either a direct TMDB API key or uses the deployed Cloud Function proxy (`/tmdbProxy`) to keep the client keyless.
- **Critic score lookup** – pull Rotten Tomatoes, Metacritic, and IMDb ratings from the OMDb-backed proxy for movie titles when you need more context.

### Backups, Restore, and Settings Utilities
Separate helper pages (`backup.json`, `restore.html`, `settings.html`) provide advanced utilities:
- **Export/import** routines for Firestore collections and locally cached preferences.
- **Environment-specific tweaks** – scripts in `scripts/` automate geolocation imports, travel KML updates, and alert workflows.
- **Monitoring aides** – Node scripts (e.g., `scripts/tempAlert.js`) integrate with Twilio or email to surface anomalies.

## Architecture Overview
- **Front end** – A hand-rolled SPA in vanilla JS, HTML, and CSS. Each tab has a dedicated module under `js/` that owns its DOM bindings, local storage, and network calls.
- **Auth & persistence** – Firebase Auth (Google provider) and Firestore handle user login state plus long-term storage for movies, tab descriptions, and other preferences. Firestore is initialized with persistent caching so the UI stays responsive offline.
- **Server** – `backend/server.js` is an Express app that serves the static bundle, proxies external APIs (TMDB Discover, OMDb, Spoonacular), and exposes helper routes for descriptions, saved movies, Plaid item creation, etc. It also normalizes responses and caches expensive calls to protect third-party rate limits.
- **Cloud Functions** – The `functions/` directory mirrors much of the server logic for deployments that rely on Firebase Functions instead of the local Express instance.
- **Shared utilities** – Reusable helpers live under `shared/` (e.g., caching primitives) so both the server and Cloud Functions share a single implementation.
- **Node scripts** – `scripts/` contains operational tooling for geodata imports, monitoring, and static asset generation. They rely on environment variables documented below.

## Configuration & Required Secrets
Create a `.env` in the project root (and optionally `backend/.env`) with the credentials you intend to use. Common settings include:

| Variable | Used By | Purpose |
| --- | --- | --- |
| `PORT` | Express server | Override the default `3003` port. |
| `HOST` | Express server | Bind address; defaults to `0.0.0.0`. |
| `SPOTIFY_CLIENT_ID` | `/api/spotify-client-id` | PKCE client ID for Spotify login. |
| `SPOONACULAR_KEY` | Spoonacular proxy | API key for recipe search. |
| `OMDB_API_KEY` (or `OMDB_KEY`/`OMDB_TOKEN`) | Movie ratings proxy | OMDb key for Rotten Tomatoes and Metacritic lookups. |
| `PLAID_CLIENT_ID`, `PLAID_SECRET`, `PLAID_ENV` | Plaid endpoints | Enable financial account linking workflows. |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS` | `/contact` endpoint | Enable contact form email delivery. |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `ALERT_PHONE` | `scripts/tempAlert.js` | SMS alerts for monitoring. |

Remember to also configure Firebase (see `firebase.json` and `.firebaserc`) if you deploy hosting or Cloud Functions.

## Local Development
1. **Install dependencies**
   ```bash
   npm install
   ```
2. **Start the backend**
   ```bash
   npm start
   ```
   This launches the Express server on `http://localhost:3003` and serves `index.html` plus the API proxies.
3. **Set up API keys** – Supply environment variables for any services you plan to use (e.g., TMDB, OMDb, Spoonacular).
4. **Optional Firebase emulators** – If you prefer not to use the production Firestore project during development, configure the Firebase emulator suite and point the app to it.

## Testing
- **Unit/integration tests** – run `npm test` to execute the Vitest suite (covers movie discovery flows, caching, and supporting helpers).
- **End-to-end tests** – run `npm run e2e` to launch Playwright scenarios when the supporting services are available.

## Troubleshooting Checklist
- **TMDB config unavailable** – ensure either `TMDB_API_KEY` is present locally or the `/tmdbProxy` Cloud Function is deployed and reachable.
- **Empty Discover results** – loosen filters (min rating, votes, or genre exclusions) so TMDB can return enough matches.
- **Spoonacular quota errors** – the proxy caches responses for six hours; if you keep seeing rate-limit messages clear the cache collection in Firestore or wait for the TTL to expire.
- **Firestore permission denials** – authenticate with Google using the Sign In button; most persistence features require a logged-in user.
- **Contact form email failures** – double-check SMTP credentials and allow-list the sender before testing locally.
