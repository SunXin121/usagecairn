# Contributing

This repository has three parts:

| Area | Path | Stack |
|---|---|---|
| Local collector | `src/`, `bin/` | Node ≥20, CommonJS |
| Server storage | `server/`, `api/` | Node functions + PostgreSQL |
| Dashboard | `dashboard/` | React 18, Vite, Tailwind |

## Setup

```bash
npm ci
npm ci --prefix dashboard
```

## Checks (must pass before a PR)

```bash
npm run ci:local   # collector + server tests, dashboard tests, production build
```

Individual pieces:

```bash
npm test                     # node --test on the collector/server suite
npm run dashboard:test       # vitest (jsdom)
npm run dashboard:build      # vite build → dashboard/dist
```

## Local workflow

```bash
node bin/token-pulse.js init --yes                       # install provider hooks
node bin/token-pulse.js connect --api-url <url> --key tt_…   # persist upload target
node bin/token-pulse.js sync                             # parse provider logs, then upload
node bin/token-pulse.js status                           # queue/backlog/upload diagnostics
```

`npm --prefix dashboard run dev` serves the dashboard with `/api` proxied to
`http://localhost:3000`; run `npx vercel dev --listen 3000` alongside it to hit
the real functions (a PostgreSQL `DATABASE_URL` is required for that).

## Conventions

- CommonJS in `src/` and `server/`, ESM/JSX in `dashboard/`.
- Token counts only — never store or upload prompts, responses or message bodies.
- `dashboard/src/styles.css` and `dashboard/tailwind.config.cjs` hold the design
  tokens; keep them in sync with `docs/VISUAL.md` when the UI changes.
- Commits: conventional style (`feat:` / `fix:` / `refactor:` / `docs:` / `test:` / `ci:`).
