# Token Tracker

[简体中文](README.zh-CN.md)

Collect AI coding token usage locally, upload scalar counts to PostgreSQL through Vercel functions, and view your private usage dashboard. Prompts and message bodies are never uploaded. Single-user by design: one access key from the environment, no accounts.

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenTracker&project-name=tokentracker&repository-name=tokentracker&env=DATABASE_URL%2CTOKENTRACKER_ACCESS_KEY&envDescription=DATABASE_URL%3A%20PostgreSQL%20connection%20string%20%28Neon%20or%20any%20Vercel%20Postgres%20integration%29.%20TOKENTRACKER_ACCESS_KEY%3A%20your%20tt_%3C64%20hex%3E%20access%20key%20-%20the%20single%20shared%20secret%20for%20uploads%20and%20the%20dashboard.%20Both%20are%20server-only%3A%20never%20prefix%20them%20with%20VITE_.&envLink=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenTracker%2Fblob%2Fmain%2Fdocs%2FVERCEL.md)
[![npm](https://img.shields.io/npm/v/tokentracker-cli.svg)](https://www.npmjs.com/package/tokentracker-cli)

![Usage dashboard](docs/screenshots/dashboard-usage-light.png)

## Getting started

### 1. Deploy the server

Click **Deploy with Vercel** above, or follow [Vercel deployment](docs/VERCEL.md). The button clones this repo into your account and asks for two **server-only** environment variables:

- `DATABASE_URL` — a PostgreSQL connection string (a free Neon database works; prefer the pooled URL).
- `TOKENTRACKER_ACCESS_KEY` — your private key in the `tt_<64 hex>` form, e.g. `tt_` + the output of `openssl rand -hex 32`. Uploads and the dashboard both authenticate with this one key.

Then, once per deployment, apply the schema to that database:

```bash
psql "$DATABASE_URL" -f server/schema.sql   # or paste server/schema.sql into the SQL editor
```

There is no account system and nothing to provision in a database — rotating the key is just changing the environment variable.

### 2. Install the collector with npx

```bash
npx tokentracker-cli@latest init --yes
npx tokentracker-cli@latest connect --api-url https://your-project.vercel.app --key tt_…
npx tokentracker-cli@latest sync
```

`init` copies a self-contained runtime (`bin` + `src` + runtime dependencies) to `~/.tokentracker/app` and writes a dependency-free `~/.tokentracker/bin/notify.cjs` handler, so the provider hooks run `node ~/.tokentracker/app/bin/tracker.js` and never depend on the npx cache — the handler only falls back to `npx --yes tokentracker-cli sync --auto` if that copy is missing. No global install is needed. Prefer a permanent command? `npm i -g tokentracker-cli`, then drop the `npx` prefix (`tokentracker init --yes`, …).

`connect` persists the upload target in `~/.tokentracker/tracker/upload.json` with mode `600`, so background hook-spawned syncs upload without shell-profile changes; `TOKENTRACKER_API_URL` / `TOKENTRACKER_ACCESS_KEY` still override it per invocation. `sync` collects usage and uploads it, `upload` replays existing local history, `status` prints queue/backlog/upload diagnostics.

### 3. Open the dashboard

Open the deployed site and enter the same `tt_…` key — the browser keeps it in session storage only.

## Updates and automation

| Trigger | What runs |
|---|---|
| Push to `main` | Vercel's Git integration redeploys the server automatically (and `.github/workflows/ci.yml` runs the test suite) |
| Tag `v*` pushed | `.github/workflows/publish-npm.yml` — `npm publish`, so `npx tokentracker-cli@latest` picks it up |

In Vercel, import the GitHub repo once (New Project → import → the settings in `vercel.json` apply); after that every push to `main` deploys automatically. The publish workflow needs the `NPM_TOKEN` secret — without it the job logs a notice and skips, so forks stay green. Details: [docs/VERCEL.md](docs/VERCEL.md).

Development checks: `npm ci && npm ci --prefix dashboard && npm run ci:local`. The integration test exercises PostgreSQL SQL through PGlite without production credentials.

[MIT](LICENSE)
