# Token Pulse

[简体中文](README.zh-CN.md)

Collect AI coding token usage locally, upload scalar counts to PostgreSQL through Vercel functions, and view your private usage dashboard. Prompts and message bodies are never uploaded. Single-user by design: one access key from the environment, no accounts.

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenPulse&project-name=token-pulse&repository-name=token-pulse&env=ACCESS_KEY&envDescription=Your%20access%20key%20-%20the%20single%20shared%20secret%20for%20uploads%20and%20the%20dashboard.%20Server-only%3A%20never%20prefix%20it%20with%20VITE_.%20The%20database%20is%20attached%20afterwards%20via%20Vercel%20Storage%20%28it%20injects%20DATABASE_URL%20automatically%29.&envLink=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenPulse%2Fblob%2Fmain%2Fdocs%2FVERCEL.md)
[![npm](https://img.shields.io/npm/v/token-pulse.svg)](https://www.npmjs.com/package/token-pulse)

![Usage dashboard](docs/screenshots/dashboard-usage-light.png)

## Getting started

### 1. Deploy the server

Click **Deploy with Vercel** above, or follow [Vercel deployment](docs/VERCEL.md). The button clones this repo into your account and asks for one **server-only** environment variable:

- `ACCESS_KEY` — your private access key, any non-empty string. Uploads and the dashboard both authenticate with this one key.

The database needs no environment variable: after the deploy, attach a PostgreSQL database in the project's **Storage** tab (Neon or any Vercel Postgres integration) and Vercel injects `DATABASE_URL` automatically. Then, once per database, apply the schema:

```bash
psql "$DATABASE_URL" -f server/schema.sql   # or paste server/schema.sql into the SQL editor
```

There is no account system and nothing to provision in a database — rotating the key is just changing the environment variable.

### 2. Install the collector with npx

```bash
npx token-pulse@latest init --yes
npx token-pulse@latest connect --api-url https://your-project.vercel.app --key <your key>
npx token-pulse@latest sync
```

`init` copies a self-contained runtime (`bin` + `src` + runtime dependencies) to `~/.token-pulse/app` and writes a dependency-free `~/.token-pulse/bin/notify.cjs` handler, so the provider hooks run `node ~/.token-pulse/app/bin/token-pulse.js` and never depend on the npx cache — the handler only falls back to `npx --yes token-pulse sync --auto` if that copy is missing. No global install is needed. Prefer a permanent command? `npm i -g token-pulse`, then drop the `npx` prefix (`token-pulse init --yes`, …).

`connect` persists the upload target in `~/.token-pulse/tracker/upload.json` with mode `600`, so background hook-spawned syncs upload without shell-profile changes; the collector-side `TOKEN_PULSE_API_URL` / `TOKEN_PULSE_ACCESS_KEY` still override it per invocation. `sync` collects usage and uploads it, `upload` replays existing local history, `status` prints queue/backlog/upload diagnostics.

### 3. Open the dashboard

Open the deployed site and enter the same access key — the browser keeps it in session storage only.

## Updates and automation

| Trigger | What runs |
|---|---|
| Push to `main` | Vercel's Git integration redeploys the server automatically (and `.github/workflows/ci.yml` runs the test suite) |
| Tag `v*` pushed | `.github/workflows/publish-npm.yml` — `npm publish`, so `npx token-pulse@latest` picks it up |

In Vercel, import the GitHub repo once (New Project → import → the settings in `vercel.json` apply); after that every push to `main` deploys automatically. The publish workflow needs the `NPM_TOKEN` secret — without it the job logs a notice and skips, so forks stay green. Details: [docs/VERCEL.md](docs/VERCEL.md).

Development checks: `npm ci && npm ci --prefix dashboard && npm run ci:local`. The integration test exercises PostgreSQL SQL through PGlite without production credentials.

[MIT](LICENSE)
