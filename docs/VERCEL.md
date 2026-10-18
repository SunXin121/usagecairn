# Vercel + PostgreSQL deployment

Waline-style: import the GitHub repo into Vercel once, set one environment variable, attach a database through Vercel Storage, apply the schema once — done. This is a single-user deployment; the one access key lives in the environment, so there are no accounts to provision.

The repo root is the Vercel project root. `vercel.json` installs the root and dashboard dependencies (`npm ci && npm ci --prefix dashboard`), builds `dashboard/dist` and deploys `api/ingest.js` and `api/usage.js` as Node functions. Vercel does **not** persist database files on function disk, so PostgreSQL must come from an integration or any reachable instance.

## One-click deploy

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenTracker&project-name=tokentracker&repository-name=tokentracker&env=ACCESS_KEY&envDescription=Your%20tt_%3C64%20hex%3E%20access%20key%20-%20the%20single%20shared%20secret%20for%20uploads%20and%20the%20dashboard.%20Server-only%3A%20never%20prefix%20it%20with%20VITE_.%20The%20database%20is%20attached%20afterwards%20via%20Vercel%20Storage%20%28it%20injects%20DATABASE_URL%20automatically%29.&envLink=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenTracker%2Fblob%2Fmain%2Fdocs%2FVERCEL.md)

The button clones this repo into your own account, creates a project named `tokentracker`, and prompts for the one **server-only** environment variable, `ACCESS_KEY`. After the deploy, attach the database in the project's **Storage** tab (which injects `DATABASE_URL` automatically) and apply [`server/schema.sql`](../server/schema.sql) once in the database SQL editor — there is no other provisioning step. The numbered steps below are the manual equivalent of the button.

## Manual setup

1. Import this repo in Vercel (New Project → import the GitHub repo, framework preset **Other**, root directory `.`). Build/install commands and output directory come from `vercel.json`.
2. Set one **server-only** environment variable on the project (Production + Preview + Development):
   - `ACCESS_KEY` — your private key in the form `tt_<64 lowercase hex>`, e.g. `tt_$(openssl rand -hex 32)`. The server hashes it with SHA-256 and compares with `timingSafeEqual`; it never appears in the database or logs. Uploads (`POST /api/ingest`) and the dashboard (`GET /api/usage`) both authenticate with `Authorization: Bearer tt_…`. Rotating the key is just editing this variable and redeploying.
3. Attach a PostgreSQL database in the project's **Storage** tab (Marketplace integration, e.g. Neon). Vercel injects `DATABASE_URL` into the functions automatically — you never paste a connection string. Use the integration's SQL editor (or `psql` with its connection string) to run [`server/schema.sql`](../server/schema.sql) once; it creates `usage_buckets` and `account_session_states`, and there is no accounts table.

Never prefix `ACCESS_KEY` with `VITE_` or put it in frontend configuration. Requests without a valid key get `401`; if the key is missing or malformed the API answers `503` instead of silently opening up.

## Deploys on push (Git integration)

Once the repo is imported, Vercel's Git integration redeploys the server on every push to `main` (and creates preview deployments for branches/PRs) — no extra workflow or Vercel token secrets needed. `.github/workflows/ci.yml` still runs the test suite on the same pushes; keep CI green so you only deploy working revisions. The CLI is published to npm separately: pushing a `v*` tag runs `.github/workflows/publish-npm.yml` (`npm publish --provenance`; needs the `NPM_TOKEN` repository secret, otherwise the job logs a notice and skips).

## Installing the collector with npx

```bash
npx tokentracker-cli@latest init --yes
npx tokentracker-cli@latest connect --api-url https://<deployment>.vercel.app --key tt_…
npx tokentracker-cli@latest sync
```

`npx` only bootstraps: `init` copies a self-contained runtime (`bin` + `src` + runtime dependencies) to `~/.tokentracker/app`, and the provider hooks run `node ~/.tokentracker/app/bin/tracker.js`, so nothing depends on the npx cache. If that copy is ever missing, `~/.tokentracker/bin/notify.cjs` falls back to `npx --yes tokentracker-cli sync --auto`. For a permanent command use `npm i -g tokentracker-cli` (bins: `tokentracker`, `tokentracker-cli`, `tracker`). Inside a repo checkout `node bin/tracker.js …` is equivalent. The published package ships only `bin/` and `src/` — `server/`, `api/` and `dashboard/` stay in the repo for Vercel.

`connect` stores the destination and key in `~/.tokentracker/tracker/upload.json` with mode `600`, so hook-spawned background syncs upload without touching your shell profile; `TOKENTRACKER_API_URL` / `TOKENTRACKER_ACCESS_KEY` still override per invocation on the collector side (`connect --show` prints the current destination, `connect --clear` removes it). `sync` scans provider records into `~/.tokentracker/tracker/queue.jsonl` and uploads snapshots; `upload` replays the same local history after a failure and is safe to repeat. On the deployed dashboard enter the same key — the browser keeps it in session storage only, and sign out clears it. Use HTTPS; never share the key or put it in screenshots, URLs or extensions.

## API

`GET /api/usage?from=YYYY-MM-DD&to=YYYY-MM-DD` returns UTC daily/model totals and estimated USD cost. The date interval is at most 10 years. `POST /api/ingest` accepts up to 200 numeric usage buckets/session states per request. Both require `Authorization: Bearer tt_…` matching the server's `ACCESS_KEY`. A `(device, source, model, hour)` bucket carries the source-file modification time and queue byte position, so an older replay cannot overwrite a newer snapshot; identical replays are idempotent. Cursor usage is deduplicated across devices by the largest whole snapshot; TRAE CN uses per-session freshness instead of summing mirrored device buckets. Requests carry only timestamps, source/model names, session IDs for TRAE CN, scalar costs and token counters — never prompts or messages.

## Local verification

`npm run ci:local` runs collector/parser tests, an in-memory PostgreSQL (PGlite) schema→upload→read test (including the env-key authentication paths), browser component tests and a production dashboard build. This requires no cloud account. For interactive local development, attach a real PostgreSQL integration (or export a `DATABASE_URL` pointing at one) and export `ACCESS_KEY=tt_…`, then run `npx vercel dev --listen 3000` in the repo root, and `npm run dashboard:dev` in another terminal (Vite proxies `/api` to port 3000). A real Postgres connection is needed only for manual API testing; tests use PGlite.

**Operational note:** the CLI keeps an append-only local queue and replays it on each upload. Large histories can take longer; a failed request leaves the queue untouched. Cost uses the repository's model pricing snapshot; unavailable model prices are displayed as $0 estimates, not proof that the provider is free.
