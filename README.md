# UsageCairn

[简体中文](README.zh-CN.md) · [MIT License](LICENSE)

UsageCairn collects AI agent token usage on your machine, uploads **counts and statistical metadata** to a server you deploy, and displays usage and estimated cost by date, source, and model. It does not upload prompts, assistant replies, or full transcripts. This is a single-user deployment: one private access key protects both the collector and the dashboard; there are no user accounts.

## How the pieces fit together

```text
Local agent logs / SQLite / usage APIs
       ↓ incremental parsing (hooks or manual sync)
~/.token-pulse/tracker/queue.jsonl + parser cursors
       ↓ HTTPS + Bearer ACCESS_KEY (only after connecting)
Vercel /api/ingest → PostgreSQL → /api/usage → browser dashboard
```

You can collect usage locally without deploying anything: skip `connect` and run `sync`. The web dashboard requires a deployed server and a connected collector.

## Prerequisites

- **Node.js 20+** and npm on the machine running your agents; macOS, Linux, and Windows are supported (some sources can also detect Windows/WSL installs).
- For the online dashboard: a Vercel project and PostgreSQL database, such as Neon through Vercel Storage/Marketplace; a SQL editor or `psql` to initialize the schema.
- A supported agent with accessible usage records. An initial sync can show zero if there is no history yet.

## 1. Deploy the server and dashboard

1. Import this repository into Vercel using [one-click deploy](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2FSunXin121%2Fusagecairn&project-name=usagecairn&repository-name=usagecairn&env=ACCESS_KEY) or **New Project → Import Git Repository**. Use the repository root as the project root. `vercel.json` defines installation, the `dashboard` build, static routing, and `/api/*` functions. The API may not work until you connect and initialize the database.
2. Set `ACCESS_KEY` in **Settings → Environment Variables** as a **server-only** secret. Generate and save a strong value locally:

   ```bash
   printf 'tt_%s\n' "$(openssl rand -hex 32)"
   ```

   Paste that value into `ACCESS_KEY`; never use a `VITE_` prefix or commit it to Git. Any non-empty value is accepted; the example format is a recommendation. Redeploy after changing environment variables.
3. Attach PostgreSQL under the Vercel project's **Storage** tab and ensure the integration supplies `DATABASE_URL` to the relevant deployment environment. If you connected it after the first deployment, redeploy from **Deployments**. An external PostgreSQL database also works if you set its `DATABASE_URL` in the server environment yourself.
4. Apply [`server/schema.sql`](server/schema.sql) once to that database, either in its SQL editor or with `psql` from a machine with database access:

   ```bash
   psql "$DATABASE_URL" -f server/schema.sql
   ```

   `$DATABASE_URL` here must be available **in the shell running `psql`**: Vercel's injected variable is not automatically available on your laptop. Obtain a connection string from the database console or use the SQL editor. Your site URL will look like `https://your-project.vercel.app`. The collector does not need database credentials.

The API exposes `POST /api/ingest` and `GET /api/usage`; both require `Authorization: Bearer <ACCESS_KEY>`. There is no accounts table or public sign-up. Do not put the access key in public build variables.

## 2. Install the collector locally

Run these commands **on the machine where your agents run**, not in Vercel. No global npm installation is required:

```bash
npx usagecairn@latest init --dry-run                 # optional: preview integrations
npx usagecairn@latest init --yes                     # install hooks and try the first sync
npx usagecairn@latest connect --api-url https://your-project.vercel.app --key 'your access key'
npx usagecairn@latest sync                           # scan all supported sources and upload
npx usagecairn@latest status                         # check destination, backlog, and last parse
```

Replace the example site and key. `--api-url` must be the **HTTPS site origin**, without `/api/ingest`, another path, or query parameters (`http://localhost` and `127.0.0.1` are accepted for local development). `init` changes hook/plugin configurations for detected agents, so preview with `--dry-run` if you have custom hooks. Other agents are read passively by `sync`. Because `init` attempts an initial sync, running `init` before `connect` stores that first snapshot locally; the subsequent `sync` uploads it.

For a global command use `npm install -g usagecairn` and replace `npx usagecairn@latest` above with `usagecairn`. To run the collector from this checkout, run `npm ci`, then `node bin/usagecairn.js init --yes`, `node bin/usagecairn.js sync`, etc.

`init` copies the runtime and its production dependencies into `~/.token-pulse/tracker/app/` and writes a dependency-free hook entry point at `~/.token-pulse/bin/notify.cjs`. Hooks run that local copy through Node.js, without relying on the npx cache; npx is a fallback if the local runtime is missing. Parser cursors, the aggregated `queue.jsonl` ledger, and upload progress live in `~/.token-pulse/tracker/`. `connect` saves the **raw access key** and URL to `~/.token-pulse/tracker/upload.json` (mode 600 where supported); protect your local account and this file. `TOKEN_PULSE_API_URL` and `TOKEN_PULSE_ACCESS_KEY` override saved credentials for an invocation. Saved credentials let background hooks upload without shell-profile changes. On Windows, `~` means your user home and file permissions follow the OS.
For existing installations, the legacy `~/.token-pulse` state directory, `TOKEN_PULSE_*` environment overrides, and `token-pulse-theme` browser preference key remain in use so upgrades do not strand queued usage, parser cursors, saved credentials, or theme settings. Keep this directory when updating; `uninstall --purge` permanently removes it.

| Command | Purpose |
| --- | --- |
| `init --dry-run` / `init --yes` | Preview / install integrations and local runtime; rerun `init` to update the hook runtime |
| `connect --show` / `connect --clear` | Show redacted connection details / remove local saved credentials |
| `sync` | Incrementally scan all sources, then upload outstanding records if connected |
| `upload` / `upload --replay` | Upload only new records / resend the complete local ledger (e.g. after a server repair) |
| `status` / `status --json` | Inspect parsing, destination, and pending bytes; the displayed local sources are **not** a complete support list |
| `uninstall` / `uninstall --purge` | Remove collector integrations; check whether you need local history before purging |

## 3. Open the dashboard and troubleshoot

Open your deployed site and enter the same key used by `connect`. The browser holds it in `sessionStorage` for the current session; the dashboard requests date/source/model aggregates from the protected `/api/usage` endpoint. Cost is an **estimate**, not a vendor invoice. The default range is about 30 days; custom ranges are limited to 10 years. Queries exceeding **50,000** raw rows fail instead of returning incomplete totals: shorten the date range.

- **401**: verify the browser key matches Vercel's `ACCESS_KEY` and that changes were redeployed. `connect --show` displays your local destination with a redacted key.
- **Server error**: inspect Vercel function logs, `DATABASE_URL`, and whether `server/schema.sql` was applied to the connected database.
- **Empty dashboard**: run `sync` and `status`; check for local agent usage, a successful `connect`, and pending queue bytes. The “Local sources” line in `status` is not a comprehensive detection report.
- **Offline/upload failure**: queued records stay on disk for the next `sync` or `upload`. If server data was repaired and the local ledger is intact, use `upload --replay`.

## Supported agents and data sources

The collector uses usage fields **actually exposed by each tool**. A local installation with readable logs/database or a valid sign-in is necessary; missing token counts cannot be reconstructed. Field coverage depends on source, version, and platform; some tools only provide a total or an estimate.

| Integration | Agents / tools | Notes |
| --- | --- | --- |
| Hooks/plugins installed by `init`, plus local scans | Claude Code, Codex CLI, AStudio, Every Code, Gemini CLI, OpenCode, OpenClaw, Grok Build, oh-my-pi (omp), CodeBuddy, WorkBuddy, WorkBuddy AI | Events trigger background sync; restart the OpenClaw gateway after installing its session plugin; manual `sync` can backfill local records |
| Passive local logs / databases / telemetry | Antigravity, Kiro, Hermes Agent, GitHub Copilot, Kimi Code, OmO, pi, PI-Desktop, Prime Agent, Craft Agents, Reasonix, Muse Code, Kilo CLI, Kilo Code, Roo Code, Zed Agent, Goose, Droid, Mimo, DuMate, ZCode, Qoder (including CN), AnythingLLM Desktop, Claude Science, DeepSeek Harness, TRAE, LM Studio, Unsloth Studio, Devin CLI, Cline, MiniMax Code, Command Code | No hook required; run `sync` to ensure new records are scanned. PI-Desktop reads usage metadata from `~/.pi-desktop/sessions/*.jsonl` as a separate source. Kiro, Copilot, etc. have multiple local storage formats; availability depends on the installed version |
| Vendor usage APIs | Cursor, TRAE Work CN | Cursor requires a local sign-in. TRAE Work CN is **opt-in**: `TOKEN_PULSE_TRAE_CN_USAGE=1` permits use of locally stored login credentials to fetch the last 30 days; no request is made by default |

`pi` and `oh-my-pi` are separate sources; so are Kilo CLI and the Kilo Code editor extension. Use `init --dry-run` to inspect proposed integrations. Not every passive reader appears in the `init` summary or `status`. Hooks only fire on relevant agent events: run `sync` manually (or configure your own scheduled task) to pick up passive sources and history.

## Internals and privacy boundary

1. **Discover and parse incrementally**: `sync` reads local JSONL, SQLite, logs, or explicitly enabled usage APIs. Cursors and event identities prevent repeated parsing; usage is grouped by source, model, and time. Hooks trigger a background `sync`; they do not send transcript contents to the server.
2. **Persist and upload**: aggregate snapshots go into the local `queue.jsonl`. Only after configuring a destination does the collector send device ID, source, model, timestamps, token breakdown, conversation count, and cost fields to `/api/ingest`; some account-level sources also send session usage state for cross-device deduplication. Prompts, responses, and complete sessions are not included in upload payloads. Upload progress is recorded separately for retries.
3. **Store and summarize**: Vercel functions check the shared key and write PostgreSQL snapshots. Upserts keyed by device/source/model/time prevent replay inflation; account-level session state supports deduplication where applicable. `/api/usage` aggregates by day and model, computes estimated costs from server-side pricing data or source-provided cost, and returns the result to the browser. The server holds no user accounts; the same access key must be held by the collector and browser.

Raw agent logs stay in their original directories. The local `~/.token-pulse/tracker/` data and PostgreSQL aggregates are still personal usage data: protect and back them up as appropriate. Rotating `ACCESS_KEY` also requires rerunning `connect` on every collector and entering the new key in each browser session.

## Updates and development checks

With Vercel's Git integration, pushes to `main` redeploy the server. Pushing a `v*` tag invokes the npm publish workflow (requires the repository's `NPM_TOKEN` secret; without it publication is skipped). Rerun `npx usagecairn@latest init --yes` after updating the collector to refresh the hook's local runtime. Check the source locally with:

```bash
npm ci
npm ci --prefix dashboard
npm run ci:local
```

These commands run Node tests, dashboard tests, and the dashboard build. Database integration tests use PGlite and do not require production credentials.
