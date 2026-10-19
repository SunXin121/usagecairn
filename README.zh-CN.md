# Token Pulse

[English](README.md) · [MIT 许可证](LICENSE)

Token Pulse 在你的电脑上读取 AI Agent 的用量记录，将 **token 计数与必要的统计元数据** 上传到你自己部署的服务，并通过网页看板按日期、来源和模型查看用量及估算费用。它不会把提示词、回复正文或完整会话上传到服务端。部署为单用户模式：没有注册系统，采集端和看板共用一个私有访问密钥。

## 架构概览

```text
本机 Agent 日志 / SQLite / 用量 API
          ↓ 本地增量解析（钩子触发或手动 sync）
~/.token-pulse/tracker/queue.jsonl  + 解析游标
          ↓ HTTPS + Bearer ACCESS_KEY（仅配置上传后）
Vercel /api/ingest → PostgreSQL → /api/usage → 浏览器看板
```

只想在本地采集也可以：不执行 `connect`，运行 `sync` 后数据保留在本机，不会上传；网页看板需要部署服务端并连接采集端。

## 前置条件

- 本机安装 **Node.js 20 或更新版本**和 npm；支持 macOS、Linux、Windows（部分来源可识别 Windows/WSL 安装，具体以本机数据为准）。
- 在线看板需要 Vercel 项目和 PostgreSQL 数据库（例如 Vercel Storage/Marketplace 集成的 Neon）；初始化数据库需要 SQL 编辑器或 `psql`。
- 至少有一个受支持的 Agent 在本机留下用量记录；没有历史数据时初次同步可能显示 0。

## 1. 部署服务端与看板

1. 将本仓库导入 Vercel（[一键部署](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2FSunXin121%2FTokenPulse&project-name=token-pulse&repository-name=token-pulse&env=ACCESS_KEY) 或 Vercel **New Project → Import Git Repository**）。保持仓库根目录作为项目根目录；`vercel.json` 已设置依赖安装、`dashboard` 构建、静态页面和 `/api/*` 函数。首次部署时数据库尚未连接，API 暂不可用是正常的。
2. 在 Vercel 的 **Settings → Environment Variables** 设置仅供服务端使用的 `ACCESS_KEY`。建议在本机生成随机密钥并妥善保存：

   ```bash
   printf '%s\n' "$(openssl rand -hex 32)"
   ```

   把输出值填到 `ACCESS_KEY`，修改环境变量后需要重新部署才能让部署生效。
3. 在 Vercel 项目的 **Storage** 页连接 PostgreSQL 数据库，并确保集成为当前部署环境注入 `DATABASE_URL`。如果数据库是在首次部署后绑定的，请在 **Deployments** 中重新部署，确保函数拿到连接变量。非 Vercel 集成的数据库也可用，但须自行在服务端环境变量中设置其 `DATABASE_URL`。
4. 对该数据库执行一次仓库内的 [`server/schema.sql`](server/schema.sql)。可以在数据库 SQL 编辑器中粘贴执行；也可以在已安装 `psql` 的机器上（需有该数据库连接权限）执行：

   ```bash
   psql "$DATABASE_URL" -f server/schema.sql
   ```

   这条命令中的 `$DATABASE_URL` 是**运行命令的机器**上的环境变量；Vercel 自动注入的变量不会自动出现在你的本机终端。可从数据库管理面板获取连接字符串，或直接使用 SQL 编辑器。初始化后项目地址形如 `https://your-project.vercel.app`。

## 2. 在使用 Agent 的电脑上安装采集端

无需全局安装；在**使用 Agent 的那台电脑**上执行（不要在 Vercel 中执行）：

```bash
npx token-pulse@latest init --dry-run                 # 可选：预览将配置的集成
npx token-pulse@latest init --yes                     # 安装钩子并尝试首次同步
npx token-pulse@latest connect --api-url https://your-project.vercel.app --key '你在 Vercel 设置的密钥'
npx token-pulse@latest sync                           # 扫描全部受支持来源并上传
npx token-pulse@latest status                         # 核查上传配置、队列积压和解析时间
```

将示例域名和密钥换成自己的值。`--api-url` 必须是 **HTTPS 站点根地址**（开发时允许 `http://localhost` / `127.0.0.1`），不要附加 `/api/ingest`、路径或查询参数。`init` 会修改已检测到的部分 Agent 的 hook/plugin 配置；建议先运行 `--dry-run`，尤其是已有自定义钩子时。部分来源无需钩子，直接由 `sync` 读取；`init` 会尝试首次同步，因此如果先 `init` 再 `connect`，初次同步只会保存在本地，之后执行 `sync` 即可上传。

如果更愿意全局安装，可运行 `npm install -g token-pulse`，然后将上述 `npx token-pulse@latest` 换成 `token-pulse`。从源码运行采集端则用 `npm ci` 后执行 `node bin/token-pulse.js init --yes`、`node bin/token-pulse.js sync` 等。

安装目录：`~/.token-pulse/tracker/app/` 是 `init` 复制的运行时代码与运行依赖，`~/.token-pulse/bin/notify.cjs` 是钩子入口；钩子优先通过 Node.js 运行本地副本，不依赖 npx 缓存，仅当本地副本不存在时才回退到 npx。`~/.token-pulse/tracker/` 还保存解析游标、`queue.jsonl` 本地统计账本和上传进度。`connect` 将站点地址与**原始密钥**写入 `~/.token-pulse/tracker/upload.json`（尽可能使用 600 权限），请保护本机账户与该文件。按次设置 `TOKEN_PULSE_API_URL` / `TOKEN_PULSE_ACCESS_KEY` 可覆盖已保存的配置；无需修改 shell 配置即可让后台钩子使用保存的密钥。Windows 上 `~` 指当前用户主目录，文件权限遵循操作系统权限机制。

| 命令 | 用途 |
| --- | --- |
| `init --dry-run` / `init --yes` | 预览 / 安装可用钩子与本地运行时；重跑 `init` 可更新运行时 |
| `connect --show` / `connect --clear` | 查看脱敏的连接信息 / 删除本机保存的连接配置 |
| `sync` | 增量扫描全部来源；已连接时继续上传待发送数据 |
| `upload` / `upload --replay` | 只上传新增本地记录 / 手动重传本地完整账本（例如服务器修复后） |
| `status` / `status --json` | 检查解析时间、上传地址和队列待发送字节数；**来源列表不是完整支持清单** |
| `uninstall` / `uninstall --purge` | 移除采集端集成；清理本地数据前请先确认是否还需要历史记录 |

## 3. 打开看板与排障

访问部署域名，输入与 `connect` 相同的密钥。浏览器只在当前会话的 `sessionStorage` 保存该值；看板通过受保护的 `/api/usage` 读取按日期、来源、模型汇总的数据。费用是估算值，并非供应商账单。默认读取近 30 天；自定义日期最多 10 年，查询超过 **50,000** 条原始记录时会报错而不是返回不完整的统计，请缩短范围。

- 看板提示 401：确认浏览器密钥与 Vercel 的 `ACCESS_KEY` 一致，重设变量后已重新部署；`connect --show` 可检查本机目标地址（密钥脱敏）。
- 看板提示服务端错误：检查 Vercel 函数日志、`DATABASE_URL` 是否已注入，以及是否对正确的数据库执行了 `server/schema.sql`。
- 看板无数据：先运行 `sync`、`status`；检查 Agent 是否已有可读取的用量、采集端是否已 `connect`、队列是否仍有待上传数据。仅靠 `status` 的“Local sources”行不能判断所有 Agent 是否被检测到。
- 断网或上传失败：本地队列保留待发送记录，下次 `sync` 或 `upload` 可重试；若服务端数据损坏但本地账本仍完整，可执行 `upload --replay`。

## 支持的 Agent / 数据来源

采集基于各工具**实际提供的用量字段**；需要本机安装、可读取的日志/数据库或有效登录，不能凭空推算未记录的 token。不同 Agent、版本和平台的字段完整度不同；部分来源只有合计或估算值。下表按接入方式归类，未列出的来源不表示自动支持：

| 接入方式 | Agent / 工具 | 说明 |
| --- | --- | --- |
| `init` 可安装钩子或插件；也可扫描已有记录 | Claude Code、Codex CLI、AStudio、Every Code、Gemini CLI、OpenCode、OpenClaw、Grok Build、oh-my-pi (omp)、CodeBuddy、WorkBuddy、WorkBuddy AI | 有事件时触发后台同步；OpenClaw 的 session plugin 安装后需重启 gateway；`sync` 可补扫相关本地数据 |
| 被动读取本地会话/数据库/遥测 | Antigravity、Kiro、Hermes Agent、GitHub Copilot、Kimi Code、OmO、pi、Prime Agent、Craft Agents、Reasonix、Muse Code、Kilo CLI、Kilo Code、Roo Code、Zed Agent、Goose、Droid、Mimo、DuMate、ZCode、Qoder（含 CN）、AnythingLLM Desktop、Claude Science、DeepSeek Harness、TRAE、LM Studio、Unsloth Studio、Devin CLI、Cline、MiniMax Code、Command Code | 不依赖上述钩子；主动运行 `sync` 才能确保扫描新数据。Kiro、Copilot 等有多种本地格式，采集范围取决于已安装版本 |
| 调用供应商用量接口 | Cursor、TRAE Work CN | Cursor 需本机已登录；TRAE Work CN 需自行设置 `TOKEN_PULSE_TRAE_CN_USAGE=1` 才会使用本地登录凭据读取近 30 天用量，默认**不**发起该请求 |

名称 `pi` 与 `oh-my-pi` 分开统计；Kilo CLI 与 Kilo Code 扩展也是不同来源。集成状态可参考 `init --dry-run` 的输出；被动来源并非都会出现在安装摘要或 `status` 中。钩子只在相应工具有事件时触发，因此被动来源或历史数据需要定期手动 `sync`（可自行配置系统定时任务）。

## 工作原理与隐私边界

1. **发现与增量解析**：`sync` 在本机读取各来源的 JSONL、SQLite、日志，或明确启用的用量 API；基于游标和事件标识避免重复解析，将 token 数按来源、模型和时间段汇总。钩子只负责触发后台 `sync`，不会把会话正文直接发给服务端。
2. **本地持久化与上传**：聚合快照写入本地 `queue.jsonl`；仅在配置连接后，将设备标识、来源、模型、时间、token 分项、会话计数及费用字段（某些来源另有用于跨设备去重的会话用量状态）批量送往 `/api/ingest`。原始会话、提示词和回复内容不在上传载荷中。上传进度单独记录，失败时可重试。
3. **服务端聚合**：Vercel 函数检查共享密钥并写入 PostgreSQL；按设备/来源/模型/时间的快照覆盖旧值以避免重传叠加，部分账号级数据用会话状态去重。`/api/usage` 汇总日期和模型，按服务端定价数据或来源提供的费用信息计算估算费用，返回给浏览器。服务端不保存账号密码；密钥作为服务端环境变量保存，浏览器及本机采集端需要持有相同密钥。

**注意**：本地原始 Agent 日志仍留在各 Agent 的数据目录；`~/.token-pulse/tracker/` 和 Vercel PostgreSQL 中的聚合记录也属于个人使用数据，请按自己的备份及访问控制策略保护。更换 `ACCESS_KEY` 后，还需在每台采集端重新执行 `connect`、在浏览器重新输入新密钥。

## 更新与源码验证

Vercel 连接仓库后，向 `main` 推送代码可触发自动部署；推送 `v*` 标签触发 npm 发布工作流（需要仓库 `NPM_TOKEN` secret，未配置时跳过）。更新采集端可重新运行 `npx token-pulse@latest init --yes` 以刷新钩子使用的本地运行时。源码检查：

```bash
npm ci
npm ci --prefix dashboard
npm run ci:local
```

检查包含 Node 测试、看板测试与看板构建；数据库相关测试使用 PGlite，不需要生产数据库凭据。
