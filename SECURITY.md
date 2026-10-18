# Security Policy

## Supported Versions

Only the **latest minor release** is supported with security fixes. This is a small project — please run a recent version.

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security reports.**

Instead, use one of these private channels:

- **GitHub Private Vulnerability Reporting**: [Report a vulnerability](https://github.com/SunXin121/TokenTracker/security/advisories/new)
- **Email**: open a GitHub issue asking the maintainer to enable an alternative private channel if you can't use GitHub Security Advisories

When reporting, please include:

- A clear description of the issue
- Steps to reproduce
- Affected version(s)
- Your assessment of impact (data exposure, privilege escalation, etc.)
- Any suggested mitigation, if you have one

You can expect an initial response within a few days. Once a fix is ready, it will ship in the next release with a credit to you (or anonymously if you prefer).

## Scope

TokenTracker reads AI CLI logs from your home directory, stores scalar usage
counts, uploads them to a PostgreSQL instance you control, and renders them in a
web dashboard. The security-relevant surfaces are:

- **`src/lib/rollout.js`** — parses provider logs. Privacy rule: only token counts,
  model names and timestamps may be extracted — never prompt or response content.
- **`src/lib/cursor-config.js`** — reads Cursor's local SQLite for a usage API
  token. That token must never leave the user's machine.
- **`src/commands/upload.js` / `server/service.cjs`** — the upload path and the
  server handler. Access keys are hashed at rest, requests are account-scoped,
  and only scalar counters leave the machine.
- **`dashboard/src/App.jsx`** — the browser stores the access key in
  `sessionStorage` only; it must never be placed in URLs or logs.

## Out of Scope

- Issues only reproducible with arbitrary modifications to the user's local files outside the documented data paths
- Vulnerabilities in dependencies that have already been disclosed and patched upstream — please report those upstream first
- Social engineering, phishing, or other non-technical attack vectors

## Privacy Commitment

TokenTracker's foundational privacy rule: **token counts and timestamps only — never any prompt content, response content, or file contents from user code**. Any change that risks violating this is treated as a security issue.
