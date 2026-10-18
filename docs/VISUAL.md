# Dashboard visual check

Captured with headless Chrome at 1440×1100 against Vite dev + a local `/api/usage`
fixture (no production data, no credentials committed).

| Capture | File |
|---|---|
| Light | [`screenshots/dashboard-usage-light.png`](screenshots/dashboard-usage-light.png) |
| Dark | [`screenshots/dashboard-usage-dark.png`](screenshots/dashboard-usage-dark.png) |

Measured pixel values from the captures (`magick … -format '%[pixel:p{x,y}]'`):

| Region | Light | Dark |
|---|---|---|
| Page wash (`oai-gray-50` / `oai-gray-950`) | `#FAFAFA` | `#0A0A0A` |
| Content card (`white` / `oai-gray-900`) | `#FFFFFF` | `#171717` |
| Mean luminance | 0.980 | 0.095 |

These match `dashboard/tailwind.config.cjs` exactly (`oai.white #fafafa`,
`oai.gray.50 #fafafa`, `oai.gray.900 #171717`, `oai.gray.950 #0a0a0a`).

## CSS cleanup check

`dashboard/src/styles.css` was reduced from 1441 lines to ~250 by dropping
page-specific rules for removed features (leaderboard, pet, cloud/billing,
heatmap, date-picker, sidebar variants) while keeping the `:root` design tokens,
the dark token block, base `html/body` rules and the `@supports` OKLCH override.

Re-captured light and dark screenshots after the trim and compared them against
the previous captures with `magick compare -metric AE`: **0 differing pixels** in
both themes, i.e. the visible result is unchanged.

Layout: 220px sidebar with icon+label nav and bottom theme/account controls, a
rounded content card inset on a page wash, period tabs above a centered oversized
total, brand-green cost line, stacked provider share bar, provider tiles, a trend
line and a scrollable daily breakdown table.
