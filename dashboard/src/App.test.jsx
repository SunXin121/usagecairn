import React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { render, screen, waitFor, cleanup, fireEvent, act } from '@testing-library/react';
import App, { fetchUsage } from './App.jsx';
import { ThemeProvider } from './ui/foundation/ThemeProvider.jsx';

beforeEach(() => {
  window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
  sessionStorage.clear();
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

test('usage fetch attaches account key and renders server totals in the dashboard', async () => {
  const key = `tt_${'a'.repeat(64)}`;
  sessionStorage.setItem('tt.accessKey', key);
  const payload = { summary: { total_tokens: 160, input_tokens: 140, output_tokens: 20, cost_usd: 0.1 }, daily: [{ day: '2026-10-18', total_tokens: 160, cost_usd: 0.1 }], models: [{ source: 'claude', model: 'claude-sonnet-4', total_tokens: 160 }] };
  const request = vi.fn().mockResolvedValue({ ok: true, json: async () => payload });
  expect((await fetchUsage(key, '2026-10-18', '2026-10-18', request)).summary.total_tokens).toBe(160);
  expect(request.mock.calls[0][0]).toContain('/api/usage?');
  expect(request.mock.calls[0][1].headers.Authorization).toBe(`Bearer ${key}`);
  vi.stubGlobal('fetch', request);
  render(<ThemeProvider><App /></ThemeProvider>);
  await waitFor(() => expect(screen.getAllByText('160').length).toBeGreaterThan(0));
  expect(screen.getByText('claude-sonnet-4')).toBeTruthy();
  expect(screen.getAllByText('$0.10').length).toBeGreaterThan(0);
  expect(screen.queryByText('排行榜')).toBeNull();
});

test('a late response from a signed-out account cannot replace the new account', async () => {
  const first = `tt_${'a'.repeat(64)}`;
  const second = `tt_${'b'.repeat(64)}`;
  window.sessionStorage.setItem('tt.accessKey', first);
  let resolveOld;
  const old = new Promise((resolve) => { resolveOld = resolve; });
  const payload = (total) => ({ summary: { total_tokens: total }, daily: [], models: [] });
  vi.stubGlobal('fetch', vi.fn((_, options) => options.headers.Authorization === `Bearer ${first}`
    ? old : Promise.resolve({ ok: true, json: async () => payload(222) })));
  render(<ThemeProvider><App /></ThemeProvider>);
  await waitFor(() => expect(fetch).toHaveBeenCalled());
  fireEvent.click(screen.getByText('退出账号'));
  fireEvent.change(screen.getByLabelText('访问密钥'), { target: { value: second } });
  fireEvent.click(screen.getByText('连接'));
  await waitFor(() => expect(screen.getAllByText('222').length).toBeGreaterThan(0));
  await act(async () => resolveOld({ ok: true, json: async () => payload(111) }));
  expect(screen.queryByText('111')).toBeNull();
  expect(screen.getAllByText('222').length).toBeGreaterThan(0);
});

test('every period tab stays inside the server date-range limit', async () => {
  const key = `tt_${'a'.repeat(64)}`;
  sessionStorage.setItem('tt.accessKey', key);
  const request = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ summary: {}, daily: [], models: [] }) });
  vi.stubGlobal('fetch', request);
  render(<ThemeProvider><App /></ThemeProvider>);
  await waitFor(() => expect(request).toHaveBeenCalled());
  // "总计" used to ask for 2000-01-01, which /api/usage rejects with HTTP 400
  // (its maximum span is 10 years), so the tab could never load.
  let expected = request.mock.calls.length;
  for (const label of ['日', '周', '月', '总计']) {
    fireEvent.click(screen.getByText(label));
    await waitFor(() => expect(request.mock.calls.length).toBeGreaterThan(expected));
    expected = request.mock.calls.length;
  }
  const spans = request.mock.calls.map(([url]) => {
    const params = new URL(url, 'http://localhost').searchParams;
    return (Date.parse(`${params.get('to')}T00:00:00Z`) - Date.parse(`${params.get('from')}T00:00:00Z`)) / 86400000;
  });
  expect(spans.length).toBeGreaterThan(0);
  expect(Math.max(...spans)).toBeLessThanOrEqual(3660);
  expect(Math.min(...spans)).toBeGreaterThanOrEqual(0);
});
