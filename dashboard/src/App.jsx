import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { BarChart3, Settings, LogOut, RefreshCw } from 'lucide-react';
import { useTheme } from './hooks/useTheme.js';

const KEY = 'tt.accessKey';
const PERIODS = [
  { id: 'day', label: '日' },
  { id: 'week', label: '周' },
  { id: 'month', label: '月' },
  { id: 'total', label: '总计' },
  { id: 'custom', label: '自定义' },
];
// Provider accents reuse the design tokens already defined in styles.css.
const SOURCE_COLORS = {
  claude: 'var(--community-claude)', codex: 'var(--community-codex)', cursor: 'var(--community-cursor)',
  opencode: 'var(--community-opencode)', gemini: 'var(--community-gemini)', kimi: 'var(--community-kimi)',
  zcode: 'var(--community-zcode)', mimo: 'var(--community-mimo)', droid: 'var(--community-droid)',
  workbuddy: 'var(--community-workbuddy)', copilot: 'var(--community-copilot)', kiro: 'var(--community-kiro)',
  grok: 'var(--community-grok)', deepseek: 'var(--community-deepseek)', minimax: 'var(--community-minimax)',
};

export async function fetchUsage(key, from, to, request = fetch) {
  const query = new URLSearchParams({ from, to });
  const response = await request(`/api/usage?${query}`, {
    headers: { Authorization: `Bearer ${key}` }, cache: 'no-store',
  });
  if (!response.ok) throw new Error(response.status === 401 ? '访问密钥无效' : `读取失败 (HTTP ${response.status})`);
  return response.json();
}

const isoDay = (date) => date.toISOString().slice(0, 10);
const today = () => isoDay(new Date());
const daysAgo = (n) => isoDay(new Date(Date.now() - n * 86400000));
const number = (value) => new Intl.NumberFormat('en-US').format(Math.round(Number(value) || 0));
const compact = (value) => new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(Number(value) || 0);
const usd = (value) => {
  const n = Number(value) || 0;
  if (n > 0 && n < 0.01) return '<$0.01';
  return `$${n.toFixed(2)}`;
};
const sourceColor = (source) => SOURCE_COLORS[String(source || '').toLowerCase()] || 'var(--community-muted)';

// The API rejects ranges wider than 10 years, so "总计" asks for the widest
// window it accepts instead of a literal epoch start that would 400.
const TOTAL_WINDOW_DAYS = 3650;
function rangeFor(period, customFrom, customTo) {
  if (period === 'custom') return { from: customFrom || daysAgo(29), to: customTo || today() };
  if (period === 'day') return { from: today(), to: today() };
  if (period === 'week') return { from: daysAgo(6), to: today() };
  if (period === 'total') return { from: daysAgo(TOTAL_WINDOW_DAYS), to: today() };
  return { from: daysAgo(29), to: today() };
}

function Card({ className = '', children }) {
  return (
    <div className={`rounded-xl border border-oai-gray-200 dark:border-oai-gray-800 bg-white dark:bg-oai-gray-900 ${className}`}>
      {children}
    </div>
  );
}

function TrendChart({ rows }) {
  const points = rows.slice(-30);
  if (points.length < 2) return null;
  const max = Math.max(...points.map((r) => Number(r.total_tokens) || 0), 1);
  const width = 100;
  const height = 100;
  const step = width / (points.length - 1);
  const line = points.map((row, i) => `${(i * step).toFixed(2)},${(height - ((Number(row.total_tokens) || 0) / max) * height).toFixed(2)}`).join(' ');
  return (
    <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="h-32 w-full" role="img" aria-label="每日用量趋势">
      <polyline points={`0,${height} ${line} ${width},${height}`} fill="var(--brand-100)" stroke="none" className="dark:hidden" opacity="0.6" />
      <polyline points={line} fill="none" stroke="var(--brand-500)" strokeWidth="1.5" vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
    </svg>
  );
}

function ModelShareBar({ models }) {
  const total = models.reduce((sum, row) => sum + (Number(row.total_tokens) || 0), 0);
  if (!total) return null;
  return (
    <div className="flex h-1.5 w-full overflow-hidden rounded-full bg-oai-gray-100 dark:bg-oai-gray-800">
      {models.map((row) => (
        <span key={`${row.source}/${row.model}`} style={{ width: `${((Number(row.total_tokens) || 0) / total) * 100}%`, backgroundColor: sourceColor(row.source) }} />
      ))}
    </div>
  );
}

function UsageHero({ period, setPeriod, range, summary, models, loading }) {
  const total = Number(summary?.total_tokens) || 0;
  const hasData = total > 0;
  return (
    <Card className="p-5 sm:p-6">
      <div className="mb-6 flex items-center gap-1 overflow-x-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden" role="tablist" aria-label="时间范围">
        {PERIODS.map((item) => (
          <button
            key={item.id}
            role="tab"
            aria-selected={period === item.id}
            onClick={() => setPeriod(item.id)}
            className={`shrink-0 rounded-md px-2.5 py-1 text-xs transition-colors duration-200 ${
              period === item.id
                ? 'bg-oai-gray-100 font-medium text-oai-black dark:bg-oai-gray-800 dark:text-white'
                : 'text-oai-gray-500 hover:text-oai-black dark:text-oai-gray-400 dark:hover:text-white'
            }`}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="mb-8 text-center">
        <div className="mb-3 text-xs uppercase tracking-wider text-oai-gray-500 dark:text-oai-gray-300">总 Tokens</div>
        <div className="text-5xl font-bold tabular-nums tracking-tight text-oai-black dark:text-white sm:text-6xl md:text-7xl">
          {loading && !hasData ? <span className="inline-block h-[1em] w-40 animate-pulse rounded-2xl bg-oai-gray-100 dark:bg-oai-gray-800" /> : compact(total)}
        </div>
        <div className="mt-4 flex items-center justify-center gap-2">
          <span className="text-xl font-bold text-oai-brand">{usd(summary?.cost_usd)}</span>
        </div>
        <div className="mt-3 text-[11px] tabular-nums text-oai-gray-400 dark:text-oai-gray-500">
          {range.from} → {range.to} · {number(summary?.conversation_count)} 次会话
        </div>
      </div>

      {models.length > 0 && (
        <div className="space-y-6">
          <ModelShareBar models={models} />
          <div className="grid grid-cols-[repeat(auto-fill,minmax(140px,1fr))] gap-3">
            {models.slice(0, 12).map((row) => (
              <div key={`${row.source}/${row.model}`} className="min-w-0 rounded-lg border border-oai-gray-200 p-3 dark:border-oai-gray-800">
                <div className="mb-1.5 flex items-center gap-1.5">
                  <span className="h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: sourceColor(row.source) }} />
                  <span className="truncate text-[11px] text-oai-gray-500 dark:text-oai-gray-400">{row.source}</span>
                </div>
                <div className="truncate text-sm font-semibold text-oai-black dark:text-white" title={row.model}>{row.model}</div>
                <div className="mt-1 text-xs tabular-nums text-oai-gray-500 dark:text-oai-gray-400">
                  {compact(row.total_tokens)} · {usd(row.cost_usd)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </Card>
  );
}

function DailyDetails({ rows }) {
  const columns = [
    ['day', '日期'], ['input_tokens', '输入'], ['output_tokens', '输出'],
    ['cached_input_tokens', '缓存读'], ['cache_creation_input_tokens', '缓存写'],
    ['reasoning_output_tokens', '推理'], ['total_tokens', '合计'], ['cost_usd', '费用'],
  ];
  return (
    <Card className="overflow-hidden">
      <h2 className="border-b border-oai-gray-100 px-4 py-3 text-sm font-semibold dark:border-oai-gray-800">每日明细</h2>
      <div className="max-h-[420px] overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-white text-oai-gray-500 dark:bg-oai-gray-900 dark:text-oai-gray-400">
            <tr>
              {columns.map(([key, label]) => (
                <th key={key} scope="col" className={`px-3 py-2 font-medium ${key === 'day' ? 'text-left' : 'text-right'}`}>{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.day} className="border-t border-oai-gray-100 tabular-nums dark:border-oai-gray-800/60">
                {columns.map(([key]) => (
                  <td key={key} className={`px-3 py-2 ${key === 'day' ? 'text-left' : 'text-right'} ${key === 'total_tokens' ? 'font-medium text-oai-black dark:text-white' : 'text-oai-gray-600 dark:text-oai-gray-300'}`}>
                    {key === 'day' ? row.day : key === 'cost_usd' ? usd(row.cost_usd) : number(row[key])}
                  </td>
                ))}
              </tr>
            ))}
            {!rows.length && (
              <tr><td colSpan={columns.length} className="px-3 py-10 text-center text-oai-gray-500">该时间段暂无数据</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

export default function App() {
  const [key, setKey] = useState(() => window.sessionStorage.getItem(KEY) || '');
  const [draft, setDraft] = useState('');
  const [period, setPeriod] = useState('month');
  const [customFrom, setCustomFrom] = useState(daysAgo(29));
  const [customTo, setCustomTo] = useState(today);
  const [view, setView] = useState('dashboard');
  const [usage, setUsage] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { theme, setTheme } = useTheme();
  const requestId = useRef(0);
  const range = useMemo(() => rangeFor(period, customFrom, customTo), [period, customFrom, customTo]);

  const refresh = useCallback(async () => {
    if (!key) return;
    const current = ++requestId.current;
    setLoading(true);
    try {
      const data = await fetchUsage(key, range.from, range.to);
      if (current === requestId.current && window.sessionStorage.getItem(KEY) === key) { setUsage(data); setError(''); }
    } catch (e) {
      if (current === requestId.current && window.sessionStorage.getItem(KEY) === key) { setError(e.message); setUsage(null); }
    } finally { if (current === requestId.current) setLoading(false); }
  }, [key, range.from, range.to]);

  useEffect(() => { void refresh(); }, [refresh]);

  const signOut = () => {
    requestId.current++;
    window.sessionStorage.removeItem(KEY);
    setKey(''); setUsage(null); setLoading(false);
  };
  const saveKey = (event) => {
    event.preventDefault();
    if (!/^tt_[a-f0-9]{64}$/.test(draft.trim())) { setError('访问密钥格式不正确'); return; }
    requestId.current++;
    window.sessionStorage.setItem(KEY, draft.trim());
    setKey(draft.trim()); setUsage(null); setDraft(''); setError('');
  };

  const navItems = [
    ['dashboard', BarChart3, '用量看板'],
    ['settings', Settings, '设置'],
  ];

  return (
    <div className="fixed inset-0 flex bg-oai-gray-50 font-sans text-oai-black dark:bg-oai-gray-950 dark:text-oai-white">
      <aside className="hidden w-[220px] shrink-0 flex-col p-3 lg:flex" aria-label="导航">
        <div className="flex items-center gap-2 px-2 py-5 text-sm font-semibold">
          <img src="/app-icon.png" alt="" width="28" height="28" className="rounded-md" />
          Token Tracker
        </div>
        <nav className="space-y-1 pt-4 text-[13px]">
          {navItems.map(([id, Icon, label]) => (
            <button
              key={id}
              onClick={() => setView(id)}
              className={`flex w-full items-center gap-2 rounded-md px-3 py-2 text-left transition-colors ${
                view === id
                  ? 'bg-oai-gray-200/70 font-medium dark:bg-oai-gray-800'
                  : 'text-oai-gray-600 hover:bg-oai-gray-200/50 dark:text-oai-gray-400 dark:hover:bg-oai-gray-800/60'
              }`}
            >
              <Icon size={16} />{label}
            </button>
          ))}
        </nav>
        <div className="mt-auto space-y-2 text-xs text-oai-gray-500">
          <button onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} className="rounded-md px-3 py-2 hover:bg-oai-gray-200 dark:hover:bg-oai-gray-800">切换主题</button>
          {key && (
            <button onClick={signOut} className="flex items-center gap-2 rounded-md px-3 py-2 hover:bg-oai-gray-200 dark:hover:bg-oai-gray-800">
              <LogOut size={14} />退出账号
            </button>
          )}
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col p-2 lg:pl-0 lg:pr-3 lg:pb-3">
        <main className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-oai-gray-200 bg-white shadow-sm dark:border-oai-gray-800 dark:bg-oai-gray-900">
          <header className="flex items-center justify-between border-b border-oai-gray-200 px-5 py-4 dark:border-oai-gray-800 sm:px-8">
            <h1 className="text-lg font-semibold">{view === 'dashboard' ? '用量看板' : '设置'}</h1>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setView(view === 'dashboard' ? 'settings' : 'dashboard')}
                className="rounded-md border border-oai-gray-200 px-3 py-1.5 text-xs dark:border-oai-gray-800 lg:hidden"
              >
                {view === 'dashboard' ? '设置' : '看板'}
              </button>
              <button onClick={refresh} disabled={!key || loading} aria-label="刷新数据" className="rounded-md p-2 hover:bg-oai-gray-100 dark:hover:bg-oai-gray-800">
                <RefreshCw size={16} className={loading ? 'animate-spin' : undefined} />
              </button>
            </div>
          </header>

          <div className="flex-1 overflow-auto px-5 py-6 sm:px-8">
            <div className="mx-auto max-w-7xl space-y-6">
              {!key ? (
                <Card className="mx-auto mt-12 max-w-md p-6">
                  <h2 className="text-base font-semibold">连接用量账号</h2>
                  <p className="mt-2 text-sm text-oai-gray-500">输入管理员生成的访问密钥。密钥仅保存在当前浏览器会话中。</p>
                  <form onSubmit={saveKey} className="mt-5 space-y-3">
                    <input
                      type="password"
                      aria-label="访问密钥"
                      autoComplete="off"
                      value={draft}
                      onChange={(e) => setDraft(e.target.value)}
                      placeholder="tt_…"
                      className="w-full rounded-lg border border-oai-gray-200 bg-transparent px-3 py-2 text-sm focus:ring-inset dark:border-oai-gray-700"
                    />
                    <button className="rounded-lg bg-oai-black px-4 py-2 text-sm text-white dark:bg-oai-white dark:text-oai-black">连接</button>
                  </form>
                </Card>
              ) : view === 'settings' ? (
                <Card className="max-w-xl p-6">
                  <h2 className="font-semibold">数据连接</h2>
                  <p className="mt-3 text-sm text-oai-gray-500">
                    当前账号已连接。CLI 使用同一访问密钥上传，执行 <code>tokentracker sync</code> 或 <code>tokentracker upload</code> 后刷新看板。
                  </p>
                  <button onClick={signOut} className="mt-5 rounded-md border border-oai-gray-200 px-3 py-2 text-sm dark:border-oai-gray-800">退出并清除会话密钥</button>
                </Card>
              ) : (
                <>
                  {period === 'custom' && (
                    <div className="flex flex-wrap items-end gap-3 text-xs text-oai-gray-500">
                      <label>从 <input type="date" value={customFrom} max={customTo} onChange={(e) => setCustomFrom(e.target.value)} className="ml-1 rounded-md border border-oai-gray-200 bg-transparent px-2 py-1.5 dark:border-oai-gray-700" /></label>
                      <label>到 <input type="date" value={customTo} min={customFrom} onChange={(e) => setCustomTo(e.target.value)} className="ml-1 rounded-md border border-oai-gray-200 bg-transparent px-2 py-1.5 dark:border-oai-gray-700" /></label>
                      <span>UTC 日期</span>
                    </div>
                  )}
                  <UsageHero
                    period={period}
                    setPeriod={setPeriod}
                    range={range}
                    summary={usage?.summary}
                    models={usage?.models || []}
                    loading={loading}
                  />
                  {usage?.daily?.length > 1 && (
                    <Card className="p-4">
                      <TrendChart rows={usage.daily} />
                    </Card>
                  )}
                  <DailyDetails rows={usage?.daily || []} />
                </>
              )}
              {error && <p role="alert" className="text-sm text-red-600">{error}</p>}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
