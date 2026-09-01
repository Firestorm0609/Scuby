import { mkdirSync } from "fs";

// ── Path constants ──

export const BOX_ROOT = process.env.BOX_ROOT ?? "/workspace/home";
export const DATA_DIR = process.env.DATA_DIR ?? `${BOX_ROOT}/data`;

export function portfolioPath(): string {
  return `${DATA_DIR}/portfolio.json`;
}

export function historyDir(): string {
  const dir = `${DATA_DIR}/history`;
  mkdirSync(dir, { recursive: true });
  return dir;
}

export function diaryPath(): string {
  return `${DATA_DIR}/diary.md`;
}

export function memoryPath(): string {
  return `${DATA_DIR}/memory.md`;
}

export function todayTradesPath(): string {
  return `${DATA_DIR}/today_trades.json`;
}

// ── Rounding ──

export function round(n: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(n * factor) / factor;
}

// ── Yahoo Finance chart API helper ──

export interface YahooChartMeta {
  symbol: string;
  shortName?: string;
  longName?: string;
  instrumentType: string;
  regularMarketPrice: number;
  regularMarketTime: number;
  currency: string;
}

export async function fetchChart(
  ticker: string,
  range = "1d",
  interval = "1d",
): Promise<{ meta: YahooChartMeta; timestamps: number[]; quotes: any } | null> {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?range=${range}&interval=${interval}`;
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });

  if (!res.ok) return null;

  const data = (await res.json()) as any;
  const result = data?.chart?.result?.[0];
  if (!result) return null;

  return {
    meta: result.meta as YahooChartMeta,
    timestamps: result.timestamp ?? [],
    quotes: result.indicators?.quote?.[0] ?? {},
  };
}

// ── Data schemas ──

export interface Holding {
  ticker: string;
  name: string;
  dollars: number;
  shares: number;
  avg_entry_price: number;
  current_price: number;
  unrealized_pnl_pct: number;
}

export interface LastAction {
  summary: string;
  timestamp: string;
}

export interface Portfolio {
  agent: string;
  updated_at: string;
  starting_balance: number;
  cash: number;
  holdings: Holding[];
  total_value: number;
  all_time_return_pct: number;
  day_number: number;
  start_date: string;
  last_action: LastAction | null;
  last_run_date: string | null;
}

export interface TradeRecord {
  action: "buy" | "sell";
  ticker: string;
  amount: number;
  price: number;
  shares: number;
  reason?: string;
}

export interface HistorySnapshot {
  date: string;
  day_number: number;
  total_value: number;
  cash: number;
  all_time_return_pct: number;
  holdings: {
    ticker: string;
    name: string;
    dollars: number;
    shares: number;
    current_price: number;
  }[];
  trades: TradeRecord[];
}

export interface ValidationResult {
  valid: boolean;
  ticker: string;
  type?: "stock" | "etf" | "metals" | "bond" | "crypto" | "option" | "future" | "unknown";
  name?: string;
  reason?: string;
}

export interface PriceQuote {
  ticker: string;
  price: number;
  name: string;
  timestamp: string;
}

export interface HistoricalPrice {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TradeResult {
  success: boolean;
  action?: "buy" | "sell";
  ticker?: string;
  amount?: number;
  price?: number;
  shares?: number;
  cash_remaining?: number;
  error?: string;
}

export interface SearchResult {
  title: string;
  url: string;
  snippet: string;
}
