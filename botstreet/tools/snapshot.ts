import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { readPortfolio, writePortfolio } from "./portfolio.js";
import { historyDir, todayTradesPath } from "./types.js";
import type { HistorySnapshot, TradeRecord } from "./types.js";

function readTodayTrades(): TradeRecord[] {
  try {
    const raw = readFileSync(todayTradesPath(), "utf-8");
    return JSON.parse(raw) as TradeRecord[];
  } catch {
    return [];
  }
}

async function saveSnapshot(): Promise<{ success: boolean; path: string; overwritten: boolean }> {
  const portfolio = readPortfolio();
  const trades = readTodayTrades();

  const today = new Date().toISOString().split("T")[0];
  const dateForFile = today.replace(/-/g, "_");

  const snapshot: HistorySnapshot = {
    date: today,
    day_number: portfolio.day_number,
    total_value: portfolio.total_value,
    cash: portfolio.cash,
    all_time_return_pct: portfolio.all_time_return_pct,
    holdings: portfolio.holdings.map((h) => ({
      ticker: h.ticker,
      name: h.name,
      dollars: h.dollars,
      shares: h.shares,
      current_price: h.current_price,
    })),
    trades,
  };

  const dir = historyDir();
  mkdirSync(dir, { recursive: true });

  const snapshotPath = `${dir}/portfolio_${dateForFile}.json`;
  const alreadyExists = existsSync(snapshotPath);

  writeFileSync(snapshotPath, JSON.stringify(snapshot, null, 2));

  // Clear today's trades
  writeFileSync(todayTradesPath(), "[]");

  // Keep day_number tied to calendar days, even if snapshots are saved multiple times.
  if (!alreadyExists) {
    portfolio.day_number += 1;
  }
  writePortfolio(portfolio);

  return { success: true, path: snapshotPath, overwritten: alreadyExists };
}

// ── CLI entry point ──

const args = process.argv.slice(2);
const subcommand = args[0];

if (subcommand !== "save") {
  console.log(JSON.stringify({ error: "usage: snapshot.ts save" }));
  process.exit(1);
}

try {
  const result = await saveSnapshot();
  console.log(JSON.stringify(result, null, 2));
} catch (e: any) {
  console.log(JSON.stringify({ error: e.message }));
  process.exit(1);
}
