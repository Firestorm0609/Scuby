import { fetchChart } from "./types.js";
import type { PriceQuote, HistoricalPrice } from "./types.js";

// ── Exported functions ──

export async function getCurrentPrice(ticker: string): Promise<PriceQuote> {
  const chart = await fetchChart(ticker.toUpperCase());

  if (!chart || !chart.meta.regularMarketPrice) {
    throw new Error(`No price data for ${ticker}`);
  }

  const { meta } = chart;
  const marketTime = new Date(meta.regularMarketTime * 1000);

  return {
    ticker: meta.symbol,
    price: meta.regularMarketPrice,
    name: meta.shortName ?? meta.longName ?? ticker,
    timestamp: marketTime.toISOString(),
  };
}

export async function getCurrentPrices(tickers: string[]): Promise<PriceQuote[]> {
  const results = await Promise.all(
    tickers.map(async (ticker) => {
      try {
        return await getCurrentPrice(ticker);
      } catch {
        return { ticker: ticker.toUpperCase(), price: 0, name: ticker, timestamp: new Date().toISOString() };
      }
    }),
  );
  return results;
}

export async function getHistoricalPrices(ticker: string, days: number): Promise<HistoricalPrice[]> {
  const range = days <= 5 ? "5d" : days <= 30 ? "1mo" : days <= 90 ? "3mo" : days <= 180 ? "6mo" : "1y";
  const chart = await fetchChart(ticker.toUpperCase(), range, "1d");

  if (!chart) throw new Error(`No chart data for ${ticker}`);

  return chart.timestamps.map((ts, i) => ({
    date: new Date(ts * 1000).toISOString().split("T")[0],
    open: chart.quotes.open?.[i] ?? 0,
    high: chart.quotes.high?.[i] ?? 0,
    low: chart.quotes.low?.[i] ?? 0,
    close: chart.quotes.close?.[i] ?? 0,
    volume: chart.quotes.volume?.[i] ?? 0,
  }));
}

// ── CLI entry point ──

const isCLI = process.argv[1]?.endsWith("prices.ts");
if (isCLI) {

const args = process.argv.slice(2);
const subcommand = args[0];

if (subcommand === "current") {
  const tickers = args.slice(1);
  if (tickers.length === 0) {
    console.log(JSON.stringify({ error: "usage: prices.ts current <TICKER> [TICKER...]" }));
    process.exit(1);
  }
  try {
    const prices = await getCurrentPrices(tickers);
    console.log(JSON.stringify(prices, null, 2));
  } catch (e: any) {
    console.log(JSON.stringify({ error: e.message }));
    process.exit(1);
  }
} else if (subcommand === "historical") {
  const ticker = args[1];
  const days = parseInt(args[2] ?? "30", 10);
  if (!ticker) {
    console.log(JSON.stringify({ error: "usage: prices.ts historical <TICKER> <DAYS>" }));
    process.exit(1);
  }
  try {
    const prices = await getHistoricalPrices(ticker, days);
    console.log(JSON.stringify(prices, null, 2));
  } catch (e: any) {
    console.log(JSON.stringify({ error: e.message }));
    process.exit(1);
  }
} else {
  console.log(JSON.stringify({ error: "usage: prices.ts <current|historical> ..." }));
  process.exit(1);
}

} // end isCLI
