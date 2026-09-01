import { fetchChart } from "./types.js";
import type { ValidationResult } from "./types.js";

const METALS_TICKERS = new Set([
  "GLD", "SLV", "IAU", "GDX", "GDXJ", "SIL", "PPLT", "PALL", "SGOL", "AAAU",
  "RING", "GLTR", "DBP", "DGL", "BAR", "OUNZ",
]);

const METALS_KEYWORDS = ["gold", "silver", "metal", "precious", "palladium", "platinum"];

const BOND_TICKERS = new Set([
  "TLT", "BND", "AGG", "IEF", "SHY", "LQD", "HYG", "VCIT", "VCSH", "GOVT",
  "BNDX", "MUB", "TIP", "SCHZ", "SPTL", "SPAB", "IGSB", "IGIB",
]);

const BOND_KEYWORDS = ["bond", "treasury", "fixed income", "aggregate", "income", "debt", "municipal"];

function classifyEtf(ticker: string, name: string): ValidationResult {
  const lowerName = name.toLowerCase();

  if (METALS_TICKERS.has(ticker) || METALS_KEYWORDS.some((kw) => lowerName.includes(kw))) {
    return { valid: true, ticker, type: "metals", name };
  }

  if (BOND_TICKERS.has(ticker) || BOND_KEYWORDS.some((kw) => lowerName.includes(kw))) {
    return { valid: false, ticker, type: "bond", name, reason: "bonds not allowed" };
  }

  return { valid: true, ticker, type: "etf", name };
}

export async function validateTicker(ticker: string): Promise<ValidationResult> {
  ticker = ticker.toUpperCase();

  try {
    const chart = await fetchChart(ticker);

    if (!chart || !chart.meta.regularMarketPrice) {
      return { valid: false, ticker, reason: "ticker not found" };
    }

    const { meta } = chart;
    const name = meta.shortName ?? meta.longName ?? ticker;
    const instrumentType = meta.instrumentType;

    switch (instrumentType) {
      case "EQUITY":
        return { valid: true, ticker, type: "stock", name };
      case "ETF":
        return classifyEtf(ticker, name);
      case "CRYPTOCURRENCY":
        return { valid: false, ticker, type: "crypto", name, reason: "crypto not allowed" };
      case "OPTION":
        return { valid: false, ticker, type: "option", name, reason: "options not allowed" };
      case "FUTURE":
        return { valid: false, ticker, type: "future", name, reason: "futures not allowed" };
      default:
        return { valid: false, ticker, type: "unknown", name, reason: `unsupported asset type: ${instrumentType}` };
    }
  } catch {
    return { valid: false, ticker, reason: "ticker not found" };
  }
}

// ── CLI entry point ──

if (process.argv[1]?.endsWith("validator.ts")) {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.log(JSON.stringify({ valid: false, ticker: "", reason: "usage: validator.ts <TICKER>" }));
    process.exit(1);
  }

  const result = await validateTicker(args[0]);
  console.log(JSON.stringify(result, null, 2));
}
