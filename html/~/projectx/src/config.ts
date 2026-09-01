/* =========================================================
   ProjectX — Configuration
========================================================= */

import dotenv from "dotenv";
import { FilterConfig } from "./types/index.js";

dotenv.config();

export const config = {
  // Solana
  solanaRpcUrl: process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com",

  // Database
  databasePath: process.env.DATABASE_PATH || "./data/tokens.db",

  // Scanner
  scanIntervalMs: parseInt(process.env.SCAN_INTERVAL_MS || "2000"),

  // Filters
  filters: {
    minLiquidityUsd: parseInt(process.env.MIN_LIQUIDITY_USD || "1000"),
    maxTokenAgeSeconds: parseInt(process.env.MAX_TOKEN_AGE_SECONDS || "3600"),
    minVolume24h: 500,
    minHolders: 10,
    requireSocials: false,
    requireWebsite: false,
    minBuySellRatio: 0.6,
  } as FilterConfig,

  // Pump.fun API endpoints
  pumpfun: {
    baseUrl: "https://frontend-api-v3.pump.fun",
    newTokenEndpoint: "/coins/latest",
    allTokensEndpoint: "/coins",
    marketDataEndpoint: "/coins/market-data",
  },
} as const;
