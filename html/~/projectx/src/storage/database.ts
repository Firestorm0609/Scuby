/* =========================================================
   ProjectX — Database Storage
========================================================= */

import { TokenWithScore } from "../types/index.js";
import { logger } from "../utils/logger.js";
import fs from "fs";
import path from "path";

interface StoredToken {
  mint: string;
  name: string;
  symbol: string;
  creator: string;
  createdAt: string;
  score: number;
  reasons: string;
  marketCap: number;
  price: number;
  volume24h: number;
  liquidity: number;
  holders: number;
  trackedAt: string;
  status: "watching" | "bought" | "sold" | "skipped";
}

export class Database {
  private filePath: string;
  private data: Map<string, StoredToken> = new Map();

  constructor(dbPath: string) {
    this.filePath = path.resolve(dbPath);
    this.load();
  }

  private load() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      if (fs.existsSync(this.filePath)) {
        const raw = fs.readFileSync(this.filePath, "utf-8");
        const entries: StoredToken[] = JSON.parse(raw);
        for (const entry of entries) {
          this.data.set(entry.mint, entry);
        }
        logger.info(`Loaded ${this.data.size} tokens from database`);
      }
    } catch (error) {
      logger.warn("Could not load database, starting fresh");
    }
  }

  private save() {
    try {
      const entries = Array.from(this.data.values());
      fs.writeFileSync(this.filePath, JSON.stringify(entries, null, 2));
    } catch (error) {
      logger.error("Failed to save database:", error);
    }
  }

  addToken(token: TokenWithScore): boolean {
    if (this.data.has(token.mint)) {
      return false; // Already tracked
    }

    const stored: StoredToken = {
      mint: token.mint,
      name: token.name,
      symbol: token.symbol,
      creator: token.creator,
      createdAt: token.createdAt.toISOString(),
      score: token.score,
      reasons: token.reasons.join(" | "),
      marketCap: token.marketCap,
      price: token.price,
      volume24h: token.volume24h,
      liquidity: token.liquidity,
      holders: token.holders,
      trackedAt: new Date().toISOString(),
      status: "watching",
    };

    this.data.set(token.mint, stored);
    this.save();
    return true;
  }

  updateStatus(mint: string, status: StoredToken["status"]) {
    const token = this.data.get(mint);
    if (token) {
      token.status = status;
      this.save();
    }
  }

  getToken(mint: string): StoredToken | undefined {
    return this.data.get(mint);
  }

  getAllTokens(): StoredToken[] {
    return Array.from(this.data.values());
  }

  getWatching(): StoredToken[] {
    return this.getAllTokens().filter((t) => t.status === "watching");
  }

  getStats() {
    const all = this.getAllTokens();
    return {
      total: all.length,
      watching: all.filter((t) => t.status === "watching").length,
      bought: all.filter((t) => t.status === "bought").length,
      sold: all.filter((t) => t.status === "sold").length,
      skipped: all.filter((t) => t.status === "skipped").length,
    };
  }
}
