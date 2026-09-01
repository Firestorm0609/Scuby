import { TokenWithScore } from "../types/index";
import { Position } from "../trading/engine";
import { StrategyConfig } from "../trading/rewriter";
import { Wallet } from "../wallet";
import { logger } from "../utils/logger";
import * as fs from "fs";
import * as path from "path";

interface InlineButton {
  text: string;
  callback_data?: string;
  url?: string;
}

interface BotCommand {
  command: string;
  description: string;
  handler: (chatId: string, args: string, userId: number) => Promise<{ text: string; buttons?: InlineButton[][] }>;
}

function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export class TelegramNotifier {
  private botToken: string;
  private chatId: string;
  private enabled: boolean;
  private commands: Map<string, BotCommand> = new Map();
  private pollingOffset = 0;
  private pollingActive = false;
  
  // Multi-step flow states
  private waitingForPrivateKey: Set<string> = new Set();
  private waitingForTradeSize: Set<string> = new Set();
  private waitingForMaxPositions: Set<string> = new Set();
  
  // Data providers
  private getPositions?: () => Position[];
  private getStrategy?: () => StrategyConfig;
  private getStats?: () => any;
  private getWallet?: () => Wallet | null;
  private switchNetwork?: (network: "mainnet" | "devnet") => Promise<void>;
  private importWallet?: (privateKey: string) => Promise<boolean>;
  private updateStrategy?: (updates: Partial<StrategyConfig>) => void;

  constructor() {
    this.botToken = process.env.TELEGRAM_BOT_TOKEN || "";
    this.chatId = process.env.TELEGRAM_CHAT_ID || "";
    this.enabled = !!(this.botToken && this.chatId);
    
    this.registerCommands();
    
    if (!this.enabled) {
      logger.warn("Telegram notifications disabled — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID");
    }
  }

  setDataProviders(
    getPositions: () => Position[],
    getStrategy: () => StrategyConfig,
    getStats: () => any,
    getWallet: () => Wallet | null,
    switchNetwork: (network: "mainnet" | "devnet") => Promise<void>,
    importWallet?: (privateKey: string) => Promise<boolean>,
    updateStrategy?: (updates: Partial<StrategyConfig>) => void
  ) {
    this.getPositions = getPositions;
    this.getStrategy = getStrategy;
    this.getStats = getStats;
    this.getWallet = getWallet;
    this.switchNetwork = switchNetwork;
    this.importWallet = importWallet;
    this.updateStrategy = updateStrategy;
  }

  private navButtons(): InlineButton[][] {
    return [
      [
        { text: "📊 Portfolio", callback_data: "cmd_portfolio" },
        { text: "📋 Positions", callback_data: "cmd_positions" },
      ],
      [
        { text: "🧠 Strategy", callback_data: "cmd_strategy" },
        { text: "📈 Stats", callback_data: "cmd_stats" },
      ],
      [
        { text: "⚙️ Settings", callback_data: "cmd_settings" },
        { text: "❓ Help", callback_data: "cmd_help" },
      ],
    ];
  }

  private registerCommands() {
    this.commands.set("start", {
      command: "/start", description: "Dashboard",
      handler: async () => this.cmdStart(),
    });
    this.commands.set("portfolio", {
      command: "/portfolio", description: "Open positions",
      handler: async () => this.cmdPortfolio(),
    });
    this.commands.set("positions", {
      command: "/positions", description: "All positions",
      handler: async () => this.cmdPositions(),
    });
    this.commands.set("strategy", {
      command: "/strategy", description: "View strategy",
      handler: async () => this.cmdStrategy(),
    });
    this.commands.set("stats", {
      command: "/stats", description: "Performance stats",
      handler: async () => this.cmdStats(),
    });
    this.commands.set("wallet", {
      command: "/wallet", description: "Wallet info",
      handler: async () => this.cmdWallet(),
    });
    this.commands.set("settings", {
      command: "/settings", description: "Settings & config",
      handler: async () => this.cmdSettings(),
    });
    this.commands.set("help", {
      command: "/help", description: "Show help",
      handler: async () => this.cmdHelp(),
    });
    this.commands.set("cancel", {
      command: "/cancel", description: "Cancel action",
      handler: async (chatId) => this.cmdCancel(chatId),
    });
  }

  // ─── Dashboard ───────────────────────────────────
  private async cmdStart(): Promise<{ text: string; buttons: InlineButton[][] }> {
    const wallet = this.getWallet?.();
    const stats = this.getStats?.();
    const strategy = this.getStrategy?.();
    const network = wallet?.getNetwork() || "devnet";
    const openCount = stats?.trading?.open || 0;
    const closedCount = stats?.trading?.closed || 0;
    const winRate = stats?.trading?.winRate || 0;
    const pnl = stats?.trading?.totalPnl || 0;
    const tracked = stats?.db?.total || 0;

    let walletLine = "❌ Not connected";
    if (wallet) {
      const addr = `${wallet.address.slice(0, 6)}...${wallet.address.slice(-4)}`;
      try {
        const balance = await wallet.getBalanceFormatted();
        walletLine = `✅ ${addr} — ${balance}`;
      } catch {
        walletLine = `✅ ${addr}`;
      }
    }

    const text = [
      "🤖 <b>PROJECTX</b>",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "Self-Improving Memecoin Trader",
      "",
      `🌐 Network: <b>${network.toUpperCase()}</b>`,
      `💳 Wallet: ${walletLine}`,
      `💰 Trade Size: <b>${strategy?.positionSize || 0.1} SOL</b>`,
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "📊 <b>DASHBOARD</b>",
      `  Tracked: ${tracked} tokens`,
      `  Positions: ${openCount} open / ${closedCount} closed`,
      `  Win Rate: ${winRate.toFixed(0)}%`,
      `  PnL: ${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} SOL`,
      `  Strategy: v${strategy?.version || "?"}`,
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    return { text, buttons: this.navButtons() };
  }

  // ─── Portfolio ───────────────────────────────────
  private async cmdPortfolio(): Promise<{ text: string; buttons: InlineButton[][] }> {
    if (!this.getPositions) return { text: "❌ Data not available", buttons: this.navButtons() };
    
    const positions = this.getPositions().filter(p => p.status === "open");
    
    if (positions.length === 0) {
      return {
        text: "📭 <b>No open positions</b>\n\nThe scanner is watching for new tokens...",
        buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]],
      };
    }

    const lines = ["📊 <b>Open Positions</b>", "━━━━━━━━━━━━━━━━━━━━━━"];
    
    for (const pos of positions) {
      const age = Math.floor((Date.now() - pos.entryTime.getTime()) / 1000);
      const mins = Math.floor(age / 60);
      const secs = age % 60;
      lines.push(
        "",
        `<b>${esc(pos.symbol)}</b>`,
        `  Entry: $${pos.entryPrice.toFixed(8)}`,
        `  Size: ${pos.amount} SOL`,
        `  Age: ${mins}m ${secs}s`,
        `  Score: ${pos.entryScore || "?"}/100`,
      );
    }

    lines.push("", "━━━━━━━━━━━━━━━━━━━━━━");
    return { text: lines.join("\n"), buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]] };
  }

  // ─── Positions ───────────────────────────────────
  private async cmdPositions(): Promise<{ text: string; buttons: InlineButton[][] }> {
    if (!this.getPositions) return { text: "❌ Data not available", buttons: this.navButtons() };
    
    const all = this.getPositions();
    
    if (all.length === 0) {
      return {
        text: "📭 <b>No positions yet</b>",
        buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]],
      };
    }

    const lines = ["📋 <b>All Positions</b>", "━━━━━━━━━━━━━━━━━━━━━━"];
    
    for (const pos of all) {
      const emoji = pos.status === "open" ? "🟢" : (pos.pnl || 0) >= 0 ? "💰" : "💸";
      lines.push("", `${emoji} <b>${esc(pos.symbol)}</b>`);
      lines.push(`  Entry: $${pos.entryPrice.toFixed(8)}`);
      lines.push(`  Size: ${pos.amount} SOL`);
      
      if (pos.status === "closed" && pos.pnlPercent !== undefined) {
        const sign = pos.pnlPercent >= 0 ? "+" : "";
        lines.push(`  PnL: ${sign}${pos.pnlPercent.toFixed(1)}%`);
        lines.push(`  Exit: ${esc(pos.exitReason)}`);
      } else {
        const age = Math.floor((Date.now() - pos.entryTime.getTime()) / 1000);
        lines.push(`  Age: ${Math.floor(age / 60)}m`);
      }
    }

    lines.push("", "━━━━━━━━━━━━━━━━━━━━━━");
    return { text: lines.join("\n"), buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]] };
  }

  // ─── Strategy ────────────────────────────────────
  private async cmdStrategy(): Promise<{ text: string; buttons: InlineButton[][] }> {
    if (!this.getStrategy) return { text: "❌ Data not available", buttons: this.navButtons() };
    
    const s = this.getStrategy();
    
    const text = [
      `🧠 <b>Strategy v${s.version}</b>`,
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      "📥 <b>ENTRY</b>",
      `  Score ≥ ${s.minScore}`,
      `  Liquidity ≥ $${s.minLiquidity.toLocaleString()}`,
      `  Buy Ratio ≥ ${(s.minBuyRatio * 100).toFixed(0)}%`,
      "",
      "📤 <b>EXIT</b>",
      `  Stop Loss: ${s.stopLossPercent}%`,
      `  Take Profit: ${s.takeProfitPercent}%`,
      `  Max Hold: ${s.maxHoldTimeSeconds}s`,
      "",
      "💰 <b>SIZING</b>",
      `  ${s.positionSize} SOL per trade`,
      `  Max ${s.maxPositions} positions`,
      "",
      "📊 <b>PERFORMANCE</b>",
      `  Win Rate: ${s.performance.winRate.toFixed(0)}%`,
      `  Avg PnL: ${s.performance.avgPnl.toFixed(1)}%`,
      `  Total Trades: ${s.performance.totalTrades}`,
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    const buttons: InlineButton[][] = [
      [{ text: "🔙 Back", callback_data: "cmd_start" }],
    ];

    return { text, buttons };
  }

  // ─── Stats ───────────────────────────────────────
  private async cmdStats(): Promise<{ text: string; buttons: InlineButton[][] }> {
    if (!this.getStats) return { text: "❌ Data not available", buttons: this.navButtons() };
    
    const stats = this.getStats();
    
    const text = [
      "📈 <b>PERFORMANCE STATS</b>",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      "💹 <b>TRADING</b>",
      `  Total: ${stats.trading?.total || 0}`,
      `  Open: ${stats.trading?.open || 0}`,
      `  Closed: ${stats.trading?.closed || 0}`,
      `  Wins: ${stats.trading?.wins || 0}`,
      `  Losses: ${stats.trading?.losses || 0}`,
      `  Win Rate: ${(stats.trading?.winRate || 0).toFixed(0)}%`,
      `  PnL: ${(stats.trading?.totalPnl || 0) >= 0 ? "+" : ""}${(stats.trading?.totalPnl || 0).toFixed(4)} SOL`,
      "",
      "🗄️ <b>DATABASE</b>",
      `  Tracked: ${stats.db?.total || 0}`,
      `  Watching: ${stats.db?.watching || 0}`,
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    return { text, buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]] };
  }

  // ─── Wallet ──────────────────────────────────────
  private async cmdWallet(): Promise<{ text: string; buttons: InlineButton[][] }> {
    const wallet = this.getWallet?.();
    const network = wallet?.getNetwork() || "devnet";
    
    if (!wallet) {
      return {
        text: [
          "💳 <b>WALLET</b>",
          "━━━━━━━━━━━━━━━━━━━━━━",
          "",
          "❌ No wallet connected",
          "",
          "Tap <b>Import Wallet</b> to add",
          "your Solana private key.",
          "",
          "⚠️ Only you see messages",
          "in this private chat.",
          "",
          "━━━━━━━━━━━━━━━━━━━━━━",
        ].join("\n"),
        buttons: [
          [{ text: "📥 Import Wallet", callback_data: "wallet_import" }],
          [{ text: "📂 Load from .env", callback_data: "wallet_load_env" }],
          [{ text: "🔙 Back", callback_data: "cmd_start" }],
        ],
      };
    }

    let balance = "Unknown";
    try { balance = await wallet.getBalanceFormatted(); } catch (e) {}

    const addr = `${wallet.address.slice(0, 8)}...${wallet.address.slice(-4)}`;

    const text = [
      "💳 <b>WALLET</b>",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      `<b>Address:</b> ${addr}`,
      `<b>Network:</b> ${network.toUpperCase()}`,
      `<b>Balance:</b> ${balance}`,
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    const buttons: InlineButton[][] = [
      [
        { text: "🟢 Devnet", callback_data: "network_devnet" },
        { text: "🔴 Mainnet", callback_data: "network_mainnet" },
      ],
      [
        { text: "🔄 Change Wallet", callback_data: "wallet_import" },
        { text: "💰 Refresh", callback_data: "wallet_info" },
      ],
      [{ text: "🔙 Back", callback_data: "cmd_start" }],
    ];

    return { text, buttons };
  }

  // ─── Settings ────────────────────────────────────
  private async cmdSettings(): Promise<{ text: string; buttons: InlineButton[][] }> {
    const wallet = this.getWallet?.();
    const strategy = this.getStrategy?.();
    const network = wallet?.getNetwork() || "devnet";
    const walletStatus = wallet ? "✅ Connected" : "❌ Not connected";
    
    const devnetLabel = network === "devnet" ? "🟢 ✓ Devnet" : "🟢 Devnet";
    const mainnetLabel = network === "mainnet" ? "🔴 ✓ Mainnet" : "🔴 Mainnet";

    const text = [
      "⚙️ <b>SETTINGS</b>",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      `🌐 <b>Network:</b> ${network.toUpperCase()}`,
      `💳 <b>Wallet:</b> ${walletStatus}`,
      `💰 <b>Trade Size:</b> ${strategy?.positionSize || 0.1} SOL`,
      `📊 <b>Max Positions:</b> ${strategy?.maxPositions || 5}`,
      `🛑 <b>Stop Loss:</b> ${strategy?.stopLossPercent || -30}%`,
      `🎯 <b>Take Profit:</b> ${strategy?.takeProfitPercent || 100}%`,
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    const buttons: InlineButton[][] = [
      [
        { text: devnetLabel, callback_data: "network_devnet" },
        { text: mainnetLabel, callback_data: "network_mainnet" },
      ],
      [
        { text: `💰 Size: ${strategy?.positionSize || 0.1} SOL`, callback_data: "set_trade_size" },
        { text: `📊 Max: ${strategy?.maxPositions || 5}`, callback_data: "set_max_positions" },
      ],
      [
        { text: `🛑 SL: ${strategy?.stopLossPercent || -30}%`, callback_data: "set_stop_loss" },
        { text: `🎯 TP: ${strategy?.takeProfitPercent || 100}%`, callback_data: "set_take_profit" },
      ],
      [
        { text: "💳 Wallet", callback_data: "cmd_wallet" },
        { text: "🧠 Strategy", callback_data: "cmd_strategy" },
      ],
      [{ text: "🔙 Back", callback_data: "cmd_start" }],
    ];

    return { text, buttons };
  }

  // ─── Help ────────────────────────────────────────
  private async cmdHelp(): Promise<{ text: string; buttons: InlineButton[][] }> {
    const text = [
      "❓ <b>HELP</b>",
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      "📱 <b>Commands:</b>",
      "  /start — Dashboard",
      "  /portfolio — Open positions",
      "  /positions — All positions",
      "  /strategy — Current strategy",
      "  /stats — Performance stats",
      "  /wallet — Wallet info",
      "  /settings — Config & sizing",
      "  /cancel — Cancel action",
      "  /help — This message",
      "",
      "🔘 <b>Buttons:</b>",
      "  Every screen has navigation",
      "  Settings lets you change trade size,",
      "  stop loss, take profit, network",
      "  Alerts have Buy/Chart/Skip",
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    return { text, buttons: [[{ text: "🔙 Back", callback_data: "cmd_start" }]] };
  }

  // ─── Cancel ──────────────────────────────────────
  private async cmdCancel(chatId: string): Promise<{ text: string; buttons: InlineButton[][] }> {
    this.waitingForPrivateKey.delete(chatId);
    this.waitingForTradeSize.delete(chatId);
    this.waitingForMaxPositions.delete(chatId);
    return { text: "✅ Cancelled.", buttons: this.navButtons() };
  }

  // ─── Send / Edit / Delete ────────────────────────
  async sendMessage(text: string, chatId?: string, buttons?: InlineButton[][]): Promise<boolean> {
    if (!this.enabled) return false;
    try {
      const url = `https://api.telegram.org/bot${this.botToken}/sendMessage`;
      const body: any = {
        chat_id: chatId || this.chatId,
        text, parse_mode: "HTML", disable_web_page_preview: true,
      };
      if (buttons && buttons.length > 0) body.reply_markup = { inline_keyboard: buttons };
      const response = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!response.ok) { const err = await response.text(); logger.error(`TG failed (${response.status}): ${err}`); return false; }
      return true;
    } catch (error) { logger.error("TG error:", error); return false; }
  }

  private async editMessage(chatId: string, messageId: number, text: string, buttons?: InlineButton[][]): Promise<boolean> {
    if (!this.enabled) return false;
    try {
      const url = `https://api.telegram.org/bot${this.botToken}/editMessageText`;
      const body: any = { chat_id: chatId, message_id: messageId, text, parse_mode: "HTML", disable_web_page_preview: true };
      if (buttons && buttons.length > 0) body.reply_markup = { inline_keyboard: buttons };
      const response = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      return response.ok;
    } catch (error) { return false; }
  }

  private async answerCallbackQuery(id: string, text?: string, showAlert?: boolean): Promise<void> {
    try {
      await fetch(`https://api.telegram.org/bot${this.botToken}/answerCallbackQuery`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ callback_query_id: id, text, show_alert: showAlert || false }),
      });
    } catch {}
  }

  private async deleteMessage(chatId: string, messageId: number): Promise<void> {
    try {
      await fetch(`https://api.telegram.org/bot${this.botToken}/deleteMessage`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_id: chatId, message_id: messageId }),
      });
    } catch {}
  }

  // ─── Alerts ──────────────────────────────────────
  async notifyHighScore(token: TokenWithScore): Promise<void> {
    const createdAtMs = token.createdAt instanceof Date ? token.createdAt.getTime() : new Date(token.createdAt).getTime();
    const ageSec = Math.floor((Date.now() - createdAtMs) / 1000);
    const ageDisplay = ageSec < 60 ? `${ageSec}s` : `${Math.floor(ageSec / 60)}m ${ageSec % 60}s`;
    
    const message = [
      `🔥 <b>HIGH SCORE: ${esc(token.symbol)}</b>`,
      "━━━━━━━━━━━━━━━━━━━━━━",
      "",
      `<b>Name:</b> ${esc(token.name)}`,
      `<b>Score:</b> ${token.score}/100`,
      `<b>Age:</b> ${ageDisplay}`,
      `<b>Liquidity:</b> $${token.liquidity.toFixed(0)}`,
      `<b>Market Cap:</b> $${token.marketCap.toFixed(0)}`,
      "",
      "<b>Reasons:</b>",
      ...token.reasons.slice(0, 5).map(r => `• ${esc(r)}`),
      "",
      "━━━━━━━━━━━━━━━━━━━━━━",
    ].join("\n");

    const buttons: InlineButton[][] = [
      [
        { text: "🟢 Buy", callback_data: `buy_${token.mint}` },
        { text: "📊 Chart", url: `https://dexscreener.com/solana/${token.mint}` },
      ],
      [
        { text: "🔗 Pump.fun", url: `https://pump.fun/coin/${token.mint}` },
        { text: "❌ Skip", callback_data: `skip_${token.mint}` },
      ],
    ];

    await this.sendMessage(message, undefined, buttons);
  }

  async notifyNewToken(token: TokenWithScore): Promise<void> {
    if (token.score < 60) return;
    const createdAtMs2 = token.createdAt instanceof Date ? token.createdAt.getTime() : new Date(token.createdAt).getTime();
    const ageSec = Math.floor((Date.now() - createdAtMs2) / 1000);
    const message = [
      `🆕 <b>${esc(token.symbol)}</b>`,
      `Score: ${token.score}/100 | Age: ${ageSec}s | Liq: $${token.liquidity.toFixed(0)}`,
    ].join("\n");
    const buttons: InlineButton[][] = [
      [
        { text: "📊 Chart", url: `https://dexscreener.com/solana/${token.mint}` },
        { text: "🔗 Pump.fun", url: `https://pump.fun/coin/${token.mint}` },
      ],
    ];
    await this.sendMessage(message, undefined, buttons);
  }

  async notifyTrade(action: string, details: string): Promise<void> {
    const emoji = action === "BUY" ? "🟢" : "🔴";
    await this.sendMessage(`${emoji} <b>${action}</b>\n\n${esc(details)}`);
  }

  async notifyStrategyRewrite(version: number, changes: string[]): Promise<void> {
    const message = [
      `🧠 <b>Strategy Rewritten — v${version}</b>`, "",
      ...changes.map(c => `• ${esc(c)}`),
    ].join("\n");
    await this.sendMessage(message, undefined, [[{ text: "📋 View Strategy", callback_data: "cmd_strategy" }]]);
  }

  // ─── Save key to .env ────────────────────────────
  private savePrivateKeyToEnv(privateKey: string): void {
    const envPath = path.join(process.cwd(), ".env");
    let envContent = "";
    try { envContent = fs.readFileSync(envPath, "utf-8"); } catch { envContent = ""; }
    const keyRegex = /^SOLANA_PRIVATE_KEY=.*$/m;
    if (keyRegex.test(envContent)) {
      envContent = envContent.replace(keyRegex, `SOLANA_PRIVATE_KEY=${privateKey}`);
    } else {
      if (envContent.length > 0 && !envContent.endsWith("\n")) envContent += "\n";
      envContent += `SOLANA_PRIVATE_KEY=${privateKey}\n`;
    }
    fs.writeFileSync(envPath, envContent, "utf-8");
    process.env.SOLANA_PRIVATE_KEY = privateKey;
  }

  // ─── Set bot commands ────────────────────────────
  private async setBotCommands(): Promise<void> {
    if (!this.enabled) return;
    try {
      const commands = [
        { command: "start", description: "Dashboard" },
        { command: "portfolio", description: "Open positions" },
        { command: "positions", description: "All positions" },
        { command: "strategy", description: "Current strategy" },
        { command: "stats", description: "Performance stats" },
        { command: "wallet", description: "Wallet info" },
        { command: "settings", description: "Config & sizing" },
        { command: "cancel", description: "Cancel action" },
        { command: "help", description: "Show help" },
      ];
      await fetch(`https://api.telegram.org/bot${this.botToken}/setMyCommands`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ commands }),
      });
      logger.success("Telegram bot commands registered");
    } catch {}
  }

  // ─── Polling ─────────────────────────────────────
  async startPolling(): Promise<void> {
    if (!this.enabled || this.pollingActive) return;
    this.pollingActive = true;
    await this.setBotCommands();
    logger.info("Telegram bot polling started");

    const poll = async () => {
      if (!this.pollingActive) return;
      try {
        const response = await fetch(`https://api.telegram.org/bot${this.botToken}/getUpdates?offset=${this.pollingOffset}&timeout=10`);
        const data = await response.json() as any;
        if (data.ok && data.result) {
          for (const update of data.result) {
            this.pollingOffset = update.update_id + 1;

            // Messages
            if (update.message) {
              const chatId = update.message.chat.id.toString();
              const text = update.message.text || "";

              // Multi-step flows
              if (this.waitingForPrivateKey.has(chatId)) {
                await this.deleteMessage(chatId, update.message.message_id);
                await this.handleWalletImport(chatId, text);
                continue;
              }
              if (this.waitingForTradeSize.has(chatId)) {
                await this.deleteMessage(chatId, update.message.message_id);
                await this.handleTradeSizeInput(chatId, text);
                continue;
              }
              if (this.waitingForMaxPositions.has(chatId)) {
                await this.deleteMessage(chatId, update.message.message_id);
                await this.handleMaxPositionsInput(chatId, text);
                continue;
              }

              // Commands
              if (text.startsWith("/")) {
                const cmd = text.split(" ")[0].slice(1).toLowerCase();
                const handler = this.commands.get(cmd);
                if (handler) {
                  const { text: replyText, buttons } = await handler.handler(chatId, text, 0);
                  await this.sendMessage(replyText, chatId, buttons);
                } else {
                  await this.sendMessage("❓ Unknown command. Type /help", chatId, this.navButtons());
                }
              }
            }

            // Callback queries
            if (update.callback_query) {
              const query = update.callback_query;
              const chatId = query.message?.chat?.id?.toString() || this.chatId;
              const messageId = query.message?.message_id;
              const data = query.data || "";

              // Nav buttons (cmd_*)
              if (data.startsWith("cmd_")) {
                const cmd = data.replace("cmd_", "");
                const handler = this.commands.get(cmd);
                if (handler) {
                  const { text: replyText, buttons } = await handler.handler(chatId, "", 0);
                  if (messageId) {
                    const edited = await this.editMessage(chatId, messageId, replyText, buttons);
                    if (!edited) await this.sendMessage(replyText, chatId, buttons);
                  } else {
                    await this.sendMessage(replyText, chatId, buttons);
                  }
                  await this.answerCallbackQuery(query.id);
                }
                continue;
              }

              await this.handleCallbackQuery(query.id, chatId, data, messageId);
            }
          }
        }
      } catch {}
      if (this.pollingActive) setTimeout(poll, 1000);
    };
    poll();
  }

  async stopPolling(): Promise<void> { this.pollingActive = false; }

  // ─── Handle callbacks ────────────────────────────
  private async handleCallbackQuery(queryId: string, chatId: string, data: string, messageId?: number): Promise<void> {
    // Network
    if (data === "network_devnet") {
      await this.switchNetwork?.("devnet");
      await this.answerCallbackQuery(queryId, "✅ Switched to Devnet");
      const { text, buttons } = await this.cmdSettings();
      if (messageId) { if (!(await this.editMessage(chatId, messageId, text, buttons))) await this.sendMessage(text, chatId, buttons); }
      else await this.sendMessage(text, chatId, buttons);
      return;
    }
    if (data === "network_mainnet") {
      await this.answerCallbackQuery(queryId);
      await this.sendMessage(
        "⚠️ <b>Switch to Mainnet?</b>\n\nThis uses <b>real SOL</b> for trades.",
        chatId,
        [
          [{ text: "✅ Confirm", callback_data: "confirm_mainnet" }, { text: "❌ Cancel", callback_data: "cmd_settings" }],
        ]
      );
      return;
    }
    if (data === "confirm_mainnet") {
      await this.switchNetwork?.("mainnet");
      await this.answerCallbackQuery(queryId, "⚠️ Switched to Mainnet");
      const { text, buttons } = await this.cmdSettings();
      if (messageId) { if (!(await this.editMessage(chatId, messageId, text, buttons))) await this.sendMessage(text, chatId, buttons); }
      else await this.sendMessage(text, chatId, buttons);
      return;
    }

    // Wallet
    if (data === "wallet_info") {
      const wallet = this.getWallet?.();
      if (wallet) {
        try { const b = await wallet.getBalanceFormatted(); await this.answerCallbackQuery(queryId, `Balance: ${b}`, true); }
        catch { await this.answerCallbackQuery(queryId, "Error", true); }
      } else await this.answerCallbackQuery(queryId, "No wallet", true);
      return;
    }
    if (data === "wallet_import") {
      await this.answerCallbackQuery(queryId);
      this.waitingForPrivateKey.add(chatId);
      await this.sendMessage(
        [
          "🔐 <b>Import Wallet</b>",
          "━━━━━━━━━━━━━━━━━━━━━━",
          "",
          "Paste your Solana <b>private key</b>.",
          "",
          "• Auto-deleted from chat",
          "• Saved to <code>.env</code>",
          "",
          "Type /cancel to abort.",
          "━━━━━━━━━━━━━━━━━━━━━━",
        ].join("\n"),
        chatId
      );
      return;
    }
    if (data === "wallet_load_env") {
      await this.answerCallbackQuery(queryId);
      const envKey = process.env.SOLANA_PRIVATE_KEY;
      if (envKey && envKey.length > 10) {
        const wallet = this.getWallet?.();
        if (wallet) {
          const addr = `${wallet.address.slice(0, 8)}...${wallet.address.slice(-4)}`;
          await this.sendMessage(`✅ <b>Loaded from .env</b>\n\n<b>Address:</b> ${addr}`, chatId, [[{ text: "💳 Wallet", callback_data: "cmd_wallet" }]]);
        } else {
          await this.sendMessage("⚠️ Key in .env but wallet failed to init.\n\nTry importing.", chatId, [[{ text: "📥 Import", callback_data: "wallet_import" }]]);
        }
      } else {
        await this.sendMessage("❌ No key in <code>.env</code>", chatId, [[{ text: "📥 Import", callback_data: "wallet_import" }]]);
      }
      return;
    }

    // Stats popup
    if (data === "stats_info") {
      const stats = this.getStats?.();
      if (stats) {
        await this.answerCallbackQuery(queryId, `Win: ${(stats.trading?.winRate || 0).toFixed(0)}% | PnL: ${(stats.trading?.totalPnl || 0).toFixed(4)} SOL`, true);
      }
      return;
    }

    // Trade size
    if (data === "set_trade_size") {
      await this.answerCallbackQuery(queryId);
      logger.info(`[TG] Trade size button tapped, waiting for input from ${chatId}`);
      this.waitingForTradeSize.add(chatId);
      const s = this.getStrategy?.();
      await this.sendMessage(
        [
          "💰 <b>Set Trade Size</b>",
          "━━━━━━━━━━━━━━━━━━━━━━",
          "",
          `Current: <b>${s?.positionSize || 0.1} SOL</b>`,
          "",
          "Send a number (e.g. <code>0.05</code>, <code>0.2</code>, <code>1.0</code>):",
          "",
          "Type /cancel to abort.",
          "━━━━━━━━━━━━━━━━━━━━━━",
        ].join("\n"),
        chatId
      );
      return;
    }

    // Max positions
    if (data === "set_max_positions") {
      await this.answerCallbackQuery(queryId);
      this.waitingForMaxPositions.add(chatId);
      const s = this.getStrategy?.();
      await this.sendMessage(
        [
          "📊 <b>Set Max Positions</b>",
          "━━━━━━━━━━━━━━━━━━━━━━",
          "",
          `Current: <b>${s?.maxPositions || 5}</b>`,
          "",
          "Send a number (e.g. <code>3</code>, <code>5</code>, <code>10</code>):",
          "",
          "Type /cancel to abort.",
          "━━━━━━━━━━━━━━━━━━━━━━",
        ].join("\n"),
        chatId
      );
      return;
    }

    // Stop loss
    if (data === "set_stop_loss") {
      await this.answerCallbackQuery(queryId);
      // Quick cycle: -10, -15, -20, -25, -30, -35, -40, -50
      const s = this.getStrategy?.();
      const current = s?.stopLossPercent || -30;
      const options = [-10, -15, -20, -25, -30, -35, -40, -50];
      const buttons: InlineButton[][] = options.map(val => [
        { text: val === current ? `${val}% ✓` : `${val}%`, callback_data: `apply_sl_${val}` },
      ]);
      buttons.push([{ text: "🔙 Back", callback_data: "cmd_settings" }]);
      await this.sendMessage(
        `🛑 <b>Stop Loss</b>\n\nCurrent: <b>${current}%</b>\nPick a new value:`,
        chatId, buttons
      );
      return;
    }

    if (data.startsWith("apply_sl_")) {
      const val = parseInt(data.replace("apply_sl_", ""));
      this.updateStrategy?.({ stopLossPercent: val });
      await this.answerCallbackQuery(queryId, `Stop loss set to ${val}%`);
      const { text, buttons } = await this.cmdSettings();
      if (messageId) { if (!(await this.editMessage(chatId, messageId, text, buttons))) await this.sendMessage(text, chatId, buttons); }
      else await this.sendMessage(text, chatId, buttons);
      return;
    }

    // Take profit
    if (data === "set_take_profit") {
      await this.answerCallbackQuery(queryId);
      const s = this.getStrategy?.();
      const current = s?.takeProfitPercent || 100;
      const options = [50, 75, 100, 150, 200, 300, 500];
      const buttons: InlineButton[][] = options.map(val => [
        { text: val === current ? `${val}% ✓` : `${val}%`, callback_data: `apply_tp_${val}` },
      ]);
      buttons.push([{ text: "🔙 Back", callback_data: "cmd_settings" }]);
      await this.sendMessage(
        `🎯 <b>Take Profit</b>\n\nCurrent: <b>${current}%</b>\nPick a new value:`,
        chatId, buttons
      );
      return;
    }

    if (data.startsWith("apply_tp_")) {
      const val = parseInt(data.replace("apply_tp_", ""));
      this.updateStrategy?.({ takeProfitPercent: val });
      await this.answerCallbackQuery(queryId, `Take profit set to ${val}%`);
      const { text, buttons } = await this.cmdSettings();
      if (messageId) { if (!(await this.editMessage(chatId, messageId, text, buttons))) await this.sendMessage(text, chatId, buttons); }
      else await this.sendMessage(text, chatId, buttons);
      return;
    }

    // Buy / Skip
    if (data.startsWith("buy_")) {
      const mint = data.replace("buy_", "");
      await this.answerCallbackQuery(queryId, "🟢 Buy signal!");
      await this.sendMessage(
        `🟢 <b>Manual Buy</b>\n\nMint: <code>${mint}</code>`,
        chatId,
        [
          [{ text: "📊 Chart", url: `https://dexscreener.com/solana/${mint}` }, { text: "🔗 Pump.fun", url: `https://pump.fun/coin/${mint}` }],
          [{ text: "🔙 Back", callback_data: "cmd_start" }],
        ]
      );
      return;
    }
    if (data.startsWith("skip_")) {
      await this.answerCallbackQuery(queryId, "❌ Skipped");
      return;
    }
  }

  // ─── Handle text inputs ──────────────────────────
  private async handleWalletImport(chatId: string, text: string): Promise<void> {
    this.waitingForPrivateKey.delete(chatId);
    const key = text.trim();
    
    if (key.length < 32 || key.length > 128) {
      await this.sendMessage("❌ <b>Invalid key</b>\n\nExpected base58 Solana private key (32-88 chars).", chatId, [
        [{ text: "🔄 Try Again", callback_data: "wallet_import" }],
        [{ text: "🔙 Back", callback_data: "cmd_wallet" }],
      ]);
      return;
    }

    await this.sendMessage("⏳ Importing...", chatId);

    if (this.importWallet) {
      try {
        const success = await this.importWallet(key);
        if (success) {
          this.savePrivateKeyToEnv(key);
          const wallet = this.getWallet?.();
          const addr = wallet ? `${wallet.address.slice(0, 8)}...${wallet.address.slice(-4)}` : "unknown";
          await this.sendMessage(
            [
              "✅ <b>Wallet Imported!</b>",
              `<b>Address:</b> ${addr}`,
              `<b>Network:</b> ${(wallet?.getNetwork() || "devnet").toUpperCase()}`,
              "",
              "Saved to <code>.env</code>.",
            ].join("\n"),
            chatId,
            [[{ text: "💳 Wallet", callback_data: "cmd_wallet" }], [{ text: "🔙 Back", callback_data: "cmd_start" }]]
          );
        } else {
          await this.sendMessage("❌ <b>Import Failed</b>\n\nInvalid key.", chatId, [
            [{ text: "🔄 Try Again", callback_data: "wallet_import" }],
          ]);
        }
      } catch {
        await this.sendMessage("❌ <b>Error</b>\n\nCheck key format.", chatId, [
          [{ text: "🔄 Try Again", callback_data: "wallet_import" }],
        ]);
      }
    } else {
      this.savePrivateKeyToEnv(key);
      await this.sendMessage("✅ <b>Key saved to .env</b>\n\nRestart bot to load.", chatId, [
        [{ text: "🔙 Back", callback_data: "cmd_start" }],
      ]);
    }
  }

  private async handleTradeSizeInput(chatId: string, text: string): Promise<void> {
    this.waitingForTradeSize.delete(chatId);
    const val = parseFloat(text.trim());
    if (isNaN(val) || val <= 0 || val > 100) {
      await this.sendMessage("❌ <b>Invalid number</b>\n\nSend a value like <code>0.05</code> or <code>0.2</code>.", chatId, [
        [{ text: "🔄 Try Again", callback_data: "set_trade_size" }],
        [{ text: "🔙 Back", callback_data: "cmd_settings" }],
      ]);
      return;
    }
    this.updateStrategy?.({ positionSize: val });
    await this.answerCallbackQuery(chatId, `Trade size set to ${val} SOL`);
    const { text: replyText, buttons } = await this.cmdSettings();
    await this.sendMessage(replyText, chatId, buttons);
  }

  private async handleMaxPositionsInput(chatId: string, text: string): Promise<void> {
    this.waitingForMaxPositions.delete(chatId);
    const val = parseInt(text.trim());
    if (isNaN(val) || val < 1 || val > 50) {
      await this.sendMessage("❌ <b>Invalid number</b>\n\nSend a value like <code>3</code> or <code>10</code>.", chatId, [
        [{ text: "🔄 Try Again", callback_data: "set_max_positions" }],
        [{ text: "🔙 Back", callback_data: "cmd_settings" }],
      ]);
      return;
    }
    this.updateStrategy?.({ maxPositions: val });
    await this.answerCallbackQuery(chatId, `Max positions set to ${val}`);
    const { text: replyText, buttons } = await this.cmdSettings();
    await this.sendMessage(replyText, chatId, buttons);
  }

  isEnabled(): boolean { return this.enabled; }
}
