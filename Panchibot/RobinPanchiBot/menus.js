import { Markup } from 'telegraf';
import { shortAddr } from './wallet.js';
import { getEthUsdPrice, getTokenMarketData, fmtUsd, getCachedEthUsdPrice, fmtTokenAmount } from './price.js';
import { BRIDGE_DIRECTION } from './bridge.js';
import {
  getUser,
  getSettings,
  getActiveWallet,
  getPosition,
  getActiveAutoRuleForPosition,
  getAllPositionsForUser,
} from './storage.js';
import { dualEthBalanceLines, gasEstimateLine } from './format.js';
import { FALLBACK_GAS_LIMIT_BUY } from './config.js';
import { positionRefs } from './state.js';

export function mainMenu() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🔍 Trade Token', 'menu_trade')],
    [Markup.button.callback('📈 Portfolio', 'menu_portfolio'), Markup.button.callback('📊 Positions', 'menu_positions')],
    [Markup.button.callback('🎯 Sniper', 'menu_snipe'), Markup.button.callback('⚡ Momentum', 'menu_momentum')],
    [Markup.button.callback('👀 Alerts', 'menu_watchlist'), Markup.button.callback('👁 Copy Trade', 'menu_copytrade')],
    [Markup.button.callback('💼 Wallets', 'menu_wallets'), Markup.button.callback('💰 Balance', 'menu_balance')],
    [Markup.button.callback('🌉 Bridge', 'menu_bridge'), Markup.button.callback('⏰ Limits', 'menu_limitorders')],
    [Markup.button.callback('🎟 Rewards', 'menu_rewards'), Markup.button.callback('⚙️ Settings', 'menu_settings')],
    [Markup.button.callback('❓ Help', 'menu_help')],
    [Markup.button.url('🐦 X', 'https://x.com/robinpanchi')],
  ]);
}

export function walletsMenu(uid) {
  const user = getUser(uid);
  const rows = user.wallets.map((w) => {
    const active = w.id === user.activeWalletId ? '✅ ' : '';
    return [Markup.button.callback(`${active}${w.name} (${shortAddr(w.address)})`, `wallet_${w.id}`)];
  });
  rows.push([
    Markup.button.callback('➕ Create New', 'wallet_create'),
    Markup.button.callback('📥 Import', 'wallet_import'),
  ]);
  rows.push([Markup.button.callback('📤 Batch Fund', 'batchfund_start')]);
  rows.push([Markup.button.callback('📥 Batch Collect', 'collect_start')]);
  rows.push([Markup.button.callback('⬅️ Back', 'menu_main')]);
  return Markup.inlineKeyboard(rows);
}

export function walletDetailMenu(walletId) {
  return Markup.inlineKeyboard([
    [Markup.button.callback('✅ Set Active', `wallet_activate_${walletId}`)],
    [Markup.button.callback('✏️ Rename', `wallet_rename_${walletId}`)],
    [Markup.button.callback('🔑 Export Key', `wallet_export_${walletId}`)],
    [Markup.button.callback('🗑 Remove', `wallet_remove_${walletId}`)],
    [Markup.button.callback('⬅️ Back', 'menu_wallets')],
  ]);
}

export function exportConfirmMenu(walletId) {
  return Markup.inlineKeyboard([
    [Markup.button.callback('⚠️ Yes, show my key', `wallet_export_confirm_${walletId}`)],
    [Markup.button.callback('❌ Cancel', 'menu_wallets')],
  ]);
}

/**
 * Settings amounts (buy presets, max buy, max bridge) are stored in ETH
 * internally, but displayed as USD (mimics FOMO-style UX). Uses the
 * short-lived cached price since this menu builder is synchronous; falls
 * back to a raw ETH label on the rare case there's no fresh cached price.
 */
function usdOrEthLabel(ethAmount) {
  const ethUsd = getCachedEthUsdPrice();
  return ethUsd ? fmtUsd(ethAmount * ethUsd) : `${ethAmount} ETH`;
}

export function settingsMenu(uid) {
  const s = getSettings(uid);
  return Markup.inlineKeyboard([
    [Markup.button.callback(`Buy presets: ${s.buyPresetsEth.map(usdOrEthLabel).join(', ')}`, 'settings_buy')],
    [Markup.button.callback(`Sell presets: ${s.sellPresetsPct.join(', ')}%`, 'settings_sell')],
    [Markup.button.callback(`Slippage: ${(s.slippageBps / 100).toFixed(2)}%`, 'settings_slippage')],
    [Markup.button.callback(`Max buy size: ${usdOrEthLabel(s.maxBuyEth)}`, 'settings_maxbuy')],
    [Markup.button.callback(`Max bridge size: ${usdOrEthLabel(s.maxBridgeEth)}`, 'settings_maxbridge')],
    [Markup.button.callback(`Gas priority: ${s.gasTier} (tap to cycle)`, 'settings_gastier')],
    [Markup.button.callback(`Low balance alert: ${s.lowBalanceThresholdEth} ETH`, 'settings_lowbalance')],
    [Markup.button.callback(`Confirm before trade: ${s.confirmTrades ? 'ON ✅' : 'OFF ❌'}`, 'settings_toggle_confirm')],
    [Markup.button.callback(`Flex card PnL: ${s.flexPnlMode.toUpperCase()} (tap to cycle)`, 'settings_flexpnl')],
    [Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);
}

export function rewardsMenu() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🔗 Get My Referral Link', 'rewards_link')],
    [Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);
}

export function bridgeMenu() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('Ethereum ➜ Robinhood', 'bridge_dir_eth_to_robinhood')],
    [Markup.button.callback('Robinhood ➜ Ethereum', 'bridge_dir_robinhood_to_eth')],
    [
      Markup.button.callback('💯 Bridge All (Eth➜Robin)', 'bridgeall_eth_to_robinhood'),
      Markup.button.callback('💯 Bridge All (Robin➜Eth)', 'bridgeall_robinhood_to_eth'),
    ],
    [Markup.button.callback('🕘 Recent Bridges', 'bridge_history')],
    [Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);
}

export function bridgeConfirmMenu(direction, amount) {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback('✅ Confirm', `bridge_confirm_${direction}_${amount}`),
      Markup.button.callback('❌ Cancel', 'menu_bridge'),
    ],
  ]);
}

export function directionLabel(direction) {
  return direction === BRIDGE_DIRECTION.ETH_TO_ROBINHOOD ? 'Ethereum ➜ Robinhood' : 'Robinhood ➜ Ethereum';
}

/**
 * `ethUsd` is passed in (fetched once by the caller, e.g. renderTokenCard)
 * so buy preset buttons can show USD amounts, e.g. "Buy $50", while the
 * underlying preset amount (and the buy_ callback payload) stays in ETH.
 */
export function tokenMenu(uid, tokenAddress, hasPosition, ethUsd) {
  const s = getSettings(uid);
  const user = getUser(uid);
  const buyLabel = (amt) => (ethUsd ? `Buy ${fmtUsd(amt * ethUsd)}` : `Buy ${amt} ETH`);
  const rows = [
    s.buyPresetsEth.map((amt) => Markup.button.callback(buyLabel(amt), `buy_${tokenAddress}_${amt}`)),
    [Markup.button.callback('✏️ Custom Buy (USD or ETH)', `custombuy_${tokenAddress}`)],
  ];
  if (hasPosition) {
    rows.push(s.sellPresetsPct.map((pct) => Markup.button.callback(`Sell ${pct}%`, `sell_${tokenAddress}_${pct}`)));
    rows.push([Markup.button.callback('✏️ Custom Sell', `customsell_${tokenAddress}`)]);
    rows.push([
      Markup.button.callback('🎯 Set TP/SL', `tpsl_${tokenAddress}`),
      Markup.button.callback('⏰ Limit Sell', `limitsell_${tokenAddress}`),
    ]);
  }
  const bottomRow = [Markup.button.callback('⏰ Limit Buy', `limitbuy_${tokenAddress}`)];
  if (user.wallets.length > 1) bottomRow.push(Markup.button.callback('📦 Batch Buy', `batchbuy_${tokenAddress}`));
  rows.push(bottomRow);
  if (hasPosition && user.wallets.length > 1) {
    rows.push([Markup.button.callback('📦 Batch Sell', `batchsell_${tokenAddress}`)]);
  }
  rows.push([
    Markup.button.callback('🔄 Refresh', `refresh_${tokenAddress}`),
    Markup.button.callback('⬅️ Back', 'menu_main'),
  ]);
  return Markup.inlineKeyboard(rows);
}

/** Multi-select wallet picker used by Batch Buy. */
export function batchSelectMenu(uid, selected) {
  const user = getUser(uid);
  const rows = user.wallets.map((w) => {
    const checked = selected.includes(w.id) ? '☑️ ' : '⬜ ';
    return [Markup.button.callback(`${checked}${w.name} (${shortAddr(w.address)})`, `batchtoggle_${w.id}`)];
  });
  rows.push([
    Markup.button.callback(`✅ Confirm (${selected.length} selected)`, 'batchconfirm'),
    Markup.button.callback('❌ Cancel', 'menu_main'),
  ]);
  return Markup.inlineKeyboard(rows);
}

/** Multi-select wallet picker used by Batch Sell — only wallets passed in `candidates`. */
export function batchSellSelectMenu(candidates, selected) {
  const rows = candidates.map((w) => {
    const checked = selected.includes(w.id) ? '☑️ ' : '⬜ ';
    return [Markup.button.callback(`${checked}${w.name} (${shortAddr(w.address)})`, `bselltoggle_${w.id}`)];
  });
  rows.push([
    Markup.button.callback(`✅ Confirm (${selected.length} selected)`, 'batchsellconfirm'),
    Markup.button.callback('❌ Cancel', 'menu_main'),
  ]);
  return Markup.inlineKeyboard(rows);
}

/** Multi-select wallet picker used by Batch Fund — only wallets passed in `candidates` (source excluded). */
export function batchFundSelectMenu(candidates, selected) {
  const rows = candidates.map((w) => {
    const checked = selected.includes(w.id) ? '☑️ ' : '⬜ ';
    return [Markup.button.callback(`${checked}${w.name} (${shortAddr(w.address)})`, `bfundtoggle_${w.id}`)];
  });
  rows.push([
    Markup.button.callback(`✅ Confirm (${selected.length} selected)`, 'bfundconfirm'),
    Markup.button.callback('❌ Cancel', 'menu_main'),
  ]);
  return Markup.inlineKeyboard(rows);
}

/** Multi-select wallet picker used by Batch Collect — sources feeding one chosen destination. */
export function collectSelectMenu(candidates, selected) {
  const rows = candidates.map((w) => {
    const checked = selected.includes(w.id) ? '☑️ ' : '⬜ ';
    return [Markup.button.callback(`${checked}${w.name} (${shortAddr(w.address)})`, `collecttoggle_${w.id}`)];
  });
  rows.push([
    Markup.button.callback(`✅ Confirm (${selected.length} selected)`, 'collectconfirm'),
    Markup.button.callback('❌ Cancel', 'menu_main'),
  ]);
  return Markup.inlineKeyboard(rows);
}

export function confirmMenu(kind, tokenAddress, value) {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback('✅ Confirm', `confirm_${kind}_${tokenAddress}_${value}`),
      Markup.button.callback('❌ Cancel', 'cancel_trade'),
    ],
  ]);
}

/** "⬅️ Back" plus a "🔄 Refresh" button — used by the Positions view. */
export function refreshBackMenu(refreshAction) {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🔄 Refresh', refreshAction), Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);
}

// ---------- Limit orders: list + cancel ----------

/**
 * Renders the user's open limit orders as text + one cancel button per
 * order. `market` is an optional Map<tokenAddress, marketData> the caller
 * can pre-fetch so symbols show up instead of raw addresses — falls back
 * to a shortened address if not supplied or lookup failed for that token.
 */
export function limitOrdersText(orders, marketByToken = new Map()) {
  if (orders.length === 0) {
    return '⏰ *Limit Orders*\n\nNo open limit orders.';
  }
  const lines = orders.map((o) => {
    const market = marketByToken.get(o.token_address);
    const label = market?.symbol ?? shortAddr(o.token_address);
    const mcapLabel = o.target_mcap != null
      ? fmtUsd(o.target_mcap)
      : `$${Number(o.trigger_price).toPrecision(4)} (price)`;
    const amountLabel = o.side === 'buy' ? `${o.amount} ETH` : `${fmtTokenAmount(Number(o.amount))} tokens`;
    const dir = o.side === 'buy' ? '≤' : '≥';
    return `*${label}* — ${o.side.toUpperCase()} ${amountLabel} @ mcap ${dir} ${mcapLabel}`;
  });
  return `⏰ *Limit Orders*\n\n${lines.join('\n')}`;
}

export function limitOrdersMenu(orders) {
  const rows = orders.map((o) => {
    const market = o._symbol || shortAddr(o.token_address);
    return [Markup.button.callback(`❌ Cancel ${o.side.toUpperCase()} ${market}`, `limitordercancel_${o.id}`)];
  });
  rows.push([Markup.button.callback('⬅️ Back', 'menu_main')]);
  return Markup.inlineKeyboard(rows);
}

// ---------- Momentum Trigger: list + cancel ----------
// `marketByToken` is an optional Map<tokenAddress, marketData> the caller
// pre-fetches (same pattern as limitOrdersText/limitOrdersMenu above) so
// Alpha/Beta show up as actual token symbols instead of shortened
// addresses. Falls back to shortAddr if not supplied or lookup failed.

export function momentumListText(triggers, marketByToken = new Map()) {
  if (triggers.length === 0) {
    return (
      '⚡ *Momentum Trigger*\n\n' +
      'No active triggers.\n\n' +
      'Set an Alpha token to watch and a Beta token to auto-buy — once Alpha rises by your chosen % from now, the bot buys Beta for you automatically.'
    );
  }
  const label = (addr) => marketByToken.get(addr)?.symbol ?? shortAddr(addr);
  const lines = triggers.map((t) =>
    `*${label(t.alpha_token)}* +${t.trigger_pct}% → auto-buy ${t.buy_amount_eth} ETH of *${label(t.beta_token)}*\n` +
    `  baseline: $${Number(t.baseline_price).toPrecision(4)}`
  );
  return `⚡ *Momentum Trigger*\n\n${lines.join('\n\n')}`;
}

export function momentumMenu(triggers, marketByToken = new Map()) {
  const label = (addr) => marketByToken.get(addr)?.symbol ?? shortAddr(addr);
  const rows = triggers.map((t) => [
    Markup.button.callback(`❌ Cancel ${label(t.alpha_token)} → ${label(t.beta_token)}`, `momentumcancel_${t.id}`),
  ]);
  rows.push([Markup.button.callback('➕ New Trigger', 'momentum_new')]);
  rows.push([Markup.button.callback('⬅️ Back', 'menu_main')]);
  return Markup.inlineKeyboard(rows);
}

// ---------- Snipe Menu ----------

export function snipeMenu(config) {
  const toggleLabel = config?.enabled ? '🔴 Pause Sniping' : '🟢 Start Sniping';
  return Markup.inlineKeyboard([
    [Markup.button.callback(toggleLabel, 'snipe_toggle')],
    [Markup.button.callback(`💰 Buy: ${config?.buyAmountEth ?? 0.01} ETH`, 'snipe_buy_amount')],
    [Markup.button.callback(`🛡 Max: ${config?.maxBuyEth ?? 0.1} ETH`, 'snipe_max_buy')],
    [Markup.button.callback(`📊 Slippage: ${((config?.slippageBps ?? 500) / 100).toFixed(2)}%`, 'snipe_slippage')],
    [Markup.button.callback(`💧 Min Liq: $${config?.minLiquidityUsd ?? 1000}`, 'snipe_min_liq')],
    [Markup.button.callback('🎯 Manual Snipe', 'snipe_manual')],
    [Markup.button.callback('📋 History', 'snipe_history')],
    [Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);
}

export function snipeHistoryText(history) {
  if (history.length === 0) return '🎯 *Snipe History*\n\nNo snipes yet.';
  const lines = history.map((s) => {
    const emoji = s.status === 'confirmed' ? '✅' : '❌';
    const time = new Date(s.created_at).toLocaleString();
    return `${emoji} *${s.token_address.slice(0, 8)}...* — ${s.eth_amount} ETH — ${time}`;
  });
  return `🎯 *Snipe History*\n\n${lines.join('\n')}`;
}

// ---------- Watchlist Menu ----------

export function watchlistMenu(alerts) {
  const rows = alerts.map((a) => {
    const label = a.token_symbol || shortAddr(a.token_address);
    return [Markup.button.callback(`❌ Cancel ${label}`, `watchcancel_${a.id}`)];
  });
  rows.push([Markup.button.callback('➕ New Alert', 'watchlist_new')]);
  rows.push([Markup.button.callback('📋 History', 'watchlist_history')]);
  rows.push([Markup.button.callback('⬅️ Back', 'menu_main')]);
  return Markup.inlineKeyboard(rows);
}

export function watchlistHistoryText(history) {
  if (history.length === 0) return '👀 *Alert History*\n\nNo alerts yet.';
  const lines = history.map((a) => {
    const emoji = a.status === 'triggered' ? '🔔' : '❌';
    const label = a.token_symbol || shortAddr(a.token_address);
    return `${emoji} *${label}* — ${a.watch_type} ${a.target_value} (${a.status})`;
  });
  return `👀 *Alert History*\n\n${lines.join('\n')}`;
}

// ---------- Copy Trade Menu ----------

export function copyTradeListText(rules) {
  if (rules.length === 0) {
    return '👁 *Copy Trade*\n\nNo wallets being followed. Add one to auto-mirror their Uniswap swaps.';
  }
  const lines = rules.map((r) => {
    const status = r.enabled ? '🟢' : '🔴';
    const label = r.target_label || shortAddr(r.target_address);
    return `${status} *${label}* — ${r.buy_amount_eth} ETH per trade`;
  });
  return `👁 *Copy Trade*\n\n${lines.join('\n')}`;
}

export function copyTradeMenu(rules) {
  const rows = rules.map((r) => {
    const label = r.target_label || shortAddr(r.target_address);
    const toggleLabel = r.enabled ? `🔴 Pause ${label}` : `🟢 Resume ${label}`;
    return [
      Markup.button.callback(toggleLabel, `copytoggle_${r.id}`),
      Markup.button.callback(`🗑`, `copyremove_${r.id}`),
    ];
  });
  rows.push([Markup.button.callback('➕ Follow Wallet', 'copytrade_new')]);
  rows.push([Markup.button.callback('📋 History', 'copytrade_history')]);
  rows.push([Markup.button.callback('⬅️ Back', 'menu_main')]);
  return Markup.inlineKeyboard(rows);
}

export function copyTradeHistoryText(history) {
  if (history.length === 0) return '👁 *Copy Trade History*\n\nNo copied trades yet.';
  const lines = history.map((c) => {
    const emoji = c.status === 'confirmed' ? '✅' : '❌';
    const time = new Date(c.created_at).toLocaleString();
    return `${emoji} *${c.side.toUpperCase()}* ${shortAddr(c.token_address)} — ${c.eth_amount} ETH — ${time}`;
  });
  return `👁 *Copy Trade History*\n\n${lines.join('\n')}`;
}

// ---------- Token info + PnL rendering ----------

export async function renderTokenCard(uid, tokenAddress) {
  const w = getActiveWallet(uid);
  if (!w) return { text: 'No active wallet. Add one first.', markup: walletsMenu(uid) };

  const market = await getTokenMarketData(tokenAddress).catch(() => null);
  const ethUsd = await getEthUsdPrice().catch(() => null);

  if (!market) {
    return {
      text: `No market data found for:\n\`${tokenAddress}\`\n\nPool may not exist yet, or DexScreener hasn't indexed it.`,
      markup: Markup.inlineKeyboard([
        [Markup.button.callback('🔄 Refresh', `refresh_${tokenAddress}`)],
        [Markup.button.callback('⬅️ Back', 'menu_main')],
      ]),
    };
  }

  const pos = getPosition(uid, w.id, tokenAddress);
  let pnlLine = '';
  if (pos && pos.tokenAmount > 0) {
    const currentValueUsd = pos.tokenAmount * market.priceUsd;
    const costUsd = ethUsd ? pos.costEth * ethUsd : null;
    if (costUsd !== null) {
      const pnlUsd = currentValueUsd - costUsd;
      const pnlPct = costUsd > 0 ? (pnlUsd / costUsd) * 100 : 0;
      const emoji = pnlUsd >= 0 ? '🟢' : '🔴';
      pnlLine = `\n\n*Your position:*\n${fmtTokenAmount(pos.tokenAmount)} ${market.symbol}\nCost: ${fmtUsd(costUsd)} | Value: ${fmtUsd(currentValueUsd)}\nPnL: ${emoji} ${fmtUsd(pnlUsd)} (${pnlPct.toFixed(1)}%)`;
    }
    if (pos.entryMcap != null) {
      pnlLine += `\nEntry mcap: ${fmtUsd(pos.entryMcap)}`;
      if (market.marketCap != null && pos.entryMcap > 0) {
        const mcapChangePct = ((market.marketCap - pos.entryMcap) / pos.entryMcap) * 100;
        const mcapEmoji = mcapChangePct >= 0 ? '🟢' : '🔴';
        pnlLine += ` → Now: ${fmtUsd(market.marketCap)} (${mcapEmoji} ${mcapChangePct >= 0 ? '+' : ''}${mcapChangePct.toFixed(1)}%)`;
      }
    }
    const rule = getActiveAutoRuleForPosition(uid, w.id, tokenAddress);
    if (rule) {
      const parts = [];
      if (rule.tp_pct != null) parts.push(`TP +${rule.tp_pct}%`);
      if (rule.sl_pct != null) parts.push(`SL -${rule.sl_pct}%`);
      pnlLine += `\n🎯 Active rule: ${parts.join(' / ')}`;
    }
  }

  const changeLine = market.priceChange24h !== null ? ` (${market.priceChange24h >= 0 ? '+' : ''}${market.priceChange24h.toFixed(1)}%)` : '';
  const walletBalance = await dualEthBalanceLines(w.address).catch(() => 'unavailable');
  // Est. network fee for a trade on this token — same estimate shown on the
  // buy/sell confirm screens, surfaced here too so it's visible before the
  // user even taps Buy/Sell.
  const gasLine = await gasEstimateLine(uid, FALLBACK_GAS_LIMIT_BUY).catch(() => '');

  const text =
    `*${market.symbol}*\n\`${tokenAddress}\`\n\n` +
    `Price: $${market.priceUsd.toPrecision(4)}${changeLine}\n` +
    `Market Cap: ${fmtUsd(market.marketCap)}\n` +
    `Liquidity: ${fmtUsd(market.liquidityUsd)}\n` +
    `Your balance:\n${walletBalance}` +
    gasLine +
    pnlLine;

  return { text, markup: tokenMenu(uid, tokenAddress, !!(pos && pos.tokenAmount > 0), ethUsd) };
}

// ---------- Positions list rendering ----------
// Covers every open position across ALL of the user's wallets. This used to
// be split between a per-wallet "Positions" view and an all-wallet
// "Portfolio" summary — they showed near-identical info, so Positions now
// does both: a combined total up top, then each position labeled with the
// wallet it's held in. Each position also gets an "Open" button so it can
// be tapped straight into its token card (switches active wallet to the
// wallet that holds it, since a trade must run from the active wallet).

export async function renderPositionsView(uid) {
  const positions = getAllPositionsForUser(uid);
  if (positions.length === 0) {
    return {
      text: '📊 No positions yet. Trade a token to open one.',
      markup: Markup.inlineKeyboard([[Markup.button.callback('⬅️ Back', 'menu_main')]]),
    };
  }

  const ethUsd = await getEthUsdPrice().catch(() => null);
  const lines = [];
  const openButtonRows = [];
  const refs = []; // index -> { walletId, tokenAddress }, stashed in state.positionRefs below
  let totalValueUsd = 0;
  let totalCostUsd = 0;
  let anyPriceUnavailable = false;

  for (const pos of positions) {
    const market = await getTokenMarketData(pos.tokenAddress).catch(() => null);
    const symbol = market?.symbol ?? shortAddr(pos.tokenAddress);
    if (market && ethUsd) {
      const valueUsd = pos.tokenAmount * market.priceUsd;
      const costUsd = pos.costEth * ethUsd;
      totalValueUsd += valueUsd;
      totalCostUsd += costUsd;
      const pnlUsd = valueUsd - costUsd;
      const pnlPct = costUsd > 0 ? (pnlUsd / costUsd) * 100 : 0;
      const emoji = pnlUsd >= 0 ? '🟢' : '🔴';
      let line = `*${symbol}* (${pos.walletName}): ${fmtTokenAmount(pos.tokenAmount)} — ${fmtUsd(valueUsd)} (${emoji} ${pnlPct.toFixed(1)}%)`;
      if (pos.entryMcap != null) line += `\n  Entry mcap: ${fmtUsd(pos.entryMcap)}`;
      lines.push(line);
    } else {
      anyPriceUnavailable = true;
      lines.push(`*${symbol}* (${pos.walletName}): ${fmtTokenAmount(pos.tokenAmount)} — price unavailable`);
    }
    // NOTE: callback_data has a hard 64-byte cap in Telegram's Bot API.
    // "openpos~<walletId>~<0xaddress>" routinely runs 70+ bytes and gets
    // silently dropped/ignored on tap — so we reference this position by a
    // short index instead, resolved against positionRefs at click time.
    const idx = refs.length;
    refs.push({ walletId: pos.walletId, tokenAddress: pos.tokenAddress });
    openButtonRows.push([
      Markup.button.callback(`🔓 Open ${symbol} (${pos.walletName})`, `openpos~${idx}`),
    ]);
  }

  positionRefs.set(String(uid), refs);

  const totalPnlUsd = totalValueUsd - totalCostUsd;
  const totalPnlPct = totalCostUsd > 0 ? (totalPnlUsd / totalCostUsd) * 100 : 0;
  const totalEmoji = totalPnlUsd >= 0 ? '🟢' : '🔴';
  const disclaimer = anyPriceUnavailable ? '\n_Totals exclude positions with unavailable pricing._' : '';

  const text =
    `📊 *Positions* — all wallets\n\n` +
    `Total value: ${fmtUsd(totalValueUsd)}\n` +
    `Total cost: ${fmtUsd(totalCostUsd)}\n` +
    `Total PnL: ${totalEmoji} ${fmtUsd(totalPnlUsd)} (${totalPnlPct.toFixed(1)}%)${disclaimer}\n\n` +
    lines.join('\n');

  const markup = Markup.inlineKeyboard([
    ...openButtonRows,
    [Markup.button.callback('🔄 Refresh', 'menu_positions_refresh'), Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);

  return { text, markup };
         }
