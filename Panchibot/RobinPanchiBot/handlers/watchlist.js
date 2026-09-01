import { Markup } from 'telegraf';
import { bot } from '../bot-instance.js';
import { pending, stopAllViewRefreshes } from '../state.js';
import { createWatchAlert, cancelWatchAlert, getActiveWatchAlertsForUser, getWatchHistory } from '../storage.js';
import { watchTypeLabel } from '../watchlist.js';
import { mainMenu, watchlistMenu, watchlistHistoryText } from '../menus.js';
import { CA_REGEX } from '../config.js';
import { getTokenMarketData } from '../price.js';
import { shortAddr } from '../wallet.js';

// ---------- Watchlist Menu ----------

bot.action('menu_watchlist', async (ctx) => {
  await ctx.answerCbQuery();
  stopAllViewRefreshes(ctx.from.id);
  const uid = ctx.from.id;
  const alerts = getActiveWatchAlertsForUser(uid);
  await ctx.editMessageText(watchlistText(alerts), {
    parse_mode: 'Markdown',
    ...watchlistMenu(alerts),
  });
});

function watchlistText(alerts) {
  if (alerts.length === 0) {
    return '👀 *Price Alerts*\n\nNo active alerts. Create one to get notified when a token hits your target price or market cap.';
  }
  const lines = alerts.map((a) => {
    const label = a.token_symbol || shortAddr(a.token_address);
    return `• *${label}* — ${watchTypeLabel(a.watch_type, a.target_value)}`;
  });
  return `👀 *Price Alerts*\n\n${lines.join('\\n')}`;
}

// ---------- Create Alert ----------

bot.action('watchlist_new', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'watch_ca' });
  await ctx.editMessageText('Paste the token contract address to watch:', { parse_mode: 'Markdown' });
});

// ---------- Cancel Alert ----------

bot.action(/^watchcancel_(.+)$/, async (ctx) => {
  const uid = ctx.from.id;
  const cancelled = cancelWatchAlert(uid, ctx.match[1]);
  await ctx.answerCbQuery(cancelled ? 'Alert cancelled' : 'Could not cancel');
  const alerts = getActiveWatchAlertsForUser(uid);
  await ctx.editMessageText(watchlistText(alerts), {
    parse_mode: 'Markdown',
    ...watchlistMenu(alerts),
  }).catch(() => {});
});

// ---------- History ----------

bot.action('watchlist_history', async (ctx) => {
  await ctx.answerCbQuery();
  const uid = ctx.from.id;
  const history = getWatchHistory(uid, 10);
  await ctx.editMessageText(watchlistHistoryText(history), {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Back', 'menu_watchlist')]]),
  });
});
