import { Markup } from 'telegraf';
import { bot } from '../bot-instance.js';
import { pending, stopAllViewRefreshes } from '../state.js';
import { createCopyRule, toggleCopyRule, removeCopyRule, getCopyRulesForUser, getCopyHistory } from '../storage.js';
import { mainMenu, copyTradeMenu, copyTradeHistoryText, copyTradeListText } from '../menus.js';
import { isRateLimited } from '../ratelimit.js';
import { shortAddr } from '../wallet.js';

// ---------- Copy Trade Menu ----------

bot.action('menu_copytrade', async (ctx) => {
  await ctx.answerCbQuery();
  stopAllViewRefreshes(ctx.from.id);
  const uid = ctx.from.id;
  const rules = getCopyRulesForUser(uid);
  await ctx.editMessageText(copyTradeListText(rules), {
    parse_mode: 'Markdown',
    ...copyTradeMenu(rules),
  });
});

// ---------- Add New ----------

bot.action('copytrade_new', async (ctx) => {
  await ctx.answerCbQuery();
  const uid = ctx.from.id;
  pending.set(uid, { type: 'copy_target' });
  await ctx.editMessageText(
    '👁 *Copy Trade*\n\nPaste the wallet address you want to follow:\n\n' +
    '_When this wallet swaps on Uniswap V2, the bot will auto-buy the same token with your configured amount._',
    { parse_mode: 'Markdown' }
  );
});

// ---------- Toggle Enable/Disable ----------

bot.action(/^copytoggle_(.+)$/, async (ctx) => {
  const uid = ctx.from.id;
  const newEnabled = toggleCopyRule(uid, ctx.match[1]);
  await ctx.answerCbQuery(newEnabled !== null ? (newEnabled ? 'Enabled' : 'Disabled') : 'Not found');
  const rules = getCopyRulesForUser(uid);
  await ctx.editMessageText(copyTradeListText(rules), {
    parse_mode: 'Markdown',
    ...copyTradeMenu(rules),
  }).catch(() => {});
});

// ---------- Remove ----------

bot.action(/^copyremove_(.+)$/, async (ctx) => {
  const uid = ctx.from.id;
  const removed = removeCopyRule(uid, ctx.match[1]);
  await ctx.answerCbQuery(removed ? 'Removed' : 'Not found');
  const rules = getCopyRulesForUser(uid);
  await ctx.editMessageText(copyTradeListText(rules), {
    parse_mode: 'Markdown',
    ...copyTradeMenu(rules),
  }).catch(() => {});
});

// ---------- History ----------

bot.action('copytrade_history', async (ctx) => {
  await ctx.answerCbQuery();
  const uid = ctx.from.id;
  const history = getCopyHistory(uid, 10);
  await ctx.editMessageText(copyTradeHistoryText(history), {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Back', 'menu_copytrade')]]),
  });
});
