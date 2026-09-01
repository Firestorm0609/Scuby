import { Markup } from 'telegraf';
import { bot } from '../bot-instance.js';
import { pending, stopAllViewRefreshes } from '../state.js';
import { getSnipeConfig, upsertSnipeConfig, getSnipeHistory } from '../storage.js';
import { manualSnipe } from '../snipe.js';
import { isRateLimited } from '../ratelimit.js';
import { mainMenu, snipeMenu, snipeHistoryText } from '../menus.js';
import { CA_REGEX } from '../config.js';
import { friendlyErrorMessage } from '../format.js';

// ---------- Snipe Menu ----------

bot.action('menu_snipe', async (ctx) => {
  await ctx.answerCbQuery();
  stopAllViewRefreshes(ctx.from.id);
  const uid = ctx.from.id;
  const config = getSnipeConfig(uid);
  await ctx.editMessageText(snipeMenuText(config), {
    parse_mode: 'Markdown',
    ...snipeMenu(config),
  });
});

function snipeMenuText(config) {
  if (!config) {
    return '🎯 *Token Sniper*\n\nDetect new Uniswap V2 pairs on Robinhood Chain and auto-buy instantly.\n\n⚠️ Not configured yet — set up your snipe settings below.';
  }
  const status = config.enabled ? '🟢 ACTIVE' : '🔴 PAUSED';
  return (
    `🎯 *Token Sniper* — ${status}\n\n` +
    `Auto-buy amount: ${config.buyAmountEth} ETH\n` +
    `Max buy: ${config.maxBuyEth} ETH\n` +
    `Slippage: ${(config.slippageBps / 100).toFixed(2)}%\n` +
    `Min liquidity: $${config.minLiquidityUsd}\n\n` +
    `_When a new pair is created, the bot checks your settings and auto-buys if liquidity meets your threshold._`
  );
}

// ---------- Toggle Sniping ----------

bot.action('snipe_toggle', async (ctx) => {
  await ctx.answerCbQuery();
  const uid = ctx.from.id;
  const config = getSnipeConfig(uid);
  if (!config) {
    pending.set(uid, { type: 'snipe_buy_amount' });
    return ctx.editMessageText('Configure your snipe settings first. Send the auto-buy amount in ETH (e.g. `0.01`):', { parse_mode: 'Markdown' });
  }
  upsertSnipeConfig(uid, { enabled: !config.enabled });
  const newConfig = getSnipeConfig(uid);
  await ctx.editMessageText(snipeMenuText(newConfig), { parse_mode: 'Markdown', ...snipeMenu(newConfig) });
});

// ---------- Settings ----------

bot.action('snipe_buy_amount', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'snipe_buy_amount' });
  await ctx.editMessageText('Send the auto-buy amount in ETH (e.g. `0.01`):', { parse_mode: 'Markdown' });
});

bot.action('snipe_max_buy', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'snipe_max_buy' });
  await ctx.editMessageText('Send the max ETH per single snipe (e.g. `0.1`):', { parse_mode: 'Markdown' });
});

bot.action('snipe_slippage', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'snipe_slippage' });
  await ctx.editMessageText('Send slippage tolerance as a percentage (e.g. `5` for 5%):', { parse_mode: 'Markdown' });
});

bot.action('snipe_min_liq', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'snipe_min_liq' });
  await ctx.editMessageText('Send minimum liquidity in USD (e.g. `1000`):', { parse_mode: 'Markdown' });
});

// ---------- Manual Snipe ----------

bot.action('snipe_manual', async (ctx) => {
  await ctx.answerCbQuery();
  pending.set(ctx.from.id, { type: 'snipe_manual_ca' });
  await ctx.editMessageText('Paste the token contract address to snipe:', { parse_mode: 'Markdown' });
});

// ---------- History ----------

bot.action('snipe_history', async (ctx) => {
  await ctx.answerCbQuery();
  const uid = ctx.from.id;
  const history = getSnipeHistory(uid, 10);
  await ctx.editMessageText(snipeHistoryText(history), {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Back', 'menu_snipe')]]),
  });
});

// ---------- Text handler for snipe settings ----------

// This is handled in the main text.js handler via the pending state machine.
// We add the snipe-specific text handlers here.

bot.action(/^snipemanual_(0x[a-fA-F0-9]{40})_([\\d.]+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  if (isRateLimited(ctx.from.id)) return ctx.reply('⏳ Slow down a bit.');
  const [, tokenAddress, ethAmountStr] = ctx.match;
  const uid = ctx.from.id;
  const ethAmount = Number(ethAmountStr);

  await ctx.reply(`🎯 Sniping with ${ethAmount} ETH...`);
  const result = await manualSnipe(uid, tokenAddress, ethAmount);

  if (result.ok) {
    const txLink = result.txHash ? `https://explorer.robinhood-chain.example/tx/${result.txHash}` : '';
    await ctx.reply(`✅ Snipe confirmed!` + (txLink ? `\n[View tx](${txLink})` : ''), { parse_mode: 'Markdown', ...mainMenu() });
  } else {
    await ctx.reply(`❌ Snipe failed: ${result.error}`, mainMenu());
  }
});
