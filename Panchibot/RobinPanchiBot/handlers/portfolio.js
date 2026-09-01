import { Markup } from 'telegraf';
import { bot } from '../bot-instance.js';
import { stopAllViewRefreshes, positionRefs } from '../state.js';
import { getUser, getAllPositionsForUser } from '../storage.js';
import { getEthUsdPrice, getTokenMarketData, fmtUsd, fmtTokenAmount } from '../price.js';
import { shortAddr } from '../wallet.js';
import { dualEthBalanceLines } from '../format.js';
import { mainMenu } from '../menus.js';

// ---------- Portfolio Dashboard ----------

bot.action('menu_portfolio', async (ctx) => {
  await ctx.answerCbQuery();
  stopAllViewRefreshes(ctx.from.id);
  const uid = ctx.from.id;

  await ctx.editMessageText('📈 *Portfolio*\n\nLoading...', { parse_mode: 'Markdown' });

  try {
    const { text, markup } = await renderPortfolio(uid);
    await ctx.editMessageText(text, { parse_mode: 'Markdown', ...markup });
  } catch (err) {
    console.error('Portfolio render failed:', err.message);
    await ctx.editMessageText('❌ Failed to load portfolio. Try again shortly.', mainMenu());
  }
});

bot.action('portfolio_refresh', async (ctx) => {
  await ctx.answerCbQuery('Refreshed');
  const uid = ctx.from.id;
  try {
    const { text, markup } = await renderPortfolio(uid);
    await ctx.editMessageText(text, { parse_mode: 'Markdown', ...markup }).catch((err) => {
      if (!err.description?.includes('message is not modified')) throw err;
    });
  } catch (err) {
    console.error('Portfolio refresh failed:', err.message);
  }
});

async function renderPortfolio(uid) {
  const user = getUser(uid);
  const positions = getAllPositionsForUser(uid);
  const ethUsd = await getEthUsdPrice().catch(() => null);

  // Gather all wallet balances
  const walletLines = [];
  let totalEth = 0;

  for (const w of user.wallets) {
    try {
      const bal = await dualEthBalanceLines(w.address).catch(() => 'unavailable');
      walletLines.push(`*${w.name}* (${shortAddr(w.address)})\\n${bal}`);
    } catch {
      walletLines.push(`*${w.name}* (${shortAddr(w.address)})\\nBalance unavailable`);
    }
  }

  // Gather all positions
  const posLines = [];
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

      posLines.push(
        `*${symbol}* (${pos.walletName}): ${fmtTokenAmount(pos.tokenAmount)}` +
        `\\n  Value: ${fmtUsd(valueUsd)} | PnL: ${emoji} ${fmtUsd(pnlUsd)} (${pnlPct.toFixed(1)}%)`
      );
    } else {
      anyPriceUnavailable = true;
      posLines.push(`*${symbol}* (${pos.walletName}): ${fmtTokenAmount(pos.tokenAmount)} — price unavailable`);
    }
  }

  const totalPnlUsd = totalValueUsd - totalCostUsd;
  const totalPnlPct = totalCostUsd > 0 ? (totalPnlUsd / totalCostUsd) * 100 : 0;
  const totalEmoji = totalPnlUsd >= 0 ? '🟢' : '🔴';

  const text =
    `📈 *Portfolio* — all wallets\n\n` +
    `*Wallets (${user.wallets.length}):*\n${walletLines.join('\\n\\n')}\n\n` +
    `*Positions (${positions.length}):*\n` +
    (posLines.length > 0 ? posLines.join('\\n') : '_No open positions_') +
    `\n\n` +
    `*Summary:*\n` +
    `Total value: ${fmtUsd(totalValueUsd)}\n` +
    `Total cost: ${fmtUsd(totalCostUsd)}\n` +
    `Total PnL: ${totalEmoji} ${fmtUsd(totalPnlUsd)} (${totalPnlPct.toFixed(1)}%)` +
    (anyPriceUnavailable ? '\\n\\n_Totals exclude positions with unavailable pricing._' : '');

  const markup = Markup.inlineKeyboard([
    [Markup.button.callback('🔄 Refresh', 'portfolio_refresh')],
    [Markup.button.callback('📊 Positions', 'menu_positions')],
    [Markup.button.callback('⬅️ Back', 'menu_main')],
  ]);

  return { text, markup };
}
