import { getActiveWatchAlerts, markWatchTriggered } from './storage.js';
import { getTokenMarketData, getEthUsdPrice } from './price.js';

/**
 * Check all active watch alerts against current prices.
 * Called periodically by the poller in pollers.js.
 * Returns an array of triggered alerts for the caller to notify.
 */
export async function checkWatchAlerts() {
  const alerts = getActiveWatchAlerts();
  const triggered = [];

  for (const alert of alerts) {
    try {
      const market = await getTokenMarketData(alert.token_address).catch(() => null);
      if (!market || !market.priceUsd) continue;

      const currentValue = market.priceUsd;
      let shouldTrigger = false;

      switch (alert.watch_type) {
        case 'price_above':
          shouldTrigger = currentValue >= alert.target_value;
          break;
        case 'price_below':
          shouldTrigger = currentValue <= alert.target_value;
          break;
        case 'mcap_above':
          shouldTrigger = market.marketCap != null && market.marketCap >= alert.target_value;
          break;
        case 'mcap_below':
          shouldTrigger = market.marketCap != null && market.marketCap <= alert.target_value;
          break;
        case 'change_up':
          // 24h change exceeds target (e.g. +20%)
          shouldTrigger = market.priceChange24h != null && market.priceChange24h >= alert.target_value;
          break;
        case 'change_down':
          // 24h drop exceeds target (e.g. -30%)
          shouldTrigger = market.priceChange24h != null && market.priceChange24h <= -alert.target_value;
          break;
      }

      if (shouldTrigger) {
        markWatchTriggered(alert.id);
        triggered.push({
          ...alert,
          currentPrice: currentValue,
          currentMcap: market.marketCap,
          symbol: market.symbol,
        });
      }
    } catch (err) {
      console.error(`Watchlist: error checking alert ${alert.id}:`, err.message);
    }
  }

  return triggered;
}

/**
 * Build a human-readable label for a watch alert type.
 */
export function watchTypeLabel(watchType, targetValue) {
  switch (watchType) {
    case 'price_above': return `Price ≥ $${targetValue}`;
    case 'price_below': return `Price ≤ $${targetValue}`;
    case 'mcap_above': return `MCap ≥ $${targetValue}`;
    case 'mcap_below': return `MCap ≤ $${targetValue}`;
    case 'change_up': return `24h change ≥ +${targetValue}%`;
    case 'change_down': return `24h change ≤ -${targetValue}%`;
    default: return `${watchType}: ${targetValue}`;
  }
}
