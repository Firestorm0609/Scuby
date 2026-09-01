import { ethers } from 'ethers';
import { provider } from './config.js';
import { getActiveCopyRules, getWallet, logCopyTrade, updateCopyRuleNonce } from './storage.js';
import { tradesInFlight, gasMultiplierFor } from './state.js';
import { getQuote, buildSwapTx, sendSwapWithGasBump } from './swap.js';
import { explorerTxUrl, friendlyErrorMessage } from './format.js';

const NATIVE_ETH = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE';

// Uniswap V2 Router swap function signatures we care about
const SWAP_SELECTORS = {
  '0x7ff36ab5': 'swapExactETHForTokens',
  '0xfb3bdb41': 'swapETHForExactTokens',
  '0x38ed1739': 'swapExactTokensForETH',
  '0x18cbafe5': 'swapExactTokensForTokens',
  '0x8803dbee': 'swapTokensForExactTokens',
  '0x5c11d795': 'swapExactTokensForTokensSupportingFeeOnTransferTokens',
  '0x791ac947': 'swapExactETHForTokensSupportingFeeOnTransferTokens',
  '0xb6f9de95': 'swapExactTokensForETHSupportingFeeOnTransferTokens',
};

// Uniswap V2 Router address on Robinhood Chain
const UNISWAP_V2_ROUTER = '0x89e5db8b5aa49aa85ac63f691524311aeb649eba';

/**
 * Parse a Uniswap V2 Router swap calldata to extract token and direction.
 * Returns { tokenAddress, side: 'buy'|'sell' } or null if not a recognized swap.
 */
function parseSwapCalldata(data) {
  if (!data || data.length < 10) return null;
  const selector = data.slice(0, 10).toLowerCase();
  const swapType = SWAP_SELECTORS[selector];
  if (!swapType) return null;

  try {
    const iface = new ethers.Interface([
      'function swapExactETHForTokens(uint256,uint256[],address,uint256)',
      'function swapETHForExactTokens(uint256,uint256[],address,uint256)',
      'function swapExactTokensForETH(uint256,uint256[],address,uint256)',
      'function swapExactTokensForTokens(uint256,uint256[],address,uint256)',
      'function swapTokensForExactTokens(uint256,uint256[],address,uint256)',
      'function swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256[],address,uint256)',
      'function swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,uint256[],address,uint256)',
      'function swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256[],address,uint256)',
    ]);

    const decoded = iface.decodeFunctionData(swapType, data);
    const path = decoded.path || decoded[1];

    if (!path || path.length < 2) return null;

    // Determine if ETH is the sell token (buy) or buy token (sell)
    const pathStr = path.map((a) => a.toLowerCase());
    const weth = '0x4200000000000000000000000000000000000006';

    if (pathStr[0] === weth) {
      // Swapping ETH -> token = buy
      return { tokenAddress: path[path.length - 1], side: 'buy', path: pathStr };
    } else if (pathStr[pathStr.length - 1] === weth) {
      // Swapping token -> ETH = sell
      return { tokenAddress: path[0], side: 'sell', path: pathStr };
    }
  } catch { /* not a valid swap calldata */ }
  return null;
}

/**
 * Get the nonce (transaction count) for an address.
 */
async function getNonce(address) {
  return provider.getTransactionCount(address, 'latest');
}

/**
 * Check a target wallet for new swap transactions and mirror them.
 * Called periodically by the copy trade poller.
 */
export async function checkCopyTrades() {
  const rules = getActiveCopyRules();
  const results = [];

  for (const rule of rules) {
    if (tradesInFlight.has(rule.uid)) continue;

    try {
      const currentNonce = await getNonce(rule.target_address);

      // First time: just record the current nonce, don't copy old trades
      if (rule.last_seen_nonce < 0) {
        updateCopyRuleNonce(rule.id, currentNonce);
        continue;
      }

      if (currentNonce <= rule.last_seen_nonce) continue; // no new txns

      // Scan new transactions
      for (let nonce = rule.last_seen_nonce + 1; nonce < currentNonce; nonce++) {
        const tx = await provider.getTransaction(rule.target_address + '_' + nonce).catch(() => null);
        if (!tx || !tx.to) continue;

        // Only care about swaps on the Uniswap V2 Router
        if (tx.to.toLowerCase() !== UNISWAP_V2_ROUTER.toLowerCase()) continue;

        const swap = parseSwapCalldata(tx.data);
        if (!swap) continue;

        // Get the wallet to trade with
        const wallet = getWallet(rule.uid, rule.wallet_id);
        if (!wallet) continue;

        results.push({
          rule,
          wallet,
          swap,
          sourceTx: tx.hash,
        });
      }

      updateCopyRuleNonce(rule.id, currentNonce);
    } catch (err) {
      console.error(`Copy trade: error checking rule ${rule.id}:`, err.message);
    }
  }

  return results;
}

/**
 * Execute a copy trade.
 */
export async function executeCopyTrade(rule, wallet, swap, sourceTx) {
  if (tradesInFlight.has(rule.uid)) return { ok: false, error: 'locked' };

  tradesInFlight.add(rule.uid);
  const gasMultiplier = gasMultiplierFor(rule.uid);

  try {
    const { tokenAddress, side } = swap;

    // Determine amount
    let ethAmount;
    if (side === 'buy') {
      // Buying tokens with ETH — use rule's buy amount, capped at max
      ethAmount = Math.min(rule.buy_amount_eth, rule.max_buy_eth);
    } else {
      // Selling tokens for ETH — we need to check our balance first
      // For now, mirror the direction but use a fixed sell percentage
      // This is a simplified approach; a production system would decode the exact amount
      ethAmount = rule.buy_amount_eth; // use same sizing
    }

    const sellAmount = ethers.parseEther(ethAmount.toString()).toString();
    const quoteParams = {
      sellToken: side === 'buy' ? NATIVE_ETH : tokenAddress,
      buyToken: side === 'buy' ? tokenAddress : NATIVE_ETH,
      sellAmount: side === 'buy' ? sellAmount : sellAmount, // for sell, this would be token amount
      taker: wallet.address,
      slippageBps: rule.slippage_bps,
    };

    // For sells, we need the token balance, not ETH amount
    if (side === 'sell') {
      // Skip sells for now — only auto-buy on copy
      logCopyTrade({ uid: rule.uid, ruleId: rule.id, sourceTx, tokenAddress, side: 'sell', ethAmount: 0, status: 'skipped', error: 'Auto-sell not supported yet' });
      return { ok: true, skipped: true };
    }

    const quote = await getQuote(quoteParams);
    const signer = new ethers.Wallet(wallet.privateKey, provider);
    const txRequest = await buildSwapTx(signer, quote);
    const { txResponse } = await sendSwapWithGasBump(signer, txRequest, { gasMultiplier });

    logCopyTrade({ uid: rule.uid, ruleId: rule.id, sourceTx, tokenAddress, side, ethAmount, status: 'confirmed', txHash: txResponse.hash });

    return { ok: true, txHash: txResponse.hash };
  } catch (err) {
    logCopyTrade({ uid: rule.uid, ruleId: rule.id, sourceTx, tokenAddress: swap.tokenAddress, side: swap.side, ethAmount: rule.buy_amount_eth, status: 'failed', error: friendlyErrorMessage(err) });
    return { ok: false, error: friendlyErrorMessage(err) };
  } finally {
    tradesInFlight.delete(rule.uid);
  }
}
