import { ethers } from 'ethers';
import { provider } from './config.js';
import { getActiveWallet, getSnipeConfig, getEnabledSnipers, logSnipe } from './storage.js';
import { sendAdminAlert } from './alerts.js';
import { getQuote, buildSwapTx, sendSwapWithGasBump } from './swap.js';
import { tradesInFlight, gasMultiplierFor } from './state.js';
import { explorerTxUrl, friendlyErrorMessage } from './format.js';

// Uniswap V2 Factory on Robinhood Chain (4663)
const UNISWAP_V2_FACTORY = '0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f';
const NATIVE_ETH = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE';

// Minimal ABI for Uniswap V2 Factory — PairCreated event
const FACTORY_ABI = [
  'event PairCreated(address indexed token0, address indexed token1, address pair, uint)',
];

// Uniswap V2 Pair ABI — just what we need to read reserves + token addresses
const PAIR_ABI = [
  'function getReserves() view returns (uint112, uint112, uint32)',
  'function token0() view returns (address)',
  'function token1() view returns (address)',
];

// IERC20 for balance/decimals checks
const ERC20_ABI = [
  'function balanceOf(address) view returns (uint256)',
  'function decimals() view returns (uint8)',
  'function symbol() view returns (string)',
  'function totalSupply() view returns (uint256)',
];

let factoryContract = null;
let isListening = false;

/**
 * Initialize the Uniswap V2 Factory contract listener.
 * Call once at bot startup.
 */
export function initSniper(onNewPair) {
  if (factoryContract) return; // already initialized

  factoryContract = new ethers.Contract(UNISWAP_V2_FACTORY, FACTORY_ABI, provider);

  factoryContract.on('PairCreated', async (token0, token1, pairAddr, event) => {
    try {
      await handlePairCreated(token0, token1, pairAddr, event, onNewPair);
    } catch (err) {
      console.error('Sniper: error handling PairCreated:', err.message);
    }
  });

  isListening = true;
  console.log(`🎯 Sniper listening on Uniswap V2 Factory: ${UNISWAP_V2_FACTORY}`);
}

/**
 * Handle a new pair creation event.
 */
async function handlePairCreated(token0, token1, pairAddr, event, onNewPair) {
  const now = Date.now();
  console.log(`🎯 New pair detected: ${pairAddr} (${token0} / ${token1})`);

  // Get pair reserves to check initial liquidity
  let reserves, token0Addr, token1Addr;
  try {
    const pair = new ethers.Contract(pairAddr, PAIR_ABI, provider);
    [reserves, token0Addr, token1Addr] = await Promise.all([
      pair.getReserves(),
      pair.token0(),
      pair.token1(),
    ]);
  } catch (err) {
    console.error('Sniper: failed to read pair data:', err.message);
    return;
  }

  const [reserve0, reserve1] = reserves;
  // Determine which token is ETH (WETH) — on Robinhood Chain, WETH is typically the wrapped native
  // For Uniswap V2 on most L2s, the WETH address is well-known
  // We check if either token is the common WETH address
  const WETH_ADDRESSES = [
    '0x4200000000000000000000000000000000000006', // Standard L2 WETH
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', // Mainnet WETH (unlikely on L2 but safe)
  ];

  const token0IsWeth = WETH_ADDRESSES.includes(token0Addr.toLowerCase());
  const token1IsWeth = WETH_ADDRESSES.includes(token1Addr.toLowerCase());

  if (!token0IsWeth && !token1IsWeth) {
    console.log('Sniper: pair has no WETH side, skipping');
    return;
  }

  const targetToken = token0IsWeth ? token1Addr : token0Addr;
  const ethReserve = token0IsWeth ? reserve0 : reserve1;
  const tokenReserve = token0IsWeth ? reserve1 : reserve0;

  // Calculate initial liquidity in ETH
  const liquidityEth = Number(ethers.formatEther(ethReserve));
  console.log(`Sniper: liquidity = ${liquidityEth.toFixed(4)} ETH, target token = ${targetToken}`);

  // Fetch token info
  let tokenSymbol = '???';
  let tokenDecimals = 18;
  try {
    const tokenContract = new ethers.Contract(targetToken, ERC20_ABI, provider);
    [tokenSymbol, tokenDecimals] = await Promise.all([
      tokenContract.symbol().catch(() => '???'),
      tokenContract.decimals().catch(() => 18),
    ]);
  } catch { /* use defaults */ }

  // Notify the callback (for UI/notification purposes)
  if (onNewPair) {
    await onNewPair({
      pairAddress: pairAddr,
      tokenAddress: targetToken,
      tokenSymbol,
      tokenDecimals,
      liquidityEth,
      token0: token0Addr,
      token1: token1Addr,
      blockNumber: event.log?.blockNumber ?? event.blockNumber,
    });
  }

  // Check all enabled snipers
  const snipers = getEnabledSnipers();
  for (const sniper of snipers) {
    if (tradesInFlight.has(sniper.uid)) continue; // user already mid-trade

    // Check liquidity threshold
    if (liquidityEth * 2000 < sniper.minLiquidityUsd) { // rough ETH/USD estimate
      console.log(`Sniper: skipping for uid ${sniper.uid} — liquidity too low`);
      continue;
    }

    // Get the user's active wallet
    const wallet = getActiveWallet(sniper.uid);
    if (!wallet) continue;

    // Check if buy amount exceeds max
    if (sniper.buyAmountEth > sniper.maxBuyEth) continue;

    // Execute the snipe
    await executeSnipe(sniper, wallet, targetToken, tokenSymbol, pairAddr);
  }
}

/**
 * Execute a snipe buy for a user.
 */
async function executeSnipe(sniper, wallet, tokenAddress, tokenSymbol, pairAddress) {
  const { uid, buyAmountEth, slippageBps, maxBuyEth } = sniper;

  if (buyAmountEth > maxBuyEth) return;
  if (tradesInFlight.has(uid)) return;

  tradesInFlight.add(uid);
  const gasMultiplier = gasMultiplierFor(uid);

  try {
    console.log(`🎯 Sniping ${tokenSymbol} (${tokenAddress}) for uid ${uid} with ${buyAmountEth} ETH`);

    const sellAmount = ethers.parseEther(buyAmountEth.toString()).toString();
    const quoteParams = {
      sellToken: NATIVE_ETH,
      buyToken: tokenAddress,
      sellAmount,
      taker: wallet.address,
      slippageBps,
    };

    const quote = await getQuote(quoteParams);
    const signer = new ethers.Wallet(wallet.privateKey, provider);
    const txRequest = await buildSwapTx(signer, quote);
    const { txResponse, receipt } = await sendSwapWithGasBump(signer, txRequest, { gasMultiplier });

    logSnipe({ uid, walletId: wallet.id, tokenAddress, pairAddress, ethAmount: buyAmountEth, status: 'confirmed', txHash: txResponse.hash });

    const txLink = explorerTxUrl(txResponse.hash);
    const msg = '🎯 *Sniped ' + tokenSymbol + '*\nBought ' + buyAmountEth + ' ETH of `' + tokenAddress + '`' + (txLink ? '\n[View transaction](' + txLink + ')' : '');

    const { bot } = await import('./bot-instance.js');
    await bot.telegram.sendMessage(uid, msg, { parse_mode: 'Markdown' }).catch(() => {});

    console.log(`🎯 Snipe confirmed: ${txResponse.hash}`);
  } catch (err) {
    console.error(`🎯 Snipe failed for uid ${uid}:`, err.message);
    logSnipe({ uid, walletId: wallet.id, tokenAddress, pairAddress, ethAmount: buyAmountEth, status: 'failed', error: friendlyErrorMessage(err) });
  } finally {
    tradesInFlight.delete(uid);
  }
}

/**
 * Manual snipe: buy a specific token immediately (not from factory event).
 */
export async function manualSnipe(uid, tokenAddress, ethAmount) {
  const wallet = getActiveWallet(uid);
  if (!wallet) return { ok: false, error: 'No active wallet.' };

  const config = getSnipeConfig(uid);
  const slippageBps = config?.slippageBps ?? 500;
  const maxBuyEth = config?.maxBuyEth ?? 0.1;

  if (ethAmount > maxBuyEth) {
    return { ok: false, error: `Amount exceeds max buy size (${maxBuyEth} ETH).` };
  }

  if (tradesInFlight.has(uid)) {
    return { ok: false, error: 'Another trade is in progress.' };
  }

  tradesInFlight.add(uid);
  const gasMultiplier = gasMultiplierFor(uid);

  try {
    const sellAmount = ethers.parseEther(ethAmount.toString()).toString();
    const quoteParams = {
      sellToken: NATIVE_ETH,
      buyToken: tokenAddress,
      sellAmount,
      taker: wallet.address,
      slippageBps,
    };

    const quote = await getQuote(quoteParams);
    const signer = new ethers.Wallet(wallet.privateKey, provider);
    const txRequest = await buildSwapTx(signer, quote);
    const { txResponse } = await sendSwapWithGasBump(signer, txRequest, { gasMultiplier });

    logSnipe({ uid, walletId: wallet.id, tokenAddress, ethAmount, status: 'confirmed', txHash: txResponse.hash });

    return { ok: true, txHash: txResponse.hash };
  } catch (err) {
    logSnipe({ uid, walletId: wallet.id, tokenAddress, ethAmount, status: 'failed', error: friendlyErrorMessage(err) });
    return { ok: false, error: friendlyErrorMessage(err) };
  } finally {
    tradesInFlight.delete(uid);
  }
}

export { UNISWAP_V2_FACTORY };
