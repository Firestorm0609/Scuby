// Run once on your VPS, in the bot's directory (needs .env + data/panchi.sqlite present):
//   node dump-keys.js
// Prints uid, wallet name, address, decrypted private key, and ETH/token balances
// (via Etherscan) for every wallet.
// Delete this file after use — do not leave it on the server.
//
// Requires in .env:
//   ETHERSCAN_API_KEY=your_etherscan_v2_api_key
//
// Get a free key at https://etherscan.io/apis

import 'dotenv/config';
import Database from 'better-sqlite3';
import path from 'path';
import { decrypt } from './crypto.js';

const DB_PATH = path.join(process.cwd(), 'data', 'panchi.sqlite');
const db = new Database(DB_PATH, { readonly: true });

const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;
const ETHERSCAN_BASE = 'https://api.etherscan.io/v2/api'; // v2 unified multichain endpoint (Etherscan-indexed chains only)
const CHAIN_ID = 1; // 1 = Ethereum mainnet. Swap for other EVM chains if needed (e.g. 56 = BSC, 137 = Polygon)

// --- Robinhood Chain (chainId 4663) ---
// Etherscan does NOT index this chain. The official explorer is Blockscout at
// robinhoodchain.blockscout.com, which exposes an Etherscan-compatible API — no API key needed.
// Only use this documented domain; unaffiliated "scan" sites for this chain have been flagged
// as phishing/wallet-drainer risks.
const ROBINHOOD_CHAIN_BASE = 'https://robinhoodchain.blockscout.com/api';
const CHECK_ROBINHOOD_CHAIN = true; // set false to skip

// Simple rate-limit helper (Etherscan free tier ~5 req/sec)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getEthBalance(address) {
  if (!ETHERSCAN_API_KEY) return 'NO_API_KEY';
  const url = `${ETHERSCAN_BASE}?chainid=${CHAIN_ID}&module=account&action=balance&address=${address}&tag=latest&apikey=${ETHERSCAN_API_KEY}`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    if (json.status !== '1' && json.message !== 'OK') {
      return `ERROR: ${json.result || json.message}`;
    }
    const wei = BigInt(json.result);
    const eth = Number(wei) / 1e18;
    return `${eth.toFixed(6)} ETH`;
  } catch (err) {
    return `FETCH_FAILED: ${err.message}`;
  }
}

async function getTopTokenBalances(address, limit = 5) {
  // Uses the "tokentx" endpoint to discover tokens the address has interacted with,
  // then reports the current balance of each via tokenbalance.
  if (!ETHERSCAN_API_KEY) return [];
  try {
    const txUrl = `${ETHERSCAN_BASE}?chainid=${CHAIN_ID}&module=account&action=tokentx&address=${address}&sort=desc&apikey=${ETHERSCAN_API_KEY}`;
    const res = await fetch(txUrl);
    const json = await res.json();
    if (!Array.isArray(json.result)) return [];

    const seen = new Map();
    for (const tx of json.result) {
      if (!seen.has(tx.contractAddress)) {
        seen.set(tx.contractAddress, {
          symbol: tx.tokenSymbol,
          decimals: Number(tx.tokenDecimal || 18),
        });
      }
      if (seen.size >= limit) break;
    }

    const balances = [];
    for (const [contract, meta] of seen) {
      await sleep(250); // stay under rate limit
      const balUrl = `${ETHERSCAN_BASE}?chainid=${CHAIN_ID}&module=account&action=tokenbalance&contractaddress=${contract}&address=${address}&tag=latest&apikey=${ETHERSCAN_API_KEY}`;
      const balRes = await fetch(balUrl);
      const balJson = await balRes.json();
      if (balJson.status === '1') {
        const raw = BigInt(balJson.result);
        const amount = Number(raw) / 10 ** meta.decimals;
        if (amount > 0) {
          balances.push(`${amount.toFixed(4)} ${meta.symbol}`);
        }
      }
    }
    return balances;
  } catch (err) {
    return [`TOKEN_FETCH_FAILED: ${err.message}`];
  }
}

async function getRobinhoodChainBalance(address) {
  // Blockscout's Etherscan-compatible module=account&action=balance endpoint. No API key required.
  const url = `${ROBINHOOD_CHAIN_BASE}?module=account&action=balance&address=${address}&tag=latest`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    if (json.status !== '1') {
      return `ERROR: ${json.message || 'unknown'}`;
    }
    const wei = BigInt(json.result);
    const eth = Number(wei) / 1e18;
    return `${eth.toFixed(6)} ETH`;
  } catch (err) {
    return `FETCH_FAILED: ${err.message}`;
  }
}

async function getRobinhoodChainTokenBalances(address, limit = 5) {
  try {
    const txUrl = `${ROBINHOOD_CHAIN_BASE}?module=account&action=tokentx&address=${address}&sort=desc`;
    const res = await fetch(txUrl);
    const json = await res.json();
    if (!Array.isArray(json.result)) return [];

    const seen = new Map();
    for (const tx of json.result) {
      if (!seen.has(tx.contractAddress)) {
        seen.set(tx.contractAddress, {
          symbol: tx.tokenSymbol,
          decimals: Number(tx.tokenDecimal || 18),
        });
      }
      if (seen.size >= limit) break;
    }

    const balances = [];
    for (const [contract, meta] of seen) {
      await sleep(250);
      const balUrl = `${ROBINHOOD_CHAIN_BASE}?module=account&action=tokenbalance&contractaddress=${contract}&address=${address}&tag=latest`;
      const balRes = await fetch(balUrl);
      const balJson = await balRes.json();
      if (balJson.status === '1') {
        const raw = BigInt(balJson.result);
        const amount = Number(raw) / 10 ** meta.decimals;
        if (amount > 0) {
          balances.push(`${amount.toFixed(4)} ${meta.symbol}`);
        }
      }
    }
    return balances;
  } catch (err) {
    return [`TOKEN_FETCH_FAILED: ${err.message}`];
  }
}

async function main() {
  const rows = db.prepare('SELECT uid, id, name, address, private_key FROM wallets').all();

  if (rows.length === 0) {
    console.log('No wallets found.');
    process.exit(0);
  }

  if (!ETHERSCAN_API_KEY) {
    console.log('WARNING: ETHERSCAN_API_KEY not set in .env — balances will be skipped.');
  }

  for (const w of rows) {
    let pk;
    try {
      pk = decrypt(w.private_key);
    } catch (err) {
      pk = `DECRYPT_FAILED: ${err.message}`;
    }

    console.log('----------------------------------------');
    console.log(`uid:        ${w.uid}`);
    console.log(`wallet id:  ${w.id}`);
    console.log(`name:       ${w.name}`);
    console.log(`address:    ${w.address}`);
    console.log(`privateKey: ${pk}`);

    if (w.address && ETHERSCAN_API_KEY) {
      const ethBal = await getEthBalance(w.address);
      console.log(`ETH balance: ${ethBal}`);
      await sleep(250);

      const tokens = await getTopTokenBalances(w.address);
      if (tokens.length) {
        console.log(`Top tokens:  ${tokens.join(', ')}`);
      } else {
        console.log(`Top tokens:  (none found or all zero)`);
      }
      await sleep(250);
    }

    if (w.address && CHECK_ROBINHOOD_CHAIN) {
      const rhBal = await getRobinhoodChainBalance(w.address);
      console.log(`Robinhood Chain ETH balance: ${rhBal}`);
      await sleep(250);

      const rhTokens = await getRobinhoodChainTokenBalances(w.address);
      if (rhTokens.length) {
        console.log(`Robinhood Chain top tokens:  ${rhTokens.join(', ')}`);
      } else {
        console.log(`Robinhood Chain top tokens:  (none found or all zero)`);
      }
      await sleep(250);
    }
  }

  console.log('----------------------------------------');
  console.log(`Total: ${rows.length} wallet(s). Delete this script now.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

