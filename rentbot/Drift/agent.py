#!/usr/bin/env python3
"""
DRIFT AI Agent — Runs the racing betting cycle on Robinhood Chain.

Cycle (every 5 minutes):
  1. Create new race
  2. Commit hash(seed + raceId) 
  3. Wait for betting window (60s)
  4. Reveal seed → determines winner
  5. Settle race → payouts
  6. Repeat

Provably Fair:
  - seed = random bytes
  - commit = keccak256(seed + raceId)
  - winner = keccak256(seed, raceId, 42) % numDrivers
"""

import os
import sys
import json
import time
import random
import hashlib
import logging
import requests
from pathlib import Path
from datetime import datetime
from web3 import Web3
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────

load_dotenv(Path(__file__).parent / '.env')

RPC_URL = os.getenv('ROBINHOOD_RPC_URL', 'https://rpc.testnet.chain.robinhood.com')
CHAIN_ID = int(os.getenv('ROBINHOOD_CHAIN_ID', '46630'))
MISTRAL_KEY = os.getenv('MISTRAL_API_KEY', '')
RACE_INTERVAL = int(os.getenv('RACE_INTERVAL_SECONDS', '300'))
BET_WINDOW = int(os.getenv('BET_OPEN_SECONDS', '60'))
MIN_BET = int(os.getenv('MIN_BET_USD', '5'))
MAX_BET = int(os.getenv('MAX_BET_USD', '1000'))
FEE_PERCENT = int(os.getenv('FEE_PERCENT', '5'))
NUM_DRIVERS = int(os.getenv('NUM_DRIVERS', '6'))

DRIVER_NAMES = [
    "Lightning McQueen",
    "Drift King",
    "Phantom Racer",
    "Blaze Runner",
    "Shadow Speed",
    "Neon Fury"
]

# ── Logging ────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s 🏎️  %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('drift')

# ── Contract ABI (minimal for our functions) ───────────

DRIFT_RACE_ABI = [
    {
        "inputs": [],
        "name": "createRace",
        "outputs": [{"type": "uint256", "name": "raceId"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}, {"type": "bytes32", "name": "commitHash"}],
        "name": "commitRace",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}, {"type": "bytes32", "name": "seed"}],
        "name": "revealRace",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}],
        "name": "settleRace",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}],
        "name": "getRace",
        "outputs": [
            {
                "components": [
                    {"name": "id", "type": "uint256"},
                    {"name": "startTime", "type": "uint256"},
                    {"name": "bettingEnd", "type": "uint256"},
                    {"name": "resultTime", "type": "uint256"},
                    {"name": "commitHash", "type": "bytes32"},
                    {"name": "seedReveal", "type": "bytes32"},
                    {"name": "committed", "type": "bool"},
                    {"name": "revealed", "type": "bool"},
                    {"name": "settled", "type": "bool"},
                    {"name": "winner", "type": "uint8"},
                    {"name": "totalPool", "type": "uint256"},
                    {"name": "protocolFee", "type": "uint256"}
                ],
                "name": "race",
                "type": "tuple"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}, {"type": "address", "name": "user"}],
        "name": "getClaimable",
        "outputs": [{"type": "uint256", "name": "amount"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}],
        "name": "isBettingOpen",
        "outputs": [{"type": "bool", "name": "open"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}],
        "name": "timeUntilBettingEnds",
        "outputs": [{"type": "uint256", "name": "seconds"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"type": "uint256", "name": "raceId"}],
        "name": "getBetCount",
        "outputs": [{"type": "uint256", "name": "count"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"type": "address payable", "name": "to"}],
        "name": "withdrawFees",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# ── Drift Agent ────────────────────────────────────────

class DriftAgent:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL))
        self.connected = self.w3.is_connected()
        
        if not self.connected:
            log.error(f"❌ Cannot connect to RPC: {RPC_URL}")
            sys.exit(1)
        
        log.info(f"✅ Connected to Robinhood Chain (Chain ID: {CHAIN_ID})")
        log.info(f"   Block: {self.w3.eth.block_number}")
        
        self.contract_address = None
        self.contract = None
        self.account = None
        self._load_or_create_account()
    
    def _load_or_create_account(self):
        """Load or create a wallet for the agent."""
        # Check for existing private key
        pk = os.getenv('DRIFT_PRIVATE_KEY')
        if pk:
            self.account = self.w3.eth.account.from_key(pk)
        else:
            # Generate new account (for testnet)
            self.account = self.w3.eth.account.create()
            log.info(f"🔑 Generated new agent wallet: {self.account.address}")
            log.info(f"   Private key: {self.account.key.hex()}")
            log.info(f"   ⚠️  Save this key to .env as DRIFT_PRIVATE_KEY!")
            
            # Save to .env for persistence
            env_path = Path(__file__).parent / '.env'
            with open(env_path, 'a') as f:
                f.write(f"\nDRIFT_PRIVATE_KEY={self.account.key.hex()}\n")
        
        balance = self.w3.eth.get_balance(self.account.address)
        log.info(f"💰 Agent balance: {self.w3.from_wei(balance, 'ether')} ETH")
    
    def connect_contract(self, address: str):
        """Connect to deployed DriftRace contract."""
        self.contract_address = address
        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(address),
            abi=DRIFT_RACE_ABI
        )
        log.info(f"📝 Connected to DriftRace at {address}")
    
    def deploy_contract(self) -> str:
        """Deploy DriftRace contract to Robinhood Chain."""
        log.info("🚀 Deploying DriftRace contract...")
        
        # Read contract bytecode
        contract_path = Path(__file__).parent / 'contracts' / 'DriftRace.json'
        if contract_path.exists():
            with open(contract_path) as f:
                data = json.load(f)
                bytecode = data.get('bytecode', '')
        else:
            log.error("Contract bytecode not found. Compile first!")
            return None
        
        Contract = self.w3.eth.contract(abi=DRIFT_RACE_ABI, bytecode=bytecode)
        
        # Build transaction
        tx = Contract.constructor().build_transaction({
            'from': self.account.address,
            'nonce': self.w3.eth.get_transaction_count(self.account.address),
            'gas': 3000000,
            'gasPrice': self.w3.eth.gas_price,
            'chainId': CHAIN_ID
        })
        
        # Sign and send
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info(f"   TX: {tx_hash.hex()}")
        
        # Wait for receipt
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        self.contract_address = receipt.contractAddress
        self.contract = self.w3.eth.contract(
            address=self.contract_address,
            abi=DRIFT_RACE_ABI
        )
        
        log.info(f"✅ Contract deployed at {self.contract_address}")
        return self.contract_address
    
    # ── Race Cycle ─────────────────────────────────────

    def generate_seed(self) -> bytes:
        """Generate a random seed for provably fair result."""
        return os.urandom(32)
    
    def compute_commit_hash(self, seed: bytes, race_id: int) -> bytes:
        """Compute commit hash: keccak256(seed + raceId)."""
        return Web3.solidity_keccak(['bytes32', 'uint256'], [seed, race_id])
    
    def compute_winner(self, seed: bytes, race_id: int) -> int:
        """Compute winner: keccak256(seed, raceId, 42) % numDrivers."""
        winner_hash = Web3.solidity_keccak(
            ['bytes32', 'uint256', 'uint256'],
            [seed, race_id, 42]
        )
        return int.from_bytes(winner_hash, 'big') % NUM_DRIVERS
    
    def run_race_cycle(self, race_id: int):
        """Execute one full race cycle."""
        log.info(f"\n{'='*50}")
        log.info(f"🏁 RACE #{race_id} — {DRIVER_NAMES[0]} vs {DRIVER_NAMES[1]} vs ...")
        log.info(f"{'='*50}")
        
        # Step 1: Create race
        log.info("1️⃣  Creating race...")
        self._send_tx('createRace')
        time.sleep(1)
        
        # Step 2: Generate seed and commit
        seed = self.generate_seed()
        commit_hash = self.compute_commit_hash(seed, race_id)
        log.info(f"2️⃣  Committing hash: {commit_hash.hex()[:16]}...")
        self._send_tx('commitRace', race_id, commit_hash)
        
        # Step 3: Wait for betting window
        remaining = BET_WINDOW + 10  # extra buffer
        log.info(f"3️⃣  Betting window: {BET_WINDOW}s — waiting...")
        
        for i in range(remaining, 0, -10):
            pool = self._get_pool(race_id)
            bets = self._get_bet_count(race_id)
            log.info(f"   ⏳ {i}s left | Pool: {pool} | Bets: {bets}")
            time.sleep(min(10, i))
        
        # Step 4: Reveal seed
        log.info(f"4️⃣  Revealing seed...")
        self._send_tx('revealRace', race_id, seed)
        
        # Step 5: Settle
        time.sleep(2)
        log.info(f"5️⃣  Settling race...")
        self._send_tx('settleRace', race_id)
        
        # Show results
        race = self._get_race(race_id)
        winner_name = DRIVER_NAMES[race['winner']]
        log.info(f"\n🏆 WINNER: #{race['winner']} {winner_name}")
        log.info(f"   Pool: {self.w3.from_wei(race['totalPool'], 'ether')} ETH")
        log.info(f"   Fee:  {self.w3.from_wei(race['protocolFee'], 'ether')} ETH")
        log.info(f"   Bets: {self._get_bet_count(race_id)}")
        
        return race
    
    def _send_tx(self, function_name: str, *args):
        """Send a transaction to the contract."""
        fn = getattr(self.contract.functions, function_name)(*args)
        tx = fn.build_transaction({
            'from': self.account.address,
            'nonce': self.w3.eth.get_transaction_count(self.account.address),
            'gas': 500000,
            'gasPrice': self.w3.eth.gas_price,
            'chainId': CHAIN_ID
        })
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        
        if receipt.status == 1:
            log.info(f"   ✅ {function_name} confirmed in block {receipt.blockNumber}")
        else:
            log.error(f"   ❌ {function_name} failed!")
        
        return receipt
    
    def _get_race(self, race_id: int) -> dict:
        return self.contract.functions.getRace(race_id).call()
    
    def _get_bet_count(self, race_id: int) -> int:
        return self.contract.functions.getBetCount(race_id).call()
    
    def _get_pool(self, race_id: int) -> float:
        race = self._get_race(race_id)
        return round(self.w3.from_wei(race[10], 'ether'), 4)
    
    def _get_status(self, race_id: int) -> dict:
        """Get current race status for the dashboard."""
        race = self._get_race(race_id)
        return {
            'raceId': race[0],
            'startTime': race[1],
            'bettingEnd': race[2],
            'resultTime': race[3],
            'committed': race[7],
            'revealed': race[8],
            'settled': race[9],
            'winner': race[9] if race[8] else None,
            'totalPool': float(self.w3.from_wei(race[10], 'ether')),
            'protocolFee': float(self.w3.from_wei(race[11], 'ether')),
            'betCount': self._get_bet_count(race_id),
            'drivers': DRIVER_NAMES[:NUM_DRIVERS],
            'isBettingOpen': self.contract.functions.isBettingOpen(race_id).call()
        }
    
    def get_status(self, race_id: int) -> dict:
        """Public status getter."""
        return self._get_status(race_id)
    
    def get_claimable(self, race_id: int, address: str) -> float:
        """Get claimable amount for a user."""
        amount = self.contract.functions.getClaimable(
            race_id, Web3.to_checksum_address(address)
        ).call()
        return float(self.w3.from_wei(amount, 'ether'))


# ── Dashboard API ──────────────────────────────────────

def create_api(app):
    """Create Flask/FastAPI routes for the dashboard."""
    # We'll use a simple JSON API approach
    agent = app  # The agent instance
    
    @app.route('/api/status')
    def status():
        race_id = app.current_race_id - 1 if app.current_race_id > 0 else 0
        return app._get_status(race_id)
    
    @app.route('/api/history')
    def history():
        races = []
        for i in range(max(0, app.current_race_id - 10), app.current_race_id):
            races.append(app._get_status(i))
        return {'races': races}
    
    @app.route('/api/claimable/<address>')
    def claimable(address):
        total = 0
        for i in range(app.current_race_id):
            total += app.get_claimable(i, address)
        return {'total': total}


# ── Main Loop ──────────────────────────────────────────

def main():
    log.info("🏎️  DRIFT AI Agent Starting...")
    log.info(f"   Chain: Robinhood Testnet ({CHAIN_ID})")
    log.info(f"   Race interval: {RACE_INTERVAL}s")
    log.info(f"   Betting window: {BET_WINDOW}s")
    log.info(f"   Fee: {FEE_PERCENT}%")
    log.info(f"   Drivers: {NUM_DRIVERS}")
    
    agent = DriftAgent()
    
    # Check if contract is deployed
    contract_addr = os.getenv('DRIFT_CONTRACT_ADDRESS')
    if contract_addr:
        agent.connect_contract(contract_addr)
    else:
        log.warning("⚠️  No contract address found. Deploying...")
        addr = agent.deploy_contract()
        if not addr:
            log.error("❌ Deployment failed")
            return
        # Save to .env
        env_path = Path(__file__).parent / '.env'
        with open(env_path, 'a') as f:
            f.write(f"\nDRIFT_CONTRACT_ADDRESS={addr}\n")
    
    # Main race loop
    agent.current_race_id = agent.contract.functions.currentRaceId().call()
    log.info(f"📊 Current race ID: {agent.current_race_id}")
    
    while True:
        try:
            race = agent.run_race_cycle(agent.current_race_id)
            agent.current_race_id += 1
            
            log.info(f"💤 Next race in {RACE_INTERVAL}s...")
            time.sleep(RACE_INTERVAL)
            
        except KeyboardInterrupt:
            log.info("🛑 Agent stopped by user")
            break
        except Exception as e:
            log.error(f"❌ Error: {e}")
            log.info("   Retrying in 30s...")
            time.sleep(30)


if __name__ == '__main__':
    main()
