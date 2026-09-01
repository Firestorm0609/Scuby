#!/usr/bin/env python3
"""
Deploy DriftRace contract to Robinhood Chain Testnet.

Usage:
  python3 deploy.py              # Deploy new contract
  python3 deploy.py --verify     # Deploy and verify on explorer
"""

import os
import sys
import json
import solcx
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# ── Config ─────────────────────────────────────────────

load_dotenv(Path(__file__).parent / '.env')

RPC_URL = os.getenv('ROBINHOOD_RPC_URL', 'https://rpc.testnet.chain.robinhood.com')
CHAIN_ID = int(os.getenv('ROBINHOOD_CHAIN_ID', '46630'))
EXPLORER = os.getenv('ROBINHOOD_EXPLORER', 'https://explorer.testnet.chain.robinhood.com')

# ── Compile ────────────────────────────────────────────

def compile_contract():
    """Compile DriftRace.sol with solcx."""
    print("🔨 Compiling DriftRace.sol...")
    
    solcx.install_solc('0.8.20')
    
    contract_path = Path(__file__).parent / 'contracts' / 'DriftRace.sol'
    source = contract_path.read_text()
    
    compiled = solcx.compile_standard({
        'language': 'Solidity',
        'sources': {
            'DriftRace.sol': {'content': source}
        },
        'settings': {
            'outputSelection': {
                '*': {
                    '*': ['abi', 'evm.bytecode']
                }
            }
        }
    }, solc_version='0.8.20')
    
    contract_data = compiled['contracts']['DriftRace.sol']['DriftRace']
    abi = contract_data['abi']
    bytecode = contract_data['evm']['bytecode']['object']
    
    # Save compiled artifacts
    output = Path(__file__).parent / 'contracts' / 'DriftRace.json'
    output.write_text(json.dumps({'abi': abi, 'bytecode': bytecode}, indent=2))
    print(f"   ✅ Compiled → {output}")
    
    return abi, bytecode

# ── Deploy ─────────────────────────────────────────────

def deploy(abi, bytecode):
    """Deploy contract to Robinhood Chain."""
    print(f"\n🚀 Deploying to Robinhood Chain Testnet...")
    print(f"   RPC: {RPC_URL}")
    print(f"   Chain: {CHAIN_ID}")
    
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    
    if not w3.is_connected():
        print("❌ Cannot connect to RPC!")
        sys.exit(1)
    
    print(f"   Block: {w3.eth.block_number}")
    
    # Load or create deployer account
    pk = os.getenv('DRIFT_PRIVATE_KEY')
    if pk:
        account = w3.eth.account.from_key(pk)
    else:
        from eth_account import Account
        account = Account.create()
        print(f"\n🔑 Generated deployer wallet: {account.address}")
        print(f"   ⚠️  Fund this wallet with testnet ETH first!")
        print(f"   Private key: {account.key.hex()}")
        
        # Save to .env
        env_path = Path(__file__).parent / '.env'
        with open(env_path, 'a') as f:
            f.write(f"\nDRIFT_PRIVATE_KEY={account.key.hex()}\n")
    
    balance = w3.eth.get_balance(account.address)
    balance_eth = w3.from_wei(balance, 'ether')
    print(f"   💰 Balance: {balance_eth} ETH")
    
    if balance < w3.to_wei(0.01, 'ether'):
        print("\n⚠️  Low balance! Get testnet ETH from:")
        print(f"   {EXPLORER}")
        print("   Or ask in Robinhood Chain Discord")
        return None
    
    # Deploy
    contract = w3.eth.contract(abi=abi, bytecode=bytecode)
    
    tx = contract.constructor().build_transaction({
        'from': account.address,
        'nonce': w3.eth.get_transaction_count(account.address),
        'gas': 3000000,
        'gasPrice': w3.eth.gas_price,
        'chainId': CHAIN_ID
    })
    
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"\n   📤 TX: {tx_hash.hex()}")
    
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    
    if receipt.status == 1:
        addr = receipt.contractAddress
        print(f"\n✅ Contract deployed!")
        print(f"   Address: {addr}")
        print(f"   Block: {receipt.blockNumber}")
        print(f"   Explorer: {EXPLORER}/address/{addr}")
        print(f"   Gas used: {receipt.gasUsed}")
        
        # Save address
        env_path = Path(__file__).parent / '.env'
        with open(env_path, 'a') as f:
            f.write(f"\nDRIFT_CONTRACT_ADDRESS={addr}\n")
        
        # Also save to localStorage key for the frontend
        print(f"\n   📝 Add to frontend: localStorage.setItem('DRIFT_CONTRACT_ADDRESS', '{addr}')")
        
        return addr
    else:
        print("❌ Deployment failed!")
        return None

# ── Main ───────────────────────────────────────────────

def main():
    print("🏎️  DRIFT Contract Deployer")
    print("=" * 40)
    
    abi, bytecode = compile_contract()
    addr = deploy(abi, bytecode)
    
    if addr:
        print("\n" + "=" * 40)
        print("🎉 Next steps:")
        print("  1. Fund the agent wallet with testnet ETH")
        print("  2. Run: python3 agent.py")
        print("  3. Open index.html in browser")
        print("=" * 40)

if __name__ == '__main__':
    main()
