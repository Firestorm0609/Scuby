#!/usr/bin/env python3
"""
DRIFT Dashboard Server

Serves the frontend and provides API endpoints for the dashboard.
Connects to the DriftRace contract on Robinhood Chain.

Usage:
  python3 server.py              # Run on port 8080
  python3 server.py --port 3000  # Custom port
"""

import os
import json
import time
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from dotenv import load_dotenv
from web3 import Web3

load_dotenv(Path(__file__).parent / '.env')

RPC_URL = os.getenv('ROBINHOOD_RPC_URL', 'https://rpc.testnet.chain.robinhood.com')
CONTRACT_ADDRESS = os.getenv('DRIFT_CONTRACT_ADDRESS', '')

DRIFT_RACE_ABI = [
    "function currentRaceId() external view returns (uint256)",
    "function getRace(uint256 raceId) external view returns (uint256 id, uint256 startTime, uint256 bettingEnd, uint256 resultTime, bytes32 commitHash, bytes32 seedReveal, bool committed, bool revealed, bool settled, uint8 winner, uint256 totalPool, uint256 protocolFee)",
    "function getBetCount(uint256 raceId) external view returns (uint256)",
    "function isBettingOpen(uint256 raceId) external view returns (bool)",
    "function timeUntilBettingEnds(uint256 raceId) external view returns (uint256)",
    "function getClaimable(uint256 raceId, address user) external view returns (uint256)",
    "function protocolFees() external view returns (uint256)",
    "function feePercent() external view returns (uint256)",
    "function numDrivers() external view returns (uint256)",
]

DRIVERS = [
    "Lightning McQueen", "Drift King", "Phantom Racer",
    "Blaze Runner", "Shadow Speed", "Neon Fury"
]

w3 = Web3(Web3.HTTPProvider(RPC_URL))
contract = None

if CONTRACT_ADDRESS:
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(CONTRACT_ADDRESS),
        abi=DRIFT_RACE_ABI
    )


class DriftHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/status':
            self._json_response(self._get_status())
        elif self.path.startswith('/api/history'):
            self._json_response(self._get_history())
        elif self.path.startswith('/api/claimable/'):
            address = self.path.split('/')[-1]
            self._json_response(self._get_claimable(address))
        elif self.path == '/api/health':
            self._json_response({
                'connected': w3.is_connected(),
                'block': w3.eth.block_number if w3.is_connected() else 0,
                'contract': CONTRACT_ADDRESS or 'not deployed',
                'chain': 46630
            })
        else:
            super().do_GET()
    
    def _json_response(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def _get_status(self):
        if not contract:
            return {'error': 'Contract not deployed'}
        
        try:
            race_id = contract.functions.currentRaceId().call()
            if race_id == 0:
                return {'error': 'No races yet', 'raceId': 0}
            
            race = contract.functions.getRace(race_id - 1).call()
            bet_count = contract.functions.getBetCount(race_id - 1).call()
            is_open = contract.functions.isBettingOpen(race_id - 1).call()
            time_left = contract.functions.timeUntilBettingEnds(race_id - 1).call()
            
            return {
                'raceId': race[0],
                'startTime': race[1],
                'bettingEnd': race[2],
                'resultTime': race[3],
                'committed': race[6],
                'revealed': race[7],
                'settled': race[8],
                'winner': race[9] if race[7] else None,
                'winnerName': DRIVERS[race[9]] if race[7] else None,
                'totalPool': float(w3.from_wei(race[10], 'ether')),
                'protocolFee': float(w3.from_wei(race[11], 'ether')),
                'betCount': bet_count,
                'isBettingOpen': is_open,
                'timeUntilBettingEnds': time_left,
                'drivers': DRIVERS[:6],
                'currentRaceId': race_id
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _get_history(self):
        if not contract:
            return {'races': []}
        
        try:
            race_id = contract.functions.currentRaceId().call()
            races = []
            start = max(0, race_id - 10)
            
            for i in range(start, race_id):
                race = contract.functions.getRace(i).call()
                bet_count = contract.functions.getBetCount(i).call()
                races.append({
                    'raceId': race[0],
                    'winner': race[9] if race[7] else None,
                    'winnerName': DRIVERS[race[9]] if race[7] else 'Pending',
                    'totalPool': float(w3.from_wei(race[10], 'ether')),
                    'betCount': bet_count,
                    'settled': race[8]
                })
            
            return {'races': races}
        except Exception as e:
            return {'error': str(e)}
    
    def _get_claimable(self, address):
        if not contract:
            return {'error': 'Contract not deployed'}
        
        try:
            race_id = contract.functions.currentRaceId().call()
            total = 0
            for i in range(race_id):
                amount = contract.functions.getClaimable(i, Web3.to_checksum_address(address)).call()
                total += float(w3.from_wei(amount, 'ether'))
            return {'total': total}
        except Exception as e:
            return {'error': str(e)}


def main():
    import sys
    
    port = 8080
    if '--port' in sys.argv:
        idx = sys.argv.index('--port')
        port = int(sys.argv[idx + 1])
    
    print("🏎️  DRIFT Dashboard Server")
    print(f"   http://localhost:{port}")
    print(f"   Contract: {CONTRACT_ADDRESS or 'not deployed'}")
    print(f"   RPC: {RPC_URL}")
    
    server = HTTPServer(('0.0.0.0', port), DriftHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")
        server.server_close()


if __name__ == '__main__':
    main()
