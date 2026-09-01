// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title DriftRace
 * @notice AI Racing Betting Platform on Robinhood Chain
 * @dev Provably fair betting with commit-reveal scheme
 *
 * Flow:
 * 1. Agent commits hash(seed + raceId) before betting opens
 * 2. Users place bets on drivers during betting window
 * 3. Agent reveals seed after betting closes
 * 4. Seed + raceId determines winner via keccak256
 * 5. 95% to winner(s), 5% to protocol
 */
contract DriftRace {
    // ── State ──────────────────────────────────────────────

    address public owner;
    uint256 public feePercent = 5;          // 5% protocol fee
    uint256 public minBet = 5 ether;        // $5 min (assuming 1 ETH ≈ $1)
    uint256 public maxBet = 1000 ether;     // $1000 max
    uint256 public numDrivers = 6;
    uint256 public raceInterval = 300;      // 5 minutes
    uint256 public betWindow = 60;          // 60 seconds betting

    uint256 public currentRaceId = 0;
    uint256 public protocolFees = 0;

    struct Race {
        uint256 id;
        uint256 startTime;
        uint256 bettingEnd;
        uint256 resultTime;
        bytes32 commitHash;                 // keccak256(seed + raceId)
        bytes32 seedReveal;
        bool committed;
        bool revealed;
        bool settled;
        uint8 winner;                       // driver index (0-5)
        uint256 totalPool;
        uint256 protocolFee;
    }

    struct Bet {
        address bettor;
        uint8 driver;
        uint256 amount;
        bool claimed;
    }

    mapping(uint256 => Race) public races;
    mapping(uint256 => Bet[]) public raceBets;
    mapping(uint256 => mapping(address => uint256)) public claimable; // raceId => user => amount

    // ── Events ─────────────────────────────────────────────

    event RaceCreated(uint256 indexed raceId, uint256 startTime, uint256 bettingEnd);
    event BetPlaced(uint256 indexed raceId, address indexed bettor, uint8 driver, uint256 amount);
    event RaceCommitted(uint256 indexed raceId, bytes32 commitHash);
    event RaceRevealed(uint256 indexed raceId, bytes32 seed, uint8 winner);
    event RaceSettled(uint256 indexed raceId, uint8 winner, uint256 totalPool, uint256 protocolFee);
    event PrizeClaimed(uint256 indexed raceId, address indexed bettor, uint256 amount);
    event FeesWithdrawn(address indexed to, uint256 amount);

    // ── Modifiers ──────────────────────────────────────────

    modifier onlyOwner() {
        require(msg.sender == owner, "Not owner");
        _;
    }

    // ── Constructor ────────────────────────────────────────

    constructor() {
        owner = msg.sender;
    }

    // ── Core Functions ─────────────────────────────────────

    /**
     * @notice Create a new race and open betting
     */
    function createRace() external onlyOwner returns (uint256) {
        uint256 raceId = currentRaceId++;
        uint256 startTime = block.timestamp;
        uint256 bettingEnd = startTime + betWindow;

        races[raceId] = Race({
            id: raceId,
            startTime: startTime,
            bettingEnd: bettingEnd,
            resultTime: 0,
            commitHash: bytes32(0),
            seedReveal: bytes32(0),
            committed: false,
            revealed: false,
            settled: false,
            winner: 0,
            totalPool: 0,
            protocolFee: 0
        });

        emit RaceCreated(raceId, startTime, bettingEnd);
        return raceId;
    }

    /**
     * @notice Agent commits hash before betting opens (commit-reveal)
     */
    function commitRace(uint256 raceId, bytes32 commitHash) external onlyOwner {
        Race storage race = races[raceId];
        require(!race.committed, "Already committed");
        require(block.timestamp <= race.bettingEnd, "Betting ended");

        race.commitHash = commitHash;
        race.committed = true;

        emit RaceCommitted(raceId, commitHash);
    }

    /**
     * @notice Place a bet on a driver
     */
    function placeBet(uint256 raceId, uint8 driver) external payable {
        Race storage race = races[raceId];
        require(race.committed, "Race not open");
        require(block.timestamp <= race.bettingEnd, "Betting closed");
        require(!race.settled, "Race settled");
        require(driver < numDrivers, "Invalid driver");
        require(msg.value >= minBet, "Below min bet");
        require(msg.value <= maxBet, "Above max bet");

        race.totalPool += msg.value;
        raceBets[raceId].push(Bet({
            bettor: msg.sender,
            driver: driver,
            amount: msg.value,
            claimed: false
        }));

        emit BetPlaced(raceId, msg.sender, driver, msg.value);
    }

    /**
     * @notice Agent reveals the seed to determine winner
     */
    function revealRace(uint256 raceId, bytes32 seed) external onlyOwner {
        Race storage race = races[raceId];
        require(race.committed, "Not committed");
        require(!race.revealed, "Already revealed");
        require(block.timestamp > race.bettingEnd, "Betting still open");

        // Verify the commit
        bytes32 hash = keccak256(abi.encodePacked(seed, raceId));
        require(hash == race.commitHash, "Invalid seed");

        // Determine winner: hash(seed, raceId) mod numDrivers
        bytes32 winnerHash = keccak256(abi.encodePacked(seed, raceId, uint256(42)));
        uint8 winner = uint8(uint256(winnerHash) % numDrivers);

        race.seedReveal = seed;
        race.winner = winner;
        race.revealed = true;
        race.resultTime = block.timestamp;

        emit RaceRevealed(raceId, seed, winner);
    }

    /**
     * @notice Settle the race — calculate payouts
     */
    function settleRace(uint256 raceId) external onlyOwner {
        Race storage race = races[raceId];
        require(race.revealed, "Not revealed");
        require(!race.settled, "Already settled");

        uint256 totalPool = race.totalPool;
        uint256 protocolFee = (totalPool * feePercent) / 100;
        uint256 winnerPool = totalPool - protocolFee;

        race.protocolFee = protocolFee;
        race.settled = true;
        protocolFees += protocolFee;

        // Calculate payout per winning bet
        Bet[] storage bets = raceBets[raceId];
        uint256 winningAmount = 0;

        for (uint256 i = 0; i < bets.length; i++) {
            if (bets[i].driver == race.winner) {
                winningAmount += bets[i].amount;
            }
        }

        // Distribute proportionally
        if (winningAmount > 0) {
            for (uint256 i = 0; i < bets.length; i++) {
                if (bets[i].driver == race.winner) {
                    uint256 payout = (bets[i].amount * winnerPool) / winningAmount;
                    claimable[raceId][bets[i].bettor] += payout;
                }
            }
        }

        emit RaceSettled(raceId, race.winner, totalPool, protocolFee);
    }

    /**
     * @notice Claim winnings from a settled race
     */
    function claimPrize(uint256 raceId) external {
        uint256 amount = claimable[raceId][msg.sender];
        require(amount > 0, "Nothing to claim");

        claimable[raceId][msg.sender] = 0;
        (bool success, ) = msg.sender.call{value: amount}("");
        require(success, "Transfer failed");

        emit PrizeClaimed(raceId, msg.sender, amount);
    }

    /**
     * @notice Withdraw accumulated protocol fees
     */
    function withdrawFees(address payable to) external onlyOwner {
        uint256 amount = protocolFees;
        require(amount > 0, "No fees");

        protocolFees = 0;
        (bool success, ) = to.call{value: amount}("");
        require(success, "Transfer failed");

        emit FeesWithdrawn(to, amount);
    }

    // ── View Functions ─────────────────────────────────────

    function getRace(uint256 raceId) external view returns (Race memory) {
        return races[raceId];
    }

    function getBetCount(uint256 raceId) external view returns (uint256) {
        return raceBets[raceId].length;
    }

    function getBet(uint256 raceId, uint256 index) external view returns (Bet memory) {
        return raceBets[raceId][index];
    }

    function getClaimable(uint256 raceId, address user) external view returns (uint256) {
        return claimable[raceId][user];
    }

    function isBettingOpen(uint256 raceId) external view returns (bool) {
        Race storage race = races[raceId];
        return race.committed && !race.settled && block.timestamp <= race.bettingEnd;
    }

    function timeUntilBettingEnds(uint256 raceId) external view returns (uint256) {
        Race storage race = races[raceId];
        if (block.timestamp >= race.bettingEnd) return 0;
        return race.bettingEnd - block.timestamp;
    }

    // ── Admin Functions ────────────────────────────────────

    function setFeePercent(uint256 _fee) external onlyOwner {
        require(_fee <= 20, "Fee too high");
        feePercent = _fee;
    }

    function setMinBet(uint256 _min) external onlyOwner {
        minBet = _min;
    }

    function setMaxBet(uint256 _max) external onlyOwner {
        maxBet = _max;
    }

    function setNumDrivers(uint8 _num) external onlyOwner {
        require(_num >= 2 && _num <= 12, "Invalid driver count");
        numDrivers = _num;
    }

    function transferOwnership(address newOwner) external onlyOwner {
        require(newOwner != address(0), "Zero address");
        owner = newOwner;
    }
}
