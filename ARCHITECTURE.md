# VultVision Intelligence System Architecture

## Overview

VultVision is a multi-layered cryptocurrency intelligence platform that analyzes pump.fun tokens on Solana to identify alpha opportunities through creator tracking, wallet intelligence, viral detection, and momentum analysis.

## Database Schema (SQLite)

### 1. tokens
Stores all discovered tokens and their performance metrics.

```sql
CREATE TABLE tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE NOT NULL,
    name TEXT,
    symbol TEXT,
    creator_address TEXT,
    created_timestamp INTEGER,
    discovered_timestamp INTEGER,
    initial_lp_sol REAL,
    initial_holders INTEGER,
    initial_mcap REAL,
    current_price_usd REAL,
    peak_price_usd REAL,
    peak_mcap REAL,
    current_holders INTEGER,
    total_volume_usd REAL,
    ath_multiplier REAL,
    status TEXT DEFAULT 'active',
    last_updated INTEGER
);
```

**Status values:** `active`, `graduated`, `rugged`, `dead`

### 2. creators
Tracks token creators and their historical performance.

```sql
CREATE TABLE creators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT UNIQUE NOT NULL,
    first_seen_date INTEGER,
    total_tokens_launched INTEGER DEFAULT 0,
    successful_tokens INTEGER DEFAULT 0,
    best_multiplier REAL DEFAULT 0,
    avg_multiplier REAL DEFAULT 0,
    consecutive_wins INTEGER DEFAULT 0,
    reputation_score INTEGER DEFAULT 0,
    is_proven BOOLEAN DEFAULT 0,
    is_serial_rugger BOOLEAN DEFAULT 0,
    last_token_address TEXT,
    last_updated INTEGER
);
```

**Reputation scoring:**
- `reputation_score`: 0-100, based on success rate and multipliers
- `is_proven`: True if 3+ tokens with >5x success
- `successful_tokens`: Count of tokens that achieved >5x

### 3. alpha_wallets
Tracks wallets that consistently enter early and profit.

```sql
CREATE TABLE alpha_wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT UNIQUE NOT NULL,
    first_tracked_date INTEGER,
    consecutive_wins INTEGER DEFAULT 0,
    total_trades_tracked INTEGER DEFAULT 0,
    win_rate_pct REAL DEFAULT 0,
    avg_entry_multiplier REAL DEFAULT 0,
    best_trade_multiplier REAL DEFAULT 0,
    total_profit_sol REAL DEFAULT 0,
    status TEXT DEFAULT 'tracking',
    last_trade_address TEXT,
    last_updated INTEGER
);
```

**Status values:** `tracking`, `alpha`, `retired`
- `alpha`: Wallet with 5+ consecutive wins or 80%+ win rate

### 4. wallet_trades
Records individual trades made by tracked wallets.

```sql
CREATE TABLE wallet_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    buy_timestamp INTEGER,
    buy_price_usd REAL,
    amount_sol REAL,
    sell_timestamp INTEGER,
    sell_price_usd REAL,
    multiplier REAL,
    is_profitable BOOLEAN,
    status TEXT DEFAULT 'holding',
    FOREIGN KEY (wallet_address) REFERENCES alpha_wallets(wallet_address),
    FOREIGN KEY (token_address) REFERENCES tokens(address)
);
```

**Status values:** `holding`, `sold`, `rugged`

### 5. viral_events
Stores trending keywords and events from social media.

```sql
CREATE TABLE viral_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL,
    platform TEXT,
    trend_score INTEGER DEFAULT 0,
    first_detected INTEGER,
    last_detected INTEGER,
    total_mentions INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT 1
);
```

**Platforms:** `twitter`, `tiktok`, `instagram`
**Trend score:** 0-100, based on mention velocity and volume

### 6. token_viral_matches
Links tokens to viral events they match.

```sql
CREATE TABLE token_viral_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT NOT NULL,
    viral_event_id INTEGER NOT NULL,
    match_score INTEGER DEFAULT 0,
    matched_at INTEGER,
    token_performance_after_24h REAL,
    FOREIGN KEY (token_address) REFERENCES tokens(address),
    FOREIGN KEY (viral_event_id) REFERENCES viral_events(id)
);
```

**Match score:** 0-100, based on keyword relevance and timing

### 7. alpha_box_entries
Curated list of high-signal tokens across all categories.

```sql
CREATE TABLE alpha_box_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT NOT NULL,
    category TEXT NOT NULL,
    alpha_score INTEGER DEFAULT 0,
    signals TEXT,
    added_at INTEGER,
    expires_at INTEGER,
    status TEXT DEFAULT 'active',
    FOREIGN KEY (token_address) REFERENCES tokens(address)
);
```

**Categories:** `proven_creator`, `alpha_wallet`, `viral`, `ultra_new`, `momentum`
**Signals:** JSON array of reasons (e.g., `["creator_5x_streak", "low_mcap", "high_lp"]`)
**Alpha score:** 0-100, composite score across all signals

---

## File Structure

```
/root/
  api.py                          # Flask main API
  Caddyfile                       # Caddy reverse proxy config
  requirements.txt                # Python dependencies
  
  /pumpfun/
    /models/
      database.py                 # SQLite connection + base queries
      token.py                    # Token model & queries
      creator.py                  # Creator model & queries
      wallet.py                   # Wallet model & queries
      viral.py                    # Viral event model & queries
    
    /services/
      background_analyzer.py      # Main token scanner (every 20s)
      creator_tracker.py          # Track creator performance (every 5min)
      wallet_intelligence.py      # Track smart wallets (every 3min)
      viral_detector.py           # Scrape social trends (every 30min)
      performance_monitor.py      # Update token prices (every 2min)
      alpha_box_manager.py        # Curate alpha box (every 1min)
    
    /routes/
      api_routes.py               # API endpoint definitions
    
    /utils/
      solana_rpc.py              # Solana RPC helpers
      scoring.py                 # Alpha scoring algorithms
      filters.py                 # Token filtering logic
  
  /data/
    pumpfun.db                    # SQLite database
    /logs/
      analyzer.log
      creator_tracker.log
      wallet_intel.log
      viral_detector.log
      performance.log
      alpha_manager.log
  
  /frontend/ (GitHub Pages)
    index.html
    app.jsx
    /components/
      AlphaBox.jsx                # Multi-category alpha display
      CategoryBox.jsx             # Individual category container
      AlphaCard.jsx               # Token card component
      ProvenCreators.jsx          # Proven creator section
      AlphaWallets.jsx            # Alpha wallet section
      ViralCoins.jsx              # Viral coins section
      TokenDetails.jsx            # Detailed token view
```

---

## Service Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    FLASK API (api.py)                       │
│  Endpoints: /api/alpha-box, /api/creators, /api/wallets    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  SQLite Database (pumpfun.db)               │
│  Tables: tokens, creators, alpha_wallets, viral_events...  │
└─────────────────────────────────────────────────────────────┘
                              ↑
        ┌────────────────────┼────────────────────┐
        ↓                    ↓                    ↓
┌──────────────┐   ┌──────────────────┐   ┌──────────────┐
│ ANALYZER     │   │ CREATOR TRACKER  │   │ WALLET INTEL │
│ Every 20s    │   │ Every 5 min      │   │ Every 3 min  │
│ Scan new     │   │ Update creator   │   │ Track smart  │
│ tokens       │   │ performance      │   │ wallet buys  │
└──────────────┘   └──────────────────┘   └──────────────┘
        ↓                    ↓                    ↓
┌──────────────┐   ┌──────────────────┐   ┌──────────────┐
│ VIRAL        │   │ PERFORMANCE      │   │ ALPHA BOX    │
│ DETECTOR     │   │ MONITOR          │   │ MANAGER      │
│ Every 30 min │   │ Every 2 min      │   │ Every 1 min  │
│ Scrape trends│   │ Update prices    │   │ Curate boxes │
└──────────────┘   └──────────────────┘   └──────────────┘
```

---

## Data Flow

### 1. Token Discovery Flow
**Service:** `background_analyzer.py`

1. Scan pump.fun API for new token launches
2. Fetch on-chain data (LP, holders, creator)
3. Insert token into `tokens` table
4. Check if creator exists in `creators` table
   - If new: create entry
   - If existing: update stats
5. Check for viral matches against `viral_events`
6. Calculate initial alpha_score
7. If score > threshold: insert into `alpha_box_entries`

**Filters applied:**
- Minimum LP: 1 SOL
- Social links present
- Non-generic name
- Creator not flagged as rugger

---

### 2. Creator Tracking Flow
**Service:** `creator_tracker.py`

1. Query all tokens grouped by `creator_address`
2. For each creator:
   - Calculate total launches
   - Count successful tokens (>5x)
   - Calculate average multiplier
   - Detect consecutive wins
   - Calculate reputation score
3. Update `creators` table
4. If creator becomes "proven" (3+ successful tokens):
   - Flag `is_proven = 1`
   - Monitor their next launch closely
5. When proven creator launches new token:
   - Auto-add to `alpha_box_entries` with category `proven_creator`

**Reputation scoring formula:**
```
reputation_score = (
    (successful_tokens / total_tokens) * 40 +
    min(avg_multiplier * 5, 30) +
    min(consecutive_wins * 5, 20) +
    min(best_multiplier, 10)
)
```

---

### 3. Wallet Intelligence Flow
**Service:** `wallet_intelligence.py`

1. Monitor tokens that graduate (>5x in 24h)
2. Fetch top 10 earliest holders from graduated tokens
3. For each holder:
   - Check if exists in `alpha_wallets`
   - If new: create entry with `status='tracking'`
   - If existing: update stats
4. Monitor new buys from tracked wallets
5. Record each buy in `wallet_trades` table
6. When wallet sells:
   - Calculate multiplier
   - Update `wallet_trades` (set `is_profitable`, `multiplier`)
   - Update wallet stats (consecutive_wins, win_rate_pct)
7. If wallet achieves 5+ consecutive wins:
   - Upgrade `status='alpha'`
8. When alpha wallet makes new buy:
   - Auto-add to `alpha_box_entries` with category `alpha_wallet`

**Alpha wallet criteria:**
- 5+ consecutive profitable trades, OR
- 80%+ win rate with 10+ tracked trades, OR
- Average entry multiplier >3x

---

### 4. Viral Detection Flow
**Service:** `viral_detector.py`

1. Scrape trending topics from:
   - Twitter trending hashtags (Twitter API)
   - TikTok trending sounds/hashtags (Apify scraper)
   - Instagram trending hashtags (Graph API)
2. Extract keywords and calculate trend scores
3. Insert/update `viral_events` table
4. Match viral keywords against token names/descriptions
5. Create entries in `token_viral_matches`
6. Calculate match_score based on:
   - Keyword exact match vs partial
   - Timing (how new is the viral event)
   - Platform reach
7. If match_score > 70 AND token passes filters:
   - Add to `alpha_box_entries` with category `viral`

**Viral scoring:**
```
trend_score = (
    (total_mentions / 1000) * 40 +
    (mention_velocity_per_hour) * 30 +
    (platform_count * 10)
)
```

---

### 5. Performance Monitoring Flow
**Service:** `performance_monitor.py`

1. Query all active tokens
2. For each token:
   - Fetch current price from DexScreener/pump.fun API
   - Calculate current mcap
   - Update `current_price_usd`, `current_holders`, `total_volume_usd`
   - If new peak: update `peak_price_usd`, `peak_mcap`, `ath_multiplier`
3. Detect rugs:
   - LP removed + price drop >90%
   - Update `status='rugged'`
4. Detect dead tokens:
   - No volume for 24h + price drop >95%
   - Update `status='dead'`
5. Update related `wallet_trades`:
   - If token rugged: set trade `status='rugged'`
   - If sell detected: update `sell_price_usd`, `multiplier`
6. Update `alpha_box_entries`:
   - Expire entries for rugged/dead tokens
   - Update `status='failed'`

---

### 6. Alpha Box Management Flow
**Service:** `alpha_box_manager.py`

1. Query all active `alpha_box_entries`
2. Recalculate alpha_score for each entry based on:
   - Age (newer = higher score)
   - Category signals strength
   - Current performance (price movement)
   - Liquidity depth
3. Expire old entries (>24h or token graduated/rugged)
4. Add new high-signal tokens from each category:
   - **Proven Creator:** Recent launches from proven creators
   - **Alpha Wallet:** Recent buys from alpha wallets
   - **Viral:** High match_score viral tokens
   - **Ultra New:** <5min old, passing all filters
   - **Momentum:** Fastest growing in last 1h
5. Limit each category to top 10 entries
6. Update `alpha_box_entries` table

**Alpha score formula:**
```
alpha_score = (
    category_signal_strength * 40 +
    (100 - age_in_minutes) * 0.3 +
    liquidity_score * 20 +
    early_momentum_score * 10
)
```

---

## API Endpoints

### GET /api/alpha-box
Returns curated alpha opportunities across all categories.

**Response:**
```json
{
  "proven_creators": [
    {
      "token_address": "ABC...XYZ",
      "name": "Token Name",
      "symbol": "TKN",
      "alpha_score": 85,
      "signals": ["creator_5x_streak", "high_lp", "low_mcap"],
      "creator": {
        "address": "DEF...123",
        "reputation_score": 92,
        "consecutive_wins": 3,
        "best_multiplier": 12.5
      },
      "current_mcap": 45000,
      "age_minutes": 15
    }
  ],
  "alpha_wallets": [...],
  "viral_coins": [...],
  "ultra_new": [...],
  "momentum": [...]
}
```

---

### GET /api/creators/top
Returns top 20 proven creators ranked by reputation.

**Query params:**
- `limit`: Number of results (default: 20)
- `min_score`: Minimum reputation score (default: 60)

**Response:**
```json
{
  "creators": [
    {
      "wallet_address": "ABC...XYZ",
      "reputation_score": 95,
      "total_tokens_launched": 8,
      "successful_tokens": 6,
      "best_multiplier": 25.3,
      "avg_multiplier": 8.7,
      "consecutive_wins": 4,
      "last_token": {
        "address": "DEF...123",
        "name": "Recent Token",
        "ath_multiplier": 12.5,
        "created_timestamp": 1705334400
      }
    }
  ]
}
```

---

### GET /api/wallets/alpha
Returns alpha wallets with their recent trades.

**Query params:**
- `min_win_rate`: Minimum win rate % (default: 70)
- `include_trades`: Include recent trades (default: true)

**Response:**
```json
{
  "wallets": [
    {
      "wallet_address": "ABC...XYZ",
      "consecutive_wins": 7,
      "win_rate_pct": 85.7,
      "total_trades_tracked": 14,
      "avg_entry_multiplier": 4.2,
      "best_trade_multiplier": 18.5,
      "recent_trades": [
        {
          "token_address": "DEF...123",
          "token_name": "Token Name",
          "buy_timestamp": 1705334400,
          "multiplier": 8.5,
          "status": "sold"
        }
      ]
    }
  ]
}
```

---

### GET /api/viral/trending
Returns current viral events and matched tokens.

**Response:**
```json
{
  "viral_events": [
    {
      "keyword": "superbowl",
      "platform": "twitter",
      "trend_score": 95,
      "total_mentions": 15000,
      "matched_tokens": [
        {
          "token_address": "ABC...XYZ",
          "name": "SuperBowl Coin",
          "match_score": 88,
          "performance_after_24h": 6.2
        }
      ]
    }
  ]
}
```

---

### GET /api/token/{address}
Returns full details for a specific token.

**Response:**
```json
{
  "token": {
    "address": "ABC...XYZ",
    "name": "Token Name",
    "symbol": "TKN",
    "creator_address": "DEF...123",
    "current_price_usd": 0.000045,
    "current_mcap": 45000,
    "ath_multiplier": 8.5,
    "status": "active"
  },
  "creator": {
    "reputation_score": 85,
    "total_tokens_launched": 5,
    "successful_tokens": 3
  },
  "viral_matches": [
    {
      "keyword": "trending_topic",
      "match_score": 75,
      "platform": "twitter"
    }
  ],
  "alpha_box_entry": {
    "category": "proven_creator",
    "alpha_score": 88,
    "signals": ["creator_proven", "high_lp"]
  }
}
```

---

## Frontend Structure

### AlphaBox Component (Main View)

```jsx
<AlphaBox>
  <CategoryBox title="🏆 Proven Creators" color="gold" category="proven_creators">
    {tokens.map(token => (
      <AlphaCard 
        token={token}
        badge={
          <CreatorBadge>
            {token.creator.consecutive_wins} win streak | 
            Last: {token.creator.best_multiplier}x
          </CreatorBadge>
        }
        score={token.alpha_score}
      />
    ))}
  </CategoryBox>
  
  <CategoryBox title="💰 Alpha Wallet Buys" color="green" category="alpha_wallets">
    {tokens.map(token => (
      <AlphaCard 
        token={token}
        badge={
          <WalletBadge>
            {formatWallet(token.wallet_address)} | 
            {token.wallet.consecutive_wins}-win streak
          </WalletBadge>
        }
        score={token.alpha_score}
      />
    ))}
  </CategoryBox>
  
  <CategoryBox title="🔥 Viral Coins" color="red" category="viral">
    {tokens.map(token => (
      <AlphaCard 
        token={token}
        badge={
          <ViralBadge>
            Trending: {token.viral_match.keyword} | 
            Score: {token.viral_match.match_score}
          </ViralBadge>
        }
        score={token.alpha_score}
      />
    ))}
  </CategoryBox>
  
  <CategoryBox title="⚡ Ultra New" color="purple" category="ultra_new">
    {/* <5min old tokens */}
  </CategoryBox>
  
  <CategoryBox title="📈 Momentum" color="blue" category="momentum">
    {/* Fastest growing tokens */}
  </CategoryBox>
</AlphaBox>
```

### AlphaCard Component

```jsx
<AlphaCard>
  <TokenHeader>
    <TokenName>{token.name}</TokenName>
    <TokenSymbol>${token.symbol}</TokenSymbol>
  </TokenHeader>
  
  <TokenStats>
    <Stat label="MCap" value={formatMcap(token.current_mcap)} />
    <Stat label="Age" value={formatAge(token.age_minutes)} />
    <Stat label="LP" value={formatSOL(token.initial_lp_sol)} />
  </TokenStats>
  
  <CategoryBadge>{badge}</CategoryBadge>
  
  <AlphaScore score={score}>
    {score}
    <Signals>
      {token.signals.map(s => <SignalTag>{s}</SignalTag>)}
    </Signals>
  </AlphaScore>
  
  <Actions>
    <Button onClick={() => copyCA(token.address)}>Copy CA</Button>
    <Button onClick={() => openDexScreener(token.address)}>Chart</Button>
  </Actions>
</AlphaCard>
```

---

## Systemd Services (Auto-restart on failure)

### /etc/systemd/system/pumpfun-analyzer.service
```ini
[Unit]
Description=PumpFun Token Analyzer
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/pumpfun
ExecStart=/usr/bin/python3 /root/pumpfun/services/background_analyzer.py
Restart=always
RestartSec=10
StandardOutput=append:/root/data/logs/analyzer.log
StandardError=append:/root/data/logs/analyzer.log

[Install]
WantedBy=multi-user.target
```

### Other Services
Create similar service files for:
- `pumpfun-creator-tracker.service`
- `pumpfun-wallet-intel.service`
- `pumpfun-viral-detector.service`
- `pumpfun-performance.service`
- `pumpfun-alpha-manager.service`

**Enable and start:**
```bash
systemctl daemon-reload
systemctl enable pumpfun-analyzer.service
systemctl start pumpfun-analyzer.service
systemctl status pumpfun-analyzer.service
```

---

## Priorities & Build Order

### PHASE 1: Core Infrastructure (Week 1)
**Goal:** Get basic token tracking + creator analysis working

1. ✅ Create database schema (`models/database.py`)
2. ✅ Build Token model (`models/token.py`)
3. ✅ Build Creator model (`models/creator.py`)
4. ✅ Migrate `background_analyzer.py` to use new DB
5. ✅ Build `creator_tracker.py` service
6. ✅ Build `performance_monitor.py` service
7. ✅ Create API endpoints for alpha box (basic)
8. ✅ Update frontend to display proven creators

**Success criteria:**
- Tokens auto-discovered and stored in DB
- Creator reputation scores updating
- Can view proven creator tokens in frontend

---

### PHASE 2: Wallet Intelligence (Week 2)
**Goal:** Track smart wallets and their buys

9. ✅ Build Wallet model (`models/wallet.py`)
10. ✅ Build `wallet_intelligence.py` service
11. ✅ Build `alpha_box_manager.py` service
12. ✅ Update API to include alpha wallet category
13. ✅ Update frontend with alpha wallet section

**Success criteria:**
- Alpha wallets identified and tracked
- New buys from alpha wallets auto-added to alpha box
- Can view alpha wallet buys in frontend

---

### PHASE 3: Viral Detection (Week 3)
**Goal:** Match tokens to social trends

14. ⏳ Set up Twitter/X API access
15. ⏳ Build Viral model (`models/viral.py`)
16. ⏳ Build `viral_detector.py` service
17. ⏳ Build viral matching logic
18. ⏳ Update API to include viral category
19. ⏳ Update frontend with viral section
20. ⏳ (Optional) Add TikTok/Instagram scraping

**Success criteria:**
- Viral events detected from Twitter
- Tokens matched to trending topics
- Can view viral coins in frontend

---

### PHASE 4: Polish & Optimization (Week 4)
**Goal:** Performance, UI, and reliability improvements

21. ⏳ Add caching layer (Redis)
22. ⏳ Optimize database queries
23. ⏳ Add rate limiting to API
24. ⏳ Improve frontend UX (animations, filters)
25. ⏳ Add historical charts
26. ⏳ Add notification system (Telegram bot?)
27. ⏳ Comprehensive error handling + logging
28. ⏳ Add unit tests

---

## External Dependencies

### Required APIs
- ✅ Solana RPC (already configured)
- ✅ Pump.fun API (already configured)
- ✅ DexScreener API (for price updates)

### Optional APIs (for viral detection)
- ⏳ Twitter/X API (requires Developer account)
  - Endpoint: GET /2/tweets/search/recent
  - Rate limit: 450 requests/15min
- ⏳ TikTok API (via Apify scraper)
  - Alternative: BeautifulSoup scraping
- ⏳ Instagram Graph API (requires Business account)

### Python Packages
```
flask
flask-cors
requests
sqlite3 (built-in)
apscheduler
python-dotenv
tweepy (for Twitter)
beautifulsoup4 (for scraping)
```

---

## Configuration

### Environment Variables (.env)
```bash
# Solana RPC
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
HELIUS_API_KEY=your_helius_key

# APIs
TWITTER_API_KEY=your_twitter_key
TWITTER_API_SECRET=your_twitter_secret
TWITTER_BEARER_TOKEN=your_bearer_token

# Database
DATABASE_PATH=/root/data/pumpfun.db

# Services
ANALYZER_INTERVAL=20
CREATOR_TRACKER_INTERVAL=300
WALLET_INTEL_INTERVAL=180
VIRAL_DETECTOR_INTERVAL=1800
PERFORMANCE_MONITOR_INTERVAL=120
ALPHA_MANAGER_INTERVAL=60

# Filters
MIN_LP_SOL=1.0
MIN_REPUTATION_SCORE=60
MIN_ALPHA_SCORE=70
```

---

## Monitoring & Alerts

### Health Checks
- API endpoint: GET /api/health
- Returns status of all background services
- Checks database connectivity
- Reports last update times

### Logging Strategy
- Separate log file per service
- Log rotation: daily, keep 7 days
- Log levels: INFO for normal, WARNING for issues, ERROR for failures
- Structured logging with timestamps + service name

### Metrics to Track
- Tokens discovered per hour
- Alpha box entry count per category
- API request rate
- Database query performance
- Service uptime

---

## Security Considerations

1. **API Rate Limiting**
   - Limit: 100 requests/min per IP
   - Prevent abuse of alpha box endpoint

2. **Database Backups**
   - Daily backup to separate disk
   - Keep 7 days of backups
   - Automated via cron job

3. **Input Validation**
   - Sanitize all token addresses
   - Validate wallet addresses
   - Prevent SQL injection

4. **API Key Security**
   - Store in .env file (never commit)
   - Rotate keys monthly
   - Use read-only permissions where possible

---

## Future Enhancements

### Short-term (1-2 months)
- Add Telegram bot for instant alerts
- Add wallet portfolio tracking
- Add historical performance charts
- Add token comparison tool

### Medium-term (3-6 months)
- Machine learning for alpha score prediction
- Sentiment analysis on token social posts
- Integration with more DEXes (Raydium, Orca)
- Mobile app (React Native)

### Long-term (6-12 months)
- Multi-chain support (Base, Ethereum)
- Advanced backtesting framework
- Community voting on creators/wallets
- Premium subscription tier with advanced features

---

## Support & Maintenance

### Weekly Tasks
- Review top performing tokens
- Adjust alpha score weights
- Monitor service logs for errors
- Check database size + optimize

### Monthly Tasks
- Analyze creator/wallet accuracy
- Update filters based on market conditions
- Review and prune old data
- Update dependencies

### Quarterly Tasks
- Full system audit
- Performance benchmarking
- User feedback analysis
- Feature roadmap planning

---

## Contact & Resources

- **GitHub:** [VultVision Repository]
- **Documentation:** /docs folder
- **API Docs:** /api/docs (Swagger)
- **Support:** [Your contact]

---

*Last updated: January 2026*
*Version: 1.0.0*
