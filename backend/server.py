"""
Airdrop Hunter — Backend Server v2.0
Chain discovery, interaction tracking, airdrop checking,
token price monitoring, multi-source scanning.
Run: python server.py
"""

import json, os, datetime, sqlite3, urllib.request, threading, time, webbrowser, xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# PyInstaller bundle path
import sys
if getattr(sys, 'frozen', False):
    BUNDLE_DIR = sys._MEIPASS
else:
    BUNDLE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HOME = os.path.expanduser("~")
DATA_DIR = os.path.join(HOME, ".airdrop_tracker")
DB_PATH = os.path.join(DATA_DIR, "airdrop_hunter.db")

# Data sources
CHAINS_URL = "https://api.llama.fi/chains"
WALLET_ADDRESS = "0x6F3933270ECEAc56BB6ae6F9726fF6831e73997F"

# Multi-chain RPC endpoints for wallet scanning
RPC_ENDPOINTS = [
    {"chain":"Ethereum","rpc":"https://eth.llamarpc.com","explorer":"https://etherscan.io/address/{addr}","symbol":"ETH"},
    {"chain":"BNB Chain","rpc":"https://binance.llamarpc.com","explorer":"https://bscscan.com/address/{addr}","symbol":"BNB"},
    {"chain":"Arbitrum","rpc":"https://arb1.arbitrum.io/rpc","explorer":"https://arbiscan.io/address/{addr}","symbol":"ETH"},
    {"chain":"Optimism","rpc":"https://mainnet.optimism.io","explorer":"https://optimistic.etherscan.io/address/{addr}","symbol":"ETH"},
    {"chain":"Polygon","rpc":"https://polygon.llamarpc.com","explorer":"https://polygonscan.com/address/{addr}","symbol":"MATIC"},
    {"chain":"Base","rpc":"https://mainnet.base.org","explorer":"https://basescan.org/address/{addr}","symbol":"ETH"},
    {"chain":"Avalanche","rpc":"https://api.avax.network/ext/bc/C/rpc","explorer":"https://snowtrace.io/address/{addr}","symbol":"AVAX"},
    {"chain":"Fantom","rpc":"https://rpc.ftm.tools","explorer":"https://ftmscan.com/address/{addr}","symbol":"FTM"},
    {"chain":"Linea","rpc":"https://rpc.linea.build","explorer":"https://lineascan.build/address/{addr}","symbol":"ETH"},
    {"chain":"Scroll","rpc":"https://rpc.scroll.io","explorer":"https://scrollscan.com/address/{addr}","symbol":"ETH"},
]
LLAMA_CHAIN_DETAIL = "https://api.llama.fi/v2/chains"
COINGECKO_SEARCH = "https://api.coingecko.com/api/v3/search?query="
COINGECKO_PRICE = "https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true"

# Crypto news RSS feeds (free, no API key)
NEWS_FEEDS = [
    {"name":"CoinDesk","url":"https://www.coindesk.com/arc/outboundfeeds/news-rss/"},
    {"name":"Cointelegraph","url":"https://cointelegraph.com/rss"},
    {"name":"Decrypt","url":"https://decrypt.co/feed"},
    {"name":"The Block","url":"https://www.theblock.co/rss.xml"},
    {"name":"Bitcoin.com","url":"https://news.bitcoin.com/feed/"},
]
NEWS_CACHE = {"items": [], "updated": 0}

# Active testnets — strong airdrop signals
TESTNETS = [
    {"name":"Monad Testnet","chain":"monad","type":"L1","status":"active","tasks":"faucet+swap+deploy","difficulty":"medium","potential":"S"},
    {"name":"Berachain Artio","chain":"berachain","type":"L1","status":"active","tasks":"faucet+swap+lp","difficulty":"easy","potential":"S"},
    {"name":"Scroll Alpha","chain":"scroll","type":"L2","status":"active","tasks":"bridge+swap+deploy","difficulty":"easy","potential":"A"},
    {"name":"Linea Testnet","chain":"linea","type":"L2","status":"active","tasks":"bridge+swap+nft","difficulty":"easy","potential":"A"},
    {"name":"Blast Testnet","chain":"blast","type":"L2","status":"active","tasks":"bridge+swap+lp","difficulty":"medium","potential":"A"},
    {"name":"Mode Testnet","chain":"mode","type":"L2","status":"active","tasks":"bridge+swap+deploy","difficulty":"easy","potential":"B"},
    {"name":"Taiko Testnet","chain":"taiko","type":"L2","status":"active","tasks":"bridge+swap","difficulty":"easy","potential":"B"},
    {"name":"Polygon zkEVM","chain":"polygon-zkevm","type":"L2","status":"active","tasks":"bridge+swap+nft","difficulty":"easy","potential":"A"},
    {"name":"Manta Pacific","chain":"manta","type":"L2","status":"active","tasks":"bridge+swap+lp","difficulty":"easy","potential":"A"},
    {"name":"Zora Network","chain":"zora","type":"L2","status":"active","tasks":"bridge+nft+deploy","difficulty":"easy","potential":"B"},
    {"name":"Mantle Testnet","chain":"mantle","type":"L2","status":"active","tasks":"bridge+swap","difficulty":"easy","potential":"B"},
    {"name":"Starknet Testnet","chain":"starknet","type":"L2","status":"active","tasks":"bridge+swap+deploy","difficulty":"medium","potential":"A"},
    {"name":"Shardeum Testnet","chain":"shardeum","type":"L1","status":"active","tasks":"faucet+swap+deploy","difficulty":"medium","potential":"A"},
    {"name":"Sei Testnet","chain":"sei","type":"L1","status":"ended","tasks":"faucet+swap+lp","difficulty":"easy","potential":"C"},
    {"name":"Sui Testnet","chain":"sui","type":"L1","status":"ended","tasks":"faucet+swap+nft","difficulty":"easy","potential":"C"},
]

os.makedirs(DATA_DIR, exist_ok=True)

# ----- Database -----
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            chain_id TEXT,
            tvl REAL DEFAULT 0,
            token TEXT DEFAULT '',
            rpc_url TEXT DEFAULT '',
            explorer TEXT DEFAULT '',
            wallet TEXT DEFAULT '',
            status TEXT DEFAULT 'new',
            swaps INTEGER DEFAULT 0,
            lp_added INTEGER DEFAULT 0,
            bridges INTEGER DEFAULT 0,
            nfts INTEGER DEFAULT 0,
            contracts INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            date_added TEXT DEFAULT (date('now')),
            date_updated TEXT DEFAULT (date('now')),
            tge_status TEXT DEFAULT 'unknown',
            airdrop_eligible INTEGER DEFAULT -1,
            claimed_amount REAL DEFAULT 0,
            claimed_date TEXT DEFAULT ''
        )
    """)
    # Token prices cache
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL UNIQUE,
            price_usd REAL DEFAULT 0,
            change_24h REAL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # Airdrop events
    conn.execute("""
        CREATE TABLE IF NOT EXISTS airdrop_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_name TEXT NOT NULL,
            token TEXT DEFAULT '',
            title TEXT DEFAULT '',
            status TEXT DEFAULT 'upcoming',
            estimated_date TEXT DEFAULT '',
            value_usd REAL DEFAULT 0,
            source TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            date_added TEXT DEFAULT (date('now'))
        )
    """)
    # Interaction log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            tx_hash TEXT DEFAULT '',
            amount_usd REAL DEFAULT 0,
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # Interaction templates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            actions TEXT DEFAULT '[]',
            frequency TEXT DEFAULT 'weekly',
            active INTEGER DEFAULT 1
        )
    """)
    # ROI tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_id INTEGER,
            cost_type TEXT DEFAULT 'gas',
            amount_usd REAL DEFAULT 0,
            chain_name TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (date('now'))
        )
    """)
    # Seed default templates if empty
    count = conn.execute("SELECT COUNT(*) FROM templates").fetchone()[0]
    if count == 0:
        defaults = [
            ("标准空投三件套", "Swap + LP + Bridge — 覆盖大多数空投要求", '["swap","lp_add","bridge"]', "weekly"),
            ("Swap交互流", "在各DEX执行代币兑换", '["swap","swap","swap"]', "daily"),
            ("LP挖矿流", "添加流动性获得收益+空投", '["lp_add","lp_add"]', "weekly"),
            ("跨链桥接流", "桥接资产覆盖多链", '["bridge","bridge"]', "weekly"),
            ("NFT铸造流", "铸造NFT获取生态空投", '["nft","nft"]', "weekly"),
            ("合约交互流", "部署/调用合约证明活跃", '["contract","contract"]', "monthly"),
        ]
        conn.executemany("INSERT INTO templates (name, description, actions, frequency) VALUES (?,?,?,?)", defaults)
    conn.commit()
    conn.close()

init_db()

# Seed known airdrop opportunities
def _seed_airdrop_intel():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM airdrop_events").fetchone()[0]
    if count == 0:
        events = [
            ("Scroll","SCR","Scroll 主网空投","upcoming","","500", "community","L2 生态，交互桥接+DEX 可获资格"),
            ("Linea","LINEA","Linea 空投","upcoming","","300", "community","Consensys 支持，桥接+Swap+NFT"),
            ("Base","","Base 生态激励","live","","200", "official","Coinbase L2，持续生态激励"),
            ("zkSync","ZK","zkSync 空投","ended","","800", "community","已完成，可参考交互模式"),
            ("Starknet","STRK","Starknet 空投","ended","","600", "community","已完成，参考交互路径"),
            ("LayerZero","ZRO","LayerZero 空投","ended","","1000", "community","跨链协议，已完成"),
            ("Arbitrum","ARB","Arbitrum 空投","ended","","1200", "community","已完成，经典案例"),
            ("Aptos","APT","Aptos 空投","ended","","900", "community","已完成，Move 生态"),
        ]
        conn.executemany(
            "INSERT INTO airdrop_events (chain_name,token,title,status,estimated_date,value_usd,source,notes) VALUES (?,?,?,?,?,?,?,?)",
            events
        )
        conn.commit()
    conn.close()

_seed_airdrop_intel()


# ----- Price Cache -----
_price_cache = {}
_price_lock = threading.Lock()
_last_price_update = 0

def _update_prices():
    """Fetch prices for tracked tokens from CoinGecko."""
    global _last_price_update
    now = time.time()
    if now - _last_price_update < 300:  # Cache 5 min
        return

    conn = get_db()
    tokens = [r[0] for r in conn.execute("SELECT DISTINCT token FROM chains WHERE token != ''").fetchall()]
    conn.close()

    if not tokens:
        return

    try:
        # Search for CoinGecko IDs
        ids = []
        for tok in tokens[:20]:  # Batch limit
            try:
                req = urllib.request.Request(COINGECKO_SEARCH + tok, headers={"User-Agent": "Mozilla/5.0"})
                data = json.loads(urllib.request.urlopen(req, timeout=10).read())
                coins = data.get("coins", [])
                if coins:
                    ids.append(coins[0]["id"])
            except:
                pass

        if not ids:
            return

        id_str = ",".join(ids[:10])
        req = urllib.request.Request(
            COINGECKO_PRICE.format(ids=id_str),
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = json.loads(urllib.request.urlopen(req, timeout=10).read())

        conn = get_db()
        for cg_id, info in data.items():
            price = info.get("usd", 0)
            change = info.get("usd_24h_change", 0)
            # Find matching token
            for tok in tokens:
                if tok.lower() in cg_id.lower() or cg_id.lower().startswith(tok.lower()):
                    conn.execute("""
                        INSERT INTO prices (token, price_usd, change_24h, updated_at)
                        VALUES (?, ?, ?, datetime('now'))
                        ON CONFLICT(token) DO UPDATE SET
                            price_usd=excluded.price_usd,
                            change_24h=excluded.change_24h,
                            updated_at=excluded.updated_at
                    """, (tok, price, change or 0))
                    with _price_lock:
                        _price_cache[tok] = {"price_usd": price, "change_24h": change or 0}
                    break
        conn.commit()
        conn.close()
        _last_price_update = now
    except Exception as e:
        pass  # Silent fail - prices are best-effort



# ----- Gas Tracker -----
def get_gas_prices():
    """Fetch gas prices from multiple sources with fallback."""
    # Try Etherscan
    try:
        req = urllib.request.Request(
            "https://api.etherscan.io/api?module=gastracker&action=gasoracle",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = json.loads(urllib.request.urlopen(req, timeout=6).read())
        if data.get("status") == "1":
            r = data["result"]
            return {
                "source": "Etherscan",
                "safe_gwei": int(r.get("SafeGasPrice", 15)),
                "standard_gwei": int(r.get("ProposeGasPrice", 20)),
                "fast_gwei": int(r.get("FastGasPrice", 30)),
                "base_fee_gwei": float(r.get("suggestBaseFee", 10)),
                "recommendation": "safe" if int(r.get("SafeGasPrice",15)) < 12 else "standard" if int(r.get("ProposeGasPrice",20)) < 20 else "wait",
                "optimal_time": "平日 UTC 14:00-22:00 (亚洲凌晨)",
                "estimated_cost_swap": round(int(r.get("ProposeGasPrice",20)) * 200000 / 1e9, 4),
                "estimated_cost_bridge": round(int(r.get("ProposeGasPrice",20)) * 400000 / 1e9, 4),
            }
    except: pass

    # Try BscScan for BSC gas
    try:
        req = urllib.request.Request(
            "https://api.bscscan.com/api?module=gastracker&action=gasoracle",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = json.loads(urllib.request.urlopen(req, timeout=6).read())
        if data.get("status") == "1":
            r = data["result"]
            return {
                "source": "BscScan",
                "safe_gwei": int(r.get("SafeGasPrice", 3)),
                "standard_gwei": int(r.get("ProposeGasPrice", 5)),
                "fast_gwei": int(r.get("FastGasPrice", 10)),
                "base_fee_gwei": float(r.get("suggestBaseFee", 1)),
                "recommendation": "safe",
                "optimal_time": "BSC Gas 常年便宜",
                "estimated_cost_swap": round(int(r.get("ProposeGasPrice",5)) * 200000 / 1e9, 6),
                "estimated_cost_bridge": round(int(r.get("ProposeGasPrice",5)) * 400000 / 1e9, 6),
            }
    except: pass

    # Smart fallback with time-based heuristics
    hour = datetime.datetime.now().hour
    if hour >= 14 and hour <= 22:
        base = 15; rec = "standard"
    elif hour >= 2 and hour <= 8:
        base = 8; rec = "safe"
    else:
        base = 12; rec = "standard"

    return {
        "source": "heuristic",
        "safe_gwei": max(base-5, 3),
        "standard_gwei": base,
        "fast_gwei": base + 10,
        "base_fee_gwei": max(base-7, 2),
        "recommendation": rec,
        "optimal_time": "周末 10:00-14:00 UTC (亚洲傍晚)",
        "estimated_cost_swap": round(base * 200000 / 1e9, 4),
        "estimated_cost_bridge": round(base * 400000 / 1e9, 4),
    }

# ----- News Aggregator -----
def fetch_news():
    """Fetch crypto news from RSS feeds with caching (15 min)."""
    global NEWS_CACHE
    now = time.time()
    if now - NEWS_CACHE["updated"] < 900 and NEWS_CACHE["items"]:
        return {"items": NEWS_CACHE["items"], "cached": True}

    items = []
    for feed in NEWS_FEEDS:
        try:
            req = urllib.request.Request(feed["url"], headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            tree = ET.parse(resp)
            root = tree.getroot()
            
            # RSS 2.0 format
            for item_elem in root.iter("item"):
                title = ""
                link = ""
                desc = ""
                pubdate = ""
                for child in item_elem:
                    if child.tag == "title": title = (child.text or "")[:200]
                    elif child.tag == "link": link = child.text or ""
                    elif child.tag == "description": desc = (child.text or "")[:300]
                    elif child.tag == "pubDate": pubdate = child.text or ""
                if title:
                    items.append({
                        "title": title,
                        "link": link,
                        "description": desc,
                        "date": pubdate,
                        "source": feed["name"],
                    })
            # Atom format fallback
            for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                title = ""
                link = ""
                summary = ""
                updated = ""
                for child in entry:
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if tag == "title": title = (child.text or "")[:200]
                    elif tag == "link" and child.get("href"): link = child.get("href", "")
                    elif tag == "summary": summary = (child.text or "")[:300]
                    elif tag == "updated": updated = child.text or ""
                if title and not any(i["title"] == title for i in items):
                    items.append({
                        "title": title, "link": link,
                        "description": summary, "date": updated,
                        "source": feed["name"],
                    })
        except Exception as e:
            items.append({
                "title": f"Feed unavailable: {feed['name']}",
                "link": "", "description": str(e)[:100],
                "date": "", "source": feed["name"],
            })

    items.sort(key=lambda x: x["date"], reverse=True)
    NEWS_CACHE = {"items": items[:50], "updated": now}
    return {"items": items[:50], "cached": False}

# ----- Wallet Scanner -----
def scan_wallet():
    """Scan wallet balance and tx count across multiple chains via JSON-RPC."""
    results = []
    for chain in RPC_ENDPOINTS:
        try:
            # Get balance
            payload = json.dumps({
                "jsonrpc":"2.0","method":"eth_getBalance",
                "params":[WALLET_ADDRESS, "latest"],"id":1
            }).encode()
            req = urllib.request.Request(chain["rpc"], data=payload,
                headers={"Content-Type":"application/json"})
            resp = json.loads(urllib.request.urlopen(req, timeout=8).read())
            balance_wei = int(resp.get("result","0x0"), 16)
            balance = balance_wei / 1e18

            # Get tx count
            payload2 = json.dumps({
                "jsonrpc":"2.0","method":"eth_getTransactionCount",
                "params":[WALLET_ADDRESS, "latest"],"id":2
            }).encode()
            req2 = urllib.request.Request(chain["rpc"], data=payload2,
                headers={"Content-Type":"application/json"})
            resp2 = json.loads(urllib.request.urlopen(req2, timeout=8).read())
            tx_count = int(resp2.get("result","0x0"), 16)

            results.append({
                "chain": chain["chain"],
                "symbol": chain["symbol"],
                "balance": round(balance, 6),
                "tx_count": tx_count,
                "explorer": chain["explorer"].replace("{addr}", WALLET_ADDRESS),
                "active": tx_count > 0,
            })
        except Exception as e:
            results.append({
                "chain": chain["chain"],
                "symbol": chain["symbol"],
                "balance": None,
                "tx_count": None,
                "error": str(e)[:100],
                "active": False,
            })

    # Count active chains
    active = sum(1 for r in results if r.get("active"))
    total_balance = sum(r["balance"] for r in results if r["balance"])
    total_tx = sum(r["tx_count"] for r in results if r["tx_count"])

    return {
        "address": WALLET_ADDRESS,
        "scanned": len(results),
        "active_chains": active,
        "total_tx": total_tx,
        "total_balance_eth_equiv": round(total_balance, 4),
        "chains": results,
        "scanned_at": datetime.datetime.now().isoformat(),
    }

# ----- Chain Discovery -----
def discover_chains():
    """Fetch chains from DefiLlama."""
    try:
        req = urllib.request.Request(CHAINS_URL, headers={"User-Agent": "Mozilla/5.0"})
        chains = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        return {"error": str(e), "candidates": []}

    ESTABLISHED = {
        "Ethereum","Bitcoin","Solana","BNB","Avalanche","Polygon",
        "Arbitrum","Optimism","Base","Sui","Aptos","Near","Fantom",
        "Cosmos","Osmosis","Injective","Sei","Tron","Cardano",
        "Polkadot","Kusama","Cronos","Celo","Gnosis","Moonbeam",
        "Kava","Flare","Algorand","Hedera","MultiversX","Tezos",
        "Klaytn","Zilliqa","Evmos","Stargaze","Juno","Secret","Axelar",
    }

    conn = get_db()
    existing = set(r[0] for r in conn.execute("SELECT name FROM chains").fetchall())
    conn.close()

    candidates = []
    for c in chains:
        name = c.get("name", "")
        tvl = c.get("tvl", 0)
        if tvl > 50_000_000 or name in ESTABLISHED or name in existing:
            continue
        candidates.append({
            "name": name,
            "tvl": tvl,
            "chain_id": str(c.get("chainId", "") or ""),
            "token": c.get("tokenSymbol", ""),
            "gecko_id": c.get("gecko_id", ""),
        })

    candidates.sort(key=lambda x: x["tvl"])
    return {"candidates": candidates, "total_scanned": len(chains), "source": "DefiLlama", "date": str(datetime.date.today())}

def score_chain(name, tvl, token_symbol, protocol_count=0, mcap=0, tvl_change_7d=0):
    """Score a chain 0-100 based on airdrop hunting potential."""
    score = 0
    details = []

    # 1. TVL Fit (25 pts) — sweet spot $100K-$50M
    if tvl < 100_000:
        score += 5; details.append("TVL极低(5)")
    elif tvl < 1_000_000:
        score += 15; details.append("TVL早期(15)")
    elif tvl < 10_000_000:
        score += 25; details.append("TVL适中(25)")
    elif tvl < 50_000_000:
        score += 20; details.append("TVL较高(20)")
    else:
        score += 10; details.append("TVL过高(10)")

    # 2. Protocol Count (15 pts) — activity indicator
    if protocol_count == 0:
        score += 3; details.append("协议数未知(3)")
    elif protocol_count < 5:
        score += 10; details.append("协议少易抢跑(10)")
    elif protocol_count < 20:
        score += 15; details.append("生态初成(15)")
    elif protocol_count < 50:
        score += 12; details.append("生态活跃(12)")
    else:
        score += 8; details.append("生态成熟(8)")

    # 3. Growth (20 pts) — momentum
    if tvl_change_7d > 50:
        score += 20; details.append("7日暴涨(20)")
    elif tvl_change_7d > 20:
        score += 15; details.append("7日高增长(15)")
    elif tvl_change_7d > 5:
        score += 10; details.append("7日正增长(10)")
    elif tvl_change_7d > -5:
        score += 5; details.append("7日持平(5)")
    else:
        score += 2; details.append("7日下降(2)")

    # 4. Token Status (15 pts) — no token = potential airdrop
    if not token_symbol:
        score += 15; details.append("未发币(15)")
    elif mcap == 0:
        score += 12; details.append("低市值(12)")
    elif mcap < 10_000_000:
        score += 10; details.append("小额MC(10)")
    elif mcap < 100_000_000:
        score += 6; details.append("中额MC(6)")
    else:
        score += 2; details.append("大额MC(2)")

    # 5. Difficulty (15 pts) — how easy to interact
    if protocol_count < 5:
        score += 15; details.append("极简交互(15)")
    elif protocol_count < 15:
        score += 12; details.append("简单交互(12)")
    elif protocol_count < 30:
        score += 8; details.append("中等交互(8)")
    else:
        score += 4; details.append("复杂交互(4)")

    # 6. Bonus: name recognition (10 pts)
    HOT_KEYWORDS = ["rollup","zk","layer","l2","defi","perp","dex","lending","nft","game","ai","rwa","restake"]
    name_lower = name.lower()
    bonus = sum(2 for kw in HOT_KEYWORDS if kw in name_lower)
    bonus = min(bonus, 10)
    score += bonus
    if bonus > 0:
        details.append(f"热门标签({bonus})")

    # Rating
    if score >= 75:
        rating = "S"
    elif score >= 60:
        rating = "A"
    elif score >= 45:
        rating = "B"
    elif score >= 30:
        rating = "C"
    else:
        rating = "D"

    return {
        "score": min(score, 100),
        "rating": rating,
        "details": details,
    }

def discover_scored():
    """Fetch chains from DefiLlama with detailed scoring."""
    try:
        req = urllib.request.Request(CHAINS_URL, headers={"User-Agent": "Mozilla/5.0"})
        chains = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        return {"error": str(e), "candidates": []}

    # Try to get detailed chain data
    detail_map = {}
    try:
        dreq = urllib.request.Request(LLAMA_CHAIN_DETAIL, headers={"User-Agent": "Mozilla/5.0"})
        details = json.loads(urllib.request.urlopen(dreq, timeout=15).read())
        for d in details:
            detail_map[d.get("name","").lower()] = {
                "protocols": d.get("protocols", 0),
                "mcap": d.get("mcap", 0),
                "change_7d": d.get("change_7d", 0),
            }
    except:
        pass

    ESTABLISHED = {
        "Ethereum","Bitcoin","Solana","BNB","Avalanche","Polygon",
        "Arbitrum","Optimism","Base","Sui","Aptos","Near","Fantom",
        "Cosmos","Osmosis","Injective","Sei","Tron","Cardano",
        "Polkadot","Kusama","Cronos","Celo","Gnosis","Moonbeam",
        "Kava","Flare","Algorand","Hedera","MultiversX","Tezos",
        "Klaytn","Zilliqa","Evmos","Stargaze","Juno","Secret","Axelar",
    }

    conn = get_db()
    existing = set(r[0] for r in conn.execute("SELECT name FROM chains").fetchall())
    conn.close()

    candidates = []
    for c in chains:
        name = c.get("name", "")
        tvl = c.get("tvl", 0)
        if tvl > 50_000_000 or name in ESTABLISHED or name in existing:
            continue

        token = c.get("tokenSymbol", "")
        chain_info = detail_map.get(name.lower(), {})
        scoring = score_chain(
            name=name,
            tvl=tvl,
            token_symbol=token,
            protocol_count=chain_info.get("protocols", 0),
            mcap=chain_info.get("mcap", 0),
            tvl_change_7d=chain_info.get("change_7d", 0),
        )

        candidates.append({
            "name": name,
            "tvl": tvl,
            "chain_id": str(c.get("chainId", "") or ""),
            "token": token,
            "gecko_id": c.get("gecko_id", ""),
            "protocols": chain_info.get("protocols", 0),
            "mcap": chain_info.get("mcap", 0),
            "change_7d": chain_info.get("change_7d", 0),
            **scoring,
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)

    # Count by rating
    rating_counts = {"S":0,"A":0,"B":0,"C":0,"D":0}
    for c in candidates:
        rating_counts[c["rating"]] += 1

    return {
        "candidates": candidates,
        "total_scanned": len(chains),
        "source": "DefiLlama",
        "date": str(datetime.date.today()),
        "rating_counts": rating_counts,
    }

def discover_hot_airdrops():
    """Return tracked chains that might have upcoming airdrops."""
    conn = get_db()
    # Chains with no token yet (TGE pending) are potential airdrop targets
    rows = conn.execute("""
        SELECT * FROM chains
        WHERE tge_status IN ('unknown', 'pending')
        AND status IN ('new', 'active')
        ORDER BY tvl DESC
        LIMIT 20
    """).fetchall()
    conn.close()
    
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "name": r["name"],
            "token": r["token"],
            "tvl": r["tvl"],
            "tge_status": r["tge_status"],
            "interactions": r["swaps"] + r["lp_added"] + r["bridges"] + r["nfts"] + r["contracts"],
            "potential": "high" if r["tvl"] > 10_000_000 else "medium" if r["tvl"] > 1_000_000 else "low",
        })
    return result

# ----- API Handlers -----
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        # Serve static files
        STATIC = os.path.join(BUNDLE_DIR, 'frontend')
        if path == '/' or path == '':
            path = '/index.html'
        static_files = ['/index.html', '/manifest.json', '/sw.js', '/icon-192.png', '/icon-512.png']
        if path in static_files:
            file_path = os.path.join(STATIC, path.lstrip('/'))
            if os.path.exists(file_path):
                ct = 'text/html' if path.endswith('.html') else 'application/json' if path.endswith('.json') else 'application/javascript' if path.endswith('.js') else 'image/png'
                self.send_response(200)
                self.send_header('Content-Type', ct)
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
                return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if path == "/api/chains":
            conn = get_db()
            rows = conn.execute("SELECT * FROM chains ORDER BY date_added DESC").fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/discover":
            result = discover_chains()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/discover/scored":
            result = discover_scored()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/airdrops":
            result = discover_hot_airdrops()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/airdrops/events":
            conn = get_db()
            rows = conn.execute("SELECT * FROM airdrop_events ORDER BY date_added DESC").fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/prices":
            _update_prices()
            conn = get_db()
            rows = conn.execute("SELECT * FROM prices ORDER BY updated_at DESC").fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/stats":
            conn = get_db()
            total = conn.execute("SELECT COUNT(*) FROM chains").fetchone()[0]
            eligible = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=1").fetchone()[0]
            claimed = conn.execute("SELECT SUM(claimed_amount) FROM chains").fetchone()[0] or 0
            active = conn.execute("SELECT COUNT(*) FROM chains WHERE swaps > 0").fetchone()[0]
            pending_airdrop = conn.execute("SELECT COUNT(*) FROM chains WHERE tge_status IN ('pending','unknown') AND status IN ('new','active')").fetchone()[0]
            conn.close()
            self.wfile.write(json.dumps({
                "total": total, "eligible": eligible,
                "claimed": round(claimed, 2), "active": active,
                "pending_airdrop": pending_airdrop
            }).encode())

        elif path == "/api/summary":
            """Dashboard summary — chains, prices, airdrops at a glance."""
            conn = get_db()
            total = conn.execute("SELECT COUNT(*) FROM chains").fetchone()[0]
            active = conn.execute("SELECT COUNT(*) FROM chains WHERE swaps > 0").fetchone()[0]
            eligible = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=1").fetchone()[0]
            claimed = conn.execute("SELECT SUM(claimed_amount) FROM chains").fetchone()[0] or 0
            pending = conn.execute("SELECT COUNT(*) FROM chains WHERE tge_status IN ('pending','unknown') AND status IN ('new','active')").fetchone()[0]
            
            # Recent chains
            recent = conn.execute("SELECT name, token, tvl, tge_status, date_added FROM chains ORDER BY date_added DESC LIMIT 5").fetchall()
            
            # Top TVL chains
            top_tvl = conn.execute("SELECT name, token, tvl, tge_status FROM chains WHERE tvl > 0 ORDER BY tvl DESC LIMIT 5").fetchall()
            
            # Price data
            prices = conn.execute("SELECT token, price_usd, change_24h FROM prices ORDER BY updated_at DESC").fetchall()
            conn.close()
            
            self.wfile.write(json.dumps({
                "stats": {"total": total, "active": active, "eligible": eligible, "claimed": round(claimed, 2), "pending_airdrop": pending},
                "recent": [dict(r) for r in recent],
                "top_tvl": [dict(r) for r in top_tvl],
                "prices": [dict(r) for r in prices],
            }, ensure_ascii=False).encode())

        elif path == "/api/export":
            conn = get_db()
            rows = conn.execute("SELECT * FROM chains ORDER BY date_added DESC").fetchall()
            conn.close()
            csv_lines = ["name,chain_id,token,wallet,status,swaps,lp_added,bridges,nfts,contracts,tge_status,airdrop_eligible,claimed_amount,date_added"]
            for r in rows:
                csv_lines.append(f'{r["name"]},{r["chain_id"]},{r["token"]},{r["wallet"]},{r["status"]},{r["swaps"]},{r["lp_added"]},{r["bridges"]},{r["nfts"]},{r["contracts"]},{r["tge_status"]},{r["airdrop_eligible"]},{r["claimed_amount"]},{r["date_added"]}')
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", "attachment; filename=airdrop_ledger.csv")
            self.end_headers()
            self.wfile.write("\n".join(csv_lines).encode())
            return

        elif path == "/api/roi":
            conn = get_db()
            total_cost = conn.execute("SELECT SUM(amount_usd) FROM costs").fetchone()[0] or 0
            total_claimed = conn.execute("SELECT SUM(claimed_amount) FROM chains").fetchone()[0] or 0
            cost_rows = conn.execute("SELECT * FROM costs ORDER BY created_at DESC LIMIT 50").fetchall()
            claimed_rows = conn.execute("SELECT name, claimed_amount, claimed_date FROM chains WHERE claimed_amount > 0 ORDER BY claimed_amount DESC").fetchall()
            conn.close()
            self.wfile.write(json.dumps({
                "total_cost": round(total_cost, 2),
                "total_claimed": round(total_claimed, 2),
                "net_profit": round(total_claimed - total_cost, 2),
                "roi_pct": round(((total_claimed - total_cost) / total_cost * 100) if total_cost > 0 else 0, 1),
                "costs": [dict(r) for r in cost_rows],
                "claims": [dict(r) for r in claimed_rows],
            }, ensure_ascii=False).encode())

        elif path == "/api/news":
            result = fetch_news()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/gas":
            result = get_gas_prices()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/wallet/scan":
            result = scan_wallet()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())

        elif path == "/api/testnets":
            self.wfile.write(json.dumps(TESTNETS, ensure_ascii=False).encode())

        elif path == "/api/templates":
            conn = get_db()
            rows = conn.execute("SELECT * FROM templates ORDER BY id").fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/interactions":
            conn = get_db()
            rows = conn.execute("""
                SELECT i.*, c.name as chain_name FROM interactions i
                LEFT JOIN chains c ON i.chain_id = c.id
                ORDER BY i.created_at DESC LIMIT 100
            """).fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/interactions/calendar":
            conn = get_db()
            rows = conn.execute("""
                SELECT date(created_at) as day, action_type, COUNT(*) as count,
                GROUP_CONCAT(DISTINCT c.name) as chains
                FROM interactions i
                LEFT JOIN chains c ON i.chain_id = c.id
                WHERE created_at >= date('now', '-30 days')
                GROUP BY date(created_at), action_type
                ORDER BY day DESC LIMIT 365
            """).fetchall()
            conn.close()
            self.wfile.write(json.dumps([dict(r) for r in rows], ensure_ascii=False).encode())

        elif path == "/api/eligibility/overview":
            conn = get_db()
            total = conn.execute("SELECT COUNT(*) FROM chains").fetchone()[0]
            eligible = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=1").fetchone()[0]
            not_eligible = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=0").fetchone()[0]
            unchecked = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=-1").fetchone()[0]
            claimed = conn.execute("SELECT SUM(claimed_amount) FROM chains").fetchone()[0] or 0
            low = conn.execute("""
                SELECT name, token, tge_status, (swaps+lp_added+bridges+nfts+contracts) as total_actions
                FROM chains WHERE status != 'done' AND tge_status IN ('unknown','pending')
                ORDER BY total_actions ASC LIMIT 10
            """).fetchall()
            conn.close()
            self.wfile.write(json.dumps({
                "total": total, "eligible": eligible, "not_eligible": not_eligible,
                "unchecked": unchecked, "claimed": round(claimed, 2),
                "needs_action": [dict(r) for r in low],
            }, ensure_ascii=False).encode())

        else:
            self.send_response(404)
            self.wfile.write(b'{"error":"not found"}')

    def do_POST(self):
        path = urlparse(self.path).path
        content_len = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_len)) if content_len else {}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if path == "/api/chains":
            conn = get_db()
            conn.execute("""
                INSERT INTO chains (name, chain_id, token, rpc_url, explorer, wallet, status, notes, date_added, date_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, date('now'), date('now'))
            """, (body.get("name",""), body.get("chain_id",""), body.get("token",""),
                  body.get("rpc_url",""), body.get("explorer",""), body.get("wallet",""),
                  body.get("status","tracking"), body.get("notes","")))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/chains/update":
            conn = get_db()
            conn.execute("""
                UPDATE chains SET
                    swaps=?, lp_added=?, bridges=?, nfts=?, contracts=?,
                    status=?, wallet=?, tge_status=?, airdrop_eligible=?,
                    claimed_amount=?, notes=?, date_updated=date('now')
                WHERE id=?
            """, (body.get("swaps",0), body.get("lp_added",0), body.get("bridges",0),
                  body.get("nfts",0), body.get("contracts",0), body.get("status","tracking"),
                  body.get("wallet",""), body.get("tge_status","unknown"),
                  body.get("airdrop_eligible",-1), body.get("claimed_amount",0),
                  body.get("notes",""), body.get("id")))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/chains/delete":
            conn = get_db()
            conn.execute("DELETE FROM chains WHERE id=?", (body.get("id"),))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/chains/mark-done":
            conn = get_db()
            conn.execute("UPDATE chains SET status='done', date_updated=date('now') WHERE id=?",
                         (body.get("id"),))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/batch-add":
            added = 0
            conn = get_db()
            for c in body.get("chains", []):
                try:
                    conn.execute("""
                        INSERT INTO chains (name, chain_id, token, date_added, date_updated)
                        VALUES (?, ?, ?, date('now'), date('now'))
                    """, (c["name"], c.get("chain_id",""), c.get("token","")))
                    added += 1
                except:
                    pass
            conn.commit()
            conn.close()
            self.wfile.write(json.dumps({"added": added}).encode())

        elif path == "/api/airdrops/events":
            conn = get_db()
            conn.execute("""
                INSERT INTO airdrop_events (chain_name, token, title, status, estimated_date, value_usd, source, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (body.get("chain_name",""), body.get("token",""), body.get("title",""),
                  body.get("status","upcoming"), body.get("estimated_date",""),
                  body.get("value_usd",0), body.get("source",""), body.get("notes","")))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/prices/refresh":
            global _last_price_update
            _last_price_update = 0
            _update_prices()
            self.wfile.write(b'{"ok":true}')


        elif path == "/api/interactions":
            conn = get_db()
            conn.execute("""
                INSERT INTO interactions (chain_id, action_type, tx_hash, amount_usd, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (body.get("chain_id"), body.get("action_type",""),
                  body.get("tx_hash",""), body.get("amount_usd",0), body.get("notes","")))
            action = body.get("action_type", "")
            field_map = {"swap": "swaps", "lp_add": "lp_added", "bridge": "bridges", "nft": "nfts", "contract": "contracts"}
            if action in field_map:
                conn.execute(f"UPDATE chains SET {field_map[action]} = {field_map[action]} + 1, status='active', date_updated=date('now') WHERE id=?", (body.get("chain_id"),))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

        elif path == "/api/chains/quick-action":
            chain_id = body.get("chain_id")
            if not chain_id:
                self.wfile.write(b'{"error":"chain_id required"}')
                return
            conn = get_db()
            actions = body.get("actions", ["swap"])
            notes = body.get("notes", "")
            field_updates = {"swaps": 0, "lp_added": 0, "bridges": 0, "nfts": 0, "contracts": 0}
            for act in actions:
                conn.execute("INSERT INTO interactions (chain_id, action_type, notes) VALUES (?,?,?)", (chain_id, act, notes))
                if act == "swap": field_updates["swaps"] += 1
                elif act == "lp_add": field_updates["lp_added"] += 1
                elif act == "bridge": field_updates["bridges"] += 1
                elif act == "nft": field_updates["nfts"] += 1
                elif act == "contract": field_updates["contracts"] += 1
            conn.execute("""
                UPDATE chains SET swaps=swaps+?, lp_added=lp_added+?, bridges=bridges+?,
                nfts=nfts+?, contracts=contracts+?, status='active', date_updated=date('now')
                WHERE id=?
            """, (field_updates["swaps"], field_updates["lp_added"], field_updates["bridges"],
                  field_updates["nfts"], field_updates["contracts"], chain_id))
            conn.commit()
            conn.close()
            self.wfile.write(json.dumps({"ok": True, "actions": len(actions)}).encode())


        elif path == "/api/costs":
            conn = get_db()
            conn.execute("INSERT INTO costs (chain_id, cost_type, amount_usd, chain_name, notes) VALUES (?,?,?,?,?)",
                (body.get("chain_id"), body.get("cost_type","gas"), body.get("amount_usd",0),
                 body.get("chain_name",""), body.get("notes","")))
            conn.commit()
            conn.close()
            self.wfile.write(b'{"ok":true}')

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

if __name__ == "__main__":
    port = 8899
    url = f"http://127.0.0.1:{port}"

    print(f"""
  ╔══════════════════════════════════════╗
  ║       🪂  Airdrop Hunter v2.0       ║
  ║   Chain Discovery & Airdrop Tracker ║
  ╠══════════════════════════════════════╣
  ║  Local:  {url}       ║
  ║  Phone:  http://<PC_IP>:{port}        ║
  ╚══════════════════════════════════════╝
""")
    
    # Auto-open browser
    try:
        webbrowser.open(url)
        print("  >>> Browser opened. If not, go to the URL above.\n")
    except:
        print("  >>> Open the URL above in your browser.\n")

    server = HTTPServer(("0.0.0.0", port), APIHandler)
    print(f"  Server listening on 0.0.0.0:{port} (LAN accessible)\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        server.shutdown()
