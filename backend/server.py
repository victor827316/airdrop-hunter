"""
Airdrop Hunter — Backend Server v2.0
Chain discovery, interaction tracking, airdrop checking,
token price monitoring, multi-source scanning.
Run: python server.py
"""

import json, os, datetime, sqlite3, urllib.request, threading, time, webbrowser
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
COINGECKO_SEARCH = "https://api.coingecko.com/api/v3/search?query="
COINGECKO_PRICE = "https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true"

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
    conn.commit()
    conn.close()

init_db()

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
