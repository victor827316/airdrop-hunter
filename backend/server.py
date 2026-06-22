"""
Airdrop Hunter — Backend Server
Chain discovery, interaction tracking, airdrop checking.
Run: python server.py
"""

import json, os, datetime, sqlite3, urllib.request, threading, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

HOME = os.path.expanduser("~")
DATA_DIR = os.path.join(HOME, ".airdrop_tracker")
DB_PATH = os.path.join(DATA_DIR, "airdrop_hunter.db")
CHAINS_URL = "https://api.llama.fi/chains"

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
    conn.commit()
    conn.close()

init_db()

# ----- Chain Discovery -----
def discover_chains():
    """Fetch chains from DefiLlama and return new candidates."""
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
    return {"candidates": candidates, "total_scanned": len(chains), "date": str(datetime.date.today())}

# ----- API Handlers -----
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        # Serve static files for PWA
        STATIC = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'frontend')
        if path == '/' or path == '':
            path = '/index.html'
        if path in ('/index.html', '/manifest.json', '/sw.js', '/icon-192.png', '/icon-512.png'):
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

        elif path == "/api/stats":
            conn = get_db()
            total = conn.execute("SELECT COUNT(*) FROM chains").fetchone()[0]
            eligible = conn.execute("SELECT COUNT(*) FROM chains WHERE airdrop_eligible=1").fetchone()[0]
            claimed = conn.execute("SELECT SUM(claimed_amount) FROM chains").fetchone()[0] or 0
            active = conn.execute("SELECT COUNT(*) FROM chains WHERE swaps > 0").fetchone()[0]
            conn.close()
            self.wfile.write(json.dumps({
                "total": total, "eligible": eligible,
                "claimed": round(claimed, 2), "active": active
            }).encode())

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

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

if __name__ == "__main__":
    port = 8899
    server = HTTPServer(("127.0.0.1", port), APIHandler)
    print(f"\n  Airdrop Hunter Server")
    print(f"  http://127.0.0.1:{port}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
