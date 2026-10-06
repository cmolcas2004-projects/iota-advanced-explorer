import json, os, sqlite3, threading, time
from datetime import datetime, timezone
from typing import Optional
import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

HORNET = os.environ.get("HORNET_URL", "http://localhost:14265")
DB = os.environ.get("DB_PATH", "traceability.db")
MAX_ATTEMPTS = 10

app = FastAPI(title="Traceability API")


def db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


with db() as c:
    c.execute("""CREATE TABLE IF NOT EXISTS messages(
        block_id TEXT PRIMARY KEY, tag TEXT, content TEXT,
        sent_at TEXT, received_at TEXT,
        is_solid INTEGER DEFAULT 0, content_valid INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending', detail TEXT,
        checked_at TEXT, attempts INTEGER DEFAULT 0)""")


for _col, _typ in [("milestone_index", "INTEGER"), ("ledger_state", "TEXT"), ("parents", "TEXT")]:
    try:
        with db() as c:
            c.execute(f"ALTER TABLE messages ADD COLUMN {_col} {_typ}")
    except sqlite3.OperationalError:
        pass


class MessageIn(BaseModel):
    block_id: str
    tag: str
    message: dict
    sent_at: Optional[str] = None


def now():
    return datetime.now(timezone.utc).isoformat()


def hex2str(h):
    return bytes.fromhex(h[2:] if h.startswith("0x") else h).decode("utf-8")


def verify(block_id):
    with db() as c:
        row = c.execute("SELECT * FROM messages WHERE block_id=?", (block_id,)).fetchone()
    if not row:
        return None
    detail, solid, valid, fetched = [], False, False, False
    milestone, ledger, parents = None, None, None
    try:
        m = requests.get(f"{HORNET}/api/core/v2/blocks/{block_id}/metadata", timeout=5)
        if m.status_code == 200:
            md = m.json()
            solid = bool(md.get("isSolid"))
            milestone = md.get("referencedByMilestoneIndex")
            ledger = md.get("ledgerInclusionState")
            parents = md.get("parents")
            detail.append(f"milestone={milestone}; ledger={ledger}")
        else:
            detail.append(f"metadata HTTP {m.status_code}")
        b = requests.get(f"{HORNET}/api/core/v2/blocks/{block_id}", timeout=5)
        if b.status_code == 200:
            fetched = True
            p = b.json().get("payload", {})
            tag_t = hex2str(p["tag"])
            data_t = json.loads(hex2str(p["data"]))
            valid = (tag_t == row["tag"] and data_t == json.loads(row["content"]))
            if not valid:
                detail.append("content mismatch")
        else:
            detail.append(f"block HTTP {b.status_code}")
    except Exception as e:
        detail.append(f"error: {e}")
    attempts = row["attempts"] + 1
    if solid and valid and milestone is not None and ledger != "conflicting":
        status = "valid"
    elif fetched and not valid:
        status = "invalid"
    elif attempts >= MAX_ATTEMPTS:
        status = "invalid"
    else:
        status = "pending"
    with db() as c:
        c.execute("""UPDATE messages SET is_solid=?, content_valid=?, status=?,
                     detail=?, checked_at=?, attempts=?, milestone_index=?, ledger_state=?,
                     parents=? WHERE block_id=?""",
                  (int(solid), int(valid), status, "; ".join(detail), now(), attempts,
                   milestone, ledger, json.dumps(parents), block_id))
    return status


def row_to_dict(r):
    d = dict(r)
    d["content"] = json.loads(d["content"])
    if d.get("parents"):
        d["parents"] = json.loads(d["parents"])
    return d


def retry_loop():
    while True:
        time.sleep(5)
        try:
            with db() as c:
                ids = [r[0] for r in c.execute("SELECT block_id FROM messages WHERE status='pending'")]
            for i in ids:
                verify(i)
        except Exception as e:
            print("retry error:", e)


@app.on_event("startup")
def start_retry():
    threading.Thread(target=retry_loop, daemon=True).start()


@app.post("/messages", status_code=201)
def create(msg: MessageIn, bg: BackgroundTasks):
    with db() as c:
        c.execute("""INSERT OR IGNORE INTO messages(block_id, tag, content, sent_at, received_at)
                     VALUES (?,?,?,?,?)""",
                  (msg.block_id, msg.tag, json.dumps(msg.message), msg.sent_at, now()))
    bg.add_task(verify, msg.block_id)
    return {"block_id": msg.block_id, "status": "pending"}


@app.get("/messages")
def list_messages(tag: Optional[str] = None, status: Optional[str] = None,
                  date_from: Optional[str] = None, date_to: Optional[str] = None,
                  limit: int = 100):
    q, args = "SELECT * FROM messages WHERE 1=1", []
    if tag:
        q += " AND tag LIKE ?"; args.append(f"%{tag}%")
    if status:
        q += " AND status=?"; args.append(status)
    if date_from:
        q += " AND received_at>=?"; args.append(date_from)
    if date_to:
        q += " AND received_at<=?"; args.append(date_to)
    q += " ORDER BY received_at DESC LIMIT ?"; args.append(limit)
    with db() as c:
        return [row_to_dict(r) for r in c.execute(q, args)]


@app.get("/messages/{block_id}")
def get_message(block_id: str):
    with db() as c:
        r = c.execute("SELECT * FROM messages WHERE block_id=?", (block_id,)).fetchone()
    if not r:
        raise HTTPException(404, "block_id not found")
    return row_to_dict(r)


@app.post("/messages/{block_id}/verify")
def force_verify(block_id: str):
    s = verify(block_id)
    if s is None:
        raise HTTPException(404, "block_id not found")
    return get_message(block_id)


from fastapi.responses import FileResponse


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


FLOW = ("COALESCE(json_extract(content,'$.correlation_id'),"
        "json_extract(content,'$.sensor_id'),json_extract(content,'$.sensor'))")
CRIT = "(tag LIKE 'alert.%' OR tag LIKE 'critical.%')"


@app.get("/flows")
def flows():
    q = f"""SELECT {FLOW} AS flow_id, COUNT(*) AS total, MAX(received_at) AS last_at,
            SUM(status='invalid') AS invalid, SUM(status='pending') AS pending
            FROM messages GROUP BY flow_id ORDER BY last_at DESC"""
    with db() as c:
        return [dict(r) for r in c.execute(q)]


@app.get("/flows/{flow_id}")
def flow_timeline(flow_id: str):
    q = f"SELECT * FROM messages WHERE {FLOW}=? ORDER BY received_at ASC"
    with db() as c:
        return [row_to_dict(r) for r in c.execute(q, (flow_id,))]


@app.get("/incidents")
def incidents(limit: int = 200):
    q = f"""SELECT *, {FLOW} AS flow_id FROM messages
            WHERE {CRIT} OR status='invalid' ORDER BY received_at ASC LIMIT ?"""
    with db() as c:
        return [row_to_dict(r) for r in c.execute(q, (limit,))]


@app.get("/incident-explorer", include_in_schema=False)
def incident_page():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "incidents.html"))
