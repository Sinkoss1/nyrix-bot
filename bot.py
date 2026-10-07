from dotenv import load_dotenv
load_dotenv()

import asyncio, base64, hashlib, json, logging, os, secrets, shutil, struct, tempfile, time, uuid, zipfile
import aiohttp, hmac, urllib.parse
from telethon import TelegramClient
from telethon.sessions import SQLiteSession
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (Message, BufferedInputFile, FSInputFile, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, MenuButtonWebApp)

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.getenv("OWNER_ID", "1915148292"))
H_MERCHANT = os.environ["HELEKET_MERCHANT"]
H_API_KEY = os.environ["HELEKET_API_KEY"]
PUBLIC_URL = os.environ["PUBLIC_URL"].rstrip("/")
DB_PATH = os.getenv("DB_PATH", "/data/database.json")
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESS_DIR = tempfile.mkdtemp(prefix="sess_")
PENDING_SESSIONS = {}   # token -> {order, dir, results, good, total, need}
_SESSION_LOCK = asyncio.Lock()

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# ---------- database ----------
def load():
    try:
        with open(DB_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"keys": {}, "users": {}, "services": {}, "orders": {},
                "invoices": {}, "owner_msgs": {}, "next_order": 1, "accounts": {}}

def hkey(k): return hashlib.sha256(k.encode()).hexdigest()

def _ensure_accounts():
    db.setdefault("accounts", {})
db = load()

def save():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(DB_PATH) or ".")
    with os.fdopen(fd, "w") as f:
        json.dump(db, f)
    os.replace(tmp, DB_PATH)

def _migrate():
    """
    Old keys were saved without 'raw'. We can't reverse sha256, so we
    re-issue a brand-new sk_ token, re-insert under the new hash, and
    drop the old hash entry. The buyer key changes but all their data
    (accounts, orders, balance) stays tied to user_id — not the key hash.
    """
    changed = False
    to_add = {}
    to_drop = []
    for kh, kdata in list(db["keys"].items()):
        if not kdata.get("raw"):
            new_raw = "sk_" + secrets.token_urlsafe(24)
            new_kh  = hkey(new_raw)
            kdata["raw"] = new_raw
            to_add[new_kh] = kdata
            to_drop.append(kh)
            # migrate accounts to new hash
            if kh in db.get("accounts", {}):
                db["accounts"][new_kh] = db["accounts"].pop(kh)
            changed = True
    for kh in to_drop:
        db["keys"].pop(kh, None)
    db["keys"].update(to_add)
    if changed:
        save()
        logging.info("_migrate: re-issued %d old key(s) without raw field", len(to_drop))
_migrate()

def money(c): return f"${c/100:.2f}"
def is_owner(m: Message): return m.from_user.id == OWNER_ID
def user_of(m: Message): return db["users"].get(str(m.from_user.id))

# ---------- Heleket ----------
def h_sign(body: str) -> str:
    return hashlib.md5((base64.b64encode(body.encode()).decode() + H_API_KEY).encode()).hexdigest()

def h_verify(data: dict) -> bool:
    data = dict(data); sign = data.pop("sign", "")
    body = json.dumps(data, separators=(",", ":")).replace("/", "\\/")
    return secrets.compare_digest(h_sign(body), sign)

class HeleketError(Exception):
    pass

async def create_invoice(order_id: str, amount_cents: int) -> str:
    body = json.dumps({"amount": f"{amount_cents/100:.2f}", "currency": "USD",
                       "order_id": order_id, "lifetime": 3600,
                       "url_callback": f"{PUBLIC_URL}/heleket"})
    async with aiohttp.ClientSession() as s:
        async with s.post("https://api.heleket.com/v1/payment", data=body,
                          headers={"merchant": H_MERCHANT, "sign": h_sign(body),
                                   "Content-Type": "application/json"}) as r:
            raw = await r.text()
            try:
                res = json.loads(raw)
            except json.JSONDecodeError:
                logging.error("Heleket non-JSON response (HTTP %s): %s", r.status, raw[:500])
                raise HeleketError(f"Heleket returned HTTP {r.status}, non-JSON body — check PUBLIC_URL/merchant/API key")

            if "result" not in res or "url" not in (res.get("result") or {}):
                # Heleket's error shape is typically {"state":1,"message":"..."}
                # or {"state":1,"errors":{...}}
                msg = res.get("message") or res.get("errors") or res
                logging.error("Heleket create_invoice failed (HTTP %s): %s", r.status, res)
                raise HeleketError(f"Heleket rejected invoice: {msg}")

    return res["result"]["url"]

async def heleket_webhook(request: web.Request):
    data = await request.json()
    if not h_verify(data):
        return web.Response(status=403)
    inv = db["invoices"].get(data.get("order_id"))
    if inv and data.get("status") in ("paid", "paid_over") and not inv["credited"]:
        inv["credited"] = True
        db["users"][inv["user"]]["balance"] += inv["amount"]
        save()
        await bot.send_message(int(inv["user"]),
            f"✅ Deposit received: {money(inv['amount'])}\nBalance: {money(db['users'][inv['user']]['balance'])}")
    return web.Response(text="ok")

# ---------- purchase ----------
def _new_code():
    return secrets.token_hex(3).upper()  # 6 hex chars

def _safe_name(s: str) -> str:
    """Filename-safe service label."""
    out = "".join(c if c.isalnum() or c in "-_" else "_" for c in (s or "service"))
    return out.strip("_") or "service"

def key_label_of(uid: str) -> str:
    """Activation-key name bound to this user (buyer label)."""
    for v in db["keys"].values():
        if v.get("user") == str(uid):
            return v.get("name") or "?"
    u = db["users"].get(str(uid))
    return (u or {}).get("name") or "?"

def _line(sid: str, svc: dict) -> dict:
    return {
        "service": sid,
        "code": _new_code(),
        "price": svc["price"],
        "charged": 0,
        "sessions": 0,
        "prepaid": 0,
    }

async def purchase(uid: str, sid: str, *, charge_now: bool = True):
    """Single-service order. charge_now=True for /buy (prepaid 1 unit)."""
    u = db["users"].get(uid)
    svc = db["services"].get(sid)
    if not u:
        return False, "User not activated."
    if not svc:
        return False, "Unknown service."
    if charge_now and u["balance"] < svc["price"]:
        return False, f"Insufficient balance ({money(u['balance'])}, need {money(svc['price'])})."

    oid = str(db["next_order"])
    db["next_order"] += 1
    line = _line(sid, svc)
    if charge_now:
        u["balance"] -= svc["price"]
        line["prepaid"] = svc["price"]
        line["charged"] = svc["price"]

    order = {
        "user": uid,
        "multi": False,
        "lines": [line],
        "status": "pending",
        "ts": int(time.time()),
        # legacy flat fields for single-order convenience
        "service": sid,
        "code": line["code"],
        "price": svc["price"],
        "charged": line["charged"],
        "sessions": 0,
        "prepaid": line["prepaid"],
    }
    db["orders"][oid] = order
    kl = key_label_of(uid)
    m = await bot.send_message(
        OWNER_ID,
        f"🆕 Order #{oid}  code: `{line['code']}`\n"
        f"Buyer: {u['name']}\n"
        f"Key: {kl}\n"
        f"Service: {svc['name']} ({sid})\n"
        f"Unit: {money(svc['price'])}/session\n"
        f"{'Prepaid 1 unit' if charge_now else 'Pay-on-delivery (automation)'}\n"
        f"Stays open — resend zips with the same code anytime.\n\n"
        f"↩️ Reply with the .zip  OR  caption = `{line['code']}`")
    db["owner_msgs"][str(m.message_id)] = oid
    save()
    return True, f"✅ Order #{oid} ({svc['name']}) code `{line['code']}`."

async def purchase_multi(uid: str, sids: list):
    """
    Multi-service automation: ONE merged order, no # in the owner card.
    Each service keeps its own code for zip routing.
    """
    u = db["users"].get(uid)
    if not u:
        return [], "User not activated."
    lines = []
    for sid in sids:
        svc = db["services"].get(sid)
        if not svc:
            return [], f"Unknown service: {sid}"
        lines.append(_line(sid, svc))

    oid = str(db["next_order"])
    db["next_order"] += 1
    order = {
        "user": uid,
        "multi": True,
        "lines": lines,
        "status": "pending",
        "ts": int(time.time()),
        "hide_number": True,
    }
    db["orders"][oid] = order

    kl = key_label_of(uid)
    svc_block = "\n".join(
        f"  • {db['services'][ln['service']]['name']} ({ln['service']}) — "
        f"`{ln['code']}` — {money(ln['price'])}/session"
        for ln in lines)
    m = await bot.send_message(
        OWNER_ID,
        f"🆕 Auto batch\n"
        f"Buyer: {u['name']}\n"
        f"Key: {kl}\n"
        f"Services:\n{svc_block}\n"
        f"Pay-on-delivery · codes stay open for more zips.\n\n"
        f"Send each .zip with caption = that service's code.")
    db["owner_msgs"][str(m.message_id)] = oid
    save()
    codes = ", ".join(f"{ln['service']}=`{ln['code']}`" for ln in lines)
    return [f"✅ Auto batch ({len(lines)} services) {codes}"], None

AUTO_EVERY = int(os.getenv("AUTO_EVERY_SECONDS", "300"))

def auto_ids(a):
    return a.get("services") or ([a["service"]] if a.get("service") else [])

def auto_min_cost(sids) -> int:
    """Minimum cents needed to run one cycle (1 session per selected service)."""
    total = 0
    for sid in sids:
        svc = db["services"].get(sid)
        if svc:
            total += svc["price"]
    return total

async def scheduler():
    while True:
        await asyncio.sleep(15)
        now = time.time()
        for uid, u in list(db["users"].items()):
            for a in u.get("autos", []):
                if not a.get("on") or a.get("next", 0) > now:
                    continue
                a["next"] = now + AUTO_EVERY
                sids = auto_ids(a)
                if not sids:
                    continue

                # Soft reserve: need at least 1× unit price per selected service
                need = auto_min_cost(sids)
                if need <= 0 or any(sid not in db["services"] for sid in sids):
                    a["on"] = False
                    save()
                    try:
                        await bot.send_message(
                            int(uid),
                            "⛔ Automation paused: a selected service is missing. Fix it in /settings.")
                    except Exception:
                        logging.exception("scheduler notify failed uid=%s", uid)
                    continue

                if u["balance"] < need:
                    a["on"] = False
                    save()
                    try:
                        await bot.send_message(
                            int(uid),
                            f"⛔ Automation paused — insufficient balance.\n"
                            f"Balance: {money(u['balance'])}\n"
                            f"Need at least {money(need)} (1 session per selected service).\n"
                            f"/deposit then Resume in /settings or the app.")
                    except Exception:
                        logging.exception("scheduler notify failed uid=%s", uid)
                    continue

                # Create order(s) — owner is notified inside purchase / purchase_multi.
                # Buyer is NOT messaged on successful cycles.
                fail = None
                try:
                    if len(sids) == 1:
                        ok, msg = await purchase(uid, sids[0], charge_now=False)
                        if not ok:
                            fail = msg
                    else:
                        _done, fail = await purchase_multi(uid, sids)
                except Exception as e:
                    logging.exception("auto cycle failed uid=%s", uid)
                    fail = str(e)[:120]

                if fail:
                    a["on"] = False
                    save()
                    try:
                        await bot.send_message(
                            int(uid),
                            f"⛔ Automation paused: {fail}")
                    except Exception:
                        logging.exception("scheduler notify failed uid=%s", uid)
                else:
                    save()

# ---------- session testing ----------
async def test_session(path: str):
    client = TelegramClient(path, API_ID, API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15)
        if not await client.is_user_authorized():
            return False, "not authorized / dead"
        me = await client.get_me()
        label = ("@" + me.username) if me.username else (me.phone or str(me.id))
        return True, label
    except Exception as e:
        return False, str(e)[:70]
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

def _order_lines(o: dict) -> list:
    """Normalize legacy single-field orders and new multi-line orders."""
    if o.get("lines"):
        return o["lines"]
    # legacy shape
    return [{
        "service": o.get("service"),
        "code": o.get("code"),
        "price": o.get("price", 0),
        "charged": o.get("charged", 0),
        "sessions": o.get("sessions", 0),
        "prepaid": o.get("prepaid", 0),
    }]

def _resolve_line(oid: str | None, code_hint: str | None):
    """
    Return (oid, order, line_index, line) for an open order.
    Code required when order is multi or when resolving by caption alone.
    """
    code = (code_hint or "").strip().upper()

    def match_in(o):
        lines = _order_lines(o)
        if code:
            for i, ln in enumerate(lines):
                if (ln.get("code") or "").upper() == code:
                    return i, ln
            return None, None
        # no code: only valid for single-line orders
        if len(lines) == 1:
            return 0, lines[0]
        return None, None

    if oid and oid in db["orders"]:
        o = db["orders"][oid]
        if o["status"] == "pending":
            i, ln = match_in(o)
            if ln is not None:
                return oid, o, i, ln
            # multi order replied without code
            if o.get("multi") and not code:
                return oid, o, None, None

    if code:
        for i, o in db["orders"].items():
            if o["status"] != "pending":
                continue
            li, ln = match_in(o)
            if ln is not None:
                return i, o, li, ln
    return None, None, None, None

async def handle_session_zip(m: Message, oid: str | None = None, code_hint: str | None = None):
    oid, o, line_i, line = _resolve_line(oid, code_hint)
    if o is not None and line is None:
        codes = ", ".join(f"`{ln['code']}`" for ln in _order_lines(o))
        return await m.answer(
            f"Multi-service batch — put the service code in the caption.\nCodes: {codes}")
    if not o or line is None:
        return await m.answer(
            "No open order matched. Reply to the order message or put the 6-char code in the caption.")

    work = os.path.join(SESS_DIR, uuid.uuid4().hex)
    os.makedirs(work)
    zpath = os.path.join(work, "in.zip")
    await bot.download(m.document, destination=zpath)
    try:
        with zipfile.ZipFile(zpath) as z:
            names = [n for n in z.namelist()
                     if n.lower().endswith(".session")
                     and not n.startswith("/") and ".." not in n]
            z.extractall(work, members=names)
    except zipfile.BadZipFile:
        shutil.rmtree(work, ignore_errors=True)
        return await m.answer("That's not a valid .zip file.")

    files = [os.path.join(r, f) for r, _, fs in os.walk(work)
             for f in fs if f.lower().endswith(".session")]
    if not files:
        shutil.rmtree(work, ignore_errors=True)
        return await m.answer("No .session files found in that zip.")

    svc = db["services"].get(line["service"], {})
    svc_name = svc.get("name", line["service"])
    status = await m.answer(
        f"🔎 Checking {len(files)} session(s) · {svc_name} (`{line['code']}`)…")
    results = {}
    for p in files:
        ok, detail = await test_session(p)
        results[os.path.relpath(p, work)] = (ok, detail)
    good = sum(1 for ok, _ in results.values() if ok)
    check_lines = [f"{'✅' if ok else '❌'} {name} — {detail}"
                   for name, (ok, detail) in results.items()]

    unit = line["price"]
    batch_cost = good * unit
    prepaid = line.get("prepaid", 0)
    already_sessions = line.get("sessions", 0)
    if already_sessions == 0 and prepaid:
        need = max(0, batch_cost - prepaid)
        refund = max(0, prepaid - batch_cost)
    else:
        need = batch_cost
        refund = 0

    u = db["users"][o["user"]]
    if need > 0 and u["balance"] < need:
        shutil.rmtree(work, ignore_errors=True)
        return await status.edit_text(
            f"Checked {svc_name} code `{line['code']}`:\n" + "\n".join(check_lines) +
            f"\n\n{good}/{len(files)} working → this batch {money(batch_cost)}.\n"
            f"Buyer short {money(need - u['balance'])}. Not sent.")

    token = uuid.uuid4().hex
    PENDING_SESSIONS[token] = {
        "order": oid, "line_i": line_i, "dir": work, "results": results,
        "good": good, "batch_cost": batch_cost, "need": need, "refund": refund,
        "svc_name": svc_name,
    }
    kb = None
    if good:
        if need == 0:
            label = f"{money(batch_cost)} (prepaid)"
        elif need < batch_cost:
            label = f"{money(need)} more (after prepaid)"
        else:
            label = money(batch_cost)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=f"✅ Send {good} → {label}",
                callback_data=f"sess:{token}:yes"),
            InlineKeyboardButton(text="✖️ Don't send", callback_data=f"sess:{token}:no"),
        ]])
    prev = already_sessions
    await status.edit_text(
        f"Checked {svc_name} code `{line['code']}`:\n" + "\n".join(check_lines) +
        (f"\n\n{good}/{len(files)} working → this batch {money(batch_cost)} "
         f"({good} × {money(unit)}).\n"
         f"This service so far: {prev} sessions · {money(line.get('charged', 0))} charged."
         if good else "\n\nNone working."),
        reply_markup=kb)
    if not good:
        shutil.rmtree(work, ignore_errors=True)
        PENDING_SESSIONS.pop(token, None)

@dp.callback_query(F.data.startswith("sess:"))
async def sess_cb(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        return await c.answer()
    parts = c.data.split(":")
    if len(parts) != 3:
        return await c.answer()
    _, token, choice = parts
    p = PENDING_SESSIONS.pop(token, None)
    await c.answer()
    if not p:
        return await c.message.edit_text(c.message.text + "\n\n(expired)")
    oid, work = p["order"], p["dir"]
    o = db["orders"].get(oid)
    if not o:
        shutil.rmtree(work, ignore_errors=True)
        return await c.message.edit_text(c.message.text + "\n\n(order gone)", reply_markup=None)

    lines = _order_lines(o)
    line_i = p.get("line_i", 0)
    if line_i is None or line_i >= len(lines):
        shutil.rmtree(work, ignore_errors=True)
        return await c.message.edit_text(c.message.text + "\n\n(line gone)", reply_markup=None)
    line = lines[line_i]
    svc_name = p.get("svc_name") or db["services"].get(line["service"], {}).get("name", line["service"])

    if choice == "yes":
        good = p["good"]
        batch_cost = p["batch_cost"]
        need = p["need"]
        refund = p.get("refund", 0)
        u = db["users"][o["user"]]
        if need > 0:
            if u["balance"] < need:
                shutil.rmtree(work, ignore_errors=True)
                return await c.message.edit_text(
                    c.message.text +
                    f"\n\nBuyer balance changed — short {money(need - u['balance'])}. Aborted.",
                    reply_markup=None)
            u["balance"] -= need
        if refund > 0:
            u["balance"] += refund

        first = line.get("sessions", 0) == 0 and line.get("prepaid", 0)
        if first:
            line["charged"] = batch_cost
        else:
            line["charged"] = line.get("charged", 0) + batch_cost
        line["sessions"] = line.get("sessions", 0) + good
        # keep lines list authoritative; mirror flat fields for single orders
        o["lines"] = lines
        if not o.get("multi"):
            o["charged"] = line["charged"]
            o["sessions"] = line["sessions"]
            o["code"] = line["code"]
            o["price"] = line["price"]
            o["prepaid"] = line.get("prepaid", 0)
        o["status"] = "pending"
        save()

        working = [f for f, (ok, _) in p["results"].items() if ok]
        fname = f"{_safe_name(svc_name)}_{good}.zip"
        outzip = os.path.join(work, fname)
        with zipfile.ZipFile(outzip, "w") as z:
            for f in working:
                z.write(os.path.join(work, f), os.path.basename(f))

        # Extract auth_key:dc_id from each working session and store for buyer
        _ensure_accounts()
        buyer_kh = next((h for h, v in db["keys"].items() if v.get("user") == o["user"]), None)
        if buyer_kh is not None:
            accs = db["accounts"].setdefault(buyer_kh, [])
            stored = 0
            for f in working:
                sess_path = os.path.join(work, f)
                try:
                    import sqlite3 as _sq
                    conn = _sq.connect(sess_path)
                    row = conn.execute(
                        "SELECT dc_id, auth_key FROM sessions LIMIT 1").fetchone()
                    conn.close()
                    if row:
                        dc_id, auth_key_bytes = row
                        auth_key_hex = auth_key_bytes.hex()
                        label = (p["results"].get(f) or (True, ""))[1]
                        if not any(a["auth_key"] == auth_key_hex for a in accs):
                            accs.append({
                                "auth_key": auth_key_hex,
                                "dc_id": dc_id,
                                "label": label,
                                "added": int(time.time()),
                            })
                            stored += 1
                except Exception as e:
                    logging.warning("Failed to extract auth_key from %s: %s", f, e)
            if stored:
                save()
                logging.info("Stored %d auth_keys for buyer %s", stored, o["user"])

        caption = (
            f"📦 {svc_name} · +{good} session(s)\n"
            f"Batch: {money(batch_cost)} · service total: {line['sessions']} sess / {money(line['charged'])}\n"
            f"Balance: {money(u['balance'])}"
        )
        await bot.send_document(
            int(o["user"]),
            FSInputFile(outzip, filename=fname),
            caption=caption)
        try:
            os.remove(outzip)
        except OSError:
            pass
        await c.message.edit_text(
            c.message.text +
            f"\n\n✅ Sent {fname} · batch {money(batch_cost)} · "
            f"{svc_name} total {line['sessions']} sess / {money(line['charged'])}.\n"
            f"Code `{line['code']}` still open for more zips.",
            reply_markup=None)
    else:
        await c.message.edit_text(c.message.text + "\n\n✖️ Not sent.", reply_markup=None)
    shutil.rmtree(work, ignore_errors=True)

# ---------- accounts helpers ----------

def _accounts_for_key(key_hash: str) -> list:
    _ensure_accounts()
    return db["accounts"].setdefault(key_hash, [])

def _session_from_auth_key(auth_key_hex: str, dc_id: int, path: str):
    """
    Write a Telethon SQLite session file from a raw auth_key hex string + dc_id.
    DC IPs (Telegram production):
    """
    import sqlite3 as _sqlite3
    DC_IPS = {1: "149.154.175.53", 2: "149.154.167.51", 3: "149.154.175.100",
               4: "149.154.167.91",  5: "91.108.56.130"}
    auth_key_bytes = bytes.fromhex(auth_key_hex)
    db_path = path if path.endswith(".session") else path + ".session"
    conn = _sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
            dc_id INTEGER PRIMARY KEY, server_address TEXT, port INTEGER,
            auth_key BLOB, takeout_id INTEGER)""")
        conn.execute("DELETE FROM sessions")
        conn.execute("INSERT INTO sessions VALUES (?,?,?,?,?)",
                     (dc_id, DC_IPS.get(dc_id, DC_IPS[2]), 443, auth_key_bytes, None))
        conn.execute("""CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY, hash INTEGER NOT NULL,
            username TEXT, phone TEXT, name TEXT, date INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS sent_files (
            md5_digest BLOB, file_size INTEGER, type INTEGER,
            id INTEGER, hash INTEGER, PRIMARY KEY(md5_digest, file_size, type))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS update_state (
            id INTEGER PRIMARY KEY, pts INTEGER, qts INTEGER,
            date INTEGER, seq INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS version (version INTEGER PRIMARY KEY)""")
        conn.execute("INSERT OR IGNORE INTO version VALUES (7)")
        conn.commit()
    finally:
        conn.close()

async def test_auth_key(auth_key_hex: str, dc_id: int) -> tuple[bool, str]:
    """Test an auth_key:dc_id pair. Returns (alive, label_or_error)."""
    work = os.path.join(SESS_DIR, uuid.uuid4().hex)
    os.makedirs(work, exist_ok=True)
    spath = os.path.join(work, "test")
    try:
        _session_from_auth_key(auth_key_hex, dc_id, spath + ".session")
        await asyncio.sleep(0.2)  # ensure SQLite lock is fully released
        ok, label = await test_session(spath)
        return ok, label
    except Exception as e:
        return False, str(e)[:80]
    finally:
        shutil.rmtree(work, ignore_errors=True)

def _build_tdata(auth_key_hex: str, dc_id: int, out_dir: str):
    """
    Build a minimal TData folder that Telegram Desktop can import.
    Uses the tdata format: key_datas + D877F783D5D3EF8C/map files.
    Requires opentele or manual binary packing.
    """
    try:
        from opentele.td import TDesktop
        from opentele.api import UseCurrentSession
        import asyncio as _asyncio

        work = tempfile.mkdtemp(prefix="tdconv_")
        spath = os.path.join(work, "conv.session")
        _session_from_auth_key(auth_key_hex, dc_id, spath)

        async def _convert():
            client = TelegramClient(spath[:-8], API_ID, API_HASH)
            await client.connect()
            tdesk = await TDesktop.FromTelethon(client, flag=UseCurrentSession)
            await client.disconnect()
            tdesk.SaveTData(out_dir)

        _asyncio.get_event_loop().run_until_complete(_convert())
        shutil.rmtree(work, ignore_errors=True)
        return True
    except Exception as e:
        logging.warning("tdata conversion failed: %s", e)
        return False

async def sessions_endpoint(request: web.Request):
    """
    GET /sessions/<raw_key>?format=session|tdata
    Buyer's personal download endpoint.
    - Checks all their auth_key:dc_id accounts
    - Deletes dead ones from DB
    - Returns a zip of working accounts in chosen format
    """
    raw_key = request.match_info["key"]
    fmt = request.rel_url.query.get("format", "session").lower()
    if fmt not in ("session", "tdata"):
        return web.Response(status=400, text="format must be session or tdata")

    kh = hkey(raw_key)
    if kh not in db.get("keys", {}):
        return web.Response(status=404, text="Invalid key")
    _ensure_accounts()
    accs = db["accounts"].get(kh, [])
    if not accs:
        return web.Response(status=200, text="No accounts stored for this key yet")

    work = os.path.join(SESS_DIR, uuid.uuid4().hex)
    os.makedirs(work)
    good_indices = []
    dead_indices = []

    async with _SESSION_LOCK:
        for i, acc in enumerate(accs):
            auth_key_hex = acc.get("auth_key", "")
            dc_id = int(acc.get("dc_id", 2))
            label = acc.get("label") or f"acc_{i+1}"

            acc_dir = os.path.join(work, f"{i}_{label}")
            os.makedirs(acc_dir, exist_ok=True)
            spath = os.path.join(acc_dir, label)

            try:
                _session_from_auth_key(auth_key_hex, dc_id, spath + ".session")
                await asyncio.sleep(0.2)  # ensure SQLite lock is fully released
                ok, detail = await test_session(spath)
            except Exception as e:
                ok, detail = False, str(e)[:60]

            if ok:
                good_indices.append((i, acc_dir, label, spath))
                acc["label"] = detail  # update label to @username
            else:
                dead_indices.append(i)
                logging.info("Dead account idx=%d label=%s: %s", i, label, detail)

    # Remove dead accounts (reverse order to keep indices valid)
    for i in sorted(dead_indices, reverse=True):
        accs.pop(i)
    if dead_indices:
        save()

    if not good_indices:
        shutil.rmtree(work, ignore_errors=True)
        return web.Response(status=200, text=f"0 working accounts. {len(dead_indices)} dead removed.")

    # Build zip
    zip_path = os.path.join(work, f"accounts_{fmt}.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for (idx, acc_dir, label, spath) in good_indices:
            if fmt == "session":
                sess_file = spath + ".session"
                if os.path.exists(sess_file):
                    zf.write(sess_file, f"{label}.session")
            else:  # tdata
                tdata_out = os.path.join(acc_dir, "tdata")
                os.makedirs(tdata_out, exist_ok=True)
                converted = _build_tdata(
                    db["accounts"][kh][idx - len([d for d in dead_indices if d < idx])
                                       if dead_indices else idx]["auth_key"],
                    db["accounts"][kh][idx - len([d for d in dead_indices if d < idx])
                                       if dead_indices else idx]["dc_id"],
                    tdata_out
                )
                if converted:
                    for root, _, files in os.walk(tdata_out):
                        for fn in files:
                            fp = os.path.join(root, fn)
                            zf.write(fp, os.path.join(label, os.path.relpath(fp, tdata_out)))
                else:
                    # fallback: include .session if tdata failed
                    sess_file = spath + ".session"
                    if os.path.exists(sess_file):
                        zf.write(sess_file, f"{label}.session")

    zip_bytes = open(zip_path, "rb").read()
    shutil.rmtree(work, ignore_errors=True)

    fname = f"accounts_{fmt}_{len(good_indices)}.zip"
    return web.Response(
        body=zip_bytes,
        content_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )

# ---------- owner handlers ----------
@dp.message(Command("newkey"), is_owner)
async def newkey(m: Message, command: CommandObject):
    if not command.args:
        return await m.answer("Usage: /newkey <name>")
    k = "sk_" + secrets.token_urlsafe(24)
    db["keys"][hkey(k)] = {"name": command.args.strip(), "user": None, "raw": k}
    save()
    await m.answer(f"Key for {command.args}:\n<code>{k}</code>\nShown once — send it to the buyer.", parse_mode="HTML")

@dp.message(Command("keys"), is_owner)
async def keys(m: Message):
    await m.answer("\n".join(f"{v['name']} — {'used' if v['user'] else 'unused'}" for v in db["keys"].values()) or "No keys.")

@dp.message(Command("addservice"), is_owner)
async def addservice(m: Message, command: CommandObject):
    try:
        sid, price, name = command.args.split(None, 2)
        db["services"][sid] = {"name": name, "price": round(float(price) * 100)}
        save()
        await m.answer(f"Saved {sid}: {name} @ ${float(price):.2f}")
    except Exception:
        await m.answer("Usage: /addservice <id> <price_usd> <name>")

@dp.message(Command("editservice"), is_owner)
async def editservice(m: Message, command: CommandObject):
    """Usage: /editservice <id> <price_usd> <name>  — updates name and/or price in place."""
    try:
        sid, price, name = command.args.split(None, 2)
        if sid not in db["services"]:
            return await m.answer(f"Unknown service id: {sid}")
        db["services"][sid] = {"name": name.strip(), "price": round(float(price) * 100)}
        save()
        await m.answer(f"Updated {sid}: {name.strip()} @ ${float(price):.2f}/session")
    except Exception:
        await m.answer("Usage: /editservice <id> <price_usd> <name>")

@dp.message(Command("delservice"), is_owner)
async def delservice(m: Message, command: CommandObject):
    db["services"].pop((command.args or "").strip(), None)
    save()
    await m.answer("Done.")

@dp.message(Command("refund"), is_owner)
async def refund(m: Message, command: CommandObject):
    o = db["orders"].get((command.args or "").strip())
    if not o or o["status"] == "refunded":
        return await m.answer("Not found / already refunded.")
    amt = sum(ln.get("charged", 0) for ln in _order_lines(o))
    if not amt:
        amt = o.get("charged") or o.get("price") or 0
    o["status"] = "refunded"
    db["users"][o["user"]]["balance"] += amt
    save()
    await bot.send_message(int(o["user"]), f"↩️ Order refunded: {money(amt)}")
    await m.answer("Refunded.")

@dp.message(Command("addaccounts"), is_owner)
async def cmd_addaccounts(m: Message, command: CommandObject):
    """
    /addaccounts <key_name>
    Then send lines of auth_key:dc_id in the same message after a newline,
    OR reply with lines as a separate message.
    Usage: /addaccounts BuyerName\\nHEX:2\\nHEX:4\\n...
    """
    if not command.args:
        return await m.answer("Usage: /addaccounts <key_name>\\n<auth_key_hex>:<dc_id>\\n...")
    lines = command.args.split("\n")
    name = lines[0].strip()
    entries = lines[1:]

    # Find key by name
    kh = None
    for h, v in db["keys"].items():
        if v.get("name", "").lower() == name.lower():
            kh = h
            break
    if not kh:
        return await m.answer(f"No key found with name: {name}")

    _ensure_accounts()
    accs = db["accounts"].setdefault(kh, [])
    added = 0
    skipped = 0
    for line in entries:
        line = line.strip()
        if not line or ":" not in line:
            continue
        parts = line.rsplit(":", 1)
        try:
            auth_key_hex = parts[0].strip()
            dc_id = int(parts[1].strip())
            assert len(auth_key_hex) == 512  # 256 bytes = 512 hex chars
            # Deduplicate
            if any(a["auth_key"] == auth_key_hex for a in accs):
                skipped += 1
                continue
            accs.append({"auth_key": auth_key_hex, "dc_id": dc_id, "label": "", "added": int(time.time())})
            added += 1
        except Exception:
            skipped += 1
    save()
    buyer_name = db["keys"][kh]["name"]
    await m.answer(f"✅ Added {added} accounts to {buyer_name}. Skipped {skipped}.\nTotal: {len(accs)}")

@dp.message(Command("listaccounts"), is_owner)
async def cmd_listaccounts(m: Message, command: CommandObject):
    """List account counts per buyer key."""
    _ensure_accounts()
    if not db["accounts"]:
        return await m.answer("No accounts stored.")
    lines = []
    for kh, accs in db["accounts"].items():
        kdata = db["keys"].get(kh, {})
        name = kdata.get("name", "?")
        lines.append(f"• {name}: {len(accs)} accounts")
    await m.answer("\n".join(lines))

@dp.message(Command("clearaccounts"), is_owner)
async def cmd_clearaccounts(m: Message, command: CommandObject):
    """Usage: /clearaccounts <key_name> — removes all accounts for that buyer."""
    name = (command.args or "").strip()
    if not name:
        return await m.answer("Usage: /clearaccounts <key_name>")
    _ensure_accounts()
    for kh, v in db["keys"].items():
        if v.get("name", "").lower() == name.lower():
            db["accounts"][kh] = []
            save()
            return await m.answer(f"Cleared all accounts for {v['name']}.")
    await m.answer(f"No key found: {name}")

@dp.message(is_owner, F.reply_to_message)
async def owner_reply(m: Message):
    oid = db["owner_msgs"].get(str(m.reply_to_message.message_id))
    if not oid:
        return
    if m.document and (m.document.file_name or "").lower().endswith(".zip"):
        return await handle_session_zip(m, oid=oid, code_hint=(m.caption or "").strip())
    o = db["orders"].get(oid)
    if not o:
        return
    await bot.send_message(int(o["user"]), f"📦 Result for order #{oid}:")
    await m.copy_to(int(o["user"]))
    o["status"] = "delivered"
    save()
    await m.answer(f"Delivered #{oid}.")

@dp.message(is_owner, F.document)
async def owner_zip_by_code(m: Message):
    """Free-standing zip: caption must be the order code."""
    if not (m.document.file_name or "").lower().endswith(".zip"):
        return
    # skip if this is a reply already handled by owner_reply
    if m.reply_to_message and str(m.reply_to_message.message_id) in db["owner_msgs"]:
        return
    caption = (m.caption or "").strip()
    if not caption:
        return  # ignore random zips without a code
    await handle_session_zip(m, oid=None, code_hint=caption)

# ---------- buyer commands ----------
@dp.message(Command("start"))
async def start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Open app", web_app=WebAppInfo(url=PUBLIC_URL))],
        [InlineKeyboardButton(text="Support", url="https://t.me/nyrixTG")],
    ])
    await m.answer("Tap below to open the app.", reply_markup=kb)

@dp.message(Command("key"))
async def key(m: Message, command: CommandObject):
    k = db["keys"].get(hkey((command.args or "").strip()))
    if not k or (k["user"] and k["user"] != str(m.from_user.id)):
        return await m.answer("❌ Invalid or already used key.")
    k["user"] = str(m.from_user.id)
    db["users"].setdefault(str(m.from_user.id), {"name": k["name"], "balance": 0, "autos": []})
    save()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Open app", web_app=WebAppInfo(url=PUBLIC_URL))],
        [InlineKeyboardButton(text="Support", url="https://t.me/nyrixTG")],
    ])
    await m.answer(
        "✅ Activated.\n/deposit <usd> – add funds\n/services – price list\n/buy <id>\n"
        "/settings – automation panel\n/balance",
        reply_markup=kb)

@dp.message(Command("balance"))
async def balance(m: Message):
    u = user_of(m)
    if u:
        await m.answer(f"Balance: {money(u['balance'])}")

@dp.message(Command("deposit"))
async def deposit(m: Message, command: CommandObject):
    if not user_of(m):
        return await m.answer("Send /key first.")
    try:
        cents = round(float(command.args) * 100)
        assert cents >= 100
    except Exception:
        return await m.answer("Usage: /deposit <usd>  (min $1)")
    oid = f"dep_{m.from_user.id}_{secrets.token_hex(4)}"
    db["invoices"][oid] = {"user": str(m.from_user.id), "amount": cents, "credited": False}
    save()
    try:
        url = await create_invoice(oid, cents)
    except HeleketError as e:
        del db["invoices"][oid]
        save()
        return await m.answer(f"⛔ Payment gateway error: {e}")
    await m.answer(f"Pay {money(cents)} here (crypto):\n{url}")

@dp.message(Command("services"))
async def services(m: Message):
    if not user_of(m):
        return await m.answer("Send /key first.")
    await m.answer(
        "\n".join(f"{i} — {s['name']} — {money(s['price'])}/session"
                  for i, s in db["services"].items()) or "No services yet.")

@dp.message(Command("buy"))
async def buy(m: Message, command: CommandObject):
    if not user_of(m):
        return await m.answer("Send /key first.")
    await m.answer((await purchase(str(m.from_user.id), (command.args or "").strip(), charge_now=True))[1])

@dp.message(Command("settings"))
async def settings(m: Message):
    u = user_of(m)
    if not u:
        return await m.answer("Send /key first.")
    lines = [f"{i+1}. {', '.join(auto_ids(a))} — {'ON' if a['on'] else 'OFF'}"
             for i, a in enumerate(u["autos"])]
    await m.answer("⚙️ Automations:\n" + ("\n".join(lines) or "none") +
                   "\n\n/auto add <id,id,...>\n/auto toggle <n>\n/auto del <n>")

@dp.message(Command("auto"))
async def auto(m: Message, command: CommandObject):
    u = user_of(m)
    if not u:
        return await m.answer("Send /key first.")
    p = (command.args or "").split()
    try:
        if p[0] == "add":
            ids = list(dict.fromkeys(p[1].split(",")))
            assert all(i in db["services"] for i in ids)
            u["autos"].append({"services": ids, "on": True, "next": time.time()})
        elif p[0] == "toggle":
            a = u["autos"][int(p[1]) - 1]
            a["on"] = not a["on"]
        elif p[0] == "del":
            u["autos"].pop(int(p[1]) - 1)
        else:
            raise ValueError
        save()
    except Exception:
        return await m.answer("Bad command. See /settings")
    await settings(m)

# ---------- Mini App API ----------
class ApiErr(Exception):
    pass

def tg_user(request) -> int:
    pairs = dict(urllib.parse.parse_qsl(request.headers.get("X-Init", "")))
    h = pairs.pop("hash", "")
    check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    ok = hmac.compare_digest(hmac.new(sk, check.encode(), hashlib.sha256).hexdigest(), h)
    if not ok or time.time() - int(pairs.get("auth_date", 0)) > 86400:
        raise web.HTTPUnauthorized()
    return json.loads(pairs["user"])["id"]

def view(uid):
    u = db["users"].get(str(uid))

    def order_summary(i, o):
        lines = _order_lines(o)
        names = []
        for ln in lines:
            nm = db["services"].get(ln["service"], {}).get("name", ln["service"])
            names.append(nm)
        return {
            "id": i,
            "name": " + ".join(names) if names else o.get("service", "?"),
            "multi": bool(o.get("multi")),
            "hide_number": bool(o.get("hide_number")),
            "status": o.get("status"),
            "ts": o.get("ts"),
            "lines": [
                {
                    "service": ln["service"],
                    "name": db["services"].get(ln["service"], {}).get("name", ln["service"]),
                    "code": ln.get("code"),
                    "price": ln.get("price", 0),
                    "charged": ln.get("charged", 0),
                    "sessions": ln.get("sessions", 0),
                }
                for ln in lines
            ],
            "user": o.get("user"),
            "price": lines[0]["price"] if len(lines) == 1 else sum(ln.get("price", 0) for ln in lines),
            "code": lines[0].get("code") if len(lines) == 1 else None,
            "charged": sum(ln.get("charged", 0) for ln in lines),
            "sessions": sum(ln.get("sessions", 0) for ln in lines),
        }

    _ensure_accounts()
    # find this user's key hash to look up accounts
    s_uid = str(uid)
    buyer_kh = next((h for h, v in db["keys"].items() if v.get("user") == s_uid), None)
    buyer_acc_count = len(db["accounts"].get(buyer_kh, [])) if buyer_kh else 0

    r = {
        "owner": uid == OWNER_ID,
        "services": db["services"],
        "user": u and {"name": u["name"], "balance": u["balance"], "autos": u["autos"]},
        "orders": [order_summary(i, o)
                   for i, o in db["orders"].items() if o["user"] == str(uid)][-30:][::-1],
        "account_count": buyer_acc_count,
    }
    if uid == OWNER_ID:
        r["pending"] = []
        for i, o in db["orders"].items():
            if o["status"] != "pending":
                continue
            s = order_summary(i, o)
            s["buyer"] = db["users"].get(o["user"], {}).get("name", o["user"])
            s["key"] = key_label_of(o["user"])
            r["pending"].append(s)
        # Full buyer list: name, raw key, activated, account count
        _ensure_accounts()
        buyers = []
        for kh, kdata in db["keys"].items():
            accs = db["accounts"].get(kh, [])
            buyers.append({
                "name": kdata.get("name", "?"),
                "raw": kdata.get("raw", ""),        # sk_… shown to owner
                "activated": bool(kdata.get("user")),
                "user_id": kdata.get("user"),
                "account_count": len(accs),
                "accounts": [
                    {"label": ac.get("label") or f"acc_{i+1}",
                     "dc_id": ac.get("dc_id"),
                     "added": ac.get("added")}
                    for i, ac in enumerate(accs)
                ],
            })
        r["buyers"] = buyers
    return r

async def act(request):
    uid = tg_user(request)
    s = str(uid)
    d = await request.json()
    a = d.get("a")
    u = db["users"].get(s)
    extra = {}
    try:
        if a == "key":
            k = db["keys"].get(hkey(str(d.get("key", "")).strip()))
            if not k or (k["user"] and k["user"] != s):
                raise ApiErr("Invalid or already used key")
            k["user"] = s
            db["users"].setdefault(s, {"name": k["name"], "balance": 0, "autos": []})
            save()
        elif a in ("deposit", "buy", "auto"):
            if not u:
                raise ApiErr("Activate a key first")
            if a == "deposit":
                cents = round(float(d["amount"]) * 100)
                if cents < 100:
                    raise ApiErr("Minimum is $1")
                oid = f"dep_{s}_{secrets.token_hex(4)}"
                db["invoices"][oid] = {"user": s, "amount": cents, "credited": False}
                save()
                try:
                    extra["url"] = await create_invoice(oid, cents)
                except HeleketError as e:
                    del db["invoices"][oid]
                    save()
                    raise ApiErr(f"Payment gateway error: {e}")
            elif a == "buy":
                ok, msg = await purchase(s, str(d.get("service")), charge_now=True)
                if not ok:
                    raise ApiErr(msg)
                extra["msg"] = msg
            else:
                op = d.get("op")
                if op == "add":
                    ids = list(dict.fromkeys(d["services"]))
                    if not ids or not all(i in db["services"] for i in ids):
                        raise ApiErr("Pick at least one valid service")
                    u["autos"].append({"services": ids, "on": True, "next": time.time()})
                elif op == "toggle":
                    u["autos"][int(d["i"])]["on"] ^= True
                elif op == "del":
                    u["autos"].pop(int(d["i"]))
                save()
        elif uid == OWNER_ID and a == "newkey":
            k = "sk_" + secrets.token_urlsafe(24)
            db["keys"][hkey(k)] = {"name": str(d["name"]).strip()[:40], "user": None, "raw": k}
            save()
            extra["key"] = k
        elif uid == OWNER_ID and a == "addsvc":
            db["services"][str(d["id"]).strip()] = {
                "name": str(d["name"]).strip(),
                "price": round(float(d["price"]) * 100),
            }
            save()
        elif uid == OWNER_ID and a == "editsvc":
            sid = str(d["id"]).strip()
            if sid not in db["services"]:
                raise ApiErr("Unknown service")
            name = str(d.get("name", "")).strip()
            if not name:
                raise ApiErr("Name required")
            price = round(float(d["price"]) * 100)
            if price < 0:
                raise ApiErr("Invalid price")
            db["services"][sid] = {"name": name, "price": price}
            save()
            extra["msg"] = f"Updated {sid}: {name} @ {money(price)}/session"
        elif uid == OWNER_ID and a == "delsvc":
            db["services"].pop(d["id"], None)
            save()
        elif uid == OWNER_ID and a == "refund":
            o = db["orders"].get(str(d["id"]))
            if not o or o["status"] == "refunded":
                raise ApiErr("Not found / already refunded")
            amt = sum(ln.get("charged", 0) for ln in _order_lines(o))
            if not amt:
                amt = o.get("charged") or o.get("price") or 0
            o["status"] = "refunded"
            db["users"][o["user"]]["balance"] += amt
            save()
            await bot.send_message(int(o["user"]), f"↩️ Order #{d['id']} refunded: {money(amt)}")
        elif uid == OWNER_ID and a == "addfunds":
            k = db["keys"].get(hkey(str(d["key"]).strip()))
            if not k or not k["user"]:
                raise ApiErr("Key not found or not activated yet")
            cents = round(float(d["amount"]) * 100)
            if not 1 <= cents <= 10**8:
                raise ApiErr("Invalid amount")
            t = db["users"][k["user"]]
            t["balance"] += cents
            save()
            await bot.send_message(
                int(k["user"]),
                f"💰 {money(cents)} added to your balance.\nBalance: {money(t['balance'])}")
            extra["msg"] = f"Added {money(cents)} to {t['name']}. Balance: {money(t['balance'])}"
        elif uid == OWNER_ID and a == "addaccounts":
            # Add auth_key:dc_id accounts to a buyer key
            # d = {secret_key: "sk_...", accounts: ["hex:dc_id", ...]}
            raw_key = str(d.get("secret_key", "")).strip()
            raw_lines = d.get("accounts", [])
            kh = hkey(raw_key)
            if kh not in db["keys"]:
                raise ApiErr("Invalid secret key")
            _ensure_accounts()
            accs = db["accounts"].setdefault(kh, [])
            added = 0
            skipped = 0
            for line in raw_lines:
                line = str(line).strip()
                if not line or ":" not in line:
                    skipped += 1
                    continue
                parts = line.rsplit(":", 1)
                try:
                    auth_key_hex = parts[0].strip()
                    dc_id = int(parts[1].strip())
                    assert len(auth_key_hex) == 512
                    if any(a["auth_key"] == auth_key_hex for a in accs):
                        skipped += 1
                        continue
                    accs.append({"auth_key": auth_key_hex, "dc_id": dc_id,
                                 "label": "", "added": int(time.time())})
                    added += 1
                except Exception:
                    skipped += 1
            save()
            extra["msg"] = f"Added {added}, skipped {skipped}. Total: {len(accs)}"
            extra["total"] = len(accs)
        elif uid == OWNER_ID and a == "listaccounts":
            _ensure_accounts()
            result = []
            for kh, accs in db["accounts"].items():
                kdata = db["keys"].get(kh, {})
                result.append({
                    "key_name": kdata.get("name", "?"),
                    "count": len(accs),
                    "accounts": [
                        {"label": ac.get("label") or f"acc_{i+1}",
                         "dc_id": ac.get("dc_id"),
                         "added": ac.get("added")}
                        for i, ac in enumerate(accs)
                    ]
                })
            extra["accounts_list"] = result
        elif uid == OWNER_ID and a == "delaccount":
            # d = {secret_key: "sk_...", index: int}
            raw_key = str(d.get("secret_key", "")).strip()
            idx = int(d.get("index", -1))
            kh = hkey(raw_key)
            if kh not in db["keys"]:
                raise ApiErr("Invalid secret key")
            _ensure_accounts()
            accs = db["accounts"].get(kh, [])
            if idx < 0 or idx >= len(accs):
                raise ApiErr("Index out of range")
            accs.pop(idx)
            save()
            extra["msg"] = f"Removed. {len(accs)} remaining."
        elif uid == OWNER_ID and a == "clearaccounts":
            raw_key = str(d.get("secret_key", "")).strip()
            kh = hkey(raw_key)
            if kh not in db["keys"]:
                raise ApiErr("Invalid secret key")
            _ensure_accounts()
            db["accounts"][kh] = []
            save()
            extra["msg"] = "All accounts cleared."
        # Buyer: view their own account count + endpoint URL
        elif a == "myaccounts":
            if not u:
                raise ApiErr("Activate a key first")
            _ensure_accounts()
            # find this user's key hash
            kh = next((h for h, v in db["keys"].items() if v.get("user") == s), None)
            count = len(db["accounts"].get(kh, [])) if kh else 0
            # The raw key is not stored (only its hash), so we expose the endpoint pattern
            extra["account_count"] = count
            extra["endpoint_note"] = "Use your secret key: GET /sessions/<your_key>?format=session|tdata"
        elif a != "me":
            raise ApiErr("Not allowed")
    except ApiErr as e:
        return web.json_response({"error": str(e)}, status=400)
    except (KeyError, ValueError, TypeError, IndexError, AssertionError, OverflowError):
        return web.json_response({"error": "Invalid input"}, status=400)
    return web.json_response({**view(uid), **extra})

async def deliver(request):
    if tg_user(request) != OWNER_ID:
        raise web.HTTPForbidden()
    f = await request.post()
    o = db["orders"].get(str(f.get("order")))
    text, file = (f.get("text") or "").strip(), f.get("file")
    has_file = file is not None and hasattr(file, "file") and file.filename
    if not o or o["status"] == "refunded" or not (text or has_file):
        return web.json_response({"error": "Nothing to send / bad order"}, status=400)
    chat = int(o["user"])
    await bot.send_message(chat, f"📦 Result for order #{f['order']}:" + (f"\n\n{text}" if text else ""))
    if has_file:
        data = BufferedInputFile(file.file.read(), filename=file.filename)
        ct = file.content_type or ""
        if ct == "audio/ogg":
            await bot.send_voice(chat, data)
        elif ct.startswith("audio/"):
            await bot.send_audio(chat, data)
        else:
            await bot.send_document(chat, data)
    o["status"] = "delivered"
    save()
    return web.json_response(view(OWNER_ID))

async def main():
    app = web.Application(client_max_size=50 * 1024 ** 2)
    app.router.add_post("/heleket", heleket_webhook)
    app.router.add_get("/", lambda r: web.FileResponse(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.html")))
    async def serve_css(r):
        resp = web.FileResponse(os.path.join(os.path.dirname(os.path.abspath(__file__)), "style.css"))
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    app.router.add_get("/style.css", serve_css)
    app.router.add_post("/api/act", act)
    app.router.add_post("/api/deliver", deliver)
    app.router.add_get("/sessions/{key}", sessions_endpoint)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()
    await bot.set_chat_menu_button(
        menu_button=MenuButtonWebApp(text="Open app", web_app=WebAppInfo(url=PUBLIC_URL)))
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())