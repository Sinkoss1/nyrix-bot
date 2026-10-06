from dotenv import load_dotenv
load_dotenv()

import asyncio, base64, hashlib, json, logging, os, secrets, shutil, tempfile, time, uuid, zipfile
import aiohttp, hmac, urllib.parse
from telethon import TelegramClient
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
                "invoices": {}, "owner_msgs": {}, "next_order": 1}
db = load()

def save():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(DB_PATH) or ".")
    with os.fdopen(fd, "w") as f:
        json.dump(db, f)
    os.replace(tmp, DB_PATH)

def money(c): return f"${c/100:.2f}"
def hkey(k): return hashlib.sha256(k.encode()).hexdigest()
def is_owner(m: Message): return m.from_user.id == OWNER_ID
def user_of(m: Message): return db["users"].get(str(m.from_user.id))

# ---------- Heleket ----------
def h_sign(body: str) -> str:
    return hashlib.md5((base64.b64encode(body.encode()).decode() + H_API_KEY).encode()).hexdigest()

def h_verify(data: dict) -> bool:
    data = dict(data); sign = data.pop("sign", "")
    body = json.dumps(data, separators=(",", ":")).replace("/", "\\/")
    return secrets.compare_digest(h_sign(body), sign)

async def create_invoice(order_id: str, amount_cents: int) -> str:
    body = json.dumps({"amount": f"{amount_cents/100:.2f}", "currency": "USD",
                       "order_id": order_id, "lifetime": 3600,
                       "url_callback": f"{PUBLIC_URL}/heleket"})
    async with aiohttp.ClientSession() as s:
        async with s.post("https://api.heleket.com/v1/payment", data=body,
                          headers={"merchant": H_MERCHANT, "sign": h_sign(body),
                                   "Content-Type": "application/json"}) as r:
            res = await r.json()
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

# ---------- owner handlers ----------
@dp.message(Command("newkey"), is_owner)
async def newkey(m: Message, command: CommandObject):
    if not command.args:
        return await m.answer("Usage: /newkey <name>")
    k = "sk_" + secrets.token_urlsafe(24)
    db["keys"][hkey(k)] = {"name": command.args.strip(), "user": None}
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
    await m.answer(f"Pay {money(cents)} here (crypto):\n{await create_invoice(oid, cents)}")

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

    r = {
        "owner": uid == OWNER_ID,
        "services": db["services"],
        "user": u and {"name": u["name"], "balance": u["balance"], "autos": u["autos"]},
        "orders": [order_summary(i, o)
                   for i, o in db["orders"].items() if o["user"] == str(uid)][-30:][::-1],
    }
    if uid == OWNER_ID:
        r["keys"] = list(db["keys"].values())
        r["pending"] = []
        for i, o in db["orders"].items():
            if o["status"] != "pending":
                continue
            s = order_summary(i, o)
            s["buyer"] = db["users"].get(o["user"], {}).get("name", o["user"])
            s["key"] = key_label_of(o["user"])
            r["pending"].append(s)
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
                extra["url"] = await create_invoice(oid, cents)
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
            db["keys"][hkey(k)] = {"name": str(d["name"]).strip()[:40], "user": None}
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
    app.router.add_get("/style.css", lambda r: web.FileResponse(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "style.css")))
    app.router.add_post("/api/act", act)
    app.router.add_post("/api/deliver", deliver)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()
    await bot.set_chat_menu_button(
        menu_button=MenuButtonWebApp(text="Open app", web_app=WebAppInfo(url=PUBLIC_URL)))
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
