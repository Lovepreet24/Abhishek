import asyncio, logging, os, random, time
from decimal import Decimal as D
import aiohttp, asyncpg
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton as IB, InlineKeyboardMarkup as IM,
                           KeyboardButton as KB, Message, ReplyKeyboardMarkup)

load_dotenv()
TOKEN = os.environ["BOT_TOKEN"]
ADMINS = {int(x) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()}
DB_URL = os.environ["DATABASE_URL"]
P_URL, P_KEY = os.environ["PROVIDER_URL"], os.environ["PROVIDER_KEY"]
TRON_KEY = os.getenv("TRONGRID_KEY", "")
SUPPORT = os.getenv("SUPPORT_USERNAME", "admin")
TRON_WALLET, EVM_WALLET = os.getenv("TRON_WALLET", ""), os.getenv("EVM_WALLET", "")
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# USDT on each chain. Add more chains here (same fields) and they appear in the bot automatically.
CHAINS = {
    "trc20": dict(name="USDT TRC20 (Tron)", kind="tron", wallet=TRON_WALLET, dec=6,
                  contract="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"),
    "bep20": dict(name="USDT BEP20 (BSC)", kind="evm", wallet=EVM_WALLET, dec=18, conf=3, look=1500,
                  contract="0x55d398326f99059ff775485246999027b3197955",
                  rpc=os.getenv("BSC_RPC", "https://bsc-rpc.publicnode.com")),
    "polygon": dict(name="USDT Polygon", kind="evm", wallet=EVM_WALLET, dec=6, conf=20, look=2000,
                    contract="0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
                    rpc=os.getenv("POLYGON_RPC", "https://polygon-bor-rpc.publicnode.com")),
    "erc20": dict(name="USDT ERC20 (Ethereum)", kind="evm", wallet=EVM_WALLET, dec=6, conf=3, look=400,
                  contract="0xdac17f958d2ee523a2206206994597c13d831ec7",
                  rpc=os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")),
}
ON = {k: c for k, c in CHAINS.items() if c["wallet"]}  # only chains whose wallet is set
POS = {}  # last scanned block per EVM chain
ACTIVE = ("Pending", "Processing", "In progress")

DEFAULTS = {"markup": "30", "inr_rate": "90", "ref_percent": "1", "upi_id": "yourupi@bank", "min_deposit": "1"}
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id BIGINT PRIMARY KEY, balance NUMERIC(14,4) DEFAULT 0, ref_by BIGINT,
  banned BOOL DEFAULT FALSE, created TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, user_id BIGINT, service_id INT, service_name TEXT,
  link TEXT, qty INT, charge NUMERIC(14,4), provider_order TEXT, status TEXT DEFAULT 'Pending', created TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS deposits(id SERIAL PRIMARY KEY, user_id BIGINT, method TEXT, amount NUMERIC(14,4),
  status TEXT DEFAULT 'pending', txid TEXT, created TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE deposits ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
"""

bot = Bot(TOKEN)
db: asyncpg.Pool = None
CACHE = {"t": 0, "svc": {}, "cats": []}


# ---------- helpers ----------
async def gs(k):
    v = await db.fetchval("SELECT v FROM settings WHERE k=$1", k)
    return v if v is not None else DEFAULTS[k]

async def tell(uid, text, **kw):
    try:
        await bot.send_message(uid, text, **kw)
    except Exception:
        pass

async def api(**p):
    p["key"] = P_KEY
    async with aiohttp.ClientSession() as s:
        async with s.post(P_URL, data=p, timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await r.json(content_type=None)

async def services():
    if time.time() - CACHE["t"] > 600 or not CACHE["svc"]:
        data = await api(action="services")
        CACHE["svc"] = {int(s["service"]): s for s in data}
        CACHE["cats"] = sorted({s["category"] for s in data})
        CACHE["t"] = time.time()
    return CACHE

def cost(rate, qty, mk):
    return (D(str(rate)) * qty / 1000 * (1 + mk / 100)).quantize(D("0.0001"))

async def send(ev, text, kb=None, **kw):
    if isinstance(ev, CallbackQuery):
        await ev.message.edit_text(text, reply_markup=kb, **kw)
        await ev.answer()
    else:
        await ev.answer(text, reply_markup=kb, **kw)

def page_kb(items, page, cb_item, cb_page, per=8, back=None):
    rows = [[IB(text=t, callback_data=cb_item(k))] for k, t in items[page * per:(page + 1) * per]]
    nav = []
    if page > 0: nav.append(IB(text="⬅️", callback_data=cb_page(page - 1)))
    if (page + 1) * per < len(items): nav.append(IB(text="➡️", callback_data=cb_page(page + 1)))
    if nav: rows.append(nav)
    if back: rows.append([IB(text="🔙 Back", callback_data=back)])
    return IM(inline_keyboard=rows)

async def credit_deposit(dep_id, txid=None):
    """Atomic + idempotent: credits user, pays referral bonus."""
    async with db.acquire() as c, c.transaction():
        d = await c.fetchrow("UPDATE deposits SET status='done', txid=COALESCE($2,txid) WHERE id=$1 AND status='pending' "
                             "RETURNING user_id, amount", dep_id, txid)
        if not d: return None
        await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", d["user_id"], d["amount"])
        ref = await c.fetchval("SELECT ref_by FROM users WHERE id=$1", d["user_id"])
        bonus = D(0)
        if ref:
            bonus = (d["amount"] * D(await gs("ref_percent")) / 100).quantize(D("0.0001"))
            await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", ref, bonus)
    await tell(d["user_id"], f"✅ ${d['amount']:.2f} added to your balance.")
    if ref and bonus > 0: await tell(ref, f"🎁 Referral bonus: ${bonus:.4f}")
    return d


# ---------- states / menu ----------
class S(StatesGroup):
    link = State(); qty = State(); usdt = State(); upi_amt = State(); upi_proof = State()

MENU = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
    [KB(text="🛒 New Order"), KB(text="💰 Add Funds")],
    [KB(text="📦 My Orders"), KB(text="🎁 Referral")],
    [KB(text="👤 Account"), KB(text="🆘 Support")]])

class BanMW(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        if u and u.id not in ADMINS and await db.fetchval("SELECT banned FROM users WHERE id=$1", u.id):
            return
        return await handler(event, data)

adm, r = Router(), Router()
adm.message.filter(F.from_user.id.in_(ADMINS))


# ---------- user: basics ----------
@r.message(CommandStart())
async def start(m: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if not await db.fetchval("SELECT 1 FROM users WHERE id=$1", uid):
        ref, a = None, command.args or ""
        if a.startswith("ref_") and a[4:].isdigit() and int(a[4:]) != uid:
            ref = int(a[4:])
            if not await db.fetchval("SELECT 1 FROM users WHERE id=$1", ref): ref = None
        await db.execute("INSERT INTO users(id,ref_by) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, ref)
    await m.answer("👋 Welcome! Order SMM services right here.", reply_markup=MENU)

async def ensure(uid):
    return await db.fetchrow("INSERT INTO users(id) VALUES($1) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id RETURNING *", uid)

@r.message(F.text == "👤 Account")
async def account(m: Message, state: FSMContext):
    await state.clear()
    u = await ensure(m.from_user.id)
    n = await db.fetchval("SELECT count(*) FROM orders WHERE user_id=$1", u["id"])
    await m.answer(f"👤 ID: {u['id']}\n💰 Balance: ${u['balance']:.2f}\n📦 Orders: {n}")

@r.message(F.text == "🆘 Support")
async def support(m: Message):
    await m.answer(f"Contact: {SUPPORT}")

@r.message(F.text == "🎁 Referral")
async def referral(m: Message, state: FSMContext):
    await state.clear()
    me = await bot.get_me()
    n = await db.fetchval("SELECT count(*) FROM users WHERE ref_by=$1", m.from_user.id)
    await m.answer(f"🎁 Earn {await gs('ref_percent')}% of every deposit your referrals make.\n\n"
                   f"Your link:\nhttps://t.me/{me.username}?start=ref_{m.from_user.id}\n\nReferrals: {n}")

@r.message(F.text == "📦 My Orders")
async def my_orders(m: Message, state: FSMContext):
    await state.clear()
    rows = await db.fetch("SELECT * FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT 10", m.from_user.id)
    if not rows: return await m.answer("No orders yet.")
    await m.answer("\n\n".join(f"#{o['id']} • {o['service_name'][:30]}\nQty {o['qty']} • ${o['charge']:.2f} • {o['status']}" for o in rows))


# ---------- user: ordering ----------
@r.message(F.text == "🛒 New Order")
@r.callback_query(F.data.startswith("cp:"))
async def cats(ev, state: FSMContext):
    await state.clear()
    page = int(ev.data[3:]) if isinstance(ev, CallbackQuery) else 0
    try:
        c = await services()
    except Exception:
        return await send(ev, "⚠️ Services unavailable, try again shortly.")
    items = [(i, n[:45]) for i, n in enumerate(c["cats"])]
    await send(ev, "Choose a category:", page_kb(items, page, lambda k: f"c:{k}:0", lambda p: f"cp:{p}"))

@r.callback_query(F.data.startswith("c:"))
async def category(cb: CallbackQuery):
    _, ci, pg = cb.data.split(":")
    ci, pg = int(ci), int(pg)
    c = await services()
    if ci >= len(c["cats"]): return await cb.answer("Refreshed, open New Order again", show_alert=True)
    name, mk = c["cats"][ci], D(await gs("markup"))
    items = [(int(s["service"]), f"{s['name'][:34]} | ${cost(s['rate'], 1000, mk):.2f}/1k")
             for s in c["svc"].values() if s["category"] == name]
    await send(cb, name, page_kb(items, pg, lambda k: f"s:{k}", lambda p: f"c:{ci}:{p}", back="cp:0"))

@r.callback_query(F.data.startswith("s:"))
async def pick(cb: CallbackQuery, state: FSMContext):
    s = (await services())["svc"].get(int(cb.data[2:]))
    if not s: return await cb.answer("Service not available", show_alert=True)
    mk = D(await gs("markup"))
    await state.set_state(S.link)
    await state.update_data(sid=int(s["service"]))
    await cb.message.answer(f"✅ {s['name']}\n💵 ${cost(s['rate'], 1000, mk):.2f} per 1000\n"
                            f"📉 Min {s['min']} | Max {s['max']}\n\nSend the link:")
    await cb.answer()

@r.message(S.link, F.text)
async def got_link(m: Message, state: FSMContext):
    if len(m.text) < 4: return await m.answer("Send a valid link.")
    await state.update_data(link=m.text.strip())
    await state.set_state(S.qty)
    await m.answer("Send quantity:")

@r.message(S.qty, F.text)
async def got_qty(m: Message, state: FSMContext):
    d = await state.get_data()
    s = (await services())["svc"].get(d.get("sid"))
    if not s or not m.text.isdigit(): return await m.answer("Send a number.")
    q = int(m.text)
    if not int(s["min"]) <= q <= int(s["max"]): return await m.answer(f"Quantity must be {s['min']} - {s['max']}.")
    price = cost(s["rate"], q, D(await gs("markup")))
    await state.update_data(qty=q)
    await m.answer(f"🧾 {s['name']}\nQty: {q}\nLink: {d['link']}\nPrice: ${price:.4f}",
                   reply_markup=IM(inline_keyboard=[[IB(text="✅ Confirm", callback_data="ok"), IB(text="❌ Cancel", callback_data="no")]]))

@r.callback_query(F.data == "no")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("Cancelled.")

@r.callback_query(F.data == "ok")
async def confirm(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    s = (await services())["svc"].get(d.get("sid"))
    if not s or "qty" not in d: return await cb.answer("Expired, start again", show_alert=True)
    uid, charge = cb.from_user.id, cost(s["rate"], d["qty"], D(await gs("markup")))
    if not await db.fetchval("UPDATE users SET balance=balance-$2 WHERE id=$1 AND balance>=$2 RETURNING 1", uid, charge):
        return await cb.message.edit_text("❌ Insufficient balance. Use 💰 Add Funds.")
    try:
        res = await api(action="add", service=d["sid"], link=d["link"], quantity=d["qty"])
    except Exception as e:
        res = {"error": str(e)}
    if "order" in res:
        oid = await db.fetchval("INSERT INTO orders(user_id,service_id,service_name,link,qty,charge,provider_order) "
                                "VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id", uid, d["sid"], s["name"], d["link"], d["qty"], charge, str(res["order"]))
        await cb.message.edit_text(f"✅ Order #{oid} placed. Charged ${charge:.4f}")
    else:
        await db.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", uid, charge)
        await cb.message.edit_text(f"❌ Order failed, refunded.\n{str(res.get('error', 'Unknown error'))[:150]}")


# ---------- user: add funds ----------
@r.message(F.text == "💰 Add Funds")
async def funds(m: Message, state: FSMContext):
    await state.clear()
    rows = [[IB(text=f"💎 {c['name']} - Auto", callback_data=f"f:{k}")] for k, c in ON.items()]
    rows.append([IB(text="🇮🇳 UPI - Manual", callback_data="f:upi")])
    await m.answer("Choose payment method:", reply_markup=IM(inline_keyboard=rows))

@r.callback_query(F.data.startswith("f:"))
async def fund_method(cb: CallbackQuery, state: FSMContext):
    k, mn = cb.data[2:], D(await gs("min_deposit"))
    if k in ON:
        await state.set_state(S.usdt)
        await state.update_data(chain=k)
        await cb.message.answer(f"{ON[k]['name']}\nEnter amount in USD (min ${mn}):")
    elif k == "upi":
        await state.set_state(S.upi_amt)
        await cb.message.answer(f"Enter amount in ₹ (min ₹{mn * D(await gs('inr_rate')):.0f}):")
    await cb.answer()

def num(t):
    try:
        v = D(t.strip())
        return v if v > 0 else None
    except Exception:
        return None

@r.message(S.usdt, F.text)
async def usdt_amt(m: Message, state: FSMContext):
    k = (await state.get_data()).get("chain")
    base = num(m.text)
    if k not in ON:
        await state.clear()
        return await m.answer("Start again from 💰 Add Funds.")
    if not base or base < D(await gs("min_deposit")): return await m.answer("Invalid amount.")
    for _ in range(50):  # unique decimals so we can match the payment
        amt = base + D(random.randint(1, 99)) / 1000
        if not await db.fetchval("SELECT 1 FROM deposits WHERE method=$1 AND status='pending' AND amount=$2", k, amt): break
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount) VALUES($1,$2,$3) RETURNING id", m.from_user.id, k, amt)
    await state.clear()
    c = ON[k]
    await m.answer(f"💎 Deposit #{dep}\nNetwork: {c['name']}\nSend EXACTLY <code>{amt}</code> USDT to:\n<code>{c['wallet']}</code>\n\n"
                   f"⏱ Valid 30 min. Balance is added automatically after confirmation.\n"
                   f"⚠️ Use ONLY this network and the exact amount, otherwise funds can be lost.", parse_mode="HTML")

@r.message(S.upi_amt, F.text)
async def upi_amt(m: Message, state: FSMContext):
    inr, rate = num(m.text), D(await gs("inr_rate"))
    if not inr or inr / rate < D(await gs("min_deposit")): return await m.answer("Invalid amount.")
    usd = (inr / rate).quantize(D("0.0001"))
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount) VALUES($1,'upi',$2) RETURNING id", m.from_user.id, usd)
    await state.set_state(S.upi_proof)
    await state.update_data(dep=dep, inr=str(inr))
    await m.answer(f"🇮🇳 Deposit #{dep}\nPay ₹{inr} to UPI ID:\n<code>{await gs('upi_id')}</code>\n\n"
                   f"Then send the UTR number or a payment screenshot here.", parse_mode="HTML")

@r.message(S.upi_proof)
async def upi_proof(m: Message, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    kb = IM(inline_keyboard=[[IB(text="✅ Approve", callback_data=f"ad:ok:{d['dep']}"), IB(text="❌ Reject", callback_data=f"ad:no:{d['dep']}")]])
    for a in ADMINS:
        try:
            await m.copy_to(a)
            await bot.send_message(a, f"UPI deposit #{d['dep']}\nUser: {m.from_user.id}\nAmount: ₹{d['inr']}", reply_markup=kb)
        except Exception:
            pass
    await m.answer("⏳ Sent for verification. Balance will be added once approved.")


# ---------- admin ----------
@r.callback_query(F.data.startswith("ad:"))
async def admin_cb(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS: return await cb.answer()
    _, act, i = cb.data.split(":")
    if act == "ok":
        txt = "✅ Approved" if await credit_deposit(int(i)) else "Already processed"
    else:
        uid = await db.fetchval("UPDATE deposits SET status='rejected' WHERE id=$1 AND status='pending' RETURNING user_id", int(i))
        if uid: await tell(uid, f"❌ Deposit #{i} was rejected. Contact support.")
        txt = "❌ Rejected" if uid else "Already processed"
    await cb.message.edit_text(f"{cb.message.text}\n\n{txt}")

@adm.message(Command("admin"))
async def admin_help(m: Message):
    cur = "\n".join(f"  {k} = {await gs(k)}" for k in DEFAULTS)
    await m.answer("🛠 Admin\n/stats\n/user <id>\n/addbal <id> <usd> (negative to deduct)\n/set <key> <value>\n"
                   "/ban <id> | /unban <id>\n/broadcast <text>\n/providerbal\n\nSettings:\n" + cur)

@adm.message(Command("stats"))
async def stats(m: Message):
    u = await db.fetchval("SELECT count(*) FROM users")
    o = await db.fetchrow("SELECT count(*) n, coalesce(sum(charge),0) s FROM orders")
    dep = await db.fetchval("SELECT coalesce(sum(amount),0) FROM deposits WHERE status='done'")
    await m.answer(f"👥 Users: {u}\n📦 Orders: {o['n']} (${o['s']:.2f})\n💰 Deposits: ${dep:.2f}")

@adm.message(Command("user"))
async def user_info(m: Message, command: CommandObject):
    if not (command.args or "").isdigit(): return await m.answer("/user <id>")
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", int(command.args))
    await m.answer(f"ID {u['id']}\nBalance ${u['balance']:.4f}\nBanned: {u['banned']}\nRef by: {u['ref_by']}" if u else "Not found")

@adm.message(Command("addbal"))
async def addbal(m: Message, command: CommandObject):
    a = (command.args or "").split()
    amt = None
    try: amt = D(a[1])
    except Exception: pass
    if len(a) != 2 or not a[0].isdigit() or amt is None: return await m.answer("/addbal <id> <usd>")
    ok = await db.fetchval("UPDATE users SET balance=balance+$2 WHERE id=$1 RETURNING 1", int(a[0]), amt)
    if ok: await tell(int(a[0]), f"💰 Balance adjusted by ${amt}")
    await m.answer("✅ Done" if ok else "User not found")

@adm.message(Command("set"))
async def set_cmd(m: Message, command: CommandObject):
    a = (command.args or "").split(None, 1)
    if len(a) != 2 or a[0] not in DEFAULTS or (a[0] != "upi_id" and num(a[1]) is None):
        return await m.answer("/set <" + "|".join(DEFAULTS) + "> <value>")
    await db.execute("INSERT INTO settings VALUES($1,$2) ON CONFLICT(k) DO UPDATE SET v=$2", a[0], a[1].strip())
    await m.answer("✅ Updated")

@adm.message(Command("ban", "unban"))
async def ban(m: Message, command: CommandObject):
    if not (command.args or "").isdigit(): return await m.answer("/ban <id>")
    await db.execute("UPDATE users SET banned=$2 WHERE id=$1", int(command.args), command.command == "ban")
    await m.answer("✅ Done")

@adm.message(Command("broadcast"))
async def broadcast(m: Message, command: CommandObject):
    if not command.args: return await m.answer("/broadcast <text>")
    ok = 0
    for row in await db.fetch("SELECT id FROM users WHERE NOT banned"):
        try:
            await bot.send_message(row["id"], command.args)
            ok += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
    await m.answer(f"📢 Sent to {ok} users")

@adm.message(Command("providerbal"))
async def provider_bal(m: Message):
    try:
        b = await api(action="balance")
        await m.answer(f"Provider balance: {b.get('balance')} {b.get('currency', '')}")
    except Exception as e:
        await m.answer(f"Error: {e}")


# ---------- background jobs ----------
async def rpc(url, method, params):
    async with aiohttp.ClientSession() as ses:
        async with ses.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                            timeout=aiohttp.ClientTimeout(total=25)) as r:
            j = await r.json(content_type=None)
    if "error" in j: raise RuntimeError(j["error"])
    return j["result"]

async def match(key, val, txid, get_ts):
    q = val.quantize(D("0.0001"))
    for p in await db.fetch("SELECT id, amount, created FROM deposits WHERE method=$1 AND status='pending'", key):
        if p["amount"] != q: continue
        if await db.fetchval("SELECT 1 FROM deposits WHERE txid=$1", txid): return
        if await get_ts() < p["created"].timestamp() - 60: return  # payment older than the deposit request
        await credit_deposit(p["id"], txid)
        return

async def const_ts(x):
    return x

async def block_ts(c, bn):
    return int((await rpc(c["rpc"], "eth_getBlockByNumber", [hex(bn), False]))["timestamp"], 16)

async def scan_tron(key, c):
    url = f"https://api.trongrid.io/v1/accounts/{c['wallet']}/transactions/trc20?only_to=true&limit=50&contract_address={c['contract']}"
    async with aiohttp.ClientSession(headers={"TRON-PRO-API-KEY": TRON_KEY} if TRON_KEY else {}) as ses:
        async with ses.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            txs = (await resp.json()).get("data", [])
    for tx in txs:
        if tx.get("to") != c["wallet"]: continue
        ts = tx["block_timestamp"] / 1000
        await match(key, D(tx["value"]) / D(10 ** c["dec"]), tx["transaction_id"], lambda ts=ts: const_ts(ts))

async def scan_evm(key, c):
    latest = int(await rpc(c["rpc"], "eth_blockNumber", []), 16) - c["conf"]
    last = max(POS.get(key, 0), latest - c["look"])
    topic_to = "0x" + "0" * 24 + c["wallet"][2:].lower()
    frm = last + 1
    while frm <= latest:
        to = min(latest, frm + 1999)
        logs = await rpc(c["rpc"], "eth_getLogs", [{"fromBlock": hex(frm), "toBlock": hex(to), "address": c["contract"],
                                                     "topics": [TRANSFER, None, topic_to]}])
        for lg in logs:
            bn = int(lg["blockNumber"], 16)
            await match(key, D(int(lg["data"], 16)) / D(10 ** c["dec"]), lg["transactionHash"], lambda bn=bn: block_ts(c, bn))
        POS[key] = to
        frm = to + 1

async def crypto_poller():
    while True:
        await asyncio.sleep(30)
        try:
            await db.execute("UPDATE deposits SET status='expired' WHERE method<>'upi' AND status='pending' AND created < now() - interval '30 minutes'")
        except Exception as e:
            logging.warning("expire: %s", e)
        for key, c in ON.items():
            try:
                if not await db.fetchval("SELECT 1 FROM deposits WHERE method=$1 AND status='pending'", key): continue
                await (scan_tron if c["kind"] == "tron" else scan_evm)(key, c)
            except Exception as e:
                logging.warning("scan %s: %s", key, e)

async def status_loop():
    while True:
        await asyncio.sleep(300)
        try:
            for o in await db.fetch("SELECT * FROM orders WHERE status = ANY($1) ORDER BY id LIMIT 100", list(ACTIVE)):
                res = await api(action="status", order=o["provider_order"])
                st = res.get("status")
                if not st or st == o["status"]: continue
                refund = D(0)
                if st in ("Canceled", "Refunded"): refund = o["charge"]
                elif st == "Partial" and o["qty"]:
                    refund = (o["charge"] * int(res.get("remains") or 0) / o["qty"]).quantize(D("0.0001"))
                async with db.acquire() as c, c.transaction():
                    if not await c.fetchval("UPDATE orders SET status=$2 WHERE id=$1 AND status=$3 RETURNING 1", o["id"], st, o["status"]):
                        continue
                    if refund > 0:
                        await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", o["user_id"], refund)
                if st in ("Completed", "Partial", "Canceled", "Refunded"):
                    extra = f" Refunded ${refund:.4f}." if refund > 0 else ""
                    await tell(o["user_id"], f"📦 Order #{o['id']}: {st}.{extra}")
        except Exception as e:
            logging.warning("status loop: %s", e)


async def main():
    global db
    logging.basicConfig(level=logging.INFO)
    db = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    await db.execute(SCHEMA)
    dp = Dispatcher()
    dp.update.outer_middleware(BanMW())
    dp.include_routers(adm, r)
    tasks = [asyncio.create_task(crypto_poller()), asyncio.create_task(status_loop())]
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
