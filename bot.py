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
ACTIVE = ("Pending", "Processing", "In progress")

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
ON = {k: c for k, c in CHAINS.items() if c["wallet"]}
POS = {}

# Everything is in INR. provider_mult converts the provider's rate into INR (1 if provider is in INR, 90 if in USD).
DEFAULTS = {"usdt_inr": "90", "ref_percent": "1", "upi_id": "yourupi@bank", "min_deposit": "50", "provider_mult": "1"}
SEED = {  # starting platforms/sections, all editable from the admin panel
    "Telegram": ["Members", "Post Views", "Reactions", "Refer"],
    "YouTube": ["Subscribers", "Views", "Likes", "Watch Time"],
    "Instagram": ["Followers", "Likes", "Views", "Comments"],
    "Facebook": ["Page Followers", "Post Likes", "Views"],
    "Twitter / X": ["Followers", "Likes", "Views", "Retweets"],
}
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id BIGINT PRIMARY KEY, balance NUMERIC(14,4) DEFAULT 0, ref_by BIGINT,
  banned BOOL DEFAULT FALSE, created TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, user_id BIGINT, service_id INT, service_name TEXT,
  link TEXT, qty INT, charge NUMERIC(14,4), provider_order TEXT, status TEXT DEFAULT 'Pending', created TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS deposits(id SERIAL PRIMARY KEY, user_id BIGINT, method TEXT, amount NUMERIC(14,4),
  status TEXT DEFAULT 'pending', txid TEXT, created TIMESTAMPTZ DEFAULT now());
ALTER TABLE deposits ADD COLUMN IF NOT EXISTS credit NUMERIC(14,4);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS platforms(id SERIAL PRIMARY KEY, name TEXT, active BOOL DEFAULT TRUE, pos INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS sections(id SERIAL PRIMARY KEY, platform_id INT REFERENCES platforms(id) ON DELETE CASCADE,
  name TEXT, active BOOL DEFAULT TRUE, pos INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS services(id SERIAL PRIMARY KEY, section_id INT REFERENCES sections(id) ON DELETE CASCADE,
  provider_id INT, name TEXT, descr TEXT DEFAULT '', rate NUMERIC(14,4), min_qty INT, max_qty INT,
  active BOOL DEFAULT TRUE, pos INT DEFAULT 0);
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE deposits ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE platforms ENABLE ROW LEVEL SECURITY;
ALTER TABLE sections ENABLE ROW LEVEL SECURITY;
ALTER TABLE services ENABLE ROW LEVEL SECURITY;
"""
TBL = {"p": "platforms", "s": "sections", "v": "services"}
BACKTO = {"p": "p", "s": "s", "v": "sv"}
PER = 8  # services per page for users

bot = Bot(TOKEN)
db: asyncpg.Pool = None
HTTP: aiohttp.ClientSession = None
SET = dict(DEFAULTS)    # settings cache (no DB hit per click)
CAT = {}                # catalog cache (no DB hit per click)
PS = {"t": 0, "d": {}}  # provider services cache (admin only)
BANNED = set()          # banned users cache (no DB hit per message)


# ======================= helpers =======================
def gs(k): return SET[k]
def money(x): return f"₹{D(x):,.2f}"
def B(t, c): return IB(text=t, callback_data=c)
def BACK(c): return [B("🔙 Back", c)]
def grid(btns, n): return [btns[i:i + n] for i in range(0, len(btns), n)]

def num(t):
    try:
        v = D(t.strip())
        return v if v.is_finite() and v > 0 else None
    except Exception:
        return None

def valid_setting(k, v):
    if k == "upi_id": return bool(v)
    try:
        d = D(v)
    except Exception:
        return False
    return d.is_finite() and (d >= 0 if k == "ref_percent" else d > 0)

def price_of(v, qty):
    return max((v["rate"] * qty / 1000).quantize(D("0.01")), D("0.01"))

def parse_price(t, cost):
    """'25' or '25%' = commission on provider cost, '₹40' or '=40' = fixed price per 1000."""
    t = t.strip().replace(",", "")
    try:
        if t[:1] in ("₹", "="):
            v = D(t[1:])
            return v.quantize(D("0.01")) if v.is_finite() and v > 0 else None
        v = D(t.rstrip("%"))
        if not v.is_finite() or v < 0 or not cost: return None
        return (cost * (1 + v / 100)).quantize(D("0.01"))
    except Exception:
        return None

async def tell(uid, text, **kw):
    try:
        await bot.send_message(uid, text, **kw)
    except Exception:
        pass

async def send(ev, text, kb=None, **kw):
    if isinstance(ev, CallbackQuery):
        res = await asyncio.gather(ev.answer(), ev.message.edit_text(text, reply_markup=kb, **kw), return_exceptions=True)
        if isinstance(res[1], Exception) and "not modified" not in str(res[1]):
            await ev.message.answer(text, reply_markup=kb, **kw)
    else:
        await ev.answer(text, reply_markup=kb, **kw)

async def load_settings():
    for row in await db.fetch("SELECT k, v FROM settings"):
        SET[row["k"]] = row["v"]

async def load_banned():
    BANNED.clear()
    BANNED.update(row["id"] for row in await db.fetch("SELECT id FROM users WHERE banned"))

async def seed():
    if await db.fetchval("SELECT 1 FROM settings WHERE k='seeded'"): return
    for i, (p, secs) in enumerate(SEED.items()):
        pid = await db.fetchval("INSERT INTO platforms(name,pos) VALUES($1,$2) RETURNING id", p, i)
        for j, s in enumerate(secs):
            await db.execute("INSERT INTO sections(platform_id,name,pos) VALUES($1,$2,$3)", pid, s, j)
    await db.execute("INSERT INTO settings VALUES('seeded','1') ON CONFLICT DO NOTHING")

async def wipe_once():
    """One-time clean start: removes all services (platforms and sections stay)."""
    if await db.fetchval("SELECT 1 FROM settings WHERE k='services_reset_v1'"): return
    await db.execute("DELETE FROM services")
    await db.execute("INSERT INTO settings VALUES('services_reset_v1','1') ON CONFLICT DO NOTHING")

async def load_catalog():
    P = await db.fetch("SELECT * FROM platforms WHERE active ORDER BY pos,id")
    Sx = await db.fetch("SELECT * FROM sections WHERE active ORDER BY pos,id")
    V = await db.fetch("SELECT * FROM services WHERE active ORDER BY pos,id")
    svc_by_sec = {}
    for v in V: svc_by_sec.setdefault(v["section_id"], []).append(v)
    sec_by_plat, sec_by_id = {}, {}
    for s in Sx:
        if s["id"] in svc_by_sec:  # empty sections stay hidden from users
            sec_by_plat.setdefault(s["platform_id"], []).append(s)
            sec_by_id[s["id"]] = s
    plat = [p for p in P if p["id"] in sec_by_plat]
    CAT.update(plat=plat, plat_by_id={p["id"]: p for p in plat}, sec_by_plat=sec_by_plat, sec_by_id=sec_by_id,
               svc_by_sec=svc_by_sec, svc_by_id={v["id"]: v for v in V})


# ======================= provider API (adapt here if your provider's format differs) =======================
async def api(**p):
    p["key"] = P_KEY
    async with HTTP.post(P_URL, data=p, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        return await resp.json(content_type=None)

async def pservices():
    if time.time() - PS["t"] > 300 or not PS["d"]:
        data = await api(action="services")
        if not isinstance(data, list): raise RuntimeError(str(data)[:150])
        PS["d"] = {int(s["service"]): s for s in data}
        PS["t"] = time.time()
    return PS["d"]

def pcost(ps):
    return D(str(ps["rate"])) * D(gs("provider_mult"))


async def credit_deposit(dep_id, txid=None):
    """Atomic + idempotent: credits user, pays referral bonus."""
    async with db.acquire() as c, c.transaction():
        d = await c.fetchrow("UPDATE deposits SET status='done', txid=COALESCE($2,txid) WHERE id=$1 AND status='pending' "
                             "RETURNING user_id, COALESCE(credit, amount) AS amt", dep_id, txid)
        if not d: return None
        await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", d["user_id"], d["amt"])
        ref = await c.fetchval("SELECT ref_by FROM users WHERE id=$1", d["user_id"])
        bonus = D(0)
        if ref:
            bonus = (d["amt"] * D(gs("ref_percent")) / 100).quantize(D("0.01"))
            await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", ref, bonus)
    await tell(d["user_id"], f"✅ {money(d['amt'])} added to your balance.")
    if ref and bonus > 0: await tell(ref, f"🎁 Referral bonus: {money(bonus)}")
    return d


# ======================= states / menu =======================
class S(StatesGroup):
    link = State(); qty = State(); usdt = State(); upi_amt = State(); upi_proof = State()

class AS(StatesGroup):   # add service (admin)
    pid = State(); comm = State(); pick = State(); name = State(); desc = State()

class E(StatesGroup):    # edits (admin)
    rename = State(); newplat = State(); newsec = State(); price = State(); desc = State()

MENU_TEXTS = {"🛒 New Order", "💰 Add Funds", "📦 My Orders", "🎁 Referral", "👤 Account", "🆘 Support"}
MENU = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
    [KB(text="🛒 New Order"), KB(text="💰 Add Funds")],
    [KB(text="📦 My Orders"), KB(text="🎁 Referral")],
    [KB(text="👤 Account"), KB(text="🆘 Support")]])

class BanMW(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        if u and u.id in BANNED and u.id not in ADMINS:
            return
        return await handler(event, data)

adm, r = Router(), Router()
adm.message.filter(F.from_user.id.in_(ADMINS))
adm.callback_query.filter(F.from_user.id.in_(ADMINS))


# ======================= ADMIN: views =======================
async def bail(m: Message, state: FSMContext):
    if m.text in MENU_TEXTS:
        await state.clear()
        await m.answer("Cancelled. Press the button again.")
        return True
    return False

async def a_home(ev):
    await send(ev, "🛠 Admin Panel", IM(inline_keyboard=[
        [B("➕ Add Service", "a:add"), B("🗂 Platforms & Services", "a:p")],
        [B("📊 Stats", "a:stats"), B("⚙️ Settings", "a:set")],
        [B("🧹 Clear all services", "a:wipe")]]))

async def stats_text():
    u = await db.fetchval("SELECT count(*) FROM users")
    o = await db.fetchrow("SELECT count(*) n, coalesce(sum(charge),0) s FROM orders")
    dep = await db.fetchval("SELECT coalesce(sum(COALESCE(credit, amount)),0) FROM deposits WHERE status='done'")
    return f"👥 Users: {u}\n📦 Orders: {o['n']} ({money(o['s'])})\n💰 Deposits: {money(dep)}"

async def v_platforms(ev):
    rows = await db.fetch("SELECT * FROM platforms ORDER BY pos,id")
    kb = [[B(f"{'✅' if p['active'] else '🙈'} {p['name']}", f"a:p:{p['id']}")] for p in rows]
    kb += [[B("➕ Add Platform", "a:np")], BACK("a:home")]
    await send(ev, "🗂 Platforms (tap to manage):", IM(inline_keyboard=kb))

async def v_platform(ev, pid):
    p = await db.fetchrow("SELECT * FROM platforms WHERE id=$1", pid)
    if not p: return await v_platforms(ev)
    secs = await db.fetch("SELECT s.*, (SELECT count(*) FROM services v WHERE v.section_id=s.id) n "
                          "FROM sections s WHERE platform_id=$1 ORDER BY pos,id", pid)
    kb = [[B(f"{'✅' if s['active'] else '🙈'} {s['name']} ({s['n']})", f"a:s:{s['id']}")] for s in secs]
    kb += [[B("➕ Add Section", f"a:ns:{pid}")],
           [B("✏️ Rename", f"a:r:p:{pid}"), B("👁 Show/Hide", f"a:t:p:{pid}")],
           [B("🗑 Delete platform", f"a:d:p:{pid}")], BACK("a:p")]
    await send(ev, f"{'✅' if p['active'] else '🙈'} {p['name']}\nSections (services count):", IM(inline_keyboard=kb))

async def v_section(ev, sid):
    s = await db.fetchrow("SELECT * FROM sections WHERE id=$1", sid)
    if not s: return await v_platforms(ev)
    vs = await db.fetch("SELECT * FROM services WHERE section_id=$1 ORDER BY pos,id", sid)
    lines, btns = [], []
    for n, v in enumerate(vs, 1):
        ps = PS["d"].get(v["provider_id"])
        warn = " ⚠️" if ps and v["rate"] < pcost(ps) else ""
        lines.append(f"{n}. {'✅' if v['active'] else '🙈'} {v['name']} | {money(v['rate'])}{warn}")
        btns.append(B(str(n), f"a:sv:{v['id']}"))
    kb = grid(btns, 5)
    kb += [[B("➕ Add Service", f"a:add:{sid}")],
           [B("✏️ Rename", f"a:r:s:{sid}"), B("👁 Show/Hide", f"a:t:s:{sid}")],
           [B("🗑 Delete section", f"a:d:s:{sid}")], BACK(f"a:p:{s['platform_id']}")]
    body = "\n".join(lines) + "\n\nTap a number to manage." if lines else "No services yet."
    await send(ev, f"{'✅' if s['active'] else '🙈'} {s['name']}\n\n{body}"[:4000], IM(inline_keyboard=kb))

async def v_service(ev, vid):
    v = await db.fetchrow("SELECT * FROM services WHERE id=$1", vid)
    if not v: return await a_home(ev)
    try:
        ps = (await pservices()).get(v["provider_id"])
    except Exception:
        ps = None
    if ps:
        cost = pcost(ps)
        profit = v["rate"] - cost
        pct = profit / cost * 100 if cost else D(0)
        info = f"Provider cost: {money(cost)}\nProfit: {money(profit)} ({pct:.1f}%)" + ("\n⚠️ PRICE IS BELOW COST!" if profit < 0 else "")
    else:
        info = "Provider cost: unavailable (API error or service removed)"
    txt = (f"{'✅ ON' if v['active'] else '🙈 OFF'}  {v['name']}\nProvider ID: {v['provider_id']}\n"
           f"Your price: {money(v['rate'])} /1000\n{info}\nMin {v['min_qty']} | Max {v['max_qty']}\n📝 {v['descr'] or '-'}")
    await send(ev, txt, IM(inline_keyboard=[
        [B("💰 Price", f"a:pr:{vid}"), B("✏️ Name", f"a:r:v:{vid}")],
        [B("📝 Description", f"a:ds:{vid}"), B("👁 On/Off", f"a:t:v:{vid}")],
        [B("🗑 Delete", f"a:d:v:{vid}")], BACK(f"a:s:{v['section_id']}")]))

async def after(ev, k, i):
    if k == "p": await v_platform(ev, i)
    elif k == "s": await v_section(ev, i)
    else: await v_service(ev, i)


# ======================= ADMIN: commands =======================
@adm.message(Command("admin"))
async def admin_cmd(m: Message, state: FSMContext):
    await state.clear()
    await a_home(m)

@adm.message(Command("cancel"))
async def cancel_cmd(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Cancelled.", reply_markup=MENU)

@adm.message(Command("user"))
async def user_info(m: Message, command: CommandObject):
    if not (command.args or "").isdigit(): return await m.answer("/user <id>")
    uid = int(command.args)
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not u: return await m.answer("Not found")
    n = await db.fetchval("SELECT count(*) FROM orders WHERE user_id=$1", uid)
    await m.answer(f"ID {u['id']}\nBalance {money(u['balance'])}\nOrders: {n}\nBanned: {u['banned']}\nRef by: {u['ref_by']}")

@adm.message(Command("addbal"))
async def addbal(m: Message, command: CommandObject):
    a = (command.args or "").split()
    amt = None
    try:
        amt = D(a[1])
        if not amt.is_finite(): amt = None
    except Exception:
        pass
    if len(a) != 2 or not a[0].isdigit() or amt is None: return await m.answer("/addbal <id> <amount in ₹> (negative to deduct)")
    ok = await db.fetchval("UPDATE users SET balance=balance+$2 WHERE id=$1 RETURNING 1", int(a[0]), amt)
    if ok: await tell(int(a[0]), f"💰 Balance adjusted by {money(amt)}")
    await m.answer("✅ Done" if ok else "User not found")

@adm.message(Command("set"))
async def set_cmd(m: Message, command: CommandObject):
    a = (command.args or "").split(None, 1)
    if len(a) != 2 or a[0] not in DEFAULTS or not valid_setting(a[0], a[1].strip()):
        return await m.answer("/set <" + "|".join(DEFAULTS) + "> <value>")
    v = a[1].strip()
    await db.execute("INSERT INTO settings VALUES($1,$2) ON CONFLICT(k) DO UPDATE SET v=$2", a[0], v)
    SET[a[0]] = v
    await m.answer("✅ Updated")

@adm.message(Command("ban", "unban"))
async def ban(m: Message, command: CommandObject):
    if not (command.args or "").isdigit(): return await m.answer("/ban <id>")
    uid, b = int(command.args), command.command == "ban"
    await db.execute("UPDATE users SET banned=$2 WHERE id=$1", uid, b)
    (BANNED.add if b else BANNED.discard)(uid)
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
        await m.answer(f"Error: {str(e)[:150]}")


# ======================= ADMIN: panel buttons =======================
async def ask(cb: CallbackQuery, state: FSMContext, st, text, **data):
    await state.set_state(st)
    await state.update_data(**data)
    await cb.message.answer(text + "\n(/cancel to stop)")
    await cb.answer()

@adm.callback_query(F.data.startswith("a:"))
async def a_cb(cb: CallbackQuery, state: FSMContext):
    p = cb.data.split(":")
    act, x = p[1], p[2:]
    await state.clear()
    if act == "home": return await a_home(cb)
    if act == "p": return await (v_platform(cb, int(x[0])) if x else v_platforms(cb))
    if act == "s": return await v_section(cb, int(x[0]))
    if act == "sv": return await v_service(cb, int(x[0]))
    if act == "stats": return await send(cb, await stats_text(), IM(inline_keyboard=[BACK("a:home")]))
    if act == "set":
        cur = "\n".join(f"{k} = {gs(k)}" for k in DEFAULTS)
        return await send(cb, "⚙️ Settings\n" + cur + "\n\nChange: /set <key> <value>\nOther commands:\n/addbal id ₹  /user id\n"
                          "/ban id  /unban id\n/broadcast text\n/providerbal", IM(inline_keyboard=[BACK("a:home")]))
    if act == "t":
        await db.execute(f"UPDATE {TBL[x[0]]} SET active = NOT active WHERE id=$1", int(x[1]))
        await load_catalog()
        return await after(cb, x[0], int(x[1]))
    if act == "d":
        k, i = x[0], int(x[1])
        return await send(cb, "⚠️ Delete this? Everything inside it is deleted too.",
                          IM(inline_keyboard=[[B("✅ Yes, delete", f"a:dd:{k}:{i}"), B("❌ No", f"a:{BACKTO[k]}:{i}")]]))
    if act == "dd":
        k, i = x[0], int(x[1])
        parent = None
        if k == "s": parent = await db.fetchval("SELECT platform_id FROM sections WHERE id=$1", i)
        if k == "v": parent = await db.fetchval("SELECT section_id FROM services WHERE id=$1", i)
        await db.execute(f"DELETE FROM {TBL[k]} WHERE id=$1", i)
        await load_catalog()
        if k == "p" or parent is None: return await v_platforms(cb)
        return await (v_platform(cb, parent) if k == "s" else v_section(cb, parent))
    if act == "wipe":
        return await send(cb, "⚠️ Delete ALL services? Platforms and sections stay.",
                          IM(inline_keyboard=[[B("✅ Yes, delete all", "a:wipe2"), B("❌ No", "a:home")]]))
    if act == "wipe2":
        await db.execute("DELETE FROM services")
        await load_catalog()
        return await a_home(cb)
    if act == "np": return await ask(cb, state, E.newplat, "Send the new platform name:")
    if act == "ns": return await ask(cb, state, E.newsec, "Send the new section name:", pid=int(x[0]))
    if act == "r": return await ask(cb, state, E.rename, "Send the new name:", k=x[0], id=int(x[1]))
    if act == "pr":
        return await ask(cb, state, E.price, "Send commission % on provider cost (e.g. 25) or a fixed price like ₹40 (per 1000):", vid=int(x[0]))
    if act == "ds": return await ask(cb, state, E.desc, "Send the new description (send - to clear):", vid=int(x[0]))
    if act == "add":
        if not await db.fetchval("SELECT 1 FROM sections"):
            return await cb.answer("Create a platform and section first.", show_alert=True)
        return await ask(cb, state, AS.pid, "Send the provider's service ID:", sec=int(x[0]) if x else None)
    await cb.answer()


# ======================= ADMIN: edit flows =======================
@adm.message(E.newplat, F.text)
async def e_newplat(m: Message, state: FSMContext):
    if await bail(m, state): return
    await state.clear()
    await db.execute("INSERT INTO platforms(name,pos) VALUES($1,(SELECT coalesce(max(pos),0)+1 FROM platforms))", m.text.strip()[:40])
    await load_catalog()
    await v_platforms(m)

@adm.message(E.newsec, F.text)
async def e_newsec(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    await state.clear()
    await db.execute("INSERT INTO sections(platform_id,name,pos) VALUES($1,$2,(SELECT coalesce(max(pos),0)+1 FROM sections))", d["pid"], m.text.strip()[:40])
    await load_catalog()
    await v_platform(m, d["pid"])

@adm.message(E.rename, F.text)
async def e_rename(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    await state.clear()
    await db.execute(f"UPDATE {TBL[d['k']]} SET name=$2 WHERE id=$1", d["id"], m.text.strip()[:80])
    await load_catalog()
    await after(m, d["k"], d["id"])

@adm.message(E.desc, F.text)
async def e_desc(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    await state.clear()
    await db.execute("UPDATE services SET descr=$2 WHERE id=$1", d["vid"], "" if m.text.strip() == "-" else m.text.strip()[:500])
    await load_catalog()
    await v_service(m, d["vid"])

@adm.message(E.price, F.text)
async def e_price(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    v = await db.fetchrow("SELECT * FROM services WHERE id=$1", d["vid"])
    if not v:
        await state.clear()
        return await m.answer("Service no longer exists.")
    try:
        ps = (await pservices()).get(v["provider_id"])
        cost = pcost(ps) if ps else None
    except Exception:
        cost = None
    price = parse_price(m.text, cost)
    if not price:
        return await m.answer("Invalid. Send a % like 25, or a fixed price like ₹40." + ("" if cost else "\n(Provider cost unavailable, so only fixed ₹ price works.)"))
    await state.clear()
    await db.execute("UPDATE services SET rate=$2 WHERE id=$1", d["vid"], price)
    await load_catalog()
    await v_service(m, d["vid"])


# ======================= ADMIN: add service flow =======================
# service ID -> provider price shown -> commission % -> platform/section -> name -> description
async def ask_name(m: Message, state: FSMContext):
    d = await state.get_data()
    await state.set_state(AS.name)
    await m.answer(f"Send the name users will see (or - to use the provider's name):\n{d['pname']}")

async def pick_plat(ev):
    rows = await db.fetch("SELECT * FROM platforms ORDER BY pos,id")
    kb = grid([B(p["name"], f"ap:p:{p['id']}") for p in rows], 2) + [[B("❌ Cancel", "ap:x")]]
    await send(ev, "Choose the platform for this service:", IM(inline_keyboard=kb))

@adm.message(AS.pid, F.text)
async def as_pid(m: Message, state: FSMContext):
    if await bail(m, state): return
    t = m.text.strip()
    if not t.isdigit(): return await m.answer("Send the numeric service ID.")
    try:
        ps = (await pservices()).get(int(t))
    except Exception as e:
        return await m.answer(f"Provider API error: {str(e)[:150]}")
    if not ps: return await m.answer("ID not found at the provider. Send another ID.")
    try:
        cost, mn, mx = pcost(ps), int(ps["min"]), int(ps["max"])
    except Exception:
        return await m.answer("Provider data for this service is invalid. Try another ID.")
    await state.update_data(prov=int(t), cost=str(cost), pname=str(ps["name"])[:80], mn=mn, mx=mx)
    await state.set_state(AS.comm)
    await m.answer(f"📌 {ps['name']}\nProvider ID: {t}\nProvider price: {money(cost)} /1000\nMin {mn} | Max {mx}\n\n"
                   f"Send your commission % (e.g. 25) — or a fixed price like ₹40 per 1000.\n(/cancel to stop)")

@adm.message(AS.comm, F.text)
async def as_comm(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    cost = D(d["cost"])
    price = parse_price(m.text, cost)
    if not price: return await m.answer("Invalid. Send a % like 25, or a fixed price like ₹40.")
    await state.update_data(rate=str(price))
    await m.answer(f"💰 Your price: {money(price)} /1000\nProfit: {money(price - cost)} per 1000" + ("\n⚠️ Below provider cost!" if price < cost else ""))
    if d.get("sec"): return await ask_name(m, state)
    await state.set_state(AS.pick)
    await pick_plat(m)

@adm.message(AS.pick)
async def as_pick_text(m: Message, state: FSMContext):
    if await bail(m, state): return
    await m.answer("Use the buttons above to choose a platform and section.")

@adm.callback_query(F.data.startswith("ap:"), AS.pick)
async def as_pick(cb: CallbackQuery, state: FSMContext):
    p = cb.data.split(":")
    if p[1] == "x":
        await state.clear()
        await cb.answer()
        return await cb.message.edit_text("Cancelled.")
    if p[1] == "b": return await pick_plat(cb)
    if p[1] == "p":
        secs = await db.fetch("SELECT * FROM sections WHERE platform_id=$1 ORDER BY pos,id", int(p[2]))
        if not secs: return await cb.answer("No sections here. Add a section first.", show_alert=True)
        kb = grid([B(s["name"], f"ap:s:{s['id']}") for s in secs], 2) + [BACK("ap:b")]
        return await send(cb, "Choose the section:", IM(inline_keyboard=kb))
    if p[1] == "s":
        await state.update_data(sec=int(p[2]))
        await cb.answer()
        return await ask_name(cb.message, state)
    await cb.answer()

@adm.message(AS.name, F.text)
async def as_name(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    t = m.text.strip()
    await state.update_data(name=d["pname"] if t == "-" else t[:80])
    await state.set_state(AS.desc)
    await m.answer("Send a short description (or - to skip):")

@adm.message(AS.desc, F.text)
async def as_desc(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    t = m.text.strip()
    try:
        vid = await db.fetchval(
            "INSERT INTO services(section_id,provider_id,name,descr,rate,min_qty,max_qty,pos) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,(SELECT coalesce(max(pos),0)+1 FROM services WHERE section_id=$1)) RETURNING id",
            d["sec"], d["prov"], d["name"], "" if t == "-" else t[:500], D(d["rate"]), d["mn"], d["mx"])
    except Exception as e:
        await state.clear()
        return await m.answer(f"Could not add: {str(e)[:150]}")
    await state.clear()
    await load_catalog()
    await m.answer("✅ Service added.")
    await v_service(m, vid)


# ======================= USER: menu buttons =======================
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

@r.message(Command("cancel"))
async def user_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Cancelled.", reply_markup=MENU)

@r.message(F.text == "👤 Account")
async def account(m: Message, state: FSMContext):
    await state.clear()
    u = await db.fetchrow("INSERT INTO users(id) VALUES($1) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id RETURNING *", m.from_user.id)
    n = await db.fetchval("SELECT count(*) FROM orders WHERE user_id=$1", u["id"])
    await m.answer(f"👤 ID: {u['id']}\n💰 Balance: {money(u['balance'])}\n📦 Orders: {n}")

@r.message(F.text == "🆘 Support")
async def support(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(f"Contact: {SUPPORT}")

@r.message(F.text == "🎁 Referral")
async def referral(m: Message, state: FSMContext):
    await state.clear()
    me = await bot.get_me()
    n = await db.fetchval("SELECT count(*) FROM users WHERE ref_by=$1", m.from_user.id)
    await m.answer(f"🎁 Earn {gs('ref_percent')}% of every deposit your referrals make.\n\n"
                   f"Your link:\nhttps://t.me/{me.username}?start=ref_{m.from_user.id}\n\nReferrals: {n}")

@r.message(F.text == "📦 My Orders")
async def my_orders(m: Message, state: FSMContext):
    await state.clear()
    rows = await db.fetch("SELECT * FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT 10", m.from_user.id)
    if not rows: return await m.answer("No orders yet.")
    await m.answer("\n\n".join(f"#{o['id']} • {o['service_name'][:40]}\nQty {o['qty']} • {money(o['charge'])} • {o['status']}" for o in rows))

@r.message(F.text == "💰 Add Funds")
async def funds(m: Message, state: FSMContext):
    await state.clear()
    rows = [[B(f"💎 {c['name']} - Auto", f"f:{k}")] for k, c in ON.items()]
    rows.append([B("🇮🇳 UPI - Manual", "f:upi")])
    await m.answer("Choose payment method:", reply_markup=IM(inline_keyboard=rows))


# ======================= USER: ordering (platform -> section -> service) =======================
@r.message(F.text == "🛒 New Order")
@r.callback_query(F.data == "o:h")
async def order_home(ev, state: FSMContext):
    await state.clear()
    plat = CAT.get("plat", [])
    if not plat: return await send(ev, "⚠️ No services available right now.")
    kb = grid([B(p["name"], f"o:p:{p['id']}") for p in plat], 2)
    await send(ev, "🛒 Choose a platform:", IM(inline_keyboard=kb))

@r.callback_query(F.data.startswith("o:p:"))
async def o_platform(cb: CallbackQuery):
    p = CAT.get("plat_by_id", {}).get(int(cb.data[4:]))
    if not p: return await cb.answer("Updated, open New Order again", show_alert=True)
    secs = CAT["sec_by_plat"].get(p["id"], [])
    kb = grid([B(s["name"], f"o:s:{s['id']}:0") for s in secs], 2) + [BACK("o:h")]
    await send(cb, f"{p['name']}\nChoose a section:", IM(inline_keyboard=kb))

@r.callback_query(F.data.startswith("o:s:"))
async def o_section(cb: CallbackQuery):
    _, _, sid, pg = cb.data.split(":")
    s = CAT.get("sec_by_id", {}).get(int(sid))
    if not s: return await cb.answer("Updated, open New Order again", show_alert=True)
    vs = CAT["svc_by_sec"][s["id"]]
    pg = max(0, min(int(pg), (len(vs) - 1) // PER))
    start_i = pg * PER
    chunk = vs[start_i:start_i + PER]
    lines = [f"{start_i + n}. {v['name']}\n    {money(v['rate'])} / 1000" for n, v in enumerate(chunk, 1)]
    kb = grid([B(str(start_i + n), f"o:v:{v['id']}") for n, v in enumerate(chunk, 1)], 4)
    nav = []
    if pg > 0: nav.append(B("⬅️", f"o:s:{s['id']}:{pg - 1}"))
    if start_i + PER < len(vs): nav.append(B("➡️", f"o:s:{s['id']}:{pg + 1}"))
    if nav: kb.append(nav)
    kb.append(BACK(f"o:p:{s['platform_id']}"))
    await send(cb, (f"{s['name']}\n\n" + "\n\n".join(lines) + "\n\nTap a number to select.")[:4000], IM(inline_keyboard=kb))

@r.callback_query(F.data.startswith("o:v:"))
async def o_service(cb: CallbackQuery, state: FSMContext):
    v = CAT.get("svc_by_id", {}).get(int(cb.data[4:]))
    if not v: return await cb.answer("Service not available", show_alert=True)
    await state.set_state(S.link)
    await state.update_data(vid=v["id"])
    await cb.message.answer(f"✅ {v['name']}\n💵 {money(v['rate'])} per 1000\n📉 Min {v['min_qty']} | Max {v['max_qty']}"
                            + (f"\n\n📝 {v['descr']}" if v["descr"] else "") + "\n\nSend the link:")
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
    v = CAT.get("svc_by_id", {}).get(d.get("vid"))
    if not v:
        await state.clear()
        return await m.answer("Service not available. Start again from 🛒 New Order.")
    if not m.text.isdigit(): return await m.answer("Send a number.")
    q = int(m.text)
    if not v["min_qty"] <= q <= v["max_qty"]: return await m.answer(f"Quantity must be {v['min_qty']} - {v['max_qty']}.")
    await state.update_data(qty=q)
    await m.answer(f"🧾 {v['name']}\nQty: {q}\nLink: {d['link']}\nPrice: {money(price_of(v, q))}",
                   reply_markup=IM(inline_keyboard=[[B("✅ Confirm", "ok"), B("❌ Cancel", "no")]]))

@r.callback_query(F.data == "no")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("Cancelled.")
    await cb.answer()

@r.callback_query(F.data == "ok")
async def confirm(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()  # cleared first, so a double tap can't place two orders
    v = CAT.get("svc_by_id", {}).get(d.get("vid"))
    if not v or "qty" not in d: return await cb.answer("Expired, start again", show_alert=True)
    await cb.answer()
    uid, charge = cb.from_user.id, price_of(v, d["qty"])
    if not await db.fetchval("UPDATE users SET balance=balance-$2 WHERE id=$1 AND balance>=$2 RETURNING 1", uid, charge):
        return await cb.message.edit_text("❌ Insufficient balance. Use 💰 Add Funds.")
    try:
        res = await api(action="add", service=v["provider_id"], link=d["link"], quantity=d["qty"])
    except Exception as e:
        res = {"error": str(e)}
    if isinstance(res, dict) and "order" in res:
        oid = await db.fetchval("INSERT INTO orders(user_id,service_id,service_name,link,qty,charge,provider_order) "
                                "VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id", uid, v["id"], v["name"], d["link"], d["qty"], charge, str(res["order"]))
        await cb.message.edit_text(f"✅ Order #{oid} placed. Charged {money(charge)}")
    else:
        await db.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", uid, charge)
        err = res.get("error", "Unknown error") if isinstance(res, dict) else "Unknown error"
        await cb.message.edit_text(f"❌ Order failed, refunded.\n{str(err)[:150]}")


# ======================= USER: add funds =======================
@r.callback_query(F.data.startswith("f:"))
async def fund_method(cb: CallbackQuery, state: FSMContext):
    k, mn = cb.data[2:], D(gs("min_deposit"))
    if k in ON:
        await state.set_state(S.usdt)
        await state.update_data(chain=k)
        await cb.message.answer(f"{ON[k]['name']}\n1 USDT = {money(gs('usdt_inr'))}\nEnter amount in USDT (min {money(mn)} worth):")
    elif k == "upi":
        await state.set_state(S.upi_amt)
        await cb.message.answer(f"Enter amount in ₹ (min {money(mn)}):")
    await cb.answer()

@r.message(S.usdt, F.text)
async def usdt_amt(m: Message, state: FSMContext):
    k = (await state.get_data()).get("chain")
    if k not in ON:
        await state.clear()
        return await m.answer("Start again from 💰 Add Funds.")
    rate = D(gs("usdt_inr"))
    base = num(m.text)
    if base: base = base.quantize(D("0.01"))
    if not base or base * rate < D(gs("min_deposit")): return await m.answer(f"Invalid amount. Minimum is {money(gs('min_deposit'))} worth of USDT.")
    for _ in range(50):  # unique decimals so we can match the payment
        amt = base + D(random.randint(1, 99)) / 1000
        if not await db.fetchval("SELECT 1 FROM deposits WHERE method=$1 AND status='pending' AND amount=$2", k, amt): break
    credit = (amt * rate).quantize(D("0.01"))
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount,credit) VALUES($1,$2,$3,$4) RETURNING id",
                            m.from_user.id, k, amt, credit)
    await state.clear()
    c = ON[k]
    await m.answer(f"💎 Deposit #{dep}\nNetwork: {c['name']}\nSend EXACTLY <code>{amt}</code> USDT to:\n<code>{c['wallet']}</code>\n\n"
                   f"You will receive: {money(credit)}\n⏱ Valid 30 min. Balance is added automatically after confirmation.\n"
                   f"⚠️ Use ONLY this network and the exact amount, otherwise funds can be lost.", parse_mode="HTML")

@r.message(S.upi_amt, F.text)
async def upi_amt(m: Message, state: FSMContext):
    inr = num(m.text)
    if inr: inr = inr.quantize(D("0.01"))
    if not inr or inr < D(gs("min_deposit")): return await m.answer(f"Invalid amount. Minimum is {money(gs('min_deposit'))}.")
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount) VALUES($1,'upi',$2) RETURNING id", m.from_user.id, inr)
    await state.set_state(S.upi_proof)
    await state.update_data(dep=dep, inr=str(inr))
    await m.answer(f"🇮🇳 Deposit #{dep}\nPay {money(inr)} to UPI ID:\n<code>{gs('upi_id')}</code>\n\n"
                   f"Then send the UTR number or a payment screenshot here.", parse_mode="HTML")

@r.message(S.upi_proof)
async def upi_proof(m: Message, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    kb = IM(inline_keyboard=[[B("✅ Approve", f"ad:ok:{d['dep']}"), B("❌ Reject", f"ad:no:{d['dep']}")]])
    for a in ADMINS:
        try:
            await m.copy_to(a)
            await bot.send_message(a, f"UPI deposit #{d['dep']}\nUser: {m.from_user.id}\nAmount: {money(d['inr'])}", reply_markup=kb)
        except Exception:
            pass
    await m.answer("⏳ Sent for verification. Balance will be added once approved.")

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
    await cb.answer()


# ======================= background jobs =======================
async def rpc(url, method, params):
    async with HTTP.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                         timeout=aiohttp.ClientTimeout(total=25)) as resp:
        j = await resp.json(content_type=None)
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
    async with HTTP.get(url, headers={"TRON-PRO-API-KEY": TRON_KEY} if TRON_KEY else {},
                        timeout=aiohttp.ClientTimeout(total=20)) as resp:
        txs = (await resp.json(content_type=None)).get("data", [])
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
                st = res.get("status") if isinstance(res, dict) else None
                if not st or st == o["status"]: continue
                refund = D(0)
                if st in ("Canceled", "Refunded"): refund = o["charge"]
                elif st == "Partial" and o["qty"]:
                    refund = (o["charge"] * int(res.get("remains") or 0) / o["qty"]).quantize(D("0.01"))
                async with db.acquire() as c, c.transaction():
                    if not await c.fetchval("UPDATE orders SET status=$2 WHERE id=$1 AND status=$3 RETURNING 1", o["id"], st, o["status"]):
                        continue
                    if refund > 0:
                        await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", o["user_id"], refund)
                if st in ("Completed", "Partial", "Canceled", "Refunded"):
                    extra = f" Refunded {money(refund)}." if refund > 0 else ""
                    await tell(o["user_id"], f"📦 Order #{o['id']}: {st}.{extra}")
        except Exception as e:
            logging.warning("status loop: %s", e)


async def main():
    global db, HTTP
    logging.basicConfig(level=logging.INFO)
    db = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    HTTP = aiohttp.ClientSession()
    await db.execute(SCHEMA)
    await seed()
    await wipe_once()
    await load_settings()
    await load_catalog()
    await load_banned()
    dp = Dispatcher()
    dp.update.outer_middleware(BanMW())
    dp.include_routers(adm, r)
    tasks = [asyncio.create_task(crypto_poller()), asyncio.create_task(status_loop())]
    try:
        await dp.start_polling(bot)
    finally:
        for t in tasks: t.cancel()
        await HTTP.close()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
