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

# Everything is INR. provider_mult converts provider rate to INR (1 if provider bills in INR, 90 if in USD).
DEFAULTS = {"usdt_inr": "90", "ref_percent": "1", "upi_id": "yourupi@bank", "min_deposit": "50", "provider_mult": "1"}
SEED = {
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

bot = Bot(TOKEN)
db: asyncpg.Pool = None
HTTP: aiohttp.ClientSession = None
SET = dict(DEFAULTS)    # settings cache
CAT = {}                # catalog cache
PS = {"t": 0, "d": {}}  # provider services cache
BANNED = set()          # banned user ids cache


# ======================= helpers =======================
def gs(k): return SET[k]
def money(x): return f"₹{D(x):,.2f}"
def B(t, c): return IB(text=t, callback_data=c)
def BACK(c): return [B("🔙 Back", c)]

def num(t):
    try:
        v = D(t.strip())
        return v if v > 0 else None
    except Exception:
        return None

def price_of(v, qty):
    return max((v["rate"] * qty / 1000).quantize(D("0.01")), D("0.01"))

def parse_price(t, cost):
    """'25' or '25%' = commission on provider cost; '₹40' or '=40' = fixed price per 1000."""
    t = t.strip().replace(",", "")
    try:
        if t[:1] in ("₹", "="):
            v = D(t[1:])
            return v.quantize(D("0.01")) if v > 0 else None
        v = D(t.rstrip("%"))
    except Exception:
        return None
    if v < 0 or not cost: return None
    return (cost * (1 + v / 100)).quantize(D("0.01"))

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
    BANNED.update(r["id"] for r in await db.fetch("SELECT id FROM users WHERE banned"))

async def seed():
    if await db.fetchval("SELECT 1 FROM settings WHERE k='seeded'"): return
    for i, (p, secs) in enumerate(SEED.items()):
        pid = await db.fetchval("INSERT INTO platforms(name,pos) VALUES($1,$2) RETURNING id", p, i)
        for j, s in enumerate(secs):
            await db.execute("INSERT INTO sections(platform_id,name,pos) VALUES($1,$2,$3)", pid, s, j)
    await db.execute("INSERT INTO settings VALUES('seeded','1') ON CONFLICT DO NOTHING")

async def load_catalog():
    P = await db.fetch("SELECT * FROM platforms WHERE active ORDER BY pos,id")
    Sx = await db.fetch("SELECT * FROM sections WHERE active ORDER BY pos,id")
    V = await db.fetch("SELECT * FROM services WHERE active ORDER BY pos,id")
    svc_by_sec = {}
    for v in V: svc_by_sec.setdefault(v["section_id"], []).append(v)
    sec_by_plat, sec_by_id = {}, {}
    for s in Sx:
        if s["id"] in svc_by_sec:  # empty sections hidden from users
            sec_by_plat.setdefault(s["platform_id"], []).append(s)
            sec_by_id[s["id"]] = s
    plat = [p for p in P if p["id"] in sec_by_plat]
    CAT.update(plat=plat, plat_by_id={p["id"]: p for p in plat}, sec_by_plat=sec_by_plat, sec_by_id=sec_by_id,
               svc_by_sec=svc_by_sec, svc_by_id={v["id"]: v for v in V})


# ======================= provider API =======================
async def api(**p):
    p["key"] = P_KEY
    async with HTTP.post(P_URL, data=p, timeout=aiohttp.ClientTimeout(total=30)) as r:
        return await r.json(content_type=None)

async def pservices():
    if time.time() - PS["t"] > 300 or not PS["d"]:
        data = await api(action="services")
        PS["d"] = {int(s["service"]): s for s in data}
        PS["t"] = time.time()
    return PS["d"]

def pcost(ps):
    return D(str(ps["rate"])) * D(gs("provider_mult"))

async def credit_deposit(dep_id, txid=None):
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

class AS(StatesGroup):   # add service
    pid = State(); comm = State(); pick = State(); name = State(); desc = State()

class E(StatesGroup):    # edits
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


# ======================= ADMIN =======================
async def bail(m: Message, state: FSMContext):
    if m.text in MENU_TEXTS:
        await state.clear()
        await m.answer("Cancelled. Press the button again.")
        return True
    return False

async def a_home(ev):
    await send(ev, "🛠 Admin Panel", IM(inline_keyboard=[
        [B("➕ Add Service", "a:add"), B("🗂 Platforms & Services", "a:p")],
        [B("📊 Stats", "a:stats"), B("⚙️ Settings", "a:set")]]))

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
    lines, kb = [], []
    for i, v in enumerate(vs, 1):
        ps = PS["d"].get(v["provider_id"])
        warn = " ⚠️ below cost" if ps and v["rate"] < pcost(ps) else ""
        lines.append(f"{i}. {'✅' if v['active'] else '🙈'} {v['name']} — {money(v['rate'])}/1k{warn}")
        kb.append(B(str(i), f"a:sv:{v['id']}"))
    rows = [kb[i:i + 5] for i in range(0, len(kb), 5)]
    rows += [[B("➕ Add Service here", f"a:add:{sid}")],
             [B("✏️ Rename", f"a:r:s:{sid}"), B("👁 Show/Hide", f"a:t:s:{sid}")],
             [B("🗑 Delete section", f"a:d:s:{sid}")], BACK(f"a:p:{s['platform_id']}")]
    await send(ev, f"{'✅' if s['active'] else '🙈'} {s['name']}\n\n" + ("\n".join(lines) or "No services yet.") +
               "\n\nTap a number to manage:", IM(inline_keyboard=rows))

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

async def pick_platform(ev):
    rows = await db.fetch("SELECT * FROM platforms ORDER BY pos,id")
    await send(ev, "Pick a platform for this service:", IM(inline_keyboard=[[B(p["name"], f"k:p:{p['id']}")] for p in rows]))

# ---- admin commands (registered first so they win over state handlers) ----
@adm.message(Command("admin"))
async def admin_cmd(m: Message, state: FSMContext):
    await state.clear()
    await a_home(m)

@adm.message(Command("cancel"))
async def cancel_cmd(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Cancelled.", reply_markup=MENU)

@adm.message(Command("set"))
async def set_cmd(m: Message, command: CommandObject):
    a = (command.args or "").split(None, 1)
    if len(a) != 2 or a[0] not in DEFAULTS or (a[0] != "upi_id" and num(a[1]) is None):
        return await m.answer("/set <" + "|".join(DEFAULTS) + "> <value>")
    await db.execute("INSERT INTO settings VALUES($1,$2) ON CONFLICT(k) DO UPDATE SET v=$2", a[0], a[1].strip())
    SET[a[0]] = a[1].strip()
    await m.answer("✅ Updated")

@adm.message(Command("user"))
async def user_info(m: Message, command: CommandObject):
    if not (command.args or "").isdigit(): return await m.answer("/user <id>")
    u = await db.fetchrow("SELECT * FROM users WHERE id=$1", int(command.args))
    await m.answer(f"ID {u['id']}\nBalance {money(u['balance'])}\nBanned: {u['banned']}\nRef by: {u['ref_by']}" if u else "Not found")

@adm.message(Command("addbal"))
async def addbal(m: Message, command: CommandObject):
    a = (command.args or "").split()
    amt = None
    try: amt = D(a[1])
    except Exception: pass
    if len(a) != 2 or not a[0].isdigit() or amt is None: return await m.answer("/addbal <id> <₹> (negative to deduct)")
    ok = await db.fetchval("UPDATE users SET balance=balance+$2 WHERE id=$1 RETURNING 1", int(a[0]), amt)
    if ok: await tell(int(a[0]), f"💰 Balance adjusted by {money(amt)}")
    await m.answer("✅ Done" if ok else "User not found")

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
        await m.answer(f"Error: {e}")

@adm.message(Command("resetservices"))
async def reset_services(m: Message, command: CommandObject):
    if (command.args or "").strip() != "yes":
        return await m.answer("This deletes ALL services (platforms & sections stay).\nSend: /resetservices yes")
    await db.execute("DELETE FROM services")
    await load_catalog()
    await m.answer("🗑 All services deleted. Add new ones from /admin → ➕ Add Service.")

# ---- admin panel callbacks ----
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
        return await send(cb, "⚙️ Settings\n" + cur + "\n\nChange: /set <key> <value>\n\nOther commands:\n/addbal id ₹\n/user id\n"
                          "/ban id  /unban id\n/broadcast text\n/providerbal\n/resetservices", IM(inline_keyboard=[BACK("a:home")]))
    if act == "t":
        await db.execute(f"UPDATE {TBL[x[0]]} SET active = NOT active WHERE id=$1", int(x[1]))
        await load_catalog()
        return await after(cb, x[0], int(x[1]))
    if act == "d":
        k, i = x[0], int(x[1])
        back = {"p": "p", "s": "s", "v": "sv"}[k]
        return await send(cb, "⚠️ Delete this? Everything inside it is deleted too.",
                          IM(inline_keyboard=[[B("✅ Yes, delete", f"a:dd:{k}:{i}"), B("❌ No", f"a:{back}:{i}")]]))
    if act == "dd":
        k, i = x[0], int(x[1])
        parent = None
        if k == "s": parent = await db.fetchval("SELECT platform_id FROM sections WHERE id=$1", i)
        if k == "v": parent = await db.fetchval("SELECT section_id FROM services WHERE id=$1", i)
        await db.execute(f"DELETE FROM {TBL[k]} WHERE id=$1", i)
        await load_catalog()
        if k == "p": return await v_platforms(cb)
        return await (v_platform(cb, parent) if k == "s" else v_section(cb, parent))
    prompts = {"np": ("Send the new platform name:", E.newplat, {}),
               "ns": ("Send the new section name:", E.newsec, {"pid": int(x[0])}),
               "r": ("Send the new name:", E.rename, {"k": x[0], "id": int(x[1])}),
               "pr": ("Send commission % (e.g. 25) or a fixed price like ₹40 (per 1000):", E.price, {"vid": int(x[0])}),
               "ds": ("Send the new description (send - to clear):", E.desc, {"vid": int(x[0])})}
    if act in prompts:
        text, st, data = prompts[act]
        await state.set_state(st)
        await state.update_data(**data)
        await cb.message.answer(text + "\n(/cancel to stop)")
        return await cb.answer()
    if act == "add":
        if not await db.fetchval("SELECT 1 FROM sections"):
            return await cb.answer("Create a platform and section first.", show_alert=True)
        await state.set_state(AS.pid)
        await state.update_data(sid=int(x[0]) if x else None)
        await cb.message.answer("Send the provider's service ID:\n(/cancel to stop)")
        return await cb.answer()

@adm.callback_query(F.data.startswith("ad:"))
async def deposit_cb(cb: CallbackQuery):
    _, act, i = cb.data.split(":")
    if act == "ok":
        txt = "✅ Approved" if await credit_deposit(int(i)) else "Already processed"
    else:
        uid = await db.fetchval("UPDATE deposits SET status='rejected' WHERE id=$1 AND status='pending' RETURNING user_id", int(i))
        if uid: await tell(uid, f"❌ Deposit #{i} was rejected. Contact support.")
        txt = "❌ Rejected" if uid else "Already processed"
    await cb.message.edit_text(f"{cb.message.text}\n\n{txt}")
    await cb.answer()

# ---- admin edit inputs ----
@adm.message(E.newplat, F.text)
async def e_newplat(m: Message, state: FSMContext):
    if await bail(m, state): return
    await state.clear()
    await db.execute("INSERT INTO platforms(name,pos) VALUES($1,(SELECT coalesce(max(pos),0)+1 FROM platforms))", m.text.strip()[:40])
    await v_platforms(m)

@adm.message(E.newsec, F.text)
async def e_newsec(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    await state.clear()
    await db.execute("INSERT INTO sections(platform_id,name,pos) VALUES($1,$2,(SELECT coalesce(max(pos),0)+1 FROM sections))", d["pid"], m.text.strip()[:40])
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
    try:
        ps = (await pservices()).get(v["provider_id"])
    except Exception:
        ps = None
    price = parse_price(m.text, pcost(ps) if ps else None)
    if not price:
        return await m.answer("Invalid. Send a % like 25, or a fixed price like ₹40." + ("" if ps else "\n(Provider cost unavailable, so only fixed ₹ price works.)"))
    await state.clear()
    await db.execute("UPDATE services SET rate=$2 WHERE id=$1", d["vid"], price)
    await load_catalog()
    await v_service(m, d["vid"])

# ---- add service flow ----
@adm.message(AS.pid, F.text)
async def as_pid(m: Message, state: FSMContext):
    if await bail(m, state): return
    if not m.text.strip().isdigit(): return await m.answer("Send the numeric service ID.")
    try:
        ps = (await pservices()).get(int(m.text))
    except Exception as e:
        return await m.answer(f"Provider API error: {str(e)[:150]}")
    if not ps: return await m.answer("ID not found at provider. Try another ID.")
    cost = pcost(ps)
    await state.update_data(pid=int(m.text), pname=ps["name"], cost=str(cost), mn=int(ps["min"]), mx=int(ps["max"]))
    await state.set_state(AS.comm)
    await m.answer(f"📦 {ps['name']}\n🗂 {ps.get('category', '')}\n💵 Provider cost: {money(cost)} /1000\n📉 Min {ps['min']} | Max {ps['max']}\n\n"
                   f"Send your commission % (e.g. 25 → {money(cost * D('1.25'))}/1000)\nor a fixed price like ₹40:")

@adm.message(AS.comm, F.text)
async def as_comm(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    price = parse_price(m.text, D(d["cost"]))
    if not price: return await m.answer("Invalid. Send a % like 25, or a fixed price like ₹40.")
    await state.update_data(price=str(price))
    if d.get("sid"):
        await state.set_state(AS.name)
        return await m.answer(f"✅ Price: {money(price)} /1000\n\nSend the service name users will see\n(send - to keep: {d['pname']})")
    await state.set_state(AS.pick)
    await m.answer(f"✅ Price: {money(price)} /1000")
    await pick_platform(m)

@adm.callback_query(AS.pick, F.data.startswith("k:"))
async def as_pick(cb: CallbackQuery, state: FSMContext):
    p = cb.data.split(":")
    if p[1] == "p":
        secs = await db.fetch("SELECT * FROM sections WHERE platform_id=$1 ORDER BY pos,id", int(p[2]))
        if not secs: return await cb.answer("This platform has no sections. Add one first.", show_alert=True)
        kb = [[B(s["name"], f"k:s:{s['id']}")] for s in secs] + [[B("🔙 Back", "k:0")]]
        return await send(cb, "Pick a section:", IM(inline_keyboard=kb))
    if p[1] == "s":
        await state.update_data(sid=int(p[2]))
        await state.set_state(AS.name)
        d = await state.get_data()
        await cb.answer()
        return await cb.message.answer(f"Send the service name users will see\n(send - to keep: {d['pname']})")
    await pick_platform(cb)

@adm.message(AS.name, F.text)
async def as_name(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    name = d["pname"] if m.text.strip() == "-" else m.text.strip()[:80]
    await state.update_data(name=name)
    await state.set_state(AS.desc)
    await m.answer("Send a description (speed, refill, etc.) or - for none:")

@adm.message(AS.desc, F.text)
async def as_desc(m: Message, state: FSMContext):
    if await bail(m, state): return
    d = await state.get_data()
    await state.clear()
    descr = "" if m.text.strip() == "-" else m.text.strip()[:500]
    vid = await db.fetchval("INSERT INTO services(section_id,provider_id,name,descr,rate,min_qty,max_qty,pos) "
                            "VALUES($1,$2,$3,$4,$5,$6,$7,(SELECT coalesce(max(pos),0)+1 FROM services)) RETURNING id",
                            d["sid"], d["pid"], d["name"], descr, D(d["price"]), d["mn"], d["mx"])
    await load_catalog()
    await m.answer("✅ Service added.")
    await v_service(m, vid)


# ======================= USER =======================
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

@r.message(Command("id"))
async def my_id(m: Message):
    await m.answer(f"Your Telegram ID: {m.from_user.id}")

@r.message(Command("cancel"))
async def u_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Cancelled.", reply_markup=MENU)

async def ensure(uid):
    return await db.fetchrow("INSERT INTO users(id) VALUES($1) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id RETURNING *", uid)

@r.message(F.text == "👤 Account")
async def account(m: Message, state: FSMContext):
    await state.clear()
    u = await ensure(m.from_user.id)
    n = await db.fetchval("SELECT count(*) FROM orders WHERE user_id=$1", u["id"])
    await m.answer(f"👤 ID: {u['id']}\n💰 Balance: {money(u['balance'])}\n📦 Orders: {n}")

@r.message(F.text == "🆘 Support")
async def support(m: Message):
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
    await m.answer("\n\n".join(f"#{o['id']} • {o['service_name']}\nQty {o['qty']} • {money(o['charge'])} • {o['status']}" for o in rows))

# ---- ordering: platform → section → service ----
async def u_platforms(ev):
    if not CAT.get("plat"): return await send(ev, "No services available right now.")
    await send(ev, "🛒 Choose a platform:", IM(inline_keyboard=[[B(p["name"], f"u:p:{p['id']}")] for p in CAT["plat"]]))

async def u_sections(ev, pid):
    p = CAT["plat_by_id"].get(pid)
    if not p: return await u_platforms(ev)
    kb = [[B(s["name"], f"u:s:{s['id']}")] for s in CAT["sec_by_plat"][pid]] + [BACK("u:p")]
    await send(ev, f"📂 {p['name']}\nChoose a category:", IM(inline_keyboard=kb))

async def u_services(ev, sid):
    s = CAT["sec_by_id"].get(sid)
    if not s: return await u_platforms(ev)
    vs, p = CAT["svc_by_sec"][sid], CAT["plat_by_id"][s["platform_id"]]
    lines = [f"{i}. {v['name']}\n    {money(v['rate'])} /1000 • Min {v['min_qty']} • Max {v['max_qty']}" for i, v in enumerate(vs, 1)]
    nums = [B(str(i), f"v:{v['id']}") for i, v in enumerate(vs, 1)]
    kb = [nums[i:i + 5] for i in range(0, len(nums), 5)] + [BACK(f"u:p:{p['id']}")]
    await send(ev, f"📂 {p['name']} › {s['name']}\n\n" + "\n".join(lines) + "\n\n👇 Tap a number to order:", IM(inline_keyboard=kb))

@r.message(F.text == "🛒 New Order")
async def new_order(m: Message, state: FSMContext):
    await state.clear()
    await u_platforms(m)

@r.callback_query(F.data.startswith("u:"))
async def u_nav(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    p = cb.data.split(":")
    if p[1] == "p" and len(p) > 2: return await u_sections(cb, int(p[2]))
    if p[1] == "s": return await u_services(cb, int(p[2]))
    await u_platforms(cb)

@r.callback_query(F.data.startswith("v:"))
async def pick(cb: CallbackQuery, state: FSMContext):
    v = CAT["svc_by_id"].get(int(cb.data[2:]))
    if not v: return await cb.answer("Service not available", show_alert=True)
    await state.set_state(S.link)
    await state.update_data(vid=v["id"])
    desc = f"📝 {v['descr']}\n" if v["descr"] else ""
    await send(cb, f"✅ {v['name']}\n{desc}💵 {money(v['rate'])} per 1000\n📉 Min {v['min_qty']} | Max {v['max_qty']}\n\n📎 Send the link:",
               IM(inline_keyboard=[BACK(f"u:s:{v['section_id']}")]))

@r.message(S.link, F.text)
async def got_link(m: Message, state: FSMContext):
    if len(m.text) < 4: return await m.answer("Send a valid link.")
    await state.update_data(link=m.text.strip())
    await state.set_state(S.qty)
    await m.answer("🔢 Send quantity:")

@r.message(S.qty, F.text)
async def got_qty(m: Message, state: FSMContext):
    d = await state.get_data()
    v = CAT["svc_by_id"].get(d.get("vid"))
    if not v: 
        await state.clear()
        return await m.answer("Service no longer available. Start again from 🛒 New Order.")
    if not m.text.isdigit(): return await m.answer("Send a number.")
    q = int(m.text)
    if not v["min_qty"] <= q <= v["max_qty"]: return await m.answer(f"Quantity must be {v['min_qty']} - {v['max_qty']}.")
    price = price_of(v, q)
    bal = (await ensure(m.from_user.id))["balance"]
    await state.update_data(qty=q)
    await m.answer(f"🧾 {v['name']}\nQty: {q}\nLink: {d['link']}\n\n💵 Price: {money(price)}\n💰 Your balance: {money(bal)}",
                   reply_markup=IM(inline_keyboard=[[B("✅ Confirm", "ok"), B("❌ Cancel", "no")]]))

@r.callback_query(F.data == "no")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await send(cb, "Cancelled.")

@r.callback_query(F.data == "ok")
async def confirm(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    v = CAT["svc_by_id"].get(d.get("vid"))
    if not v or "qty" not in d: return await cb.answer("Expired, start again", show_alert=True)
    uid, charge = cb.from_user.id, price_of(v, d["qty"])
    if not await db.fetchval("UPDATE users SET balance=balance-$2 WHERE id=$1 AND balance>=$2 RETURNING 1", uid, charge):
        return await send(cb, "❌ Insufficient balance. Use 💰 Add Funds.")
    try:
        res = await api(action="add", service=v["provider_id"], link=d["link"], quantity=d["qty"])
    except Exception as e:
        res = {"error": str(e)}
    if isinstance(res, dict) and "order" in res:
        oid = await db.fetchval("INSERT INTO orders(user_id,service_id,service_name,link,qty,charge,provider_order) "
                                "VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id", uid, v["id"], v["name"], d["link"], d["qty"], charge, str(res["order"]))
        await send(cb, f"✅ Order #{oid} placed!\n{v['name']}\nQty {d['qty']} • Charged {money(charge)}")
    else:
        await db.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", uid, charge)
        err = res.get("error", "Unknown error") if isinstance(res, dict) else "Provider error"
        await send(cb, f"❌ Order failed, amount refunded.\n{str(err)[:150]}")

# ---- add funds ----
@r.message(F.text == "💰 Add Funds")
async def funds(m: Message, state: FSMContext):
    await state.clear()
    rows = [[B(f"💎 {c['name']} - Auto", f"f:{k}")] for k, c in ON.items()]
    rows.append([B("🇮🇳 UPI - Manual", "f:upi")])
    await m.answer("Choose payment method:", reply_markup=IM(inline_keyboard=rows))

@r.callback_query(F.data.startswith("f:"))
async def fund_method(cb: CallbackQuery, state: FSMContext):
    k, mn = cb.data[2:], D(gs("min_deposit"))
    if k in ON:
        await state.set_state(S.usdt)
        await state.update_data(chain=k)
        await cb.message.answer(f"{ON[k]['name']}\nRate: 1 USDT = ₹{gs('usdt_inr')}\nEnter amount in ₹ (min {money(mn)}):")
    elif k == "upi":
        await state.set_state(S.upi_amt)
        await cb.message.answer(f"Enter amount in ₹ (min {money(mn)}):")
    await cb.answer()

@r.message(S.usdt, F.text)
async def usdt_amt(m: Message, state: FSMContext):
    k = (await state.get_data()).get("chain")
    inr = num(m.text)
    if k not in ON:
        await state.clear()
        return await m.answer("Start again from 💰 Add Funds.")
    if not inr or inr < D(gs("min_deposit")): return await m.answer("Invalid amount.")
    inr = inr.quantize(D("0.01"))
    base = (inr / D(gs("usdt_inr"))).quantize(D("0.01"))
    for _ in range(50):  # unique decimals so the payment can be matched
        amt = base + D(random.randint(1, 99)) / 1000
        if not await db.fetchval("SELECT 1 FROM deposits WHERE method=$1 AND status='pending' AND amount=$2", k, amt): break
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount,credit) VALUES($1,$2,$3,$4) RETURNING id", m.from_user.id, k, amt, inr)
    await state.clear()
    c = ON[k]
    await m.answer(f"💎 Deposit #{dep}\nNetwork: {c['name']}\nSend EXACTLY <code>{amt}</code> USDT to:\n<code>{c['wallet']}</code>\n\n"
                   f"You will receive {money(inr)}.\n⏱ Valid 30 min. Balance is added automatically after confirmation.\n"
                   f"⚠️ Use ONLY this network and the exact amount, otherwise funds can be lost.", parse_mode="HTML")

@r.message(S.upi_amt, F.text)
async def upi_amt(m: Message, state: FSMContext):
    inr = num(m.text)
    if not inr or inr < D(gs("min_deposit")): return await m.answer("Invalid amount.")
    inr = inr.quantize(D("0.01"))
    dep = await db.fetchval("INSERT INTO deposits(user_id,method,amount,credit) VALUES($1,'upi',$2,$2) RETURNING id", m.from_user.id, inr)
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
            await bot.send_message(a, f"UPI deposit #{d['dep']}\nUser: {m.from_user.id}\nAmount: ₹{d['inr']}", reply_markup=kb)
        except Exception:
            pass
    await m.answer("⏳ Sent for verification. Balance will be added once approved.")


# ======================= background jobs =======================
async def rpc(url, method, params):
    async with HTTP.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                         timeout=aiohttp.ClientTimeout(total=25)) as r:
        j = await r.json(content_type=None)
    if "error" in j: raise RuntimeError(j["error"])
    return j["result"]

async def match(key, val, txid, get_ts):
    q = val.quantize(D("0.0001"))
    for p in await db.fetch("SELECT id, amount, created FROM deposits WHERE method=$1 AND status='pending'", key):
        if p["amount"] != q: continue
        if await db.fetchval("SELECT 1 FROM deposits WHERE txid=$1", txid): return
        if await get_ts() < p["created"].timestamp() - 60: return
        await credit_deposit(p["id"], txid)
        return

async def const_ts(x):
    return x

async def block_ts(c, bn):
    return int((await rpc(c["rpc"], "eth_getBlockByNumber", [hex(bn), False]))["timestamp"], 16)

async def scan_tron(key, c):
    url = f"https://api.trongrid.io/v1/accounts/{c['wallet']}/transactions/trc20?only_to=true&limit=50&contract_address={c['contract']}"
    async with HTTP.get(url, headers={"TRON-PRO-API-KEY": TRON_KEY} if TRON_KEY else {}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
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
    logging.info("Admins: %s", ADMINS)
    db = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    await db.execute(SCHEMA)
    await seed()
    await load_settings()
    await load_banned()
    await load_catalog()
    HTTP = aiohttp.ClientSession()
    dp = Dispatcher()
    dp.update.outer_middleware(BanMW())
    dp.include_routers(adm, r)
    asyncio.create_task(crypto_poller())
    asyncio.create_task(status_loop())
    try:
        await dp.start_polling(bot)
    finally:
        await HTTP.close()

if __name__ == "__main__":
    asyncio.run(main())
