#!/usr/bin/env python3
"""
Gate.io Funding Rate Monitor Bot — Railway-ready (webhook mode)

Architecture:
  - Flask web server handles Telegram webhook POSTs  → Railway health check passes
  - Background thread runs the funding rate monitor  → alerts on rate changes
  - Upstash Redis persists tickers, rates, chat list
"""

import os
import time
import logging
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, request, abort

# ─────────────────────────────────────────
# ENV CONFIG — set these in Railway Variables
# ─────────────────────────────────────────
BOT_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]          # any random string you choose
RAILWAY_URL    = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RAILWAY_STATIC_URL", "")  # your Railway domain
UPSTASH_URL    = os.environ["UPSTASH_REDIS_REST_URL"]
UPSTASH_TOKEN  = os.environ["UPSTASH_REDIS_REST_TOKEN"]
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SEC", "60"))
PORT           = int(os.getenv("PORT", "8080"))        # Railway injects PORT automatically
# ─────────────────────────────────────────

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
app = Flask(__name__)


# ══════════════════════════════════════════
#  UPSTASH REDIS
# ══════════════════════════════════════════

def redis(command: list):
    try:
        resp = requests.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=command,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("result")
    except Exception as e:
        log.error(f"Redis error: {e}")
        return None

def redis_get(key):        return redis(["GET", key])
def redis_set(key, val):   redis(["SET", key, str(val)])
def redis_del(key):        redis(["DEL", key])
def redis_smembers(key):   return set(redis(["SMEMBERS", key]) or [])
def redis_sadd(key, val):  redis(["SADD", key, val])
def redis_srem(key, val):  redis(["SREM", key, val])


# ══════════════════════════════════════════
#  TICKER REGISTRY
# ══════════════════════════════════════════

TICKERS_KEY = "funding_bot:tickers"

def get_tickers() -> set:
    return redis_smembers(TICKERS_KEY)

def add_ticker(contract: str) -> bool:
    if contract in get_tickers():
        return False
    redis_sadd(TICKERS_KEY, contract)
    return True

def remove_ticker(contract: str) -> bool:
    if contract not in get_tickers():
        return False
    redis_srem(TICKERS_KEY, contract)
    redis_del(f"funding_bot:rate:{contract}")
    return True


# ══════════════════════════════════════════
#  GATE.IO
# ══════════════════════════════════════════

def fetch_funding_rate(contract: str) -> dict | None:
    url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{contract}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return {"error": "not_found"}
        resp.raise_for_status()
        data         = resp.json()
        rate         = float(data.get("funding_rate", 0))
        interval_sec = int(data.get("funding_interval", 28800))
        next_ts      = int(data.get("funding_next_apply", 0))
        return {
            "contract":   contract,
            "rate":       rate,
            "rate_pct":   round(rate * 100, 6),
            "interval_h": interval_sec // 3600,
            "next_apply": datetime.fromtimestamp(next_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
    except Exception as e:
        log.error(f"Fetch error [{contract}]: {e}")
        return None


# ══════════════════════════════════════════
#  TELEGRAM HELPERS
# ══════════════════════════════════════════

def send(chat_id, text: str, reply_to: int = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10).raise_for_status()
    except Exception as e:
        log.error(f"Telegram send error: {e}")

def broadcast(text: str):
    for cid in redis_smembers("funding_bot:chats"):
        send(cid, text)

def register_webhook():
    if not RAILWAY_URL:
        log.error("RAILWAY_PUBLIC_DOMAIN is not set. Go to Railway → Settings → Networking → Generate Domain, then add RAILWAY_PUBLIC_DOMAIN=yourapp.up.railway.app in Variables.")
        return
    url = f"https://{RAILWAY_URL}/webhook/{WEBHOOK_SECRET}"
    resp = requests.post(
        f"{TELEGRAM_API}/setWebhook",
        json={"url": url, "allowed_updates": ["message"]},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        log.info(f"Webhook registered: {url}")
    else:
        log.error(f"Webhook registration failed: {result}")


# ══════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════

def cmd_add(chat_id, args, mid):
    if not args:
        send(chat_id, "⚠️ Usage: <code>/add BTC_USDT</code>", reply_to=mid)
        return
    contract = args[0].upper().strip()
    data = fetch_funding_rate(contract)
    if data is None:
        send(chat_id, "❌ Could not reach Gate.io. Try again shortly.", reply_to=mid)
        return
    if data.get("error") == "not_found":
        send(chat_id,
            f"❌ <code>{contract}</code> not found on Gate.io.\n\n"
            f"Check the contract name, e.g. <code>BTC_USDT</code>",
            reply_to=mid,
        )
        return
    added = add_ticker(contract)
    if not added:
        send(chat_id, f"ℹ️ <code>{contract}</code> is already being monitored.", reply_to=mid)
        return
    redis_set(f"funding_bot:rate:{contract}", data["rate"])
    send(chat_id,
        f"✅ <b>Added</b> <code>{contract}</code>\n"
        f"📊 Current rate : <code>{data['rate_pct']}%</code> / {data['interval_h']}h\n"
        f"⏰ Next funding : {data['next_apply']}",
        reply_to=mid,
    )


def cmd_delete(chat_id, args, mid):
    if not args:
        send(chat_id, "⚠️ Usage: <code>/delete BTC_USDT</code>", reply_to=mid)
        return
    contract = args[0].upper().strip()
    if remove_ticker(contract):
        send(chat_id, f"🗑 <b>Removed</b> <code>{contract}</code> from monitoring.", reply_to=mid)
    else:
        send(chat_id, f"ℹ️ <code>{contract}</code> wasn't in the list.", reply_to=mid)


def cmd_list(chat_id, mid):
    tickers = sorted(get_tickers())
    if not tickers:
        send(chat_id, "📭 No tickers monitored yet.\n\nUse <code>/add BTC_USDT</code> to start.", reply_to=mid)
        return
    lines = "\n".join(f"  • <code>{t}</code>" for t in tickers)
    send(chat_id, f"📋 <b>Monitored tickers ({len(tickers)})</b>\n\n{lines}", reply_to=mid)


def cmd_status(chat_id, mid):
    tickers = sorted(get_tickers())
    if not tickers:
        send(chat_id, "📭 No tickers yet. Use <code>/add BTC_USDT</code>", reply_to=mid)
        return
    lines = []
    for t in tickers:
        data = fetch_funding_rate(t)
        if data and not data.get("error"):
            lines.append(f"  <code>{t}</code>  →  <b>{data['rate_pct']}%</b> / {data['interval_h']}h")
        else:
            lines.append(f"  <code>{t}</code>  →  ⚠️ fetch error")
    send(chat_id,
        f"📡 <b>Live Funding Rates</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{chr(10).join(lines)}\n\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}",
        reply_to=mid,
    )


def cmd_help(chat_id, mid):
    send(chat_id,
        "🤖 <b>Funding Rate Monitor</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "/add &lt;ticker&gt;     — Start monitoring a contract\n"
        "/delete &lt;ticker&gt;  — Stop monitoring a contract\n"
        "/list            — Show all monitored tickers\n"
        "/status          — Live rates for all tickers\n"
        "/help            — Show this message\n\n"
        "Example: <code>/add XTI_USDT</code>",
        reply_to=mid,
    )


def handle_update(update: dict):
    msg  = update.get("message", {})
    text = msg.get("text", "")
    if not text or not text.startswith("/"):
        return
    chat_id = msg["chat"]["id"]
    mid     = msg["message_id"]
    # Register chat for broadcast alerts
    redis_sadd("funding_bot:chats", str(chat_id))
    parts = text.strip().split()
    cmd   = parts[0].split("@")[0].lower()
    args  = parts[1:]

    if   cmd == "/add":                 cmd_add(chat_id, args, mid)
    elif cmd == "/delete":              cmd_delete(chat_id, args, mid)
    elif cmd == "/list":                cmd_list(chat_id, mid)
    elif cmd == "/status":              cmd_status(chat_id, mid)
    elif cmd in ("/help", "/start"):    cmd_help(chat_id, mid)


# ══════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════

@app.get("/")
def health():
    """Railway health check endpoint."""
    tickers = sorted(get_tickers())
    return {
        "status":   "ok",
        "tickers":  tickers,
        "count":    len(tickers),
        "checked":  datetime.now(timezone.utc).isoformat(),
    }

@app.post(f"/webhook/<secret>")
def webhook(secret):
    if secret != WEBHOOK_SECRET:
        abort(403)
    update = request.get_json(force=True, silent=True)
    if update:
        try:
            handle_update(update)
        except Exception as e:
            log.error(f"Update handling error: {e}")
    return "ok", 200


# ══════════════════════════════════════════
#  MONITOR LOOP (background thread)
# ══════════════════════════════════════════

def monitor_loop():
    log.info("Monitor loop started.")
    while True:
        time.sleep(CHECK_INTERVAL)
        tickers = get_tickers()
        if not tickers:
            continue
        for contract in tickers:
            data = fetch_funding_rate(contract)
            if not data or data.get("error"):
                continue
            key       = f"funding_bot:rate:{contract}"
            prev_rate = redis_get(key)
            curr_rate = data["rate"]
            if prev_rate is None:
                redis_set(key, curr_rate)
                continue
            if float(prev_rate) != curr_rate:
                prev_pct   = round(float(prev_rate) * 100, 6)
                curr_pct   = data["rate_pct"]
                delta      = round(curr_pct - prev_pct, 6)
                sign       = "+" if delta >= 0 else ""
                direction  = "📈 INCREASED" if delta > 0 else "📉 DECREASED"
                annualized = round(curr_pct * (24 / data["interval_h"]) * 365, 2)
                broadcast(
                    f"🔔 <b>Funding Rate Alert — {contract}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚡ {direction}\n\n"
                    f"  Before : <code>{prev_pct}%</code>\n"
                    f"  After  : <code>{curr_pct}%</code>\n"
                    f"  Delta  : <code>{sign}{delta}%</code>\n"
                    f"  APR est: <code>{annualized}%</code>\n\n"
                    f"⏰ Next funding: {data['next_apply']}\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
                )
                log.info(f"Alert sent [{contract}]: {prev_pct}% → {curr_pct}%")
                redis_set(key, curr_rate)


# ══════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════

def main():
    log.info("Bot starting…")
    register_webhook()
    threading.Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
