#!/usr/bin/env python3
"""
Gate.io Funding Rate Monitor Bot — Railway-ready (long polling)

Architecture:
  - Minimal HTTP server on $PORT  → satisfies Railway health check
  - Long polling thread           → receives Telegram commands
  - Monitor thread                → checks funding rates, sends alerts
  - Upstash Redis                 → persists tickers, rates, settings
"""

import os
import time
import logging
import threading
import requests
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

# ─────────────────────────────────────────
# ENV CONFIG — set these in Railway Variables
# ─────────────────────────────────────────
BOT_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
UPSTASH_URL    = os.environ["UPSTASH_REDIS_REST_URL"]
UPSTASH_TOKEN  = os.environ["UPSTASH_REDIS_REST_TOKEN"]
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SEC", "60"))
PORT           = int(os.getenv("PORT", "8080"))
# ─────────────────────────────────────────

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════
#  HEALTH CHECK SERVER
# ══════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        tickers = sorted(get_tickers())
        body = ("ok | tickers: " + (", ".join(tickers) or "none")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Health check server on port %d", PORT)
    server.serve_forever()


# ══════════════════════════════════════════
#  UPSTASH REDIS
# ══════════════════════════════════════════

def redis(command: list):
    try:
        resp = requests.post(
            UPSTASH_URL,
            headers={"Authorization": "Bearer " + UPSTASH_TOKEN},
            json=command,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("result")
    except Exception as e:
        log.error("Redis error: %s", e)
        return None

def redis_get(key):        return redis(["GET", key])
def redis_set(key, val):   redis(["SET", key, str(val)])
def redis_del(key):        redis(["DEL", key])
def redis_smembers(key):   return set(redis(["SMEMBERS", key]) or [])
def redis_sadd(key, val):  redis(["SADD", key, val])
def redis_srem(key, val):  redis(["SREM", key, val])


# ══════════════════════════════════════════
#  SETTINGS
# ══════════════════════════════════════════

TICKERS_KEY   = "funding_bot:tickers"
THRESHOLD_KEY = "funding_bot:threshold"

def get_threshold() -> float:
    """Minimum absolute rate change (in %) required to fire an alert."""
    val = redis_get(THRESHOLD_KEY)
    return float(val) if val else 0.1  # default 0.1%

def set_threshold(val: float):
    redis_set(THRESHOLD_KEY, val)


# ══════════════════════════════════════════
#  TICKER REGISTRY
# ══════════════════════════════════════════

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
    redis_del("funding_bot:rate:" + contract)
    redis_del("funding_bot:interval:" + contract)
    return True


# ══════════════════════════════════════════
#  GATE.IO
# ══════════════════════════════════════════

def fetch_funding_rate(contract: str) -> dict | None:
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts/" + contract
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
        log.error("Fetch error [%s]: %s", contract, e)
        return None


# ══════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════

def send(chat_id, text: str, reply_to: int = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    try:
        requests.post(TELEGRAM_API + "/sendMessage", json=payload, timeout=10).raise_for_status()
    except Exception as e:
        log.error("Telegram send error: %s", e)

def broadcast(text: str):
    for cid in redis_smembers("funding_bot:chats"):
        send(cid, text)


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
            "❌ <code>" + contract + "</code> not found on Gate.io.\n\n"
            "Check the contract name, e.g. <code>BTC_USDT</code>",
            reply_to=mid,
        )
        return
    added = add_ticker(contract)
    if not added:
        send(chat_id, "ℹ️ <code>" + contract + "</code> is already being monitored.", reply_to=mid)
        return
    redis_set("funding_bot:rate:" + contract, data["rate"])
    redis_set("funding_bot:interval:" + contract, data["interval_h"])
    send(chat_id,
        "✅ <b>Added</b> <code>" + contract + "</code>\n"
        "📊 Current rate : <code>" + str(data["rate_pct"]) + "%</code> / " + str(data["interval_h"]) + "h\n"
        "⏰ Next funding : " + data["next_apply"],
        reply_to=mid,
    )

def cmd_delete(chat_id, args, mid):
    if not args:
        send(chat_id, "⚠️ Usage: <code>/delete BTC_USDT</code>", reply_to=mid)
        return
    contract = args[0].upper().strip()
    if remove_ticker(contract):
        send(chat_id, "🗑 <b>Removed</b> <code>" + contract + "</code> from monitoring.", reply_to=mid)
    else:
        send(chat_id, "ℹ️ <code>" + contract + "</code> wasn't in the list.", reply_to=mid)

def cmd_list(chat_id, mid):
    tickers = sorted(get_tickers())
    if not tickers:
        send(chat_id, "📭 No tickers monitored yet.\n\nUse <code>/add BTC_USDT</code> to start.", reply_to=mid)
        return
    lines = "\n".join("  • <code>" + t + "</code>" for t in tickers)
    send(chat_id, "📋 <b>Monitored tickers (" + str(len(tickers)) + ")</b>\n\n" + lines, reply_to=mid)

def cmd_status(chat_id, mid):
    tickers = sorted(get_tickers())
    if not tickers:
        send(chat_id, "📭 No tickers yet. Use <code>/add BTC_USDT</code>", reply_to=mid)
        return
    lines = []
    for t in tickers:
        data = fetch_funding_rate(t)
        if data and not data.get("error"):
            lines.append("  <code>" + t + "</code>  →  <b>" + str(data["rate_pct"]) + "%</b> / " + str(data["interval_h"]) + "h")
        else:
            lines.append("  <code>" + t + "</code>  →  ⚠️ fetch error")
    send(chat_id,
        "📡 <b>Live Funding Rates</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines) + "\n\n"
        "🕐 " + datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        reply_to=mid,
    )

def cmd_threshold(chat_id, args, mid):
    if not args:
        current = get_threshold()
        send(chat_id,
            "⚙️ <b>Alert Threshold</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Current : <code>" + str(current) + "%</code>\n\n"
            "Alerts fire only when rate changes by <b>at least " + str(current) + "%</b>.\n"
            "Funding interval changes (e.g. 8h → 4h) always alert.\n\n"
            "To change: <code>/threshold 0.5</code>",
            reply_to=mid,
        )
        return
    try:
        val = float(args[0].replace(",", "."))
        if val < 0:
            raise ValueError()
    except ValueError:
        send(chat_id, "⚠️ Provide a positive number.\nExample: <code>/threshold 0.5</code>", reply_to=mid)
        return
    set_threshold(val)
    send(chat_id,
        "✅ <b>Threshold set to " + str(val) + "%</b>\n"
        "Alerts will fire when rate changes by at least <b>" + str(val) + "%</b>.\n"
        "Interval changes always alert regardless.",
        reply_to=mid,
    )

def cmd_interval(chat_id, args, mid):
    """Toggle alerts for funding interval changes (e.g. 8h -> 4h -> 1h)."""
    current = redis_get("funding_bot:interval_alerts")
    is_on   = current != "false"   # default ON
    if not args:
        state = "ON ✅" if is_on else "OFF ❌"
        send(chat_id,
            "⏱ <b>Interval Change Alerts:</b> " + state + "\n\n"
            "Fires when the exchange shortens the funding window "
            "(e.g. 8h → 4h → 1h), signalling extreme market conditions.\n\n"
            "Toggle: <code>/interval on</code> or <code>/interval off</code>",
            reply_to=mid,
        )
        return
    arg = args[0].lower()
    if arg == "on":
        redis_set("funding_bot:interval_alerts", "true")
        send(chat_id,
            "✅ <b>Interval change alerts ON</b>\n"
            "You will be notified when the funding window shortens (8h → 4h → 1h).",
            reply_to=mid,
        )
    elif arg == "off":
        redis_set("funding_bot:interval_alerts", "false")
        send(chat_id,
            "❌ <b>Interval change alerts OFF</b>\n"
            "Only rate value changes will trigger alerts.",
            reply_to=mid,
        )
    else:
        send(chat_id, "⚠️ Usage: <code>/interval on</code> or <code>/interval off</code>", reply_to=mid)


def cmd_help(chat_id, mid):
    send(chat_id,
        "🤖 <b>Funding Rate Monitor</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "/add &lt;ticker&gt;       — Start monitoring a contract\n"
        "/delete &lt;ticker&gt;    — Stop monitoring a contract\n"
        "/list              — Show all monitored tickers\n"
        "/status            — Live rates for all tickers\n"
        "/threshold [n]     — Set min % change to alert\n"
        "/interval [on|off] — Alert when funding window shortens\n"
        "/help              — Show this message\n\n"
        "Example: <code>/add XTI_USDT</code>\n"
        "Example: <code>/threshold 0.5</code>",
        reply_to=mid,
    )

def handle_update(update: dict):
    msg  = update.get("message", {})
    text = msg.get("text", "")
    if not text or not text.startswith("/"):
        return
    chat_id = msg["chat"]["id"]
    mid     = msg["message_id"]
    redis_sadd("funding_bot:chats", str(chat_id))
    parts = text.strip().split()
    cmd   = parts[0].split("@")[0].lower()
    args  = parts[1:]

    if   cmd == "/add":                 cmd_add(chat_id, args, mid)
    elif cmd == "/delete":              cmd_delete(chat_id, args, mid)
    elif cmd == "/list":                cmd_list(chat_id, mid)
    elif cmd == "/status":              cmd_status(chat_id, mid)
    elif cmd == "/threshold":           cmd_threshold(chat_id, args, mid)
    elif cmd == "/interval":            cmd_interval(chat_id, args, mid)
    elif cmd in ("/help", "/start"):    cmd_help(chat_id, mid)


# ══════════════════════════════════════════
#  POLLING LOOP
# ══════════════════════════════════════════

def polling_loop():
    log.info("Polling loop started.")
    offset = int(redis_get("funding_bot:tg_offset") or "0")
    while True:
        try:
            resp = requests.get(
                TELEGRAM_API + "/getUpdates",
                params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=40,
            )
            resp.raise_for_status()
            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                redis_set("funding_bot:tg_offset", offset)
                handle_update(upd)
        except Exception as e:
            log.error("Poll error: %s", e)
            time.sleep(5)


# ══════════════════════════════════════════
#  MONITOR LOOP
# ══════════════════════════════════════════

def monitor_loop():
    log.info("Monitor loop started.")
    while True:
        time.sleep(CHECK_INTERVAL)
        threshold = get_threshold()
        for contract in get_tickers():
            data = fetch_funding_rate(contract)
            if not data or data.get("error"):
                continue

            rate_key     = "funding_bot:rate:" + contract
            interval_key = "funding_bot:interval:" + contract
            prev_rate    = redis_get(rate_key)
            curr_rate    = data["rate"]
            prev_interval = int(redis_get(interval_key) or data["interval_h"])
            curr_interval = data["interval_h"]
            interval_changed = prev_interval != curr_interval

            # First time seeing this ticker
            if prev_rate is None:
                redis_set(rate_key, curr_rate)
                redis_set(interval_key, curr_interval)
                continue

            prev_pct = round(float(prev_rate) * 100, 6)
            curr_pct = data["rate_pct"]
            delta    = round(curr_pct - prev_pct, 6)

            # Check interval alert setting (default ON)
            interval_alerts_on = redis_get("funding_bot:interval_alerts") != "false"
            # Alert if: interval changed (and interval alerts on) OR rate delta exceeds threshold
            if not (interval_changed and interval_alerts_on) and abs(delta) < threshold:
                log.info("Skipping [%s]: delta %+.6f%% below threshold %.4f%%", contract, delta, threshold)
                redis_set(rate_key, curr_rate)
                continue

            sign       = "+" if delta >= 0 else ""
            direction  = "📈 INCREASED" if delta > 0 else "📉 DECREASED"
            annualized = round(curr_pct * (24 / curr_interval) * 365, 2)
            interval_line = (
                "\n⏱ <b>Interval changed: " + str(prev_interval) + "h → " + str(curr_interval) + "h</b>"
                if interval_changed else ""
            )

            broadcast(
                "🔔 <b>Funding Rate Alert — " + contract + "</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ " + direction + "\n\n"
                "  Before : <code>" + str(prev_pct) + "%</code>\n"
                "  After  : <code>" + str(curr_pct) + "%</code>\n"
                "  Delta  : <code>" + sign + str(delta) + "%</code>\n"
                "  APR est: <code>" + str(annualized) + "%</code>\n\n"
                "⏰ Next funding: " + data["next_apply"] + "\n"
                "🕐 " + datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                + interval_line
            )
            log.info("Alert sent [%s]: %+.6f%% (threshold=%.4f%%)", contract, delta, threshold)
            redis_set(rate_key, curr_rate)
            redis_set(interval_key, curr_interval)


# ══════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════

def main():
    log.info("Bot starting…")
    threading.Thread(target=start_health_server, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    polling_loop()

if __name__ == "__main__":
    main()
