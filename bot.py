"""Minimal Render Telegram controller.

Render responsibilities:
- Respond only to /start in the owner's private bot chat.
- Show whether the Colab/Telethon worker heartbeat is recent.
- Receive authenticated worker heartbeat/disconnect requests.
- Best-effort self-ping for Render health.

All file commands and file processing are handled by the Colab Telethon client.
No PostgreSQL or job queue is used by this controller.
"""

from __future__ import annotations

import hmac
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, request


# ---------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------

def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = required_env("BOT_TOKEN")
WORKER_API_TOKEN = required_env("WORKER_API_TOKEN")
WEBHOOK_SECRET = required_env("TELEGRAM_WEBHOOK_SECRET")

try:
    OWNER_USER_ID = int(required_env("OWNER_USER_ID"))
except ValueError as exc:
    raise RuntimeError("OWNER_USER_ID must be a numeric Telegram user ID") from exc

# Optional comma-separated Telegram user IDs allowed to use /start.
# File commands are NOT handled by Render.
ADMIN_USER_IDS = set()
for raw_id in os.getenv("ADMIN_USER_IDS", "").split(","):
    raw_id = raw_id.strip()
    if raw_id:
        try:
            ADMIN_USER_IDS.add(int(raw_id))
        except ValueError:
            raise RuntimeError("ADMIN_USER_IDS must contain comma-separated numeric IDs")

RENDER_PUBLIC_URL = (
    os.getenv("RENDER_PUBLIC_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or ""
).strip().rstrip("/")

if not RENDER_PUBLIC_URL:
    raise RuntimeError("Set RENDER_PUBLIC_URL or use Render's RENDER_EXTERNAL_URL")

if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    raise RuntimeError("TELEGRAM_WEBHOOK_SECRET must use only letters, numbers, _ and -")

PORT = int(os.getenv("PORT", "10000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ---------------------------------------------------------------------
# App and state
# ---------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("render-start-controller")

app = Flask(__name__)
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

state_lock = threading.Lock()
worker_state: dict[str, Any] = {
    "connected": False,
    "worker_id": "",
    "last_seen": 0.0,
    "details": "Colab has not connected yet",
}
webhook_lock = threading.Lock()
webhook_ready = False


# ---------------------------------------------------------------------
# Telegram API
# ---------------------------------------------------------------------

def telegram_call(method: str, payload: dict[str, Any]) -> Any:
    response = requests.post(
        f"{TG_API}/{method}",
        json=payload,
        timeout=25,
    )
    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            f"Telegram API returned HTTP {response.status_code}: {response.text[:500]}"
        )

    if not response.ok or not data.get("ok"):
        description = data.get("description", response.text[:500])
        raise RuntimeError(
            f"Telegram API {method} failed (HTTP {response.status_code}): {description}"
        )
    return data.get("result")


def send_message(chat_id: int, text: str, reply_to: int | None = None) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text[:4000],
        "disable_web_page_preview": True,
    }
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    telegram_call("sendMessage", payload)


def configure_webhook() -> None:
    global webhook_ready
    if webhook_ready:
        return
    with webhook_lock:
        if webhook_ready:
            return
        telegram_call(
            "setWebhook",
            {
                "url": f"{RENDER_PUBLIC_URL}/webhook",
                "secret_token": WEBHOOK_SECRET,
                "allowed_updates": ["message"],
                "drop_pending_updates": False,
            },
        )
        webhook_ready = True
        log.info("Telegram webhook configured")


# ---------------------------------------------------------------------
# Worker status
# ---------------------------------------------------------------------

def worker_snapshot() -> dict[str, Any]:
    now = time.time()
    with state_lock:
        last_seen = float(worker_state["last_seen"] or 0)
        age = int(now - last_seen) if last_seen else None
        online = bool(worker_state["connected"] and age is not None and age <= 100)
        return {
            "online": online,
            "worker_id": worker_state["worker_id"],
            "last_seen_seconds_ago": age,
            "details": worker_state["details"],
        }


def start_text() -> str:
    status = worker_snapshot()
    colab_line = "🟢 CONNECTED" if status["online"] else "🔴 OFFLINE"
    if status["last_seen_seconds_ago"] is None:
        last_seen = "Never"
    else:
        last_seen = f"{status['last_seen_seconds_ago']} seconds ago"

    return (
        "🤖 COSMIC X BOT\n\n"
        "Render is your lightweight controller. Colab + Telethon handles all file work.\n\n"
        "COMMANDS (handled by Colab):\n"
        "/singlethumbnail — Apply one thumbnail to a file/batch.\n"
        "/batchthumbnail — Apply a thumbnail with episode filename numbering.\n"
        "/convert — Convert a batch of supported files into videos (MP4).\n\n"
        "WORKFLOW:\n"
        "1. Keep your Colab processor cell running.\n"
        "2. Send the command in the Telegram chat monitored by Colab.\n"
        "3. Send your files to the configured intake channel.\n"
        "4. Colab processes them and uploads results to the destination channel.\n\n"
        f"COLAB STATUS: {colab_line}\n"
        f"Last heartbeat: {last_seen}\n\n"
        "Use /start again to refresh this status."
    )


def is_allowed(user_id: int) -> bool:
    return user_id == OWNER_USER_ID or user_id in ADMIN_USER_IDS


# ---------------------------------------------------------------------
# Webhook: Render handles ONLY /start
# ---------------------------------------------------------------------

@app.post("/webhook")
def telegram_webhook():
    supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(supplied, WEBHOOK_SECRET):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    update_data = request.get_json(silent=True) or {}
    message = update_data.get("message")
    if not message:
        return jsonify({"ok": True})

    sender = message.get("from") or {}
    chat = message.get("chat") or {}
    user_id = sender.get("id")
    chat_id = chat.get("id")
    message_id = message.get("message_id")

    if not user_id or not chat_id or chat.get("type") != "private":
        return jsonify({"ok": True})

    if not is_allowed(int(user_id)):
        return jsonify({"ok": True})

    text = (message.get("text") or "").strip()
    command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text else ""

    # Deliberately no other command handling in Render.
    if command == "/start":
        try:
            send_message(int(chat_id), start_text(), int(message_id) if message_id else None)
        except Exception:
            log.exception("Could not send /start response")

    return jsonify({"ok": True})


# ---------------------------------------------------------------------
# Colab worker API
# ---------------------------------------------------------------------

def worker_auth_error():
    supplied = request.headers.get("X-Worker-Token", "")
    if not supplied or not hmac.compare_digest(supplied, WORKER_API_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return None


@app.post("/worker/heartbeat")
def worker_heartbeat():
    auth_error = worker_auth_error()
    if auth_error:
        return auth_error

    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", "colab"))[:128]
    details = str(data.get("details", "Colab active"))[:1000]

    with state_lock:
        worker_state["connected"] = True
        worker_state["worker_id"] = worker_id
        worker_state["last_seen"] = time.time()
        worker_state["details"] = details

    return jsonify({"ok": True, "server_time": datetime.now(timezone.utc).isoformat()})


@app.post("/worker/disconnect")
def worker_disconnect():
    auth_error = worker_auth_error()
    if auth_error:
        return auth_error

    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", ""))[:128]

    with state_lock:
        if not worker_id or worker_state["worker_id"] == worker_id:
            worker_state["connected"] = False
            worker_state["details"] = "Colab worker disconnected"

    return jsonify({"ok": True})


# ---------------------------------------------------------------------
# Health endpoints and best-effort Render self-ping
# ---------------------------------------------------------------------

@app.get("/")
def index():
    return jsonify({
        "ok": True,
        "service": "cosmic-x-render-controller",
        "render_commands": ["/start"],
        "worker": worker_snapshot(),
    })


@app.get("/health")
def health():
    # Also retry webhook setup if Telegram was temporarily unavailable at boot.
    try:
        configure_webhook()
        webhook_status = "configured"
    except Exception as exc:
        webhook_status = f"pending: {str(exc)[:180]}"
        log.warning("Webhook setup not ready: %s", exc)

    return jsonify({
        "ok": True,
        "service": "cosmic-x-render-controller",
        "webhook": webhook_status,
        "worker": worker_snapshot(),
        "time": datetime.now(timezone.utc).isoformat(),
    })


def self_ping_loop():
    # Best effort only. This does not keep a free Render service awake forever.
    while True:
        time.sleep(9 * 60)
        try:
            requests.get(f"{RENDER_PUBLIC_URL}/health", timeout=20)
        except Exception:
            log.exception("Render self-ping failed")


_ping_thread = threading.Thread(
    target=self_ping_loop,
    name="render-self-ping",
    daemon=True,
)
_ping_thread.start()


if __name__ == "__main__":
    configure_webhook()
    app.run(host="0.0.0.0", port=PORT)
