"""
Telegram Render Controller — bot.py
===================================

Render web service for a Telegram bot + Google Colab worker.

Python: 3.11+
Install:
    pip install -r requirements.txt

Required environment variables:
    BOT_TOKEN                  Telegram Bot API token
    OWNER_USER_ID              Telegram numeric user ID of the owner
    TELEGRAM_WEBHOOK_SECRET    Random secret (1-256 chars, A-Z/a-z/0-9/_/-)
    RENDER_PUBLIC_URL          Public Render URL, e.g. https://your-service.onrender.com
    WORKER_API_TOKEN           Long random secret shared with Colab

Recommended:
    DATABASE_URL               PostgreSQL URL for durable jobs/admins.
                               SQLite is used if omitted, but Render's free
                               filesystem is ephemeral.
    INTAKE_CHANNEL_ID          Private channel ID where bot forwards thumbnail
                               photos for the Telethon Colab account.
    MAX_BATCH_LINKS            Default 50
    MAX_PARALLEL_JOBS          Default 4 (Colab worker decides actual concurrency)
    SELF_PING_URL              Optional URL to ping. Self-pinging is NOT a
                               reliable way to prevent Render Free sleep.
    SELF_PING_INTERVAL_SECONDS Default 600
    PORT                       Render supplies this automatically.

Bot commands (owner/admin only):
    /start
    /help
    /status
    /connect
    /disconnect
    /jobs
    /cancel JOB_ID
    /drive URL [URL ...]
    /download URL [URL ...]
    /convert FORMAT URL [URL ...]
    /batchthumbnail TEMPLATE [START_EP]
    /singlethumbnail TEMPLATE
    /batchdone
    /batchcancel
    /addadmin USER_ID
    /removeadmin USER_ID
    /admins

Batch thumbnail workflow:
    /batchthumbnail "Series - Episode {ep}" 1
    Send up to 50 Google Drive links (one or more messages).
    Send one thumbnail photo to the bot.
    /batchdone

Single thumbnail workflow:
    /singlethumbnail "My custom filename"
    Send one Google Drive link.
    Send one thumbnail photo.
    /batchdone

Notes:
- The bot does not download Drive files. It queues URLs for Colab.
- The Colab worker must implement the matching /worker/* API.
- For large video inputs, Colab's Telethon user session should download
  from Drive or from a private Telegram intake channel. Do not use the
  Bot API getFile endpoint for multi-GB video downloads.
- Bot forwards thumbnail photos to INTAKE_CHANNEL_ID. Add the bot as an
  administrator there with permission to post messages. The Telethon
  account used by Colab must be a member of that channel.
- This controller never stores Telegram bot tokens or worker tokens in jobs.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request
from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
    select,
    update,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


# ---------------------------------------------------------------------
# Configuration — no credentials or IDs are hard-coded.
# ---------------------------------------------------------------------

def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = env_required("BOT_TOKEN")
OWNER_USER_ID = int(env_required("OWNER_USER_ID"))
WEBHOOK_SECRET = env_required("TELEGRAM_WEBHOOK_SECRET")
RENDER_PUBLIC_URL = (
    os.getenv("RENDER_PUBLIC_URL")
    or os.getenv("RENDER_EXTERNAL_URL", "")
).strip().rstrip("/")

if not RENDER_PUBLIC_URL:
    raise RuntimeError("Missing Render public URL")
WORKER_API_TOKEN = env_required("WORKER_API_TOKEN")

INTAKE_CHANNEL_ID = os.getenv("INTAKE_CHANNEL_ID", "").strip()
MAX_BATCH_LINKS = max(1, min(int(os.getenv("MAX_BATCH_LINKS", "50")), 50))
MAX_PARALLEL_JOBS = max(1, min(int(os.getenv("MAX_PARALLEL_JOBS", "4")), 20))
SELF_PING_URL = os.getenv("SELF_PING_URL", "").strip()
SELF_PING_INTERVAL = max(60, int(os.getenv("SELF_PING_INTERVAL_SECONDS", "600")))
PORT = int(os.getenv("PORT", "10000"))

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./bot_data.sqlite3"

if DATABASE_URL.startswith("sqlite:"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


# ---------------------------------------------------------------------
# Logging and Flask
# ---------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("telegram-render-controller")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
TG_FILE_API = f"https://api.telegram.org/file/bot{BOT_TOKEN}"


# ---------------------------------------------------------------------
# Database models
# ---------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class Admin(Base):
    __tablename__ = "admins"
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    added_by: Mapped[int] = mapped_column(Integer, nullable=False)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow())


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="queued", index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WorkerState(Base):
    __tablename__ = "worker_state"
    key: Mapped[str] = mapped_column(String(32), primary_key=True)
    connected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details: Mapped[str] = mapped_column(Text, nullable=False, default="")


class Draft(Base):
    __tablename__ = "drafts"
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mode: Mapped[str] = mapped_column(String(24), nullable=False)
    template: Mapped[str] = mapped_column(Text, nullable=False, default="")
    start_ep: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    links_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    thumbnail_file_id: Mapped[str] = mapped_column(Text, nullable=False, default="")
    thumbnail_message_id: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow())


Base.metadata.create_all(engine)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------

def tg_call(method: str, payload: dict[str, Any] | None = None, timeout: int = 25) -> dict[str, Any]:
    response = requests.post(
        f"{TG_API}/{method}",
        json=payload or {},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API {method} failed: {data}")
    return data["result"]


def send_message(chat_id: int, text: str, reply_to: int | None = None) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text[:4000],
        "disable_web_page_preview": True,
    }
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    tg_call("sendMessage", payload)


def safe_send(chat_id: int, text: str, reply_to: int | None = None) -> None:
    try:
        send_message(chat_id, text, reply_to)
    except Exception:
        log.exception("Could not send Telegram message to chat %s", chat_id)


def forward_thumbnail_to_intake(chat_id: int, message_id: int) -> int | None:
    if not INTAKE_CHANNEL_ID:
        raise RuntimeError("INTAKE_CHANNEL_ID is not configured.")
    result = tg_call(
        "forwardMessage",
        {
            "chat_id": INTAKE_CHANNEL_ID,
            "from_chat_id": chat_id,
            "message_id": message_id,
        },
    )
    return int(result["message_id"])


def is_authorized(user_id: int) -> bool:
    if user_id == OWNER_USER_ID:
        return True
    with SessionLocal() as db:
        return db.get(Admin, user_id) is not None


def is_owner(user_id: int) -> bool:
    return user_id == OWNER_USER_ID


def get_worker_state(db) -> WorkerState:
    state = db.get(WorkerState, "colab")
    if state is None:
        state = WorkerState(key="colab", connected=False, worker_id="", details="")
        db.add(state)
        db.commit()
    return state


# ---------------------------------------------------------------------
# Validation and job helpers
# ---------------------------------------------------------------------

DRIVE_ID_RE = re.compile(r"/file/d/([A-Za-z0-9_-]+)")
DRIVE_OPEN_RE = re.compile(r"[?&]id=([A-Za-z0-9_-]+)")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def normalize_url(url: str) -> str:
    url = url.strip().rstrip(".,);]")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Not a valid HTTP(S) URL: {url}")
    return url


def is_google_drive_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"drive.google.com", "docs.google.com"}


def extract_urls(text: str) -> list[str]:
    found = URL_RE.findall(text or "")
    return [normalize_url(x) for x in found]


def create_job(owner_id: int, job_type: str, payload: dict[str, Any]) -> str:
    job_id = uuid.uuid4().hex[:16]
    now = utcnow()
    with SessionLocal() as db:
        db.add(
            Job(
                id=job_id,
                owner_id=owner_id,
                job_type=job_type,
                status="queued",
                payload_json=json.dumps(payload, ensure_ascii=False),
                progress=0,
                message="Waiting for Colab worker",
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()
    return job_id


def get_draft(db, user_id: int) -> Draft | None:
    return db.get(Draft, user_id)


def set_draft(user_id: int, mode: str, template: str, start_ep: int = 1) -> None:
    with SessionLocal() as db:
        existing = db.get(Draft, user_id)
        if existing:
            existing.mode = mode
            existing.template = template
            existing.start_ep = start_ep
            existing.links_json = "[]"
            existing.thumbnail_file_id = ""
            existing.thumbnail_message_id = ""
            existing.created_at = utcnow()
        else:
            db.add(
                Draft(
                    user_id=user_id,
                    mode=mode,
                    template=template,
                    start_ep=start_ep,
                    links_json="[]",
                    thumbnail_file_id="",
                    thumbnail_message_id="",
                )
            )
        db.commit()


def cancel_draft(user_id: int) -> bool:
    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if not draft:
            return False
        db.delete(draft)
        db.commit()
        return True


def append_draft_links(user_id: int, urls: list[str]) -> tuple[int, int]:
    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if not draft:
            return 0, 0
        existing = json.loads(draft.links_json or "[]")
        for url in urls:
            if url not in existing and len(existing) < MAX_BATCH_LINKS:
                existing.append(url)
        draft.links_json = json.dumps(existing)
        db.commit()
        return len(existing), MAX_BATCH_LINKS


def set_draft_thumbnail(user_id: int, file_id: str, forwarded_message_id: int) -> None:
    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if not draft:
            return
        draft.thumbnail_file_id = file_id
        draft.thumbnail_message_id = str(forwarded_message_id)
        db.commit()


# ---------------------------------------------------------------------
# Help and command handling
# ---------------------------------------------------------------------

HELP_TEXT = """🤖 Render + Colab Controller

Only the owner and registered admins can use this bot.

GENERAL
/start — Show welcome and connection status.
/help — Show all commands and examples.
/status — Show Render, Colab and queue status.
/connect — Ask Colab to confirm/refresh its connection status.
/disconnect — Mark the Colab worker unavailable.
/jobs — List your recent jobs.
/cancel JOB_ID — Cancel your job (owner can cancel any job).

GOOGLE DRIVE
/drive URL [URL ...] — Queue one or more Google Drive links for download.
/drive accepts multiple links in one message, or links on separate messages
when used inside a thumbnail draft.

DIRECT DOWNLOAD
/download URL [URL ...] — Queue direct HTTP(S) download links.

CONVERSION
/convert FORMAT URL [URL ...] — Queue Drive/direct links for conversion.
Example: /convert mp4 https://drive.google.com/file/d/FILE_ID/view

THUMBNAILS
/batchthumbnail TEMPLATE [START_EP] — Start a batch of up to 50 Drive links.
Example: /batchthumbnail "Series - Episode {ep}" 1
Then send Drive links, send one thumbnail photo, and use /batchdone.
/singlethumbnail TEMPLATE — Start one-video thumbnail job.
Example: /singlethumbnail "My Movie Hindi Dubbed"
Then send one Drive link, send one thumbnail photo, and use /batchdone.
/batchdone — Submit the current thumbnail draft to Colab.
/batchcancel — Discard your current thumbnail draft.

ADMINISTRATION (OWNER ONLY)
/addadmin USER_ID — Authorize a Telegram numeric user ID.
/removeadmin USER_ID — Revoke admin access.
/admins — List registered admins.

NOTES
• Drive links are queued; Colab performs the actual download.
• Batch thumbnail uses one shared thumbnail and a {ep} template.
• Colab assigns episode numbers starting at START_EP.
• Jobs are processed as quickly as the Colab worker's configured
  concurrency and available runtime permit.
• A heartbeat only reports connectivity; it cannot keep a Colab runtime alive.
• Use /batchcancel if you started the wrong thumbnail workflow.
"""


def command_parts(text: str) -> tuple[str, str]:
    parts = (text or "").strip().split(maxsplit=1)
    command = parts[0].split("@", 1)[0].lower() if parts else ""
    args = parts[1].strip() if len(parts) > 1 else ""
    return command, args


def handle_command(user_id: int, chat_id: int, message_id: int, text: str) -> None:
    command, args = command_parts(text)

    if command in ("/start", "/help"):
        safe_send(chat_id, HELP_TEXT, message_id)
        return

    if command == "/status":
        with SessionLocal() as db:
            worker = get_worker_state(db)
            age = None
            if worker.last_seen:
                last_seen = worker.last_seen
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                age = max(0, int((utcnow() - last_seen).total_seconds()))
            queued = db.scalar(select(Job).where(Job.status == "queued").count()) if False else None
            queued_count = len(db.scalars(select(Job).where(Job.status == "queued")).all())
            running_count = len(db.scalars(select(Job).where(Job.status == "running")).all())
        worker_status = "CONNECTED" if worker.connected and age is not None and age < 120 else "OFFLINE"
        safe_send(
            chat_id,
            "📊 System status\n"
            f"Render: ONLINE\n"
            f"Colab: {worker_status}\n"
            f"Last heartbeat: {age if age is not None else 'never'} seconds ago\n"
            f"Queued jobs: {queued_count}\n"
            f"Running jobs: {running_count}\n"
            f"Parallel job target: {MAX_PARALLEL_JOBS}",
            message_id,
        )
        return

    if command == "/connect":
        with SessionLocal() as db:
            worker = get_worker_state(db)
            worker.details = "Owner/admin requested connection check"
            db.commit()
        safe_send(chat_id, "🔎 Connection check requested. Colab must send a heartbeat to /worker/heartbeat.", message_id)
        return

    if command == "/disconnect":
        with SessionLocal() as db:
            worker = get_worker_state(db)
            worker.connected = False
            worker.details = "Marked disconnected by Telegram command"
            db.commit()
        safe_send(chat_id, "🔌 Colab marked offline. New jobs remain queued.", message_id)
        return

    if command == "/jobs":
        with SessionLocal() as db:
            rows = db.scalars(
                select(Job).order_by(Job.created_at.desc()).limit(15)
            ).all()
        if not rows:
            safe_send(chat_id, "No jobs found.", message_id)
            return
        lines = ["🗂 Recent jobs:"]
        for job in rows:
            if user_id != OWNER_USER_ID and job.owner_id != user_id:
                continue
            lines.append(f"• {job.id} | {job.job_type} | {job.status} | {job.progress}%")
        safe_send(chat_id, "\n".join(lines)[:3900], message_id)
        return

    if command == "/cancel":
        if not args:
            safe_send(chat_id, "Usage: /cancel JOB_ID", message_id)
            return
        job_id = args.split()[0]
        with SessionLocal() as db:
            job = db.get(Job, job_id)
            if not job:
                safe_send(chat_id, "Job not found.", message_id)
                return
            if user_id != OWNER_USER_ID and job.owner_id != user_id:
                safe_send(chat_id, "You can only cancel your own jobs.", message_id)
                return
            if job.status in ("completed", "failed", "cancelled"):
                safe_send(chat_id, f"Job is already {job.status}.", message_id)
                return
            job.status = "cancel_requested"
            job.updated_at = utcnow()
            db.commit()
        safe_send(chat_id, f"🛑 Cancellation requested for {job_id}. Colab will stop it when it checks job status.", message_id)
        return

    if command == "/addadmin":
        if not is_owner(user_id):
            return
        try:
            new_id = int(args.split()[0])
            if new_id <= 0:
                raise ValueError
        except (ValueError, IndexError):
            safe_send(chat_id, "Usage: /addadmin USER_ID", message_id)
            return
        if new_id == OWNER_USER_ID:
            safe_send(chat_id, "The owner is already the owner.", message_id)
            return
        with SessionLocal() as db:
            existing = db.get(Admin, new_id)
            if existing:
                safe_send(chat_id, "That user is already an admin.", message_id)
                return
            db.add(Admin(user_id=new_id, added_by=user_id))
            db.commit()
        safe_send(chat_id, f"✅ Admin added: {new_id}", message_id)
        return

    if command == "/removeadmin":
        if not is_owner(user_id):
            return
        try:
            remove_id = int(args.split()[0])
        except (ValueError, IndexError):
            safe_send(chat_id, "Usage: /removeadmin USER_ID", message_id)
            return
        with SessionLocal() as db:
            admin = db.get(Admin, remove_id)
            if not admin:
                safe_send(chat_id, "That user is not an admin.", message_id)
                return
            db.delete(admin)
            db.commit()
        safe_send(chat_id, f"🗑 Admin removed: {remove_id}", message_id)
        return

    if command == "/admins":
        if not is_owner(user_id):
            return
        with SessionLocal() as db:
            admins = db.scalars(select(Admin).order_by(Admin.user_id)).all()
        text_out = "👮 Admins:\n" + ("\n".join(str(a.user_id) for a in admins) if admins else "No additional admins.")
        safe_send(chat_id, text_out, message_id)
        return

    if command == "/batchthumbnail":
        if not args:
            safe_send(chat_id, 'Usage: /batchthumbnail "Series - Episode {ep}" [START_EP]', message_id)
            return
        match = re.match(r'^"(.+?)"(?:\s+(\d+))?$', args)
        if match:
            template = match.group(1)
            start_ep = int(match.group(2) or 1)
        else:
            pieces = args.rsplit(maxsplit=1)
            if len(pieces) == 2 and pieces[1].isdigit():
                template, start_ep = pieces[0], int(pieces[1])
            else:
                template, start_ep = args, 1
        if "{ep}" not in template:
            safe_send(chat_id, "Your batch rename template must contain {ep}.", message_id)
            return
        set_draft(user_id, "batch_thumbnail", template, start_ep)
        safe_send(
            chat_id,
            f"🧩 Batch thumbnail draft started.\nTemplate: {template}\nStarting episode: {start_ep}\n"
            f"Send up to {MAX_BATCH_LINKS} Google Drive links, then send one thumbnail photo, then /batchdone.",
            message_id,
        )
        return

    if command == "/singlethumbnail":
        if not args:
            safe_send(chat_id, 'Usage: /singlethumbnail "Custom filename"', message_id)
            return
        template = args.strip().strip('"')
        set_draft(user_id, "single_thumbnail", template, 1)
        safe_send(chat_id, "🖼 Single thumbnail draft started. Send one Google Drive link, then one thumbnail photo, then /batchdone.", message_id)
        return

    if command == "/batchcancel":
        removed = cancel_draft(user_id)
        safe_send(chat_id, "Draft discarded." if removed else "No active draft.", message_id)
        return

    if command == "/batchdone":
        submit_thumbnail_draft(user_id, chat_id, message_id)
        return

    if command in ("/drive", "/download"):
        if not args:
            safe_send(chat_id, f"Usage: {command} URL [URL ...]", message_id)
            return
        try:
            urls = extract_urls(args)
        except ValueError as exc:
            safe_send(chat_id, str(exc), message_id)
            return
        if not urls:
            safe_send(chat_id, "No valid URLs found.", message_id)
            return
        if len(urls) > MAX_BATCH_LINKS:
            safe_send(chat_id, f"Maximum {MAX_BATCH_LINKS} links per job.", message_id)
            return
        if command == "/drive" and not all(is_google_drive_url(u) for u in urls):
            safe_send(chat_id, "Use Google Drive URLs with /drive. Use /download for direct links.", message_id)
            return
        job_id = create_job(
            user_id,
            "drive_download" if command == "/drive" else "direct_download",
            {"urls": urls, "caption_mode": "filename"},
        )
        safe_send(chat_id, f"📥 Job queued: {job_id}\nFiles: {len(urls)}\nColab will download and send each file to the destination channel with its filename as caption.", message_id)
        return

    if command == "/convert":
        parts = args.split()
        if len(parts) < 2:
            safe_send(chat_id, "Usage: /convert FORMAT URL [URL ...]", message_id)
            return
        output_format = parts[0].lower().lstrip(".")
        try:
            urls = extract_urls(" ".join(parts[1:]))
        except ValueError as exc:
            safe_send(chat_id, str(exc), message_id)
            return
        if not urls:
            safe_send(chat_id, "No valid URLs found.", message_id)
            return
        if len(urls) > MAX_BATCH_LINKS:
            safe_send(chat_id, f"Maximum {MAX_BATCH_LINKS} links per job.", message_id)
            return
        job_id = create_job(
            user_id,
            "convert",
            {"urls": urls, "format": output_format, "caption_mode": "filename"},
        )
        safe_send(chat_id, f"🎞 Conversion job queued: {job_id}\nFiles: {len(urls)}\nOutput: {output_format}", message_id)
        return

    safe_send(chat_id, "Unknown command. Use /help.", message_id)


def submit_thumbnail_draft(user_id: int, chat_id: int, message_id: int) -> None:
    with SessionLocal() as db:
        draft = get_draft(db, user_id)
        if not draft:
            safe_send(chat_id, "No active thumbnail draft. Start with /batchthumbnail or /singlethumbnail.", message_id)
            return
        links = json.loads(draft.links_json or "[]")
        mode = draft.mode
        template = draft.template
        start_ep = draft.start_ep
        thumb_msg = draft.thumbnail_message_id

        expected = 1 if mode == "single_thumbnail" else MAX_BATCH_LINKS
        if not links:
            safe_send(chat_id, "Add at least one Google Drive link before submitting.", message_id)
            return
        if mode == "single_thumbnail" and len(links) != 1:
            safe_send(chat_id, "Single thumbnail mode accepts exactly one Drive link.", message_id)
            return
        if not thumb_msg:
            safe_send(chat_id, "Send a thumbnail photo before /batchdone.", message_id)
            return

        payload = {
            "urls": links,
            "template": template,
            "start_ep": start_ep,
            "thumbnail": {
                "intake_channel_id": INTAKE_CHANNEL_ID,
                "message_id": int(thumb_msg),
            },
            "caption_mode": "filename",
            "max_parallel_files": MAX_PARALLEL_JOBS,
        }
        job_type = mode
        job_id = uuid.uuid4().hex[:16]
        now = utcnow()
        db.add(
            Job(
                id=job_id,
                owner_id=user_id,
                job_type=job_type,
                status="queued",
                payload_json=json.dumps(payload, ensure_ascii=False),
                progress=0,
                message="Waiting for Colab worker",
                created_at=now,
                updated_at=now,
            )
        )
        db.delete(draft)
        db.commit()

    safe_send(chat_id, f"✅ {mode.replace('_', ' ').title()} job queued.\nJob: {job_id}\nFiles: {len(links)}\nTemplate: {template}\nColab will use the shared thumbnail and preserve original video quality unless the job type requires conversion.", message_id)


def handle_non_command_message(user_id: int, chat_id: int, message: dict[str, Any]) -> None:
    text = message.get("text") or message.get("caption") or ""
    urls = extract_urls(text) if text else []

    with SessionLocal() as db:
        draft = get_draft(db, user_id)
        if not draft:
            return

        if urls:
            if not all(is_google_drive_url(u) for u in urls):
                safe_send(chat_id, "Thumbnail workflows accept Google Drive links only. Use /batchcancel to discard.", message.get("message_id"))
                return
            current = json.loads(draft.links_json or "[]")
            if draft.mode == "single_thumbnail" and current:
                safe_send(chat_id, "Single thumbnail mode accepts only one Drive link.", message.get("message_id"))
                return
            if len(current) + len(urls) > MAX_BATCH_LINKS:
                safe_send(chat_id, f"That would exceed the {MAX_BATCH_LINKS}-file limit.", message.get("message_id"))
                return
            for url in urls:
                if url not in current:
                    current.append(url)
            draft.links_json = json.dumps(current)
            db.commit()
            safe_send(chat_id, f"📎 Added {len(urls)} link(s). Draft total: {len(current)}.", message.get("message_id"))
            return

        photo = message.get("photo")
        if photo:
            if not INTAKE_CHANNEL_ID:
                safe_send(chat_id, "INTAKE_CHANNEL_ID is not configured; cannot forward thumbnail.", message.get("message_id"))
                return
            largest = photo[-1]
            try:
                forwarded_id = forward_thumbnail_to_intake(chat_id, int(message["message_id"]))
                draft.thumbnail_file_id = largest["file_id"]
                draft.thumbnail_message_id = str(forwarded_id)
                db.commit()
                safe_send(chat_id, "🖼 Thumbnail received and forwarded to the private intake channel. Send /batchdone when ready.", message.get("message_id"))
            except Exception as exc:
                log.exception("Thumbnail forwarding failed")
                safe_send(chat_id, f"Could not forward thumbnail: {str(exc)[:500]}", message.get("message_id"))
            return


# ---------------------------------------------------------------------
# Telegram webhook
# ---------------------------------------------------------------------

@app.post("/webhook")
def telegram_webhook():
    supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(supplied, WEBHOOK_SECRET):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    update_data = request.get_json(silent=True) or {}
    message = update_data.get("message") or update_data.get("edited_message")
    if not message:
        return jsonify({"ok": True})

    sender = message.get("from") or {}
    user_id = sender.get("id")
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")

    if not user_id or not chat_id or not message_id:
        return jsonify({"ok": True})

    # Ignore groups/channels. The control bot is private-chat only.
    if chat.get("type") != "private":
        return jsonify({"ok": True})

    # Silently ignore every non-owner/non-admin update.
    if not is_authorized(int(user_id)):
        return jsonify({"ok": True})

    text = message.get("text") or ""
    if text.startswith("/"):
        try:
            handle_command(int(user_id), int(chat_id), int(message_id), text)
        except Exception:
            log.exception("Command handling failed")
            safe_send(int(chat_id), "❌ Command failed. Check Render logs or try /help.", int(message_id))
    else:
        try:
            handle_non_command_message(int(user_id), int(chat_id), message)
        except Exception:
            log.exception("Message handling failed")
            safe_send(int(chat_id), "❌ Could not process this message.", int(message_id))

    return jsonify({"ok": True})


# ---------------------------------------------------------------------
# Worker API — Colab polls and reports job progress.
# ---------------------------------------------------------------------

def require_worker_auth():
    supplied = request.headers.get("X-Worker-Token", "")
    if not supplied or not hmac.compare_digest(supplied, WORKER_API_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return None


@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "telegram-render-controller",
        "time": utcnow().isoformat(),
    })


@app.post("/worker/heartbeat")
def worker_heartbeat():
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", "colab"))[:128]
    details = str(data.get("details", ""))[:1000]
    with SessionLocal() as db:
        state = get_worker_state(db)
        state.connected = True
        state.worker_id = worker_id
        state.last_seen = utcnow()
        state.details = details
        db.commit()
    return jsonify({"ok": True, "server_time": utcnow().isoformat()})


@app.post("/worker/disconnect")
def worker_disconnect():
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", ""))[:128]
    with SessionLocal() as db:
        state = get_worker_state(db)
        if not worker_id or state.worker_id == worker_id:
            state.connected = False
            state.details = "Worker detached"
            db.commit()
    return jsonify({"ok": True})


@app.post("/worker/jobs/claim")
def worker_claim_job():
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", "colab"))[:128]

    with SessionLocal() as db:
        state = get_worker_state(db)
        if not state.connected or not state.last_seen:
            return jsonify({"ok": False, "error": "worker_not_connected"}), 409
        last_seen = state.last_seen
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        age = (utcnow() - last_seen).total_seconds()
        if age > 120:
            state.connected = False
            db.commit()
            return jsonify({"ok": False, "error": "heartbeat_stale"}), 409

        # Select one queued job. For a single Colab notebook this is enough;
        # Colab may call this endpoint repeatedly up to its own concurrency.
        job = db.scalar(
            select(Job)
            .where(Job.status == "queued")
            .order_by(Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if not job:
            return jsonify({"ok": True, "job": None})

        job.status = "running"
        job.worker_id = worker_id
        job.message = "Claimed by Colab"
        job.updated_at = utcnow()
        db.commit()

        return jsonify({
            "ok": True,
            "job": {
                "id": job.id,
                "owner_id": job.owner_id,
                "type": job.job_type,
                "payload": json.loads(job.payload_json),
            },
        })


@app.post("/worker/jobs/<job_id>/status")
def worker_job_status(job_id: str):
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    progress = max(0, min(100, int(data.get("progress", 0))))
    message = str(data.get("message", ""))[:1500]
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        if job.status == "cancel_requested":
            return jsonify({"ok": True, "cancel_requested": True})
        if job.status != "running":
            return jsonify({"ok": False, "error": f"job_not_running:{job.status}"}), 409
        job.progress = progress
        job.message = message
        job.updated_at = utcnow()
        db.commit()
        owner_id = job.owner_id
    # The worker may send status frequently; the Colab implementation should
    # throttle user notifications and use this endpoint for state only.
    return jsonify({"ok": True, "cancel_requested": False, "owner_id": owner_id})


@app.get("/worker/jobs/<job_id>")
def worker_get_job(job_id: str):
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        return jsonify({
            "ok": True,
            "id": job.id,
            "status": job.status,
            "progress": job.progress,
            "message": job.message,
        })


@app.post("/worker/jobs/<job_id>/complete")
def worker_job_complete(job_id: str):
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    result_text = str(data.get("message", "Completed successfully"))[:1500]
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        if job.status == "cancel_requested":
            job.status = "cancelled"
        else:
            job.status = "completed"
        job.progress = 100
        job.message = result_text
        job.updated_at = utcnow()
        job.finished_at = utcnow()
        owner_id = job.owner_id
        db.commit()
    safe_send(owner_id, f"✅ Job {job_id}: {job.status}\n{result_text}")
    return jsonify({"ok": True, "status": job.status})


@app.post("/worker/jobs/<job_id>/fail")
def worker_job_fail(job_id: str):
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    data = request.get_json(silent=True) or {}
    error_text = str(data.get("error", "Unknown error"))[:1500]
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        job.status = "failed"
        job.message = error_text
        job.updated_at = utcnow()
        job.finished_at = utcnow()
        owner_id = job.owner_id
        db.commit()
    safe_send(owner_id, f"❌ Job {job_id} failed\n{error_text}")
    return jsonify({"ok": True})


@app.get("/worker/jobs/<job_id>/cancel-check")
def worker_cancel_check(job_id: str):
    auth_error = require_worker_auth()
    if auth_error:
        return auth_error
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        return jsonify({"ok": True, "cancel_requested": job.status == "cancel_requested"})


# ---------------------------------------------------------------------
# Optional Render self-ping. This is best-effort only; an outbound
# request from a service is not a guaranteed substitute for an external
# uptime monitor sending inbound requests.
# ---------------------------------------------------------------------

def start_self_ping_thread() -> None:
    if not SELF_PING_URL:
        return

    import threading

    def loop():
        while True:
            try:
                requests.get(SELF_PING_URL, timeout=15)
            except Exception:
                log.exception("Self-ping failed")
            time.sleep(SELF_PING_INTERVAL)

    thread = threading.Thread(target=loop, daemon=True, name="self-ping")
    thread.start()


# ---------------------------------------------------------------------
# Startup: configure Telegram webhook.
# ---------------------------------------------------------------------

def configure_webhook() -> None:
    webhook_url = f"{RENDER_PUBLIC_URL}/webhook"
    try:
        result = tg_call(
            "setWebhook",
            {
                "url": webhook_url,
                "secret_token": WEBHOOK_SECRET,
                "allowed_updates": ["message", "edited_message"],
                "drop_pending_updates": False,
            },
        )
        log.info("Telegram webhook configured: %s", result)
    except Exception:
        log.exception("Could not configure Telegram webhook at startup")


@app.get("/")
def index():
    return jsonify({
        "ok": True,
        "service": "telegram-render-controller",
        "health": "/health",
        "webhook": "/webhook",
    })


# Run on module import as well as direct execution, so Gunicorn deployments
# also register the webhook and start the optional best-effort ping thread.
configure_webhook()
start_self_ping_thread()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
