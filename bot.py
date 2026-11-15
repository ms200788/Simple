"""
Telegram Render Controller
==========================
Render runs the Telegram controller and persistent job queue.
Google Colab is an optional worker: it processes queued jobs whenever connected.

Python 3.11+
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request
from sqlalchemy import (
    Boolean, DateTime, Integer, String, Text, create_engine, select, update
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = env_required("BOT_TOKEN")
try:
    OWNER_USER_ID = int(env_required("OWNER_USER_ID"))
except ValueError as exc:
    raise RuntimeError("OWNER_USER_ID must be a numeric Telegram user ID") from exc

WEBHOOK_SECRET = env_required("TELEGRAM_WEBHOOK_SECRET")
WORKER_API_TOKEN = env_required("WORKER_API_TOKEN")

RENDER_PUBLIC_URL = (
    os.getenv("RENDER_PUBLIC_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or ""
).strip().rstrip("/")
if not RENDER_PUBLIC_URL:
    raise RuntimeError(
        "Missing Render public URL. Set RENDER_PUBLIC_URL or use Render's RENDER_EXTERNAL_URL."
    )

if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    raise RuntimeError("TELEGRAM_WEBHOOK_SECRET must contain only A-Z, a-z, 0-9, _ or -")

INTAKE_CHANNEL_ID = os.getenv("INTAKE_CHANNEL_ID", "").strip()
MAX_BATCH_LINKS = max(1, min(int(os.getenv("MAX_BATCH_LINKS", "50")), 50))
MAX_PARALLEL_JOBS = max(1, min(int(os.getenv("MAX_PARALLEL_JOBS", "2")), 20))
JOB_MAX_ATTEMPTS = max(1, min(int(os.getenv("JOB_MAX_ATTEMPTS", "3")), 10))
STALE_JOB_HOURS = max(1, min(int(os.getenv("STALE_JOB_HOURS", "12")), 168))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PORT = int(os.getenv("PORT", "10000"))

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is required for reliable Render deployment. "
        "Use the PostgreSQL Internal Database URL."
    )

# Render commonly supplies postgres:// or postgresql:// URLs.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)

if DATABASE_URL.startswith("sqlite:"):
    raise RuntimeError("SQLite is disabled for production. Configure Render PostgreSQL in DATABASE_URL.")

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("telegram-render-controller")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=3,
    max_overflow=2,
    connect_args={"connect_timeout": 10},
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_INIT_LOCK = threading.Lock()
DB_READY = False
WEBHOOK_CONFIGURED = False
WEBHOOK_LOCK = threading.Lock()


# ---------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Admin(Base):
    __tablename__ = "admins"
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    added_by: Mapped[int] = mapped_column(Integer, nullable=False)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


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
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
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
    thumbnail_message_id: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


def ensure_database() -> bool:
    """Initialize tables lazily so a temporary DB outage doesn't crash Gunicorn boot."""
    global DB_READY
    if DB_READY:
        return True
    with DB_INIT_LOCK:
        if DB_READY:
            return True
        try:
            Base.metadata.create_all(engine)
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            DB_READY = True
            log.info("PostgreSQL connection and schema are ready.")
            return True
        except Exception:
            log.exception("Database is not ready yet; will retry on the next request.")
            return False


@app.before_request
def database_guard():
    # Health check remains available during a temporary DB outage.
    if request.path in ("/health", "/"):
        return None
    if not ensure_database():
        return jsonify({"ok": False, "error": "database_unavailable"}), 503
    return None


# ---------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------

def tg_call(method: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    response = requests.post(f"{TG_API}/{method}", json=payload or {}, timeout=timeout)
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


def forward_thumbnail_to_intake(chat_id: int, message_id: int) -> int:
    if not INTAKE_CHANNEL_ID:
        raise RuntimeError("INTAKE_CHANNEL_ID is not configured in Render.")
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


def get_worker_state(db) -> WorkerState:
    state = db.get(WorkerState, "colab")
    if state is None:
        state = WorkerState(key="colab", connected=False, worker_id="", details="")
        db.add(state)
        db.commit()
    return state


# ---------------------------------------------------------------------
# URL, job and draft helpers
# ---------------------------------------------------------------------

URL_RE = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)


def normalize_url(url: str) -> str:
    url = url.strip().rstrip(".,);]}")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Invalid HTTP(S) URL: {url}")
    return url


def is_google_drive_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "drive.google.com" or host.endswith(".drive.google.com") or host == "docs.google.com"


def extract_urls(text: str) -> list[str]:
    found = URL_RE.findall(text or "")
    result = []
    for item in found:
        result.append(normalize_url(item))
    return result


def create_job(owner_id: int, job_type: str, payload: dict[str, Any]) -> str:
    job_id = uuid.uuid4().hex[:16]
    now = utcnow()
    with SessionLocal() as db:
        db.add(Job(
            id=job_id,
            owner_id=owner_id,
            job_type=job_type,
            status="queued",
            payload_json=json.dumps(payload, ensure_ascii=False),
            progress=0,
            message="Waiting for Colab worker",
            created_at=now,
            updated_at=now,
        ))
        db.commit()
    return job_id


def set_draft(user_id: int, mode: str, template: str, start_ep: int = 1) -> None:
    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if draft:
            draft.mode = mode
            draft.template = template
            draft.start_ep = start_ep
            draft.links_json = "[]"
            draft.thumbnail_message_id = ""
            draft.created_at = utcnow()
        else:
            db.add(Draft(
                user_id=user_id,
                mode=mode,
                template=template,
                start_ep=start_ep,
                links_json="[]",
                thumbnail_message_id="",
            ))
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
            return 0, MAX_BATCH_LINKS
        existing = json.loads(draft.links_json or "[]")
        added = 0
        for url in urls:
            if url not in existing and len(existing) < MAX_BATCH_LINKS:
                existing.append(url)
                added += 1
        draft.links_json = json.dumps(existing)
        db.commit()
        return len(existing), added


# ---------------------------------------------------------------------
# Help and commands
# ---------------------------------------------------------------------

HELP_TEXT = """🤖 Telegram + Render + Colab

Only the owner and registered admins can use this bot.

GENERAL
/start — Show help.
/help — Show all commands.
/status — Render, Colab and queue status.
/connect — Explain/check Colab heartbeat.
/disconnect — Mark Colab offline.
/jobs — Show your recent jobs.
/cancel JOB_ID — Request job cancellation.

GOOGLE DRIVE
/drive URL [URL ...] — Queue Google Drive links.
/download URL [URL ...] — Queue direct HTTP(S) links.
/convert FORMAT URL [URL ...] — Queue conversion.

THUMBNAILS
/batchthumbnail "Series - Episode {ep}" [START_EP]
/singlethumbnail "Custom filename"
Send Drive link(s), then send one photo, then /batchdone.
/batchdone — Submit the thumbnail job.
/batchcancel — Discard the draft.

ADMIN (OWNER ONLY)
/addadmin USER_ID
/removeadmin USER_ID
/admins

The Render bot stays online and stores jobs in PostgreSQL.
Colab performs downloads, conversions and Telegram uploads when connected.
Jobs remain queued while Colab is offline.
"""


def command_parts(text: str) -> tuple[str, str]:
    parts = (text or "").strip().split(maxsplit=1)
    command = parts[0].split("@", 1)[0].lower() if parts else ""
    return command, parts[1].strip() if len(parts) > 1 else ""


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
            queued_count = db.scalar(select(Job.id).where(Job.status == "queued").count()) if False else len(
                db.scalars(select(Job.id).where(Job.status == "queued")).all()
            )
            running_count = len(db.scalars(select(Job.id).where(Job.status == "running")).all())
            failed_count = len(db.scalars(select(Job.id).where(Job.status == "failed")).all())
            connected = bool(worker.connected and age is not None and age < 120)
        safe_send(
            chat_id,
            "📊 System status\n"
            "Render: ONLINE\n"
            f"Colab: {'CONNECTED' if connected else 'OFFLINE'}\n"
            f"Last heartbeat: {age if age is not None else 'never'} seconds ago\n"
            f"Queued: {queued_count} | Running: {running_count} | Failed: {failed_count}\n"
            f"Max batch links: {MAX_BATCH_LINKS}",
            message_id,
        )
        return

    if command == "/connect":
        with SessionLocal() as db:
            worker = get_worker_state(db)
            age = None
            if worker.last_seen:
                last_seen = worker.last_seen
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                age = int((utcnow() - last_seen).total_seconds())
            online = bool(worker.connected and age is not None and age < 120)
        safe_send(
            chat_id,
            "🟢 Colab heartbeat is recent." if online else
            "🟠 Colab is not connected. Start the Colab notebook; queued jobs will wait.",
            message_id,
        )
        return

    if command == "/disconnect":
        with SessionLocal() as db:
            worker = get_worker_state(db)
            worker.connected = False
            worker.details = "Marked disconnected by owner/admin"
            db.commit()
        safe_send(chat_id, "Colab marked offline. Queued jobs are preserved.", message_id)
        return

    if command == "/jobs":
        with SessionLocal() as db:
            rows = db.scalars(select(Job).order_by(Job.created_at.desc()).limit(30)).all()
            lines = ["🗂 Recent jobs:"]
            for job in rows:
                if user_id != OWNER_USER_ID and job.owner_id != user_id:
                    continue
                lines.append(
                    f"• {job.id} | {job.job_type} | {job.status} | {job.progress}%"
                )
        safe_send(chat_id, "\n".join(lines)[:3900] if len(lines) > 1 else "No jobs found.", message_id)
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
        safe_send(chat_id, f"🛑 Cancellation requested for {job_id}.", message_id)
        return

    if command in ("/addadmin", "/removeadmin", "/admins"):
        if not is_owner(user_id):
            safe_send(chat_id, "Only the owner can manage admins.", message_id)
            return
        if command == "/admins":
            with SessionLocal() as db:
                admins = db.scalars(select(Admin).order_by(Admin.user_id)).all()
            safe_send(
                chat_id,
                "👮 Registered admins:\n" + (
                    "\n".join(str(a.user_id) for a in admins) if admins else "No additional admins."
                ),
                message_id,
            )
            return
        try:
            target_id = int(args.split()[0])
            if target_id <= 0:
                raise ValueError
        except (ValueError, IndexError):
            safe_send(chat_id, f"Usage: {command} USER_ID", message_id)
            return
        if target_id == OWNER_USER_ID:
            safe_send(chat_id, "The owner already has full access.", message_id)
            return
        with SessionLocal() as db:
            admin = db.get(Admin, target_id)
            if command == "/addadmin":
                if admin:
                    safe_send(chat_id, "That user is already an admin.", message_id)
                    return
                db.add(Admin(user_id=target_id, added_by=user_id))
                db.commit()
                safe_send(chat_id, f"✅ Admin added: {target_id}", message_id)
            else:
                if not admin:
                    safe_send(chat_id, "That user is not an admin.", message_id)
                    return
                db.delete(admin)
                db.commit()
                safe_send(chat_id, f"🗑 Admin removed: {target_id}", message_id)
        return

    if command == "/batchthumbnail":
        if not args:
            safe_send(chat_id, 'Usage: /batchthumbnail "Series - Episode {ep}" [START_EP]', message_id)
            return
        match = re.match(r'^"(.+?)"(?:\s+(\d+))?$', args)
        if match:
            template = match.group(1).strip()
            start_ep = int(match.group(2) or 1)
        else:
            parts = args.rsplit(maxsplit=1)
            if len(parts) == 2 and parts[1].isdigit():
                template, start_ep = parts[0].strip().strip('"'), int(parts[1])
            else:
                template, start_ep = args.strip().strip('"'), 1
        if "{ep}" not in template:
            safe_send(chat_id, "Batch template must contain {ep}, e.g. Series - Episode {ep}.", message_id)
            return
        if start_ep < 0:
            safe_send(chat_id, "START_EP must be zero or greater.", message_id)
            return
        set_draft(user_id, "batch_thumbnail", template, start_ep)
        safe_send(
            chat_id,
            f"🧩 Batch draft started.\nTemplate: {template}\nStart episode: {start_ep}\n"
            f"Send up to {MAX_BATCH_LINKS} Drive links, then one photo, then /batchdone.",
            message_id,
        )
        return

    if command == "/singlethumbnail":
        if not args:
            safe_send(chat_id, 'Usage: /singlethumbnail "Custom filename"', message_id)
            return
        template = args.strip().strip('"')
        if not template:
            safe_send(chat_id, "Please provide a filename template.", message_id)
            return
        set_draft(user_id, "single_thumbnail", template, 1)
        safe_send(chat_id, "🖼 Single thumbnail draft started. Send exactly one Drive link, then one photo, then /batchdone.", message_id)
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
            safe_send(chat_id, "No valid links found.", message_id)
            return
        if len(urls) > MAX_BATCH_LINKS:
            safe_send(chat_id, f"Maximum {MAX_BATCH_LINKS} links per job.", message_id)
            return
        if command == "/drive" and not all(is_google_drive_url(url) for url in urls):
            safe_send(chat_id, "Use Google Drive links with /drive. Use /download for direct links.", message_id)
            return
        job_id = create_job(
            user_id,
            "drive_download" if command == "/drive" else "direct_download",
            {"urls": urls, "caption_mode": "filename"},
        )
        safe_send(chat_id, f"📥 Job queued: {job_id}\nFiles: {len(urls)}\nColab will process it when connected.", message_id)
        return

    if command == "/convert":
        parts = args.split()
        if len(parts) < 2:
            safe_send(chat_id, "Usage: /convert FORMAT URL [URL ...]", message_id)
            return
        output_format = parts[0].lower().lstrip(".")
        if not re.fullmatch(r"[a-z0-9]{2,8}", output_format):
            safe_send(chat_id, "Invalid output format.", message_id)
            return
        try:
            urls = extract_urls(" ".join(parts[1:]))
        except ValueError as exc:
            safe_send(chat_id, str(exc), message_id)
            return
        if not urls:
            safe_send(chat_id, "No valid links found.", message_id)
            return
        if len(urls) > MAX_BATCH_LINKS:
            safe_send(chat_id, f"Maximum {MAX_BATCH_LINKS} links per job.", message_id)
            return
        job_id = create_job(
            user_id,
            "convert",
            {"urls": urls, "format": output_format, "caption_mode": "filename"},
        )
        safe_send(chat_id, f"🎞 Conversion job queued: {job_id}\nFiles: {len(urls)}\nColab will process it when connected.", message_id)
        return

    safe_send(chat_id, "Unknown command. Use /help.", message_id)


def submit_thumbnail_draft(user_id: int, chat_id: int, message_id: int) -> None:
    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if not draft:
            safe_send(chat_id, "No active thumbnail draft.", message_id)
            return
        links = json.loads(draft.links_json or "[]")
        if not links:
            safe_send(chat_id, "Add at least one Google Drive link first.", message_id)
            return
        if draft.mode == "single_thumbnail" and len(links) != 1:
            safe_send(chat_id, "Single thumbnail mode accepts exactly one Drive link.", message_id)
            return
        if not draft.thumbnail_message_id:
            safe_send(chat_id, "Send a valid thumbnail photo before /batchdone.", message_id)
            return
        if not INTAKE_CHANNEL_ID:
            safe_send(chat_id, "INTAKE_CHANNEL_ID is missing in Render.", message_id)
            return

        payload = {
            "urls": links,
            "template": draft.template,
            "start_ep": draft.start_ep,
            "thumbnail": {
                "intake_channel_id": INTAKE_CHANNEL_ID,
                "message_id": int(draft.thumbnail_message_id),
            },
            "caption_mode": "filename",
            "max_parallel_files": MAX_PARALLEL_JOBS,
        }
        job_type = draft.mode
        job_id = uuid.uuid4().hex[:16]
        now = utcnow()
        db.add(Job(
            id=job_id,
            owner_id=user_id,
            job_type=job_type,
            status="queued",
            payload_json=json.dumps(payload, ensure_ascii=False),
            progress=0,
            message="Waiting for Colab worker",
            created_at=now,
            updated_at=now,
        ))
        db.delete(draft)
        db.commit()

    safe_send(
        chat_id,
        f"✅ {job_type.replace('_', ' ').title()} queued.\nJob: {job_id}\n"
        f"Files: {len(links)}\nTemplate: {payload['template']}\n"
        "Colab will apply the shared thumbnail when connected.",
        message_id,
    )


def handle_non_command_message(user_id: int, chat_id: int, message: dict[str, Any]) -> None:
    message_id = int(message["message_id"])
    text = message.get("text") or message.get("caption") or ""
    urls = extract_urls(text) if text else []

    with SessionLocal() as db:
        draft = db.get(Draft, user_id)
        if not draft:
            return

        if urls:
            if not all(is_google_drive_url(url) for url in urls):
                safe_send(chat_id, "Thumbnail drafts accept Google Drive links only.", message_id)
                return
            current = json.loads(draft.links_json or "[]")
            if draft.mode == "single_thumbnail" and current:
                safe_send(chat_id, "Single thumbnail mode accepts exactly one Drive link.", message_id)
                return
            if len(current) + len(urls) > MAX_BATCH_LINKS:
                safe_send(chat_id, f"That exceeds the {MAX_BATCH_LINKS}-file limit.", message_id)
                return
            added = 0
            for url in urls:
                if url not in current:
                    current.append(url)
                    added += 1
            draft.links_json = json.dumps(current)
            db.commit()
            safe_send(chat_id, f"📎 Added {added} new link(s). Draft total: {len(current)}.", message_id)
            return

        photos = message.get("photo")
        if photos:
            # Telegram photo messages contain multiple sizes. The largest variant
            # is forwarded; Colab retrieves it from the intake channel using Telethon.
            largest = photos[-1]
            file_size = int(largest.get("file_size", 0) or 0)
            width = int(largest.get("width", 0) or 0)
            height = int(largest.get("height", 0) or 0)
            if width < 100 or height < 100:
                safe_send(chat_id, "❌ Thumbnail is too small. Send a clear image at least 100×100 pixels.", message_id)
                return
            if file_size and file_size > 19 * 1024 * 1024:
                safe_send(chat_id, "❌ Thumbnail photo is too large. Send it as a compressed Telegram photo, not a document.", message_id)
                return
            if not INTAKE_CHANNEL_ID:
                safe_send(chat_id, "INTAKE_CHANNEL_ID is not configured in Render.", message_id)
                return
            try:
                forwarded_id = forward_thumbnail_to_intake(chat_id, message_id)
                draft.thumbnail_message_id = str(forwarded_id)
                db.commit()
                safe_send(chat_id, "🖼 Thumbnail received and forwarded to intake. Send /batchdone when ready.", message_id)
            except Exception as exc:
                log.exception("Thumbnail forwarding failed")
                safe_send(
                    chat_id,
                    "❌ Could not forward the thumbnail. Check that the bot is an administrator "
                    "in the private intake channel and can post messages.\n"
                    f"Telegram error: {str(exc)[:350]}",
                    message_id,
                )
            return

        if message.get("document"):
            safe_send(chat_id, "Please send the thumbnail as a Telegram photo, not as a document/file.", message_id)


# ---------------------------------------------------------------------
# Telegram webhook and health
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
    chat = message.get("chat") or {}
    user_id = sender.get("id")
    chat_id = chat.get("id")
    message_id = message.get("message_id")

    if not user_id or not chat_id or not message_id or chat.get("type") != "private":
        return jsonify({"ok": True})

    try:
        if not is_authorized(int(user_id)):
            return jsonify({"ok": True})
        text = message.get("text") or ""
        if text.startswith("/"):
            handle_command(int(user_id), int(chat_id), int(message_id), text)
        else:
            handle_non_command_message(int(user_id), int(chat_id), message)
    except Exception:
        log.exception("Webhook update processing failed")
        safe_send(int(chat_id), "❌ Could not process that message. Check Render logs.", int(message_id))

    return jsonify({"ok": True})


@app.get("/")
def index():
    return jsonify({
        "ok": True,
        "service": "telegram-render-controller",
        "mode": "controller_only",
        "health": "/health",
        "webhook": "/webhook",
    })


@app.get("/health")
def health():
    db_ok = ensure_database()
    return jsonify({
        "ok": bool(db_ok),
        "service": "telegram-render-controller",
        "database": "connected" if db_ok else "unavailable",
        "time": utcnow().isoformat(),
    }), (200 if db_ok else 503)


# ---------------------------------------------------------------------
# Worker API. Colab must use these exact routes and X-Worker-Token header.
# ---------------------------------------------------------------------

def require_worker_auth():
    supplied = request.headers.get("X-Worker-Token", "")
    if not supplied or not hmac.compare_digest(supplied, WORKER_API_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return None


@app.post("/worker/heartbeat")
def worker_heartbeat():
    auth = require_worker_auth()
    if auth:
        return auth
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
    auth = require_worker_auth()
    if auth:
        return auth
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
    auth = require_worker_auth()
    if auth:
        return auth
    data = request.get_json(silent=True) or {}
    worker_id = str(data.get("worker_id", "colab"))[:128]
    now = utcnow()
    stale_before = now - timedelta(hours=STALE_JOB_HOURS)

    with SessionLocal() as db:
        state = get_worker_state(db)
        last_seen = state.last_seen
        if not state.connected or not last_seen:
            return jsonify({"ok": False, "error": "worker_not_connected"}), 409
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        if (now - last_seen).total_seconds() > 120:
            state.connected = False
            db.commit()
            return jsonify({"ok": False, "error": "heartbeat_stale"}), 409

        # Re-queue jobs left running by a worker that disappeared long ago.
        stale_jobs = db.scalars(
            select(Job).where(
                Job.status == "running",
                Job.updated_at < stale_before,
                Job.attempts < JOB_MAX_ATTEMPTS,
            )
        ).all()
        for old_job in stale_jobs:
            old_job.status = "queued"
            old_job.worker_id = ""
            old_job.message = "Re-queued after stale worker interruption"
            old_job.updated_at = now

        exhausted = db.scalars(
            select(Job).where(
                Job.status == "running",
                Job.updated_at < stale_before,
                Job.attempts >= JOB_MAX_ATTEMPTS,
            )
        ).all()
        for old_job in exhausted:
            old_job.status = "failed"
            old_job.message = "Worker disappeared; retry limit reached."
            old_job.finished_at = now
            old_job.updated_at = now
        db.flush()

        # Lock one queue row so two Colab pollers don't claim the same job.
        job = db.scalar(
            select(Job)
            .where(Job.status == "queued")
            .order_by(Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if not job:
            db.commit()
            return jsonify({"ok": True, "job": None})

        job.status = "running"
        job.worker_id = worker_id
        job.attempts += 1
        job.message = f"Claimed by {worker_id}"
        job.updated_at = now
        db.commit()

        return jsonify({
            "ok": True,
            "job": {
                "id": job.id,
                "owner_id": job.owner_id,
                "type": job.job_type,
                "payload": json.loads(job.payload_json),
                "attempt": job.attempts,
            },
        })


@app.post("/worker/jobs/<job_id>/status")
def worker_job_status(job_id: str):
    auth = require_worker_auth()
    if auth:
        return auth
    data = request.get_json(silent=True) or {}
    try:
        progress = max(0, min(100, int(data.get("progress", 0))))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "invalid_progress"}), 400
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
        return jsonify({"ok": True, "cancel_requested": False, "owner_id": job.owner_id})


@app.get("/worker/jobs/<job_id>")
def worker_get_job(job_id: str):
    auth = require_worker_auth()
    if auth:
        return auth
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


@app.get("/worker/jobs/<job_id>/cancel-check")
def worker_cancel_check(job_id: str):
    auth = require_worker_auth()
    if auth:
        return auth
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        return jsonify({"ok": True, "cancel_requested": job.status == "cancel_requested"})


@app.post("/worker/jobs/<job_id>/complete")
def worker_job_complete(job_id: str):
    auth = require_worker_auth()
    if auth:
        return auth
    data = request.get_json(silent=True) or {}
    result_text = str(data.get("message", "Completed successfully"))[:1500]
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_not_found"}), 404
        job.status = "cancelled" if job.status == "cancel_requested" else "completed"
        job.progress = 100
        job.message = result_text
        job.updated_at = utcnow()
        job.finished_at = utcnow()
        owner_id = job.owner_id
        final_status = job.status
        db.commit()
    safe_send(owner_id, f"✅ Job {job_id}: {final_status}\n{result_text}")
    return jsonify({"ok": True, "status": final_status})


@app.post("/worker/jobs/<job_id>/fail")
def worker_job_fail(job_id: str):
    auth = require_worker_auth()
    if auth:
        return auth
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


# ---------------------------------------------------------------------
# Webhook registration
# ---------------------------------------------------------------------

def configure_webhook_once() -> None:
    global WEBHOOK_CONFIGURED
    with WEBHOOK_LOCK:
        if WEBHOOK_CONFIGURED:
            return
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
            WEBHOOK_CONFIGURED = True
            log.info("Telegram webhook configured: %s", result)
        except Exception:
            # Do not kill Gunicorn if Telegram is temporarily unreachable.
            log.exception("Webhook registration failed; will retry on health checks.")


@app.get("/webhook/setup")
def webhook_setup():
    # Manual retry endpoint, protected by worker token.
    auth = require_worker_auth()
    if auth:
        return auth
    configure_webhook_once()
    return jsonify({"ok": WEBHOOK_CONFIGURED}), (200 if WEBHOOK_CONFIGURED else 503)


# Initial startup is deliberately non-fatal if Telegram or PostgreSQL is
# temporarily unavailable. Gunicorn can still boot and /health remains visible.
try:
    ensure_database()
except Exception:
    log.exception("Initial database setup failed.")

try:
    configure_webhook_once()
except Exception:
    log.exception("Initial webhook setup failed.")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
