import atexit
import hmac
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps

import qbittorrentapi
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, has_app_context, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, text

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except Exception:
    croniter = None
    CRONITER_AVAILABLE = False

APP_VERSION = "v1.3.6"

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "autoiso-v2-secret-key")
app.permanent_session_lifetime = timedelta(days=7)

app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{os.getenv('DB_PATH', '/data/autoiso.db')}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

TZ = timezone(timedelta(hours=8))
WAITING_TAG = os.getenv("QB_WAITING_TAG", "待封装")
PACKING_TAG = os.getenv("QB_PACKING_TAG", "封装中")
DONE_TAG = os.getenv("QB_DONE_TAG", "已封装")
FAILED_TAG = os.getenv("QB_FAILED_TAG", "封装失败")
DEFAULT_QB_MONITOR_TAG = os.getenv("QB_MONITOR_TAG", WAITING_TAG)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")
DEFAULT_ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
DEFAULT_UPLOAD_CRON = os.getenv("DEFAULT_UPLOAD_CRON", "0 2 * * *")
DEFAULT_CLOUDDRIVE_PATH = os.getenv("DEFAULT_CLOUDDRIVE_PATH", "/CloudNAS/115/电影备份")
DEFAULT_AGENT_TOKEN = os.getenv("AGENT_TOKEN", "autoiso_secret_token")
DEFAULT_RENAME_TRIGGER_TAGS = os.getenv("RENAME_TRIGGER_TAGS", "MOVIEPILOT, 已整理")
DEFAULT_RENAME_FINISH_TAG = os.getenv("RENAME_FINISH_TAG", "已重命名")
DEFAULT_MP_STAGING_PATH = os.getenv("MP_STAGING_PATH", "/Downloads/MP-LINK缓存区")
DEFAULT_MP_FINAL_PATH = os.getenv("MP_FINAL_PATH", "/Downloads/115-LINK")
DEFAULT_RENAME_ENABLED = os.getenv("RENAME_ENABLED", "1")
DEFAULT_RENAME_MOVE_ALL = os.getenv("RENAME_MOVE_ALL", "0")
PACK_POLL_INTERVAL_MINUTES = int(os.getenv("PACK_POLL_INTERVAL_MINUTES", "2"))
LOG_FILE = os.getenv("LOG_FILE", "/data/autoiso.log")

STATUS_PROCESSING = "Processing"
STATUS_PACKED_PENDING_UPLOAD = "封装完成，待上传"
STATUS_PACKED_ONLY = "已封装"
STATUS_UPLOADING = "上传中"
STATUS_UPLOADED = "已上传至网盘"
STATUS_FAILED = "Failed"
PACKING_SUFFIX = ".packing"
UPLOADING_SUFFIX = ".uploading"
CLOUDDRIVE_DELIVERY_NOTICE = "文件已成功移入 CloudDrive 底层挂载点，请耐心等待 CloudDrive 在后台努力搬运至云端！🚀"

ACTIVE_TASKS = {}
ACTIVE_TASKS_LOCK = threading.Lock()
SCRAPE_INFLIGHT = set()
SCRAPE_INFLIGHT_LOCK = threading.Lock()
AGENT_TASKS = {}
AGENT_TASKS_LOCK = threading.Lock()
AGENT_PENDING_TASKS = {}
AGENT_PENDING_TASKS_LOCK = threading.Lock()
WAITING_TAG_TASKS = {}
WAITING_TAG_TASKS_LOCK = threading.Lock()
AGENT_TASK_TTL_SECONDS = int(os.getenv("AGENT_TASK_TTL_SECONDS", "3600"))
UPLOAD_WHITELIST = set()
UPLOAD_WHITELIST_LOCK = threading.Lock()
current_uploading_file = None
upload_command = "running"
UPLOAD_COMMAND_LOCK = threading.Lock()
current_upload_status = {
    "active": False,
    "file_name": "",
    "status": "idle",
}
UPLOAD_PROGRESS = {
    "active": False,
    "file_name": "",
    "status": "uploading",
    "percentage": 0.0,
    "speed_mbps": 0.0,
    "eta": "-",
    "bytes_done": 0,
    "total_bytes": 0,
}
UPLOAD_PROGRESS_LOCK = threading.Lock()
UPLOAD_ENGINE_LOCK = threading.Lock()
GB = 1024**3
LOG_TS_RE = re.compile(r"^(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:,\d{3})?\s")
TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
SCRAPE_STATUS_SUCCESS = "成功"
SCRAPE_STATUS_FAILED = "失败"
SCRAPE_NOISE_RE = re.compile(
    r"(?i)\b(?:2160p|1080p|720p|480p|4k|uhd|blu[\s-]?ray|bdrip|brrip|remux|web[\s-]?dl|webrip|hdtv|dvdrip|x264|x265|h\.?264|h\.?265|hevc|avc|aac|ac3|dts(?:-?hd)?|truehd|atmos|hdr10\+?|dolby[\s-]?vision|dv|10bit|8bit|proper|repack|extended|uncut|sample|subs?|multi|dual(?:audio)?|国语|粤语|中字|简繁|内封|官译)\b"
)
SCRAPE_EPISODE_RE = re.compile(r"(?i)\b(?:S\d{1,2}E\d{1,2}|\d{1,2}x\d{1,2}|第\d+[季集])\b")
SCRAPE_YEAR_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
AGENT_UPLOAD_POLICIES = {"instant", "pause", "global", "custom"}
AGENT_UPLOAD_DEFAULT_POLICY = "instant"


def compute_agent_speed_mbps(previous, progress, file_size_gb, now_dt, fallback_speed=0.0):
    if not file_size_gb or file_size_gb <= 0:
        return fallback_speed, None

    try:
        progress_pct = max(0.0, min(100.0, float(progress or 0.0)))
    except (TypeError, ValueError):
        progress_pct = 0.0

    total_bytes = float(file_size_gb) * GB
    processed_bytes = total_bytes * (progress_pct / 100.0)

    if not isinstance(previous, dict):
        return fallback_speed, processed_bytes

    prev_bytes = previous.get("last_processed_bytes")
    prev_time = previous.get("last_update")
    if not isinstance(prev_time, datetime):
        return fallback_speed, processed_bytes

    try:
        prev_bytes_val = float(prev_bytes)
    except (TypeError, ValueError):
        return fallback_speed, processed_bytes

    delta_time = (now_dt - prev_time).total_seconds()
    delta_bytes = processed_bytes - prev_bytes_val
    if delta_time <= 0 or delta_bytes < 0:
        return fallback_speed, processed_bytes

    speed_bps = delta_bytes / delta_time
    speed_mbps = speed_bps / (1024 * 1024)
    return speed_mbps, processed_bytes


class QBServer(db.Model):
    __tablename__ = "qb_servers"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    url = db.Column(db.String(255), nullable=False)
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(255), nullable=False)


class PackHistory(db.Model):
    __tablename__ = "pack_history"

    id = db.Column(db.Integer, primary_key=True)
    task_name = db.Column(db.String(255), nullable=False)
    qb_server_id = db.Column(db.Integer, db.ForeignKey("qb_servers.id"), nullable=False)
    status = db.Column(db.String(20), nullable=False)
    start_time = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(TZ))
    end_time = db.Column(db.DateTime, nullable=True)
    file_size_gb = db.Column(db.Float, nullable=True)
    message = db.Column(db.Text, nullable=True)
    info = db.Column(db.Text, nullable=True)
    upload_start_time = db.Column(db.Text, nullable=True)
    upload_end_time = db.Column(db.Text, nullable=True)

    qb_server = db.relationship("QBServer", backref=db.backref("histories", lazy=True))


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), nullable=False, unique=True)
    value = db.Column(db.Text, nullable=False)


class UploadHistory(db.Model):
    __tablename__ = "upload_history"

    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, nullable=True, index=True)
    filename = db.Column(db.String(512), nullable=False, unique=True, index=True)
    status = db.Column(db.String(32), nullable=False, default="pending")
    uploaded_at = db.Column(db.DateTime, nullable=True)
    upload_start_time = db.Column(db.Text, nullable=True)
    upload_end_time = db.Column(db.Text, nullable=True)
    message = db.Column(db.Text, nullable=True)
    auto_upload = db.Column(db.Boolean, nullable=True)


class ScrapeRecord(db.Model):
    __tablename__ = "scrape_records"

    id = db.Column(db.Integer, primary_key=True)
    original_name = db.Column(db.String(512), nullable=False, unique=True, index=True)
    tmdb_id = db.Column(db.Integer, nullable=True, index=True)
    title = db.Column(db.String(255), nullable=True)
    year = db.Column(db.String(8), nullable=True)
    poster_url = db.Column(db.String(512), nullable=True)
    overview = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(16), nullable=False, default=SCRAPE_STATUS_FAILED)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.now(TZ),
        onupdate=lambda: datetime.now(TZ),
    )


class AgentNode(db.Model):
    __tablename__ = "agent_nodes"

    id = db.Column(db.Integer, primary_key=True)
    node_name = db.Column(db.String(128), nullable=False, unique=True, index=True)
    qb_url = db.Column(db.String(255), nullable=False)
    qb_user = db.Column(db.String(128), nullable=False)
    qb_pass = db.Column(db.String(255), nullable=False)
    temp_path = db.Column(db.String(512), nullable=False)
    cd2_path = db.Column(db.String(512), nullable=False)
    auto_upload = db.Column(db.Boolean, nullable=False, default=True)
    upload_policy = db.Column(db.String(16), nullable=False, default=AGENT_UPLOAD_DEFAULT_POLICY)
    upload_cron = db.Column(db.String(64), nullable=False, default="")


class RenameRule(db.Model):
    __tablename__ = "rename_rules"

    id = db.Column(db.Integer, primary_key=True)
    qb_tag = db.Column(db.String(255), nullable=False, unique=True)
    suffix = db.Column(db.String(255), nullable=False)


def setup_logging():
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger_obj = logging.getLogger("autoiso")
    logger_obj.setLevel(logging.INFO)

    if logger_obj.handlers:
        return logger_obj

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger_obj.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger_obj.addHandler(stream_handler)

    return logger_obj


logger = setup_logging()
if not CRONITER_AVAILABLE:
    logger.warning("croniter 未安装，/api/agent/can_upload 将回退 APScheduler 判定。")
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


def now_local():
    return datetime.now(TZ)


def require_login(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


@app.before_request
def auth_guard():
    endpoint = request.endpoint or ""
    public_endpoints = {"login", "static"}
    if endpoint in public_endpoints:
        return
    if request.path in [
        "/api/agent/report",
        "/api/agent/pending",
        "/api/agent/config",
        "/api/agent/can_upload",
        "/api/agent/rename_rules",
    ]:
        return
    if session.get("logged_in"):
        return
    if request.path.startswith("/api/"):
        return jsonify({"error": "未登录或会话已过期"}), 401
    return redirect(url_for("login"))


def ensure_schema():
    db.create_all()
    with db.engine.begin() as conn:
        result = conn.execute(text("PRAGMA table_info(pack_history)"))
        columns = {row[1] for row in result.fetchall()}
        if "file_size_gb" not in columns:
            conn.execute(text("ALTER TABLE pack_history ADD COLUMN file_size_gb FLOAT"))
        if "info" not in columns:
            conn.execute(text("ALTER TABLE pack_history ADD COLUMN info TEXT"))
        if "upload_start_time" not in columns:
            conn.execute(text("ALTER TABLE pack_history ADD COLUMN upload_start_time TEXT"))
        if "upload_end_time" not in columns:
            conn.execute(text("ALTER TABLE pack_history ADD COLUMN upload_end_time TEXT"))

        upload_result = conn.execute(text("PRAGMA table_info(upload_history)"))
        upload_columns = {row[1] for row in upload_result.fetchall()}
        if "task_id" not in upload_columns:
            conn.execute(text("ALTER TABLE upload_history ADD COLUMN task_id INTEGER"))
        if "upload_start_time" not in upload_columns:
            conn.execute(text("ALTER TABLE upload_history ADD COLUMN upload_start_time TEXT"))
        if "upload_end_time" not in upload_columns:
            conn.execute(text("ALTER TABLE upload_history ADD COLUMN upload_end_time TEXT"))
        if "auto_upload" not in upload_columns:
            conn.execute(text("ALTER TABLE upload_history ADD COLUMN auto_upload BOOLEAN"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS scrape_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_name VARCHAR(512) NOT NULL UNIQUE,
                    tmdb_id INTEGER,
                    title VARCHAR(255),
                    year VARCHAR(8),
                    poster_url VARCHAR(512),
                    overview TEXT,
                    status VARCHAR(16) NOT NULL DEFAULT '失败',
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_records_original_name ON scrape_records(original_name)"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS agent_nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_name VARCHAR(128) NOT NULL UNIQUE,
                    qb_url VARCHAR(255) NOT NULL,
                    qb_user VARCHAR(128) NOT NULL,
                    qb_pass VARCHAR(255) NOT NULL,
                    temp_path VARCHAR(512) NOT NULL,
                    cd2_path VARCHAR(512) NOT NULL,
                    auto_upload BOOLEAN NOT NULL DEFAULT 1,
                    upload_policy VARCHAR(16) NOT NULL DEFAULT 'instant',
                    upload_cron VARCHAR(64) NOT NULL DEFAULT ''
                )
                """
            )
        )
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_nodes_node_name ON agent_nodes(node_name)"))
        agent_result = conn.execute(text("PRAGMA table_info(agent_nodes)"))
        agent_columns = {row[1] for row in agent_result.fetchall()}
        if "auto_upload" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN auto_upload BOOLEAN NOT NULL DEFAULT 1"))
        if "upload_policy" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN upload_policy VARCHAR(16) NOT NULL DEFAULT 'instant'"))
        if "upload_cron" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN upload_cron VARCHAR(64) NOT NULL DEFAULT ''"))
        
        conn.execute(
            text(
                """
                UPDATE agent_nodes
                SET upload_policy = 'instant'
                WHERE upload_policy IS NULL
                   OR TRIM(upload_policy) = ''
                   OR LOWER(TRIM(upload_policy)) NOT IN ('instant', 'pause', 'global', 'custom')
                """
            )
        )
        conn.execute(text("UPDATE agent_nodes SET upload_cron = '' WHERE upload_cron IS NULL"))
        
        conn.execute(
            text(
                """
                INSERT INTO qb_servers(name, url, username, password)
                SELECT 'NAS', 'local://nas', '-', '-'
                WHERE NOT EXISTS (SELECT 1 FROM qb_servers WHERE name = 'NAS')
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE pack_history
                SET qb_server_id = (SELECT id FROM qb_servers WHERE name = 'NAS' LIMIT 1)
                WHERE qb_server_id IN (SELECT id FROM qb_servers WHERE url LIKE 'agent://%')
                """
            )
        )
        conn.execute(text("DELETE FROM qb_servers WHERE url LIKE 'agent://%'"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS rename_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qb_tag VARCHAR(255) NOT NULL UNIQUE,
                    suffix VARCHAR(255) NOT NULL
                )
                """
            )
        )
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_rename_rules_qb_tag ON rename_rules(qb_tag)"))


def get_setting(key):
    item = SystemSetting.query.filter_by(key=key).first()
    return item.value if item else ""


def set_setting(key, value):
    item = SystemSetting.query.filter_by(key=key).first()
    if item:
        item.value = value
    else:
        db.session.add(SystemSetting(key=key, value=value))


def delete_setting(key):
    item = SystemSetting.query.filter_by(key=key).first()
    if item:
        db.session.delete(item)


def get_auth_credentials():
    username = get_setting("auth_username") or DEFAULT_ADMIN_USERNAME
    password = get_setting("auth_password") or DEFAULT_ADMIN_PASSWORD
    return username, password


def get_upload_cron():
    return get_setting("upload_cron") or DEFAULT_UPLOAD_CRON


def get_clouddrive_path():
    return get_setting("clouddrive_path") or DEFAULT_CLOUDDRIVE_PATH


def get_qb_monitor_tag():
    return (get_setting("qb_monitor_tag") or DEFAULT_QB_MONITOR_TAG).strip() or DEFAULT_QB_MONITOR_TAG


def get_agent_token():
    return (get_setting("agent_token") or "").strip() or DEFAULT_AGENT_TOKEN


def get_rename_trigger_tags_raw():
    return (get_setting("rename_trigger_tags") or DEFAULT_RENAME_TRIGGER_TAGS).strip() or DEFAULT_RENAME_TRIGGER_TAGS


def get_rename_finish_tag():
    return (get_setting("rename_finish_tag") or DEFAULT_RENAME_FINISH_TAG).strip() or DEFAULT_RENAME_FINISH_TAG


def get_mp_staging_path():
    return (get_setting("mp_staging_path") or DEFAULT_MP_STAGING_PATH).strip() or DEFAULT_MP_STAGING_PATH


def get_mp_final_path():
    return (get_setting("mp_final_path") or DEFAULT_MP_FINAL_PATH).strip() or DEFAULT_MP_FINAL_PATH


def get_rename_enabled():
    raw = (get_setting("rename_enabled") or "").strip()
    if raw == "":
        return DEFAULT_RENAME_ENABLED
    return raw


def get_rename_move_all():
    raw = (get_setting("rename_move_all") or "").strip()
    if raw == "":
        return DEFAULT_RENAME_MOVE_ALL
    return raw


def normalize_agent_upload_policy(value):
    policy = (value or "").strip().lower()
    return policy if policy in AGENT_UPLOAD_POLICIES else AGENT_UPLOAD_DEFAULT_POLICY


def build_task_name_keys(name):
    safe_name = os.path.basename(str(name or "").strip())
    if not safe_name:
        return set()
    no_ext = os.path.splitext(safe_name)[0]
    keys = {safe_name.lower()}
    if no_ext:
        keys.add(no_ext.lower())
    return keys


def build_pending_history_block_set():
    blocking_statuses = {
        "封装中",
        "待上传",
        "待上传 (阻塞中)",
        "上传中",
        "成功",
        "已上传",
        "已封装",
        STATUS_PACKED_PENDING_UPLOAD,
        STATUS_PACKED_ONLY,
        STATUS_UPLOADED,
    }
    rows = (
        db.session.query(PackHistory.task_name)
        .filter(PackHistory.status.in_(list(blocking_statuses)))
        .all()
    )
    blocked = set()
    for row in rows:
        blocked.update(build_task_name_keys(row[0]))
    return blocked


def extract_node_name_from_history_info(info_text):
    info = str(info_text or "").strip()
    if not info:
        return ""
    match = re.search(r"node\s*=\s*([^\s;]+)", info, flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def resolve_pack_history_node_name(row):
    if not row:
        return "NAS"
    node_name = extract_node_name_from_history_info(row.info)
    if node_name:
        return node_name
    return row.qb_server.name if row.qb_server else "NAS"


def get_global_proxy():
    return (get_setting("global_proxy") or "").strip()


def get_delete_after_upload():
    value = (get_setting("delete_after_upload") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_global_auto_upload():
    raw = get_setting("global_auto_upload")
    if raw == "":
        return True
    return parse_bool(raw, default=True)


def get_task_auto_upload(filename):
    safe_name = os.path.basename(str(filename or "").strip())
    if not safe_name:
        return get_global_auto_upload()
    row = UploadHistory.query.filter_by(filename=safe_name).first()
    if row and row.auto_upload is not None:
        return bool(row.auto_upload)
    return get_global_auto_upload()


def set_task_auto_upload(filename, enabled):
    safe_name = os.path.basename(str(filename or "").strip())
    if not safe_name:
        raise ValueError("文件名不合法")
    row = UploadHistory.query.filter_by(filename=safe_name).first()
    if not row:
        row = UploadHistory(filename=safe_name, status="pending", message="")
        db.session.add(row)
    row.auto_upload = bool(enabled)
    db.session.commit()
    return row


def init_task_auto_upload(filename, task_id=None):
    safe_name = os.path.basename(str(filename or "").strip())
    if not safe_name:
        raise ValueError("文件名不合法")
    global_enabled = bool(get_global_auto_upload())
    row = UploadHistory.query.filter_by(filename=safe_name).first()
    if not row:
        row = UploadHistory(
            filename=safe_name,
            task_id=task_id,
            status="pending" if global_enabled else "packed",
            message="",
            auto_upload=global_enabled,
        )
        db.session.add(row)
    else:
        if task_id is not None:
            row.task_id = task_id
        row.auto_upload = global_enabled
    db.session.commit()
    return global_enabled


def apply_auto_upload_status_for_task(filename, enabled):
    safe_name = os.path.basename(str(filename or "").strip())
    if not safe_name:
        return

    upload_row = UploadHistory.query.filter_by(filename=safe_name).first()
    if not upload_row:
        upload_row = UploadHistory(filename=safe_name, status="pending" if enabled else "packed", message="")
        db.session.add(upload_row)
    else:
        cur_upload_status = (upload_row.status or "").strip().lower()
        if enabled and cur_upload_status == "packed":
            upload_row.status = "pending"
        elif not enabled and cur_upload_status == "pending":
            upload_row.status = "packed"

    pack_row = find_pack_row_for_upload(safe_name)
    if not pack_row:
        return

    cur_pack_status = (pack_row.status or "").strip()
    pending_statuses = {STATUS_PACKED_PENDING_UPLOAD, "待上传", "pending"}
    if enabled and cur_pack_status == STATUS_PACKED_ONLY:
        pack_row.status = STATUS_PACKED_PENDING_UPLOAD
    elif (not enabled) and cur_pack_status in pending_statuses:
        pack_row.status = STATUS_PACKED_ONLY


def apply_global_auto_upload_status(enabled):
    pending_statuses = {STATUS_PACKED_PENDING_UPLOAD, "待上传", "pending"}
    if enabled:
        PackHistory.query.filter(PackHistory.status == STATUS_PACKED_ONLY).update(
            {"status": STATUS_PACKED_PENDING_UPLOAD},
            synchronize_session=False,
        )
        UploadHistory.query.filter_by(status="packed").update({"status": "pending"}, synchronize_session=False)
    else:
        PackHistory.query.filter(PackHistory.status.in_(list(pending_statuses))).update(
            {"status": STATUS_PACKED_ONLY},
            synchronize_session=False,
        )
        UploadHistory.query.filter_by(status="pending").update({"status": "packed"}, synchronize_session=False)


def get_enable_tmdb():
    return parse_bool(get_setting("enable_tmdb"), default=False)


def get_tmdb_api_key():
    return (get_setting("tmdb_api_key") or "").strip()


def extract_year(value):
    m = SCRAPE_YEAR_RE.search(str(value or ""))
    return m.group(1) if m else ""


def clean_filename(name):
    raw = os.path.basename(str(name or "").strip())
    raw = os.path.splitext(raw)[0]
    if not raw:
        return "", ""

    year_match = SCRAPE_YEAR_RE.search(raw)
    year_token = year_match.group(1) if year_match else ""
    core = raw[: year_match.start()] if year_match else raw

    text_name = core.replace(".", " ").replace("_", " ").replace("-", " ")
    text_name = re.sub(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}|【[^】]*】|（[^）]*）", " ", text_name)
    text_name = SCRAPE_EPISODE_RE.sub(" ", text_name)
    text_name = SCRAPE_NOISE_RE.sub(" ", text_name)
    text_name = re.sub(r"[^\w\u4e00-\u9fff]+", " ", text_name, flags=re.UNICODE)
    text_name = re.sub(r"\s+", " ", text_name).strip()

    if not text_name:
        text_name = re.sub(r"[\._\-]+", " ", core).strip() or raw
    return text_name, year_token


def clean_agent_report_filename(filename):
    raw = os.path.basename(str(filename or "").strip())
    raw = os.path.splitext(raw)[0]
    if not raw:
        return ""
    text_name = raw.replace(".", " ").replace("_", " ").replace("-", " ")
    text_name = re.sub(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}|【[^】]*】|（[^）]*）", " ", text_name)
    text_name = SCRAPE_EPISODE_RE.sub(" ", text_name)
    text_name = SCRAPE_NOISE_RE.sub(" ", text_name)
    text_name = re.sub(r"\s+", " ", text_name).strip()
    return text_name or raw.replace(".", " ").strip()


def normalize_poster_path(poster_value):
    v = (poster_value or "").strip()
    if not v:
        return ""
    if v.startswith(TMDB_IMAGE_BASE):
        return v[len(TMDB_IMAGE_BASE) :]
    return v if v.startswith("/") else ""


def build_poster_url(poster_path):
    p = normalize_poster_path(poster_path)
    return f"{TMDB_IMAGE_BASE}{p}" if p else ""


def tmdb_request(path, *, params=None, api_key=None, proxy_url=None, timeout=10):
    token = (api_key or "").strip() or get_tmdb_api_key()
    if not token:
        raise ValueError("TMDB API Key 未配置")

    req_params = {"api_key": token}
    if params:
        req_params.update(params)

    request_kwargs = {"params": req_params, "timeout": timeout}
    proxy = (proxy_url or "").strip()
    if proxy:
        request_kwargs["proxies"] = {"http": proxy, "https": proxy}
    return requests.get(f"{TMDB_API_BASE}{path}", **request_kwargs)


def format_tmdb_item(item):
    if not item or not item.get("id"):
        return None
    title = (item.get("title") or item.get("name") or item.get("original_title") or item.get("original_name") or "").strip()
    release_date = (item.get("release_date") or item.get("first_air_date") or "").strip()
    year = extract_year(item.get("year") or release_date)
    poster_path = normalize_poster_path(item.get("poster_path") or item.get("poster_url") or "")
    return {
        "id": int(item.get("id")),
        "title": title or f"TMDB#{item.get('id')}",
        "year": year,
        "poster_url": build_poster_url(poster_path),
        "poster_path": poster_path,
        "overview": (item.get("overview") or "").strip(),
    }


def build_display_name(raw_name):
    original = str(raw_name or "").strip()
    if not original:
        return ""

    title = ""
    year = ""

    record = ScrapeRecord.query.filter_by(original_name=original).first()
    if record and record.status == SCRAPE_STATUS_SUCCESS:
        title = (record.title or "").strip()
        year = (record.year or "").strip()
    else:
        clean_title, clean_year = clean_filename(original)
        title = (clean_title or "").strip()
        year = (clean_year or "").strip()

    if not title:
        title = original
    return f"{title} ({year})" if year else title


def search_tmdb_candidates(keyword, limit=10, year=None):
    kw = (keyword or "").strip()
    if not kw:
        return []

    proxy = get_global_proxy()
    year_token = extract_year(year)
    results = []

    if kw.isdigit():
        tmdb_id = int(kw)
        for media_type in ("movie", "tv"):
            try:
                resp = tmdb_request(
                    f"/{media_type}/{tmdb_id}",
                    params={"language": "zh-CN"},
                    proxy_url=proxy,
                )
            except (ValueError, requests.exceptions.RequestException):
                continue
            if resp.status_code == 200:
                data = resp.json() or {}
                data["media_type"] = media_type
                item = format_tmdb_item(data)
                if item:
                    results.append(item)
        return results[:limit]

    def append_results(items, default_media_type=""):
        seen = {(x.get("id"), x.get("title"), x.get("year")) for x in results}
        for item in items:
            if default_media_type and not item.get("media_type"):
                item["media_type"] = default_media_type
            media_type = (item.get("media_type") or "").lower()
            if media_type not in {"movie", "tv", ""}:
                continue
            formatted = format_tmdb_item(item)
            if not formatted:
                continue
            sig = (formatted.get("id"), formatted.get("title"), formatted.get("year"))
            if sig in seen:
                continue
            seen.add(sig)
            results.append(formatted)
            if len(results) >= limit:
                break

    if year_token:
        resp = tmdb_request(
            "/search/movie",
            params={"query": kw, "year": year_token, "language": "zh-CN", "page": 1, "include_adult": "false"},
            proxy_url=proxy,
        )
        if resp.status_code == 200:
            append_results(resp.json().get("results", []), default_media_type="movie")
        if len(results) < limit:
            resp = tmdb_request(
                "/search/tv",
                params={"query": kw, "first_air_date_year": year_token, "language": "zh-CN", "page": 1, "include_adult": "false"},
                proxy_url=proxy,
            )
            if resp.status_code == 200:
                append_results(resp.json().get("results", []), default_media_type="tv")

    if len(results) < limit:
        resp = tmdb_request(
            "/search/multi",
            params={"query": kw, "language": "zh-CN", "page": 1, "include_adult": "false"},
            proxy_url=proxy,
        )
        if resp.status_code == 200:
            append_results(resp.json().get("results", []))

    return results


def upsert_scrape_record(original_name, tmdb_data=None, status=SCRAPE_STATUS_FAILED):
    name = (original_name or "").strip()
    if not name:
        return None

    row = ScrapeRecord.query.filter_by(original_name=name).first()
    if not row:
        row = ScrapeRecord(original_name=name)

    tmdb_data = tmdb_data or {}
    row.tmdb_id = int(tmdb_data["id"]) if tmdb_data.get("id") else None
    row.title = (tmdb_data.get("title") or "").strip() or None
    row.year = extract_year(tmdb_data.get("year") or tmdb_data.get("release_date") or tmdb_data.get("first_air_date")) or None
    row.poster_url = normalize_poster_path(tmdb_data.get("poster_path") or tmdb_data.get("poster_url") or "")
    row.overview = (tmdb_data.get("overview") or "").strip() or None
    row.status = status
    row.updated_at = now_local()
    db.session.add(row)
    db.session.commit()
    return row


def auto_scrape_for_original_name(original_name, preferred_keyword=""):
    if not get_enable_tmdb():
        return None

    name = (original_name or "").strip()
    if not name:
        return None

    cached = ScrapeRecord.query.filter_by(original_name=name).first()
    if cached and cached.status == SCRAPE_STATUS_SUCCESS:
        logger.info("💡 [TMDB刮削] 命中本地缓存: %s -> %s", name, cached.title or cached.tmdb_id)
        return cached

    clean_title, clean_year = clean_filename(name)
    forced_keyword = (preferred_keyword or "").strip()
    keyword = forced_keyword or clean_title or name
    search_year = extract_year(forced_keyword) or clean_year
    try:
        matches = search_tmdb_candidates(keyword, limit=1, year=search_year)
        if not matches and search_year:
            matches = search_tmdb_candidates(keyword, limit=1)
        if not matches and clean_title and clean_title != keyword:
            matches = search_tmdb_candidates(clean_title, limit=1, year=clean_year)
        if not matches and clean_title and clean_title != keyword and clean_year:
            matches = search_tmdb_candidates(clean_title, limit=1)
        if not matches and keyword != name:
            matches = search_tmdb_candidates(name, limit=1)
        if matches:
            row = upsert_scrape_record(name, matches[0], SCRAPE_STATUS_SUCCESS)
            logger.info("🎬 [TMDB刮削] 成功匹配: %s -> 《%s》(%s)", name, row.title, row.year)
            return row
        row = upsert_scrape_record(name, {}, SCRAPE_STATUS_FAILED)
        logger.info(" [TMDB刮削] 未找到匹配项，已加入待处理队列: %s", name)
        return row
    except Exception as exc:
        logger.warning("TMDB 刮削异常: %s | 错误: %s", name, exc)
        return upsert_scrape_record(name, {}, SCRAPE_STATUS_FAILED)


def trigger_auto_scrape_async(original_name, search_keyword=""):
    if not get_enable_tmdb():
        return
    name = (original_name or "").strip()
    if not name:
        return
    keyword = (search_keyword or "").strip()

    cached = ScrapeRecord.query.filter_by(original_name=name, status=SCRAPE_STATUS_SUCCESS).first()
    if cached:
        return

    with SCRAPE_INFLIGHT_LOCK:
        if name in SCRAPE_INFLIGHT:
            return
        SCRAPE_INFLIGHT.add(name)

    def _worker(task_name, task_keyword):
        with app.app_context():
            try:
                auto_scrape_for_original_name(task_name, preferred_keyword=task_keyword)
            except Exception:
                logger.exception("异步 TMDB 刮削失败: %s", task_name)
            finally:
                with SCRAPE_INFLIGHT_LOCK:
                    SCRAPE_INFLIGHT.discard(task_name)

    threading.Thread(target=_worker, args=(name, keyword), daemon=True).start()


def get_notify_flag(key, default=True):
    raw = get_setting(key)
    if raw == "":
        return default
    return parse_bool(raw, default=default)


def parse_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def format_eta(seconds):
    if seconds is None or seconds < 0:
        return "-"
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def update_upload_progress(
    active=False,
    file_name="",
    status="uploading",
    percentage=0.0,
    speed_mbps=0.0,
    eta="-",
    bytes_done=0,
    total_bytes=0,
):
    with UPLOAD_PROGRESS_LOCK:
        UPLOAD_PROGRESS.update(
            {
                "active": bool(active),
                "file_name": file_name or "",
                "status": status or "uploading",
                "percentage": round(float(percentage), 2),
                "speed_mbps": round(float(speed_mbps), 2),
                "eta": eta or "-",
                "bytes_done": int(bytes_done or 0),
                "total_bytes": int(total_bytes or 0),
            }
        )


def get_upload_progress_snapshot():
    with UPLOAD_PROGRESS_LOCK:
        return dict(UPLOAD_PROGRESS)


def set_upload_command(command):
    global upload_command
    with UPLOAD_COMMAND_LOCK:
        upload_command = command


def get_upload_command():
    with UPLOAD_COMMAND_LOCK:
        return upload_command


def update_current_upload_status(active=False, file_name="", status="idle"):
    current_upload_status.update(
        {
            "active": bool(active),
            "file_name": file_name or "",
            "status": status or "idle",
        }
    )


def get_current_upload_status_snapshot():
    return dict(current_upload_status)


def is_valid_upload_file(name):
    if not name:
        return False
    if name.startswith("."):
        return False
    if name.endswith(PACKING_SUFFIX):
        return False
    file_path = os.path.join(OUTPUT_DIR, name)
    return os.path.isfile(file_path)


def get_pending_upload_files():
    try:
        names = sorted(os.listdir(OUTPUT_DIR))
    except FileNotFoundError:
        return []
    except Exception:
        logger.exception("扫描输出目录失败: %s", OUTPUT_DIR)
        return []

    uploaded_names = {
        row.filename
        for row in UploadHistory.query.with_entities(UploadHistory.filename).filter_by(status="uploaded").all()
    }
    pending = []
    for name in names:
        if name.endswith(PACKING_SUFFIX):
            continue
        if not is_valid_upload_file(name):
            continue
        if name in uploaded_names:
            continue
        pending.append(name)
    return pending


def get_upload_list_files():
    try:
        names = sorted(os.listdir(OUTPUT_DIR))
    except FileNotFoundError:
        return []
    except Exception:
        logger.exception("扫描输出目录失败: %s", OUTPUT_DIR)
        return []

    rows = []
    for name in names:
        if not is_valid_upload_file(name):
            continue
        rows.append(name)
    return rows


def mark_upload_status(filename, status, message="", task_id=None):
    if not filename:
        return
    global_auto_upload = bool(get_global_auto_upload())
    row = UploadHistory.query.filter_by(task_id=task_id).first() if task_id else None
    if not row:
        row = UploadHistory.query.filter_by(filename=filename).first()
    if not row:
        row = UploadHistory(
            filename=filename,
            task_id=task_id,
            status=status,
            message=message or "",
            auto_upload=global_auto_upload,
        )
        db.session.add(row)
    else:
        is_new_task_cycle = task_id is not None and row.task_id != task_id
        if task_id is not None:
            row.task_id = task_id
        if is_new_task_cycle:
            row.auto_upload = global_auto_upload
        row.filename = filename
        row.status = status
        row.message = message or ""
    if status == "uploaded":
        row.uploaded_at = now_local()
    db.session.commit()


def find_pack_row_for_upload(file_name):
    safe_name = os.path.basename(file_name or "")
    if not safe_name:
        return None
    task_name = os.path.splitext(safe_name)[0]
    iso_path = os.path.join(OUTPUT_DIR, safe_name)
    iso_msg = f"ISO: {iso_path}"
    file_msg = f"FILE: {iso_path}"

    row = (
        PackHistory.query.filter(
            PackHistory.task_name.in_([safe_name, task_name]),
            PackHistory.message.in_([iso_msg, file_msg]),
            PackHistory.status.in_([STATUS_PACKED_PENDING_UPLOAD, STATUS_UPLOADING]),
        )
        .order_by(PackHistory.id.desc())
        .first()
    )
    if row:
        return row

    return (
        PackHistory.query.filter(
            PackHistory.task_name.in_([safe_name, task_name]),
            PackHistory.message.in_([iso_msg, file_msg]),
        )
        .order_by(PackHistory.id.desc())
        .first()
    )


def get_or_create_external_server_id():
    server = QBServer.query.filter_by(name="NAS").first()
    if server:
        return server.id
    server = QBServer(name="NAS", url="local://nas", username="-", password="-")
    db.session.add(server)
    db.session.commit()
    return server.id


def upsert_agent_history_status(node_name, file_name, status, file_size_gb=0.0):
    safe_name = os.path.basename(str(file_name or "").strip())
    if not safe_name:
        return None

    safe_status = (status or "").strip().lower()
    status_map = {
        "packing": "封装中",
        "uploading": "上传中",
        "pending_upload": "待上传 (阻塞中)",
        "finished": "成功",
        "error": "失败",
    }
    mapped_status = status_map.get(safe_status)
    if not mapped_status:
        return None

    now_dt = now_local()
    normalized_size_gb = max(0.0, float(file_size_gb or 0.0))
    node_label = (node_name or "").strip() or "VPS"
    server_id = get_or_create_external_server_id()
    task_name_no_ext = os.path.splitext(safe_name)[0]
    row = (
        PackHistory.query.filter(
            PackHistory.qb_server_id == server_id,
            PackHistory.task_name.in_([safe_name, task_name_no_ext]),
            PackHistory.message.like("Agent report:%"),
            PackHistory.info == f"node={node_label}",
        )
        .order_by(PackHistory.id.desc())
        .first()
    )

    if safe_status == "packing":
        if not row:
            row = PackHistory(
                task_name=safe_name[:255],
                qb_server_id=server_id,
                status=mapped_status,
                start_time=now_dt,
                file_size_gb=normalized_size_gb,
                message="Agent report: packing",
                info=f"node={node_label}",
            )
            db.session.add(row)
            db.session.commit()
            return row
        row.status = mapped_status
        if not row.start_time:
            row.start_time = now_dt
        if normalized_size_gb > 0 or not (row.file_size_gb and row.file_size_gb > 0):
            row.file_size_gb = normalized_size_gb
        row.message = "Agent report: packing"
        row.info = f"node={node_label}"
        db.session.commit()
        return row

    if not row:
        row = PackHistory(
            task_name=safe_name[:255],
            qb_server_id=server_id,
            status=mapped_status,
            start_time=now_dt,
            message=f"Agent report: {safe_status}",
            info=f"node={node_label}",
        )
        db.session.add(row)

    row.status = mapped_status
    row.message = f"Agent report: {safe_status}"
    row.info = f"node={node_label}"
    if normalized_size_gb > 0 or not (row.file_size_gb and row.file_size_gb > 0):
        row.file_size_gb = normalized_size_gb

    # === 精准打刻 VPS 的各个时间点 ===
    if safe_status == "pending_upload":
        if not row.end_time:
            row.end_time = now_dt
    elif safe_status == "uploading":
        if not row.end_time:
            row.end_time = now_dt
        if not row.upload_start_time:
            row.upload_start_time = format_db_time(now_dt)
    elif safe_status == "finished":
        if not row.end_time:
            row.end_time = now_dt
        if not row.upload_start_time:
            row.upload_start_time = format_db_time(now_dt)
        row.upload_end_time = format_db_time(now_dt)

    db.session.commit()
    return row


def finalize_pack_history_after_upload(file_name, dst_path, task_id=None):
    if not has_app_context():
        return
    if not file_name:
        return
    row = db.session.get(PackHistory, task_id) if task_id else None
    if row:
        row.status = STATUS_UPLOADED
        row.end_time = now_local()
        row.message = f"已上传: {dst_path}"
        row.info = ""
        if not row.upload_start_time:
            row.upload_start_time = format_db_time(now_local())
        row.upload_end_time = format_db_time(now_local())
        db.session.commit()
        return

    ext_server_id = get_or_create_external_server_id()
    now_dt = now_local()
    safe_task_name = file_name[:255]
    size_gb = 0.0
    try:
        size_gb = round(os.path.getsize(dst_path) / GB, 3)
    except OSError:
        size_gb = 0.0
    new_row = PackHistory(
        task_name=safe_task_name,
        qb_server_id=ext_server_id,
        status=STATUS_UPLOADED,
        start_time=now_dt,
        end_time=now_dt,
        file_size_gb=size_gb,
        message=f"已上传: {dst_path}",
        info="外部文件",
        upload_start_time=format_db_time(now_dt),
        upload_end_time=format_db_time(now_dt),
    )
    db.session.add(new_row)
    db.session.commit()


def upload_file_with_progress(src_path, dst_path, delete_after=False, task_id=None):
    global current_uploading_file
    chunk_size = 10 * 1024 * 1024
    total_bytes = os.path.getsize(src_path)
    bytes_done = 0
    start_ts = time.time()
    upload_started_at = now_local()
    update_upload_timestamps(os.path.basename(src_path), start_time=upload_started_at, task_id=task_id)
    temp_dst_path = f"{dst_path}{UPLOADING_SUFFIX}"
    aborted = False
    paused_logged = False

    try:
        if os.path.exists(temp_dst_path):
            os.remove(temp_dst_path)
    except OSError:
        logger.warning("清理旧上传临时文件失败: %s", temp_dst_path)

    update_upload_progress(
        active=True,
        file_name=os.path.basename(src_path),
        status="uploading",
        percentage=0.0,
        speed_mbps=0.0,
        eta="-",
        bytes_done=0,
        total_bytes=total_bytes,
    )

    try:
        with open(src_path, "rb") as rf, open(temp_dst_path, "wb") as wf:
            while True:
                command = get_upload_command()
                if command == "aborted":
                    logger.warning("上传收到中止指令，文件=%s", os.path.basename(src_path))
                    aborted = True
                    break
                if command == "paused":
                    if not paused_logged:
                        logger.info("上传已暂停，文件=%s", os.path.basename(src_path))
                        paused_logged = True
                    percent = (bytes_done / total_bytes * 100) if total_bytes > 0 else 0.0
                    update_upload_progress(
                        active=True,
                        file_name=os.path.basename(src_path),
                        status="paused",
                        percentage=percent,
                        speed_mbps=0.0,
                        eta="paused",
                        bytes_done=bytes_done,
                        total_bytes=total_bytes,
                    )
                    update_current_upload_status(
                        active=True,
                        file_name=os.path.basename(src_path),
                        status="paused",
                    )
                    time.sleep(1)
                    continue

                if paused_logged:
                    logger.info("上传已恢复，文件=%s", os.path.basename(src_path))
                    paused_logged = False
                update_current_upload_status(
                    active=True,
                    file_name=os.path.basename(src_path),
                    status="uploading",
                )

                chunk = rf.read(chunk_size)
                if not chunk:
                    break

                wf.write(chunk)
                bytes_done += len(chunk)
                elapsed = max(time.time() - start_ts, 1e-6)
                speed_bps = bytes_done / elapsed
                speed_mbps = speed_bps / (1024 * 1024)
                remaining = max(total_bytes - bytes_done, 0)
                eta_seconds = (remaining / speed_bps) if speed_bps > 0 else -1
                percent = (bytes_done / total_bytes * 100) if total_bytes > 0 else 100.0
                update_upload_progress(
                    active=True,
                    file_name=os.path.basename(src_path),
                    status="uploading",
                    percentage=percent,
                    speed_mbps=speed_mbps,
                    eta=format_eta(eta_seconds),
                    bytes_done=bytes_done,
                    total_bytes=total_bytes,
                )
            wf.flush()

        if aborted:
            try:
                if os.path.isfile(temp_dst_path):
                    os.remove(temp_dst_path)
            except OSError:
                logger.warning("中止上传后清理临时文件失败: %s", temp_dst_path)
            current_uploading_file = None
            logger.info("上传已中止并清理临时文件，文件=%s", os.path.basename(src_path))
            return False

        os.rename(temp_dst_path, dst_path)

        if delete_after:
            os.remove(src_path)

        update_upload_timestamps(os.path.basename(src_path), end_time=now_local(), task_id=task_id)
        finalize_pack_history_after_upload(os.path.basename(src_path), dst_path, task_id=task_id)
        return True
    except Exception:
        try:
            if os.path.isfile(temp_dst_path):
                os.remove(temp_dst_path)
        except OSError:
            pass
        raise

def build_cron_trigger(cron_expr):
    expr = (cron_expr or "").strip() or DEFAULT_UPLOAD_CRON
    try:
        return CronTrigger.from_crontab(expr), expr
    except Exception:
        logger.exception("Cron 表达式无效，回退默认值: %s", expr)
        fallback = DEFAULT_UPLOAD_CRON
        return CronTrigger.from_crontab(fallback), fallback


def is_now_in_cron_window(cron_expr, now_dt=None, window_minutes=5):
    expr = (cron_expr or "").strip()
    if not expr:
        return False
    current = now_dt or now_local()
    try:
        if CRONITER_AVAILABLE:
            prev_fire = croniter(expr, current).get_prev(datetime)
        else:
            trigger = CronTrigger.from_crontab(expr, timezone=TZ)
            prev_fire = trigger.get_prev_fire_time(None, current)
    except Exception:
        return False
    if not prev_fire:
        return False
    return 0 <= (current - prev_fire).total_seconds() <= max(1, int(window_minutes)) * 60


def apply_upload_scheduler(cron_expr):
    trigger, normalized = build_cron_trigger(cron_expr)
    set_setting("upload_cron", normalized)
    db.session.commit()

    job = scheduler.get_job("upload_job")
    if job:
        scheduler.reschedule_job("upload_job", trigger=trigger)
    else:
        scheduler.add_job(
            func=process_uploads,
            trigger=trigger,
            id="upload_job",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
    logger.info("上传调度 Cron 已应用: %s", normalized)
    return normalized


def format_seconds(seconds):
    seconds = int(max(0, seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

# === 完美的单位进位转换 ===
def format_data_size(gb_value):
    if gb_value >= (1000 * 1024):
        val = gb_value / (1024 * 1024)
        unit = "PB"
    elif gb_value >= 1000:
        val = gb_value / 1024
        unit = "TB"
    else:
        val = gb_value
        unit = "GB"

    if val >= 100:
        return f"{val:.1f} {unit}"
    else:
        return f"{val:.2f} {unit}"


def format_speed_mbps(speed_mbps):
    try:
        value = float(speed_mbps or 0.0)
    except (TypeError, ValueError):
        value = 0.0
    if value <= 0:
        return "0 MB/s"
    if value < 1:
        return f"{value * 1024:.0f} KB/s"
    return f"{value:.1f} MB/s"


def format_db_time(dt):
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_db_time(raw):
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def update_upload_timestamps(file_name, start_time=None, end_time=None, task_id=None):
    if not has_app_context() or not file_name:
        return
    safe_name = os.path.basename(file_name)

    row = UploadHistory.query.filter_by(task_id=task_id).first() if task_id else None
    if not row and task_id is None:
        row = UploadHistory.query.filter_by(filename=safe_name).first()
    if row:
        if start_time is not None:
            row.upload_start_time = format_db_time(start_time)
            row.upload_end_time = ""
        if end_time is not None:
            row.upload_end_time = format_db_time(end_time)

    pack_row = db.session.get(PackHistory, task_id) if task_id else None
    if pack_row:
        if start_time is not None:
            pack_row.upload_start_time = format_db_time(start_time)
            pack_row.upload_end_time = ""
        if end_time is not None:
            pack_row.upload_end_time = format_db_time(end_time)

    if row or pack_row:
        db.session.commit()


def get_path_size_bytes(path):
    if not path or not os.path.exists(path):
        return 0
    if os.path.isfile(path):
        try:
            return os.path.getsize(path)
        except OSError:
            return 0

    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                total += os.path.getsize(file_path)
            except OSError:
                continue
    return total

def extract_error_tail(stderr, stdout):
    lines = [ln.strip() for ln in (stderr or "").splitlines() if ln.strip()]
    filtered = []
    for ln in lines:
        low = ln.lower()
        if low.startswith("genisoimage "):
            continue
        filtered.append(ln)

    target = filtered if filtered else lines
    if target:
        return "\n".join(target[-5:])

    out_lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip()]
    if out_lines:
        return "\n".join(out_lines[-3:])
    return "unknown error"


def send_tg_notification(text_msg):
    token = get_setting("tg_token")
    chat_id = get_setting("tg_chat_id")
    return send_tg_notification_with_config(text_msg, token, chat_id)


def send_tg_notification_with_config(text_msg, token, chat_id, proxy_url=None):
    if not token or not chat_id:
        return False, "tg_token 或 tg_chat_id 未配置"

    if proxy_url is None:
        proxy_url = get_global_proxy()
    proxy_url = (proxy_url or "").strip()

    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    request_kwargs = {"json": {"chat_id": chat_id, "text": text_msg}, "timeout": 15}
    if proxy_url:
        request_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}

    try:
        resp = requests.post(api_url, **request_kwargs)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            return False, str(data)
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def make_qb_client(server: QBServer):
    client = qbittorrentapi.Client(
        host=server.url,
        username=server.username,
        password=server.password,
        REQUESTS_ARGS={"timeout": 20},
    )
    client.auth_log_in()
    return client


def is_qb_connection_exception(exc):
    if exc is None:
        return False
    text = f"{type(exc).__name__}: {exc}".lower()
    keys = (
        "nameresolutionerror",
        "connectionerror",
        "apiconnectionerror",
        "newconnectionerror",
        "maxretryerror",
        "failed to establish a new connection",
        "failed to resolve",
        "temporary failure in name resolution",
        "nodename nor servname provided",
        "getaddrinfo failed",
    )
    return any(k in text for k in keys)


def is_invalid_qb_server_url(raw_url):
    url = (raw_url or "").strip().lower()
    if not url:
        return True
    if "agent://" in url:
        return True
    if "://agent" in url:
        return True
    return False


def should_suppress_qb_alert(server, exc):
    url = (getattr(server, "url", "") or "").strip().lower()
    if "agent" in url:
        return True
    if is_invalid_qb_server_url(url):
        return True
    return is_qb_connection_exception(exc)


def has_waiting_tag(tags_str):
    monitor_tag = get_qb_monitor_tag()
    tags = [tag.strip() for tag in (tags_str or "").split(",") if tag.strip()]
    return monitor_tag in tags


def parse_qb_tags(tags_str):
    return [tag.strip() for tag in (tags_str or "").split(",") if tag.strip()]


def get_rename_trigger_tags():
    raw = get_rename_trigger_tags_raw()
    return [tag.strip() for tag in raw.split(",") if tag.strip()]


def resolve_rename_suffix(tags_str):
    tags = set(parse_qb_tags(tags_str))
    if not tags:
        return ""
    rules = RenameRule.query.order_by(RenameRule.id.asc()).all()
    suffixes = []
    for rule in rules:
        qb_tag = (rule.qb_tag or "").strip()
        if qb_tag and qb_tag in tags:
            suffixes.append((rule.suffix or "").strip())
    return "".join(suffixes)


def append_suffix_before_ext(filename, suffix):
    if not suffix:
        return filename
    base, ext = os.path.splitext(filename)
    return f"{base}{suffix}{ext}"


def insert_suffix_smart(filename, suffix):
    if not suffix:
        return filename
    name_no_ext, ext = os.path.splitext(filename)
    if ext.lower() not in [".mkv", ".mp4", ".avi", ".ts", ".iso", ".rmvb"]:
        name_no_ext = filename
        ext = ""

    import re
    # 1. 尝试匹配年份 (19xx 或 20xx)
    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", name_no_ext)
    if year_match:
        pos = year_match.end()
        return name_no_ext[:pos] + suffix + name_no_ext[pos:] + ext

    # 2. 如果没有年份，寻找常见的分辨率/媒介标签作为电影名结束的锚点
    qual_match = re.search(
        r"\b(2160p|1080p|720p|4k|8k|bluray|web-dl|webrip|remux|x264|x265|hevc)\b",
        name_no_ext,
        re.IGNORECASE,
    )
    if qual_match:
        pos = qual_match.start()
        if pos > 0 and name_no_ext[pos - 1] in [".", " ", "-"]:
            pos -= 1
        return name_no_ext[:pos] + suffix + name_no_ext[pos:] + ext

    # 3. 兜底方案：直接插在名字最后（扩展名之前）
    return name_no_ext + suffix + ext


def qb_remove_tags(client, torrent_hash, tags):
    if not torrent_hash or not tags:
        return
    tags_text = ",".join([x for x in tags if x])
    try:
        client.torrents_remove_tags(tags=tags_text, torrent_hashes=torrent_hash)
    except TypeError:
        client.torrents_remove_tags(tags=tags_text, hashes=torrent_hash)


def qb_add_tags(client, torrent_hash, tags):
    if not torrent_hash or not tags:
        return
    tags_text = ",".join([x for x in tags if x])
    try:
        client.torrents_add_tags(tags=tags_text, torrent_hashes=torrent_hash)
    except TypeError:
        client.torrents_add_tags(tags=tags_text, hashes=torrent_hash)


def try_bypass_rename(server, client, torrent):
    if get_rename_enabled() != "1":
        return False
    tags_str = getattr(torrent, "tags", "")
    tags = set(parse_qb_tags(tags_str))
    trigger_tags = get_rename_trigger_tags()
    finish_tag = get_rename_finish_tag()
    staging_path = get_mp_staging_path()
    final_path = get_mp_final_path()
    if not trigger_tags:
        return False
    if not tags or not all(tag in tags for tag in trigger_tags):
        return False
    if finish_tag and finish_tag in tags:
        return False

    rename_suffix = resolve_rename_suffix(tags_str)
    torrent_hash = getattr(torrent, "hash", "") or ""
    target_base = (getattr(torrent, "name", "") or "").strip()
    if not target_base:
        return False

    target_filenames = {target_base}
    if torrent_hash:
        try:
            files = client.torrents_files(torrent_hash=torrent_hash)
            for f in files or []:
                base = os.path.splitext(os.path.basename(getattr(f, "name", "") or ""))[0]
                if base:
                    target_filenames.add(base)
        except Exception as exc:
            logger.warning("获取种子文件列表失败: hash=%s err=%s", torrent_hash, exc)

    logger.info("旁路改名命中触发条件: task=%s targets=%s", target_base, sorted(target_filenames))

    if not staging_path or not os.path.isdir(staging_path):
        logger.warning("旁路改名缓存区不存在: %s", staging_path)
        return False

    moved = []
    for dirpath, _dirnames, filenames in os.walk(staging_path):
        for fname in filenames:
            if os.path.splitext(fname)[0] not in target_filenames:
                continue
            rel_dir = os.path.relpath(dirpath, staging_path)
            dest_dir = os.path.join(final_path, rel_dir)
            os.makedirs(dest_dir, exist_ok=True)
            new_name = insert_suffix_smart(fname, rename_suffix)
            src_path = os.path.join(dirpath, fname)
            dst_path = os.path.join(dest_dir, new_name)
            try:
                shutil.move(src_path, dst_path)
            except Exception as exc:
                logger.exception("旁路转移失败: %s -> %s, err=%s", src_path, dst_path, exc)
                continue
            moved.append((src_path, dst_path))

    if moved:
        for src_path, dst_path in moved:
            logger.info("🔖 [旁路转移] 成功移动: %s -> %s", src_path, dst_path)
        if finish_tag:
            qb_add_tags(client, torrent_hash, [finish_tag])
        return True

    logger.info("🔎 [旁路转移] 未找到匹配的缓存区文件: task=%s", target_base)
    return False


def resolve_source_path(torrent):
    content_path = getattr(torrent, "content_path", None)
    if content_path:
        return content_path
    save_path = getattr(torrent, "save_path", "")
    name = getattr(torrent, "name", "")
    return os.path.join(save_path, name)


def pack_to_iso(task_name, source_path, vol_id):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    iso_path = os.path.join(OUTPUT_DIR, f"{task_name}.iso")
    temp_iso_path = f"{iso_path}{PACKING_SUFFIX}"

    if os.path.exists(iso_path):
        os.remove(iso_path)
    if os.path.exists(temp_iso_path):
        os.remove(temp_iso_path)

    cmd = [
        "genisoimage",
        "-udf",
        "-iso-level",
        "3",
        "-allow-limited-size",
        "-V",
        vol_id,
        "-o",
        temp_iso_path,
        source_path,
    ]
    logger.info("开始封装 ISO，任务=%s，源目录=%s，目标文件=%s", task_name, source_path, iso_path)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    logger.info("封装命令结束，任务=%s，返回码=%s", task_name, proc.returncode)
    if proc.returncode == 0:
        os.rename(temp_iso_path, iso_path)
    else:
        try:
            if os.path.exists(temp_iso_path):
                os.remove(temp_iso_path)
        except OSError:
            logger.warning("清理封装临时文件失败: %s", temp_iso_path)
    if proc.returncode != 0 and proc.stderr:
        logger.error("封装命令错误输出，任务=%s:\n%s", task_name, proc.stderr)
    return proc.returncode == 0, iso_path, proc.stdout, proc.stderr


def get_iso_path_for_history(row: PackHistory):
    message = (row.message or "").strip()
    if message.startswith("ISO:"):
        return message.split("ISO:", 1)[1].strip()
    return os.path.join(OUTPUT_DIR, f"{row.task_name}.iso")


def process_uploads():
    global current_uploading_file
    if not UPLOAD_ENGINE_LOCK.acquire(blocking=False):
        logger.info("上传引擎忙碌，本轮跳过")
        return

    with app.app_context():
        try:
            clouddrive_path = (get_clouddrive_path() or "").strip() or DEFAULT_CLOUDDRIVE_PATH
            delete_after_upload = get_delete_after_upload()
            try:
                os.makedirs(clouddrive_path, exist_ok=True)
            except Exception:
                logger.exception("准备 CloudDrive 路径失败: %s", clouddrive_path)
                return

            pending_names = get_pending_upload_files()
            logger.info("📡 [定时调度] 触发全局上传任务，开始扫描待处理文件... 当前队列发现 %s 个任务", len(pending_names))
            global_auto_upload = get_global_auto_upload()

            for file_name in pending_names:
                src_path = os.path.join(OUTPUT_DIR, file_name)
                dst_path = os.path.join(clouddrive_path, file_name)
                task_name = os.path.splitext(file_name)[0]
                row = find_pack_row_for_upload(file_name)
                task_id = row.id if row else None
                task_auto_upload = get_task_auto_upload(file_name)
                should_auto_upload = global_auto_upload and task_auto_upload
                if not should_auto_upload:
                    reason = "global auto upload disabled" if not global_auto_upload else "task auto upload disabled"
                    if row:
                        row.status = STATUS_PACKED_ONLY
                        row.info = reason
                        db.session.commit()
                    mark_upload_status(file_name, "packed", reason, task_id=task_id)
                    logger.info("自动上传已跳过，文件=%s，原因=%s", file_name, reason)
                    continue

                if row:
                    row.status = STATUS_UPLOADING
                    row.info = f"uploading: {src_path}"
                    db.session.commit()
                mark_upload_status(file_name, "uploading", f"uploading: {src_path}", task_id=task_id)

                try:
                    if not os.path.isfile(src_path):
                        raise FileNotFoundError(f"file not found: {src_path}")

                    current_uploading_file = file_name
                    set_upload_command("running")
                    update_current_upload_status(active=True, file_name=file_name, status="uploading")
                    if get_notify_flag("notify_upload_start", True):
                        logger.info("[NAS] 任务 '%s' 开始转移至 CloudDrive。", file_name)
                        send_tg_notification(f"[AutoISO] 📤 开始转移至 CloudDrive：{file_name}")

                    logger.info("🚀 [网盘转移] 开始排队上传: %s", file_name)
                    uploaded_ok = upload_file_with_progress(
                        src_path,
                        dst_path,
                        delete_after=delete_after_upload,
                        task_id=task_id,
                    )
                    if not uploaded_ok:
                        mark_upload_status(file_name, "aborted", "upload aborted by user", task_id=task_id)
                        if row:
                            row.status = "中止上传"
                            row.info = "upload aborted by user"
                            db.session.commit()
                        continue

                    if row:
                        row.status = STATUS_UPLOADED
                        row.end_time = now_local()
                        row.message = f"已上传: {dst_path}"
                        row.info = ""
                        db.session.commit()
                    mark_upload_status(file_name, "uploaded", f"uploaded to {dst_path}", task_id=task_id)
                    logger.info("🎉 [网盘转移] 成功送达挂载点: %s", file_name)
                    if get_notify_flag("notify_upload_end", True):
                        logger.info("[NAS] 任务 '%s' 成功推入 CloudDrive，等待搬运。", file_name)
                        send_tg_notification(f"[AutoISO] {CLOUDDRIVE_DELIVERY_NOTICE}")
                except Exception as exc:
                    mark_upload_status(file_name, "failed", str(exc), task_id=task_id)
                    if row:
                        row.status = STATUS_PACKED_PENDING_UPLOAD
                        row.info = f"upload failed: {exc}"
                        db.session.commit()
                    logger.exception("上传失败，任务=%s，源=%s，目标=%s", task_name, src_path, dst_path)
                finally:
                    current_uploading_file = None
                    set_upload_command("running")
                    update_current_upload_status(active=False, file_name="", status="idle")
                    update_upload_progress(
                        active=False,
                        file_name="",
                        status="uploading",
                        percentage=0.0,
                        speed_mbps=0.0,
                        eta="-",
                    )
        finally:
            UPLOAD_ENGINE_LOCK.release()

def process_single_upload(file_name):
    global current_uploading_file
    safe_name = os.path.basename(file_name or "")
    if not safe_name or safe_name.startswith(".") or safe_name != file_name:
        logger.warning("手动上传参数非法: %s", file_name)
        return

    with UPLOAD_ENGINE_LOCK:
        with app.app_context():
            if not is_valid_upload_file(safe_name):
                logger.warning("手动上传文件不存在: %s", safe_name)
                return

            clouddrive_path = (get_clouddrive_path() or "").strip() or DEFAULT_CLOUDDRIVE_PATH
            delete_after_upload = get_delete_after_upload()
            try:
                os.makedirs(clouddrive_path, exist_ok=True)
            except Exception:
                logger.exception("准备 CloudDrive 路径失败: %s", clouddrive_path)
                return

            src_path = os.path.join(OUTPUT_DIR, safe_name)
            dst_path = os.path.join(clouddrive_path, safe_name)
            row = find_pack_row_for_upload(safe_name)
            task_id = row.id if row else None
            if row:
                row.status = STATUS_UPLOADING
                row.info = f"uploading: {src_path}"
                db.session.commit()
            mark_upload_status(safe_name, "uploading", f"uploading: {src_path}", task_id=task_id)

            try:
                current_uploading_file = safe_name
                set_upload_command("running")
                update_current_upload_status(active=True, file_name=safe_name, status="uploading")
                if get_notify_flag("notify_upload_start", True):
                    logger.info("[NAS] 任务 '%s' 开始转移至 CloudDrive。", safe_name)
                    send_tg_notification(f"[AutoISO] 📤 开始转移至 CloudDrive：{safe_name}")

                logger.info("🚀 [手动转移] 开始上传: %s", safe_name)
                uploaded_ok = upload_file_with_progress(
                    src_path,
                    dst_path,
                    delete_after=delete_after_upload,
                    task_id=task_id,
                )
                if not uploaded_ok:
                    mark_upload_status(safe_name, "aborted", "upload aborted by user", task_id=task_id)
                    if row:
                        row.status = "中止上传"
                        row.info = "upload aborted by user"
                        db.session.commit()
                    return

                if row:
                    row.status = STATUS_UPLOADED
                    row.end_time = now_local()
                    row.message = f"已上传: {dst_path}"
                    row.info = ""
                    db.session.commit()
                mark_upload_status(safe_name, "uploaded", f"uploaded to {dst_path}", task_id=task_id)
                if get_notify_flag("notify_upload_end", True):
                    logger.info("[NAS] 任务 '%s' 成功推入 CloudDrive，等待搬运。", safe_name)
                    send_tg_notification(f"[AutoISO] {CLOUDDRIVE_DELIVERY_NOTICE}")
                logger.info("🎉 [手动转移] 成功送达: %s", safe_name)
            except Exception as exc:
                mark_upload_status(safe_name, "failed", str(exc), task_id=task_id)
                if row:
                    row.status = STATUS_PACKED_PENDING_UPLOAD
                    row.info = f"upload failed: {exc}"
                    db.session.commit()
                logger.exception("手动上传失败: %s", safe_name)
            finally:
                current_uploading_file = None
                set_upload_command("running")
                update_current_upload_status(active=False, file_name="", status="idle")
                update_upload_progress(
                    active=False,
                    file_name="",
                    status="uploading",
                    percentage=0.0,
                    speed_mbps=0.0,
                    eta="-",
                )

def process_one_torrent(server: QBServer, client, torrent):
    original_name = getattr(torrent, "name", "")
    tags_str = getattr(torrent, "tags", "")
    rename_suffix = resolve_rename_suffix(tags_str)
    final_name = insert_suffix_smart(original_name, rename_suffix)
    safe_final_name = os.path.basename(final_name) if final_name else original_name

    candidate_names = [x for x in [original_name, safe_final_name] if x]
    existing = None
    if candidate_names:
        existing = (
            PackHistory.query.filter(
                PackHistory.qb_server_id == server.id,
                PackHistory.status == STATUS_PROCESSING,
                PackHistory.task_name.in_(candidate_names),
            )
            .order_by(PackHistory.id.desc())
            .first()
        )
    if existing:
        return

    source_path = resolve_source_path(torrent)
    source_size_bytes = get_path_size_bytes(source_path)

    history = PackHistory(
        task_name=safe_final_name,
        qb_server_id=server.id,
        status=STATUS_PROCESSING,
        start_time=now_local(),
        file_size_gb=round(source_size_bytes / GB, 3),
    )
    db.session.add(history)
    db.session.commit()
    try:
        expected_output_name = safe_final_name if os.path.isfile(source_path) else f"{safe_final_name}.iso"
        init_task_auto_upload(expected_output_name, history.id)
    except Exception:
        logger.exception("初始化任务自动上传状态失败，任务=%s", torrent.name)

    started = now_local()
    torrent_hash = getattr(torrent, "hash", "")
    task_key = f"{server.id}:{torrent_hash or torrent.name}"
    iso_task_name = safe_final_name or torrent.name
    with ACTIVE_TASKS_LOCK:
        ACTIVE_TASKS[task_key] = {
            "task_name": safe_final_name,
            "server_name": server.name,
            "source_path": source_path,
            "source_size_bytes": source_size_bytes,
            "iso_path": os.path.join(OUTPUT_DIR, f"{iso_task_name}.iso{PACKING_SUFFIX}"),
            "start_time": started,
            "task_id": history.id,
        }

    try:
        if os.path.isfile(source_path):
            actual_filename = os.path.basename(source_path)
            single_final_name = insert_suffix_smart(actual_filename, rename_suffix)
            output_file = os.path.join(OUTPUT_DIR, single_final_name)
            logger.info("📦 [本地封装] 检测到单文件，开始极速转存: %s", torrent.name)
            logger.info(
                "[节点:%s] 检测到单文件任务，跳过 ISO 封装，直接复制到待上传区：%s",
                server.name,
                os.path.basename(source_path),
            )
            try:
                if os.path.abspath(source_path) != os.path.abspath(output_file):
                    shutil.copy2(source_path, output_file)
            except Exception as exc:
                history.status = STATUS_FAILED
                history.end_time = now_local()
                history.message = f"single file copy failed: {exc}"
                history.info = str(exc)
                db.session.commit()
                qb_remove_tags(client, torrent_hash, [PACKING_TAG])
                qb_add_tags(client, torrent_hash, [FAILED_TAG])
                if get_notify_flag("notify_pack_end", True):
                    logger.info("[节点:%s] 任务 '%s' 单文件转存失败。", server.name, torrent.name)
                    send_tg_notification(
                        f"[AutoISO] ❌ 单文件转存失败 | 节点: {server.name} | 任务: {torrent.name}"
                    )
                logger.exception("单文件任务处理失败，[节点:%s] 任务=%s", server.name, torrent.name)
                return
            finished = now_local()
            duration = format_seconds((finished - started).total_seconds())
            output_name = os.path.basename(output_file)
            auto_upload_enabled = init_task_auto_upload(output_name, history.id)
            history.status = STATUS_PACKED_PENDING_UPLOAD if auto_upload_enabled else STATUS_PACKED_ONLY
            history.end_time = finished
            history.message = f"FILE: {output_file}"
            history.info = "single file copied to output"
            history.file_size_gb = round((os.path.getsize(output_file) / GB), 3) if os.path.isfile(output_file) else history.file_size_gb
            db.session.commit()
            mark_upload_status(
                output_name,
                "pending" if auto_upload_enabled else "packed",
                "single file ready",
                task_id=history.id,
            )
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True):
                logger.info("[节点:%s] 任务 '%s' 单文件转存成功，等待上传。", server.name, torrent.name)
                send_tg_notification(
                    f"[AutoISO] ✅ 转存成功，等待上传 | 节点: {server.name} | 任务: {torrent.name}"
                )
            logger.info("单文件任务处理完成，[节点:%s] 任务=%s，耗时=%s", server.name, torrent.name, duration)
            return

        if not os.path.isdir(source_path):
            history.status = STATUS_FAILED
            history.end_time = now_local()
            history.message = f"source path not found: {source_path}"
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            if get_notify_flag("notify_pack_end", True):
                logger.info("[节点:%s] 任务 '%s' 封装失败(目录不存在)。", server.name, torrent.name)
                send_tg_notification(f"[AutoISO] ❌ 封装失败 (目录不存在) | 节点: {server.name} | 任务: {torrent.name}")
            logger.error("封装失败：源目录不存在，任务=%s，路径=%s", torrent.name, source_path)
            return

        safe_vol_id = re.sub(r"[^a-zA-Z0-9_]", "_", torrent.name or "AUTOISO")[:30]
        logger.info("💿 [本地封装] 正在将目录打包为 ISO 镜像: %s", torrent.name)
        ok, iso_path, out, err = pack_to_iso(iso_task_name, source_path, safe_vol_id)
        finished = now_local()
        duration = format_seconds((finished - started).total_seconds())

        if ok:
            iso_name = os.path.basename(iso_path)
            auto_upload_enabled = init_task_auto_upload(iso_name, history.id)
            history.status = STATUS_PACKED_PENDING_UPLOAD if auto_upload_enabled else STATUS_PACKED_ONLY
            history.end_time = finished
            history.message = f"ISO: {iso_path}"
            history.info = ""
            db.session.commit()
            mark_upload_status(
                iso_name,
                "pending" if auto_upload_enabled else "packed",
                "iso ready",
                task_id=history.id,
            )
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True):
                logger.info("[节点:%s] 任务 '%s' 封装成功，等待上传。", server.name, torrent.name)
                send_tg_notification(f"[AutoISO] ✅ 封装成功，等待上传 | 节点: {server.name} | 任务: {torrent.name}")
            logger.info(" [本地封装] ISO 生成完毕: %s，耗时: %s", torrent.name, duration)
        else:
            history.status = STATUS_FAILED
            history.end_time = finished
            history.message = extract_error_tail(err, out)
            history.info = (err or "").strip()
            if history.info:
                logger.error("封装错误详情，[节点:%s] 任务=%s，stderr=%s", server.name, torrent.name, history.info)
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            if get_notify_flag("notify_pack_end", True):
                logger.info("[节点:%s] 任务 '%s' 封装失败。", server.name, torrent.name)
                send_tg_notification(f"[AutoISO] ❌ 封装失败 | 节点: {server.name} | 任务: {torrent.name}")
            logger.error("封装失败，[节点:%s] 任务=%s，耗时=%s，原因=%s", server.name, torrent.name, duration, history.message)
    finally:
        with ACTIVE_TASKS_LOCK:
            ACTIVE_TASKS.pop(task_key, None)

def process_all_qbs():
    with app.app_context():
        servers = QBServer.query.all()
        monitor_tag = get_qb_monitor_tag()
        logger.info("开始轮询 qB 节点，监控标签=%s", monitor_tag)
        current_waiting_tasks = {}
        for server in servers:
            if is_invalid_qb_server_url(getattr(server, "url", "")):
                logger.warning("跳过无效 qB 节点 URL: node=%s url=%s", server.name, server.url)
                continue
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
                logger.info("[节点:%s] 扫描 qB 任务完成，发现 %s 个种子", server.name, len(torrents))
            except Exception as exc:
                if should_suppress_qb_alert(server, exc):
                    logger.warning("qB 节点连接失败(已抑制告警): node=%s err=%s", server.name, exc)
                else:
                    logger.exception("qB 节点连接失败，节点=%s", server.name)
                    logger.info("[节点:%s] 任务 '未知' 节点连接失败，已发送通知。", server.name)
                    send_tg_notification(f"[AutoISO] 节点连接失败 | 节点: {server.name} | 错误: {exc}")
                continue

            # === [智能雷达] 预扫描 MP 缓存区 ===
            rename_enabled = get_setting("rename_enabled") == "1"
            move_all = get_setting("rename_move_all") == "1"
            staging_files = []
            if rename_enabled and move_all:
                src_dir = get_setting("mp_staging_path")
                if src_dir and os.path.exists(src_dir):
                    try:
                        for root, _dirs, files in os.walk(src_dir):
                            for f in files:
                                if f.lower().endswith((".mkv", ".mp4", ".ts", ".iso", ".avi", ".rmvb")):
                                    # 修复Bug：记录相对路径（包含子目录），而不仅仅是文件名
                                    rel_dir = os.path.relpath(root, src_dir)
                                    rel_path = os.path.join(rel_dir, f) if rel_dir != "." else f
                                    staging_files.append(rel_path)
                    except Exception:
                        pass

            for torrent in torrents:
                original_name = getattr(torrent, "name", "")
                tags_str = getattr(torrent, "tags", "")
                parsed_tags = set(parse_qb_tags(tags_str))

                # 判断是否是准备执行旁路改名的新任务（必须不包含已重命名标签，防止无限死循环触发）
                is_bypass_pending = False
                if rename_enabled:
                    trigger_tags = get_rename_trigger_tags()
                    finish_tag = get_rename_finish_tag()

                    if not finish_tag or finish_tag not in parsed_tags:
                        has_trigger = trigger_tags and parsed_tags and all(tag in parsed_tags for tag in trigger_tags)

                        if has_trigger:
                            # 1. VIP 流程（带标签）：触发正常改名、刮削入库与历史记录
                            is_bypass_pending = True
                        elif move_all:
                            # 2. 静默清道夫流程：包含子目录结构完整搬运
                            task_base = (getattr(torrent, "name", "") or "").split("-")[0].strip()
                            if task_base:
                                matched_files = [sf for sf in staging_files if task_base in sf or sf in task_base]
                                if matched_files:
                                    src_dir = get_setting("mp_staging_path")
                                    dst_dir = get_setting("mp_final_path")
                                    if src_dir and dst_dir:
                                        success = False
                                        for f in matched_files:
                                            src_file = os.path.join(src_dir, f)
                                            dst_file = os.path.join(dst_dir, f)
                                            try:
                                                # 修复Bug：因为有可能是嵌套子目录，所以要用 dirname 确保目标父目录存在
                                                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                                                shutil.move(src_file, dst_file)
                                                logger.info("🚚 [静默转移] 成功转移未打标文件: %s", f)
                                                success = True
                                                if f in staging_files:
                                                    staging_files.remove(f)
                                            except Exception as e:
                                                logger.error("[静默转移] 文件转移失败 %s: %s", f, e)

                                        # 搬运完成后，打上结案标签（如果没有配置完成标签，则兜底打上已转移）
                                        if success:
                                            final_tag = finish_tag if finish_tag else "已转移"
                                            try:
                                                torrent_hash = getattr(torrent, "hash", "") or ""
                                                if torrent_hash:
                                                    qb_add_tags(client, torrent_hash, [final_tag])
                                                else:
                                                    torrent.add_tags(tags=final_tag)
                                            except Exception:
                                                pass

                # 只要是待封装，或者是全新准备旁路的任务，立刻提前生成最终名字并触发刮削
                if has_waiting_tag(tags_str) or is_bypass_pending:
                    try:
                        rename_suffix = resolve_rename_suffix(tags_str)
                        scraped_name = insert_suffix_smart(original_name, rename_suffix)
                        trigger_auto_scrape_async(scraped_name, search_keyword=original_name)
                    except Exception:
                        pass

                try:
                    try_bypass_rename(server, client, torrent)
                except Exception:
                    logger.exception("旁路改名流程异常，节点=%s 任务=%s", server.name, original_name)

                if not has_waiting_tag(tags_str):
                    continue
                torrent_hash = getattr(torrent, "hash", "") or ""
                task_key = f"{server.name}:{torrent_hash or getattr(torrent, 'name', '')}"
                size_bytes = int(getattr(torrent, "size", 0) or getattr(torrent, "total_size", 0) or 0)
                current_waiting_tasks[task_key] = {
                    "task_name": getattr(torrent, "name", ""),
                    "node": server.name,
                    "size_gb": round(size_bytes / GB, 3),
                }
                if float(getattr(torrent, "progress", 0)) != 1.0:
                    continue

                try:
                    qb_remove_tags(client, torrent_hash, [monitor_tag])
                    qb_add_tags(client, torrent_hash, [PACKING_TAG])
                    if get_notify_flag("notify_pack_start", True):
                        logger.info("[节点:%s] 任务 '%s' 开始封装。", server.name, torrent.name)
                        send_tg_notification(f"[AutoISO] 🚀 开始封装 | 节点: {server.name} | 任务: {torrent.name}")
                    logger.info("开始封装任务，[节点:%s] 任务=%s", server.name, getattr(torrent, "name", "unknown"))
                    process_one_torrent(server, client, torrent)
                except Exception as exc:
                    logger.exception("任务处理异常，[节点:%s] 任务=%s", server.name, getattr(torrent, "name", "unknown"))
                    try:
                        qb_remove_tags(client, torrent_hash, [PACKING_TAG, monitor_tag])
                        qb_add_tags(client, torrent_hash, [FAILED_TAG])
                    except Exception:
                        logger.warning("任务异常后更新 qB 标签失败: node=%s task=%s", server.name, getattr(torrent, "name", "unknown"))
                    failed_record = PackHistory(
                        task_name=getattr(torrent, "name", "unknown"),
                        qb_server_id=server.id,
                        status=STATUS_FAILED,
                        start_time=now_local(),
                        end_time=now_local(),
                        message=f"Unhandled error: {exc}",
                    )
                    db.session.add(failed_record)
                    db.session.commit()
                    if should_suppress_qb_alert(server, exc):
                        logger.warning(
                            "任务处理异常(已抑制告警): node=%s task=%s err=%s",
                            server.name,
                            getattr(torrent, "name", "unknown"),
                            exc,
                        )
                    else:
                        logger.info(
                            "[节点:%s] 任务 '%s' 处理异常，已发送通知。",
                            server.name,
                            getattr(torrent, "name", "unknown"),
                        )
                        send_tg_notification(
                            f"[AutoISO] ❌ 任务异常 | 节点: {server.name} | 任务: {getattr(torrent, 'name', 'unknown')}"
                        )
        with WAITING_TAG_TASKS_LOCK:
            previous_tasks = dict(WAITING_TAG_TASKS)
            previous_keys = set(previous_tasks.keys())
            current_keys = set(current_waiting_tasks.keys())
            added_keys = current_keys - previous_keys
            removed_keys = previous_keys - current_keys
            WAITING_TAG_TASKS.clear()
            WAITING_TAG_TASKS.update(current_waiting_tasks)

        for key in added_keys:
            meta = current_waiting_tasks.get(key, {})
            task_name = meta.get("task_name") or "unknown"
            node_name = meta.get("node") or "unknown"
            size_gb = meta.get("size_gb")
            size_text = f"{size_gb:.2f}GB" if isinstance(size_gb, (int, float)) else "-"
            msg = f"[系统] 检测到新任务打上标签加入队列: {task_name} | 节点: {node_name} | 大小: {size_text}"
            logger.info(msg)
            if get_notify_flag("notify_task_add", True):
                send_tg_notification(f"[AutoISO] {msg}")

        for key in removed_keys:
            meta = previous_tasks.get(key, {})
            task_name = meta.get("task_name") or "unknown"
            node_name = meta.get("node") or "unknown"
            msg = f"[系统] 任务被移出待处理队列(取消标签/被删): {task_name} | 节点: {node_name}"
            logger.info(msg)
            if get_notify_flag("notify_task_cancel", True):
                send_tg_notification(f"[AutoISO] {msg}")

def parse_recent_log_entries(hours=24):
    if not os.path.exists(LOG_FILE):
        return []

    cutoff = datetime.now() - timedelta(hours=hours)
    entries = []
    current_lines = []
    current_dt = None

    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = LOG_TS_RE.match(line)
            if m:
                if current_lines and current_dt:
                    entries.append((current_dt, "\n".join(current_lines)))
                current_lines = [line]
                try:
                    current_dt = datetime.strptime(m.group("dt"), "%Y-%m-%d %H:%M:%S")
                except Exception:
                    current_dt = None
            else:
                if current_lines:
                    current_lines.append(line)

    if current_lines and current_dt:
        entries.append((current_dt, "\n".join(current_lines)))

    recent = [item for item in entries if item[0] >= cutoff]
    recent.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in recent]


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if session.get("logged_in"):
            return redirect(url_for("index"))
        return render_template("login.html", error="", version=APP_VERSION)

    input_username = (request.form.get("username") or "").strip()
    input_password = (request.form.get("password") or "").strip()
    auth_username, auth_password = get_auth_credentials()
    if input_username == auth_username and input_password == auth_password:
        session["logged_in"] = True
        session.permanent = True
        return redirect(url_for("index"))
    return render_template("login.html", error="账号或密码错误", version=APP_VERSION)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@require_login
def index():
    return render_template("index.html", version=APP_VERSION)


@app.route("/history")
@require_login
def history_page():
    return render_template("history.html", version=APP_VERSION)


@app.route("/api/qb/test", methods=["POST"])
def test_qb_connection():
    payload = request.get_json(force=True)
    url = (payload.get("url") or "").strip()
    username = (payload.get("username") or "").strip()
    password = (payload.get("password") or "").strip()
    if not all([url, username, password]):
        return jsonify({"error": "参数不完整"}), 400
    try:
        client = qbittorrentapi.Client(
            host=url,
            username=username,
            password=password,
            REQUESTS_ARGS={"timeout": 15},
        )
        client.auth_log_in()
        return jsonify({"ok": True, "message": "连接成功"})
    except Exception as exc:
        return jsonify({"error": f"连接失败: {exc}"}), 400


@app.route("/api/qbservers", methods=["GET"])
def list_qbservers():
    items = QBServer.query.order_by(QBServer.id.desc()).all()
    return jsonify([{"id": x.id, "name": x.name, "url": x.url, "username": x.username} for x in items])


@app.route("/api/qbservers", methods=["POST"])
def add_qbserver():
    payload = request.get_json(force=True)
    name = (payload.get("name") or "").strip()
    url = (payload.get("url") or "").strip()
    username = (payload.get("username") or "").strip()
    password = (payload.get("password") or "").strip()
    if not all([name, url, username, password]):
        return jsonify({"error": "参数不完整"}), 400
    if QBServer.query.filter_by(name=name).first():
        return jsonify({"error": "节点别名已存在"}), 400
    db.session.add(QBServer(name=name, url=url, username=username, password=password))
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/qb/update", methods=["POST"])
def update_qbserver():
    payload = request.get_json(force=True) or {}
    raw_id = payload.get("id")
    alias = (payload.get("alias") or "").strip()
    url = (payload.get("url") or "").strip()
    username = (payload.get("username") or "").strip()
    password = (payload.get("password") or "").strip()

    try:
        server_id = int(raw_id)
    except (TypeError, ValueError):
        return jsonify({"error": "无效的节点 ID"}), 400

    if not all([alias, url, username]):
        return jsonify({"error": "alias、url、username 不能为空"}), 400

    server = db.session.get(QBServer, server_id)
    if not server:
        return jsonify({"error": "节点不存在"}), 404

    conflict = QBServer.query.filter(QBServer.name == alias, QBServer.id != server_id).first()
    if conflict:
        return jsonify({"error": "节点别名已存在"}), 400

    server.name = alias
    server.url = url
    server.username = username
    if password:
        server.password = password

    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/qbservers/<int:server_id>", methods=["DELETE"])
def delete_qbserver(server_id):
    item = db.session.get(QBServer, server_id)
    if not item:
        return jsonify({"error": "节点不存在"}), 404
    try:
        fallback_id = get_or_create_external_server_id()
        if fallback_id == item.id:
            alt = QBServer.query.filter(QBServer.id != item.id).order_by(QBServer.id.asc()).first()
            if alt:
                fallback_id = alt.id
            else:
                alt = QBServer(name="DeletedNode", url="local://deleted", username="-", password="-")
                db.session.add(alt)
                db.session.flush()
                fallback_id = alt.id

        PackHistory.query.filter_by(qb_server_id=item.id).update(
            {"qb_server_id": fallback_id},
            synchronize_session=False,
        )
        db.session.delete(item)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as exc:
        db.session.rollback()
        logger.exception("删除 qB 节点失败: id=%s", server_id)
        return jsonify({"error": f"delete failed: {exc}"}), 500


def serialize_agent_node(row):
    return {
        "id": row.id,
        "node_name": row.node_name,
        "qb_url": row.qb_url,
        "qb_user": row.qb_user,
        "qb_pass": row.qb_pass,
        "temp_path": row.temp_path,
        "cd2_path": row.cd2_path,
        "auto_upload": bool(getattr(row, "auto_upload", True)),
        "upload_policy": normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY)),
        "upload_cron": (getattr(row, "upload_cron", "") or "").strip(),
    }


@app.route("/api/agent_nodes", methods=["GET"])
def list_agent_nodes():
    rows = AgentNode.query.order_by(AgentNode.id.desc()).all()
    return jsonify([serialize_agent_node(row) for row in rows])


@app.route("/api/agent_nodes", methods=["POST"])
def add_agent_node():
    payload = request.get_json(force=True) or {}
    node_name = (payload.get("node_name") or "").strip()
    qb_url = (payload.get("qb_url") or "").strip()
    qb_user = (payload.get("qb_user") or "").strip()
    qb_pass = (payload.get("qb_pass") or "").strip()
    temp_path = (payload.get("temp_path") or "").strip()
    cd2_path = (payload.get("cd2_path") or "").strip()
    auto_upload = parse_bool(payload.get("auto_upload"), default=True)
    upload_policy = normalize_agent_upload_policy(payload.get("upload_policy"))
    upload_cron = (payload.get("upload_cron") or "").strip()

    if not all([node_name, qb_url, qb_user, qb_pass, temp_path, cd2_path]):
        return jsonify({"error": "参数不完整"}), 400
    if AgentNode.query.filter_by(node_name=node_name).first():
        return jsonify({"error": "节点名称已存在"}), 400

    try:
        row = AgentNode(
            node_name=node_name,
            qb_url=qb_url,
            qb_user=qb_user,
            qb_pass=qb_pass,
            temp_path=temp_path,
            cd2_path=cd2_path,
            auto_upload=auto_upload,
            upload_policy=upload_policy,
            upload_cron=upload_cron,
        )
        db.session.add(row)
        db.session.commit()
        return jsonify({"ok": True, "node": serialize_agent_node(row)})
    except Exception:
        db.session.rollback()
        logger.exception("新增边缘节点失败")
        return jsonify({"error": "新增失败"}), 500


@app.route("/api/agent_nodes/update", methods=["POST"])
def update_agent_node():
    payload = request.get_json(force=True) or {}
    raw_id = payload.get("id")
    try:
        node_id = int(raw_id)
    except (TypeError, ValueError):
        return jsonify({"error": "无效的节点 ID"}), 400

    node_name = (payload.get("node_name") or "").strip()
    qb_url = (payload.get("qb_url") or "").strip()
    qb_user = (payload.get("qb_user") or "").strip()
    qb_pass = (payload.get("qb_pass") or "").strip()
    temp_path = (payload.get("temp_path") or "").strip()
    cd2_path = (payload.get("cd2_path") or "").strip()
    auto_upload = parse_bool(payload.get("auto_upload"), default=True)
    upload_policy = normalize_agent_upload_policy(payload.get("upload_policy"))
    upload_cron = (payload.get("upload_cron") or "").strip()
    if not all([node_name, qb_url, qb_user, qb_pass, temp_path, cd2_path]):
        return jsonify({"error": "参数不完整"}), 400

    row = db.session.get(AgentNode, node_id)
    if not row:
        return jsonify({"error": "节点不存在"}), 404

    conflict = AgentNode.query.filter(AgentNode.node_name == node_name, AgentNode.id != node_id).first()
    if conflict:
        return jsonify({"error": "节点名称已存在"}), 400

    try:
        row.node_name = node_name
        row.qb_url = qb_url
        row.qb_user = qb_user
        row.qb_pass = qb_pass
        row.temp_path = temp_path
        row.cd2_path = cd2_path
        row.auto_upload = auto_upload
        row.upload_policy = upload_policy
        row.upload_cron = upload_cron
        db.session.commit()
        return jsonify({"ok": True, "node": serialize_agent_node(row)})
    except Exception:
        db.session.rollback()
        logger.exception("更新边缘节点失败")
        return jsonify({"error": "更新失败"}), 500


@app.route("/api/agent_nodes/toggle_policy", methods=["POST"])
def toggle_agent_node_policy():
    payload = request.get_json(force=True) or {}
    node_name = (payload.get("node") or "").strip()
    if not node_name:
        return jsonify({"error": "node 不能为空"}), 400

    row = AgentNode.query.filter_by(node_name=node_name).first()
    if not row:
        return jsonify({"error": "节点不存在"}), 404

    current_policy = normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY))
    row.upload_policy = "custom" if current_policy == "global" else "global"
    db.session.commit()
    return jsonify({"ok": True, "node": serialize_agent_node(row)})


@app.route("/api/agent_nodes/<int:node_id>", methods=["DELETE"])
def delete_agent_node(node_id):
    row = db.session.get(AgentNode, node_id)
    if not row:
        return jsonify({"error": "节点不存在"}), 404
    try:
        db.session.delete(row)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception:
        db.session.rollback()
        logger.exception("删除边缘节点失败")
        return jsonify({"error": "删除失败"}), 500


@app.route("/api/settings/telegram", methods=["GET"])
def get_telegram_settings():
    return jsonify(
        {
            "tg_token": get_setting("tg_token"),
            "tg_chat_id": get_setting("tg_chat_id"),
        }
    )


@app.route("/api/settings/telegram", methods=["POST"])
def save_telegram_settings():
    payload = request.get_json(force=True) or {}
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()
    if not tg_token or not tg_chat_id:
        return jsonify({"error": "tg_token 和 tg_chat_id 不能为空"}), 400
    set_setting("tg_token", tg_token)
    set_setting("tg_chat_id", tg_chat_id)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/telegram/test", methods=["POST"])
def test_telegram_settings():
    payload = request.get_json(force=True) or {}
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()
    ok, msg = send_tg_notification_with_config("TG 通知测试成功！", tg_token, tg_chat_id)
    if not ok:
        return jsonify({"error": msg}), 400
    return jsonify({"ok": True})


@app.route("/api/tmdb/test", methods=["POST"])
def test_tmdb_connection():
    payload = request.get_json(force=True) or {}
    tmdb_api_key = (payload.get("tmdb_api_key") or "").strip()
    global_proxy = (payload.get("global_proxy") or "").strip()
    if not tmdb_api_key:
        return jsonify({"error": "请先填写 TMDB API Key"}), 400

    url = f"https://api.themoviedb.org/3/authentication?api_key={tmdb_api_key}"
    request_kwargs = {"timeout": 10}
    if global_proxy:
        request_kwargs["proxies"] = {"http": global_proxy, "https": global_proxy}

    try:
        resp = requests.get(url, **request_kwargs)
    except (
        requests.exceptions.ProxyError,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    ):
        return jsonify({"error": "代理连接失败或网络超时，请检查代理设置"}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"TMDB 请求失败: {exc}"}), 400

    if resp.status_code == 200:
        return jsonify({"message": "TMDB API 连接成功！"}), 200
    if resp.status_code == 401:
        return jsonify({"error": "API Key 无效，请检查是否填写正确"}), 400
    return jsonify({"error": f"TMDB 接口返回异常状态码: {resp.status_code}"}), 400


@app.route("/api/agent/report", methods=["POST"])
def agent_report():
    payload = request.get_json(force=True) or {}
    token = str(payload.get("token") or "").strip()
    expected_token = (get_setting("agent_token") or "").strip()
    if not expected_token or not token or not hmac.compare_digest(token, expected_token):
        return jsonify({"error": "forbidden"}), 403

    node = str(payload.get("node") or "").strip() or "VPS"
    task_name = os.path.basename(str(payload.get("task_name") or payload.get("filename") or "").strip())
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"packing", "uploading", "pending_upload", "finished", "error"}:
        return jsonify({"error": "invalid status"}), 400
    if not task_name:
        return jsonify({"error": "filename 不能为空"}), 400
    # 极端的安全兜底：只允许剔除明确的物理文件后缀，绝不误伤 PT 站带点的发布组名称
    safe_final_name = task_name
    for ext in ['.iso', '.mkv', '.mp4', '.ts', '.avi']:
        if safe_final_name.lower().endswith(ext):
            safe_final_name = safe_final_name[:-len(ext)]
            break

    try:
        progress = max(0.0, min(100.0, float(payload.get("progress", 0) or 0)))
    except (TypeError, ValueError):
        progress = 0.0
    try:
        speed_mbps = float(payload.get("speed_mbps", 0) or 0)
    except (TypeError, ValueError):
        speed_mbps = 0.0
    eta_text = str(payload.get("eta_text") or "-").strip() or "-"
    last_update = now_local()

    file_size_gb = 0.0
    try:
        if payload.get("size_gb") is not None:
            file_size_gb = float(payload.get("size_gb") or 0)
        elif payload.get("file_size_gb") is not None:
            file_size_gb = float(payload.get("file_size_gb") or 0)
        elif payload.get("file_size_bytes") is not None:
            file_size_gb = float(payload.get("file_size_bytes") or 0) / GB
    except (TypeError, ValueError):
        file_size_gb = 0.0

    server_id = get_or_create_external_server_id()
    
    # 查找旧状态，用于触发状态变更通知
    old_row = (
        PackHistory.query.filter(
            PackHistory.qb_server_id == server_id,
            PackHistory.task_name == safe_final_name,
            PackHistory.message.like("Agent report:%"),
            PackHistory.info == f"node={node}",
        )
        .order_by(PackHistory.id.desc())
        .first()
    )
    old_status = old_row.status if old_row else None

    try:
        updated_row = upsert_agent_history_status(node, safe_final_name, status, file_size_gb=file_size_gb)
        if updated_row and updated_row.task_name != safe_final_name:
            updated_row.task_name = safe_final_name[:255]
            db.session.commit()
        new_status = updated_row.status if updated_row else None
    except Exception:
        logger.exception("Agent 状态入库失败: node=%s file=%s status=%s", node, safe_final_name, status)
        return jsonify({"error": "history update failed"}), 500

    # 精准发送 Telegram 通知
    if new_status and old_status != new_status:
        logger.info("🔔 [节点动态] 边缘节点 '%s' 上的任务 '%s' 状态变更为: 【%s】", node, safe_final_name, new_status)
        if new_status == "封装中":
            if get_notify_flag("notify_pack_start", True):
                logger.info("[节点:%s] 任务 '%s' 开始封装。", node, safe_final_name)
                send_tg_notification(f"[AutoISO] 🚀 开始封装 | 节点: {node} | 任务: {safe_final_name}")
        elif new_status == "待上传 (阻塞中)":
            if get_notify_flag("notify_pack_end", True):
                logger.info("[节点:%s] 任务 '%s' 封装完成，等待放行。", node, safe_final_name)
                send_tg_notification(f"[AutoISO] ✅ 封装完成，等待放行 | 节点: {node} | 任务: {safe_final_name}")
        elif new_status == "上传中":
            if get_notify_flag("notify_upload_start", True):
                logger.info("[节点:%s] 任务 '%s' 开始转移至 CloudDrive。", node, safe_final_name)
                send_tg_notification(f"[AutoISO] 📤 开始转移至 CloudDrive | 节点: {node} | 任务: {safe_final_name}")
        elif new_status == "成功":
            if get_notify_flag("notify_upload_end", True):
                logger.info("[节点:%s] 任务 '%s' 成功推入 CloudDrive，等待搬运。", node, safe_final_name)
                send_tg_notification(f"[AutoISO] {CLOUDDRIVE_DELIVERY_NOTICE}")
        elif new_status == "失败":
            if get_notify_flag("notify_pack_end", True) or get_notify_flag("notify_upload_end", True):
                logger.info("[节点:%s] 任务 '%s' 处理失败。", node, safe_final_name)
                send_tg_notification(f"[AutoISO] ❌ 任务处理失败 | 节点: {node} | 任务: {safe_final_name}")

    if status in {"finished", "error"}:
        with AGENT_TASKS_LOCK:
            AGENT_TASKS.pop(safe_final_name, None)
    else:
        previous_task = None
        with AGENT_TASKS_LOCK:
            previous_task = AGENT_TASKS.get(safe_final_name)

        computed_speed, processed_bytes = compute_agent_speed_mbps(
            previous_task,
            progress,
            file_size_gb,
            last_update,
            fallback_speed=speed_mbps,
        )
        final_speed = speed_mbps if speed_mbps > 0 else computed_speed

        with AGENT_TASKS_LOCK:
            AGENT_TASKS[safe_final_name] = {
                "node": node,
                "filename": safe_final_name,
                "status": status,
                "progress": progress,
                "speed_mbps": final_speed,
                "speed_text": format_speed_mbps(final_speed),
                "eta_text": eta_text,
                "last_update": last_update,
                "last_processed_bytes": processed_bytes,
            }

    if status in {"packing", "pending_upload", "finished"}:
        try:
            trigger_auto_scrape_async(safe_final_name, search_keyword=safe_final_name)
        except Exception:
            logger.exception("Agent %s 派发 TMDB 刮削失败: node=%s file=%s", status, node, safe_final_name)
    return jsonify({"ok": True, "status": "accepted"})


@app.route("/api/agent/pending", methods=["POST"])
def agent_pending_report():
    payload = request.get_json(force=True) or {}
    token = str(payload.get("token") or "").strip()
    expected_token = (get_setting("agent_token") or "").strip()
    if not expected_token or not token or not hmac.compare_digest(token, expected_token):
        return jsonify({"error": "forbidden"}), 403

    node = str(payload.get("node") or "").strip() or "VPS"
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return jsonify({"error": "tasks must be a list"}), 400

    normalized = []
    for i in payload.get("tasks", []):
        if not isinstance(i, dict):
            continue
        title = str(i.get("title") or "").strip()
        tags_str = str(i.get("tags") or "").strip()

        try:
            rename_suffix = resolve_rename_suffix(tags_str)
            scraped_name = insert_suffix_smart(title, rename_suffix)
            trigger_auto_scrape_async(scraped_name, search_keyword=title)
        except Exception:
            pass

        normalized.append(
            {
                "title": title,
                "display_name": str(i.get("display_name") or title).strip(),
                "size_gb": float(i.get("size_gb", 0)),
                "added_on": str(i.get("added_on") or "").strip(),
                "node_alias": node,
                "tags": tags_str,
            }
        )

    with AGENT_PENDING_TASKS_LOCK:
        AGENT_PENDING_TASKS[node] = {
            "updated_at": now_local(),
            "tasks": normalized,
        }

    return jsonify({"ok": True, "node": node, "count": len(normalized)})


@app.route("/api/agent/config", methods=["GET"])
def get_agent_config():
    token = (request.args.get("token") or "").strip()
    node_name = (request.args.get("node_name") or "").strip()
    if not token or not hmac.compare_digest(token, get_agent_token()):
        return jsonify({"error": "Forbidden"}), 403
    if not node_name:
        return jsonify({"error": "node_name 不能为空"}), 400

    row = AgentNode.query.filter_by(node_name=node_name).first()
    if not row:
        return jsonify({"error": "节点不存在"}), 404
    data = serialize_agent_node(row)
    data["delete_after_upload"] = bool(get_delete_after_upload())
    return jsonify(data)


@app.route("/api/agent/rename_rules", methods=["GET"])
def agent_rename_rules():
    token = (request.args.get("token") or "").strip()
    if not token or not hmac.compare_digest(token, get_agent_token()):
        return jsonify({"error": "Forbidden"}), 403

    rules = RenameRule.query.order_by(RenameRule.id.asc()).all()
    return jsonify(
        {
            "ok": True,
            "rules": [{"qb_tag": r.qb_tag, "suffix": r.suffix} for r in rules],
        }
    )


@app.route("/api/agent/whitelist_upload", methods=["POST"])
def whitelist_agent_upload():
    payload = request.get_json(force=True) or {}
    safe_name = os.path.basename(str(payload.get("filename") or "").strip())
    if not safe_name:
        return jsonify({"error": "filename 不能为空"}), 400
    with UPLOAD_WHITELIST_LOCK:
        UPLOAD_WHITELIST.add(safe_name)
    return jsonify({"ok": True, "filename": safe_name})


@app.route("/api/agent/can_upload", methods=["GET"])
def agent_can_upload():
    token = (request.args.get("token") or "").strip()
    node_name = (request.args.get("node_name") or "").strip()
    filename = os.path.basename(str(request.args.get("filename") or "").strip())
    if not token or not hmac.compare_digest(token, get_agent_token()):
        return jsonify({"error": "Forbidden"}), 403
    if not node_name:
        return jsonify({"error": "node_name 不能为空"}), 400

    row = AgentNode.query.filter_by(node_name=node_name).first()
    if not row:
        return jsonify({"error": "节点不存在"}), 404

    if filename:
        with UPLOAD_WHITELIST_LOCK:
            if filename in UPLOAD_WHITELIST:
                UPLOAD_WHITELIST.discard(filename)
                return jsonify({"can_upload": True})

    policy = normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY))
    if policy == "instant":
        return jsonify({"can_upload": True})
    if policy == "pause":
        return jsonify({"can_upload": False})
    if policy == "global":
        can_upload = is_now_in_cron_window(get_upload_cron(), window_minutes=5)
        return jsonify({"can_upload": bool(can_upload)})
    if policy == "custom":
        can_upload = is_now_in_cron_window(getattr(row, "upload_cron", ""), window_minutes=5)
        return jsonify({"can_upload": bool(can_upload)})
    return jsonify({"can_upload": True})


@app.route("/api/settings/telegram", methods=["DELETE"])
def clear_telegram_settings():
    delete_setting("tg_token")
    delete_setting("tg_chat_id")
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/scrape/records", methods=["GET"])
def list_scrape_records():
    rows = ScrapeRecord.query.order_by(ScrapeRecord.updated_at.desc(), ScrapeRecord.id.desc()).all()
    return jsonify(
        [
            {
                "id": row.id,
                "original_name": row.original_name,
                "tmdb_id": row.tmdb_id,
                "title": row.title or "",
                "year": row.year or "",
                "poster_url": build_poster_url(row.poster_url),
                "overview": row.overview or "",
                "status": row.status,
                "updated_at": format_db_time(row.updated_at),
            }
            for row in rows
        ]
    )


@app.route("/api/scrape/records", methods=["DELETE"])
def delete_scrape_records():
    payload = request.get_json(force=True) or {}
    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "ids 不能为空"}), 400

    norm_ids = []
    for v in ids:
        try:
            iv = int(v)
        except (TypeError, ValueError):
            continue
        if iv > 0:
            norm_ids.append(iv)
    norm_ids = sorted(set(norm_ids))
    if not norm_ids:
        return jsonify({"error": "ids 无效"}), 400

    try:
        bind_params = {}
        placeholders = []
        for i, rid in enumerate(norm_ids):
            key = f"id_{i}"
            placeholders.append(f":{key}")
            bind_params[key] = rid
        sql = text(f"DELETE FROM scrape_records WHERE id IN ({', '.join(placeholders)})")
        db.session.execute(sql, bind_params)
        db.session.commit()
        return jsonify({"message": "删除成功"})
    except Exception:
        db.session.rollback()
        logger.exception("删除刮削记录失败")
        return jsonify({"error": "删除失败"}), 500


@app.route("/api/scrape/search", methods=["POST"])
def scrape_search_tmdb():
    payload = request.get_json(force=True) or {}
    keyword = (payload.get("keyword") or "").strip()
    if not keyword:
        return jsonify({"error": "keyword 不能为空"}), 400
    try:
        results = search_tmdb_candidates(keyword, limit=10)
        return jsonify({"results": results})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"TMDB 查询失败: {exc}"}), 400


@app.route("/api/scrape/bind", methods=["POST"])
def scrape_bind_tmdb():
    payload = request.get_json(force=True) or {}
    original_name = (payload.get("original_name") or "").strip()
    tmdb_data = payload.get("tmdb_data") or {}
    if not original_name:
        return jsonify({"error": "original_name 不能为空"}), 400
    if not isinstance(tmdb_data, dict) or not tmdb_data.get("id"):
        return jsonify({"error": "tmdb_data 无效"}), 400

    formatted = format_tmdb_item(tmdb_data)
    if not formatted:
        return jsonify({"error": "tmdb_data 格式不正确"}), 400

    row = upsert_scrape_record(original_name, formatted, SCRAPE_STATUS_SUCCESS)
    return jsonify(
        {
            "ok": True,
            "record": {
                "id": row.id,
                "original_name": row.original_name,
                "tmdb_id": row.tmdb_id,
                "title": row.title or "",
                "year": row.year or "",
                "poster_url": build_poster_url(row.poster_url),
                "overview": row.overview or "",
                "status": row.status,
                "updated_at": format_db_time(row.updated_at),
            },
        }
    )


@app.route("/api/rename_rules", methods=["GET"])
def list_rename_rules():
    rows = RenameRule.query.order_by(RenameRule.id.desc()).all()
    return jsonify([{"id": r.id, "qb_tag": r.qb_tag, "suffix": r.suffix} for r in rows])


@app.route("/api/rename_rules", methods=["POST"])
def add_rename_rule():
    payload = request.get_json(force=True) or {}
    qb_tag = (payload.get("qb_tag") or "").strip()
    suffix = (payload.get("suffix") or "").strip()
    if not qb_tag or not suffix:
        return jsonify({"error": "qb_tag 和 suffix 不能为空"}), 400
    if RenameRule.query.filter_by(qb_tag=qb_tag).first():
        return jsonify({"error": "该 qB 标签已存在"}), 400
    row = RenameRule(qb_tag=qb_tag, suffix=suffix)
    db.session.add(row)
    db.session.commit()
    return jsonify({"ok": True, "id": row.id})


@app.route("/api/rename_rules/update", methods=["POST"])
def update_rename_rule():
    payload = request.get_json(force=True) or {}
    rule_id = payload.get("id")
    qb_tag = (payload.get("qb_tag") or "").strip()
    suffix = (payload.get("suffix") or "").strip()

    try:
        rule_id = int(rule_id)
    except (TypeError, ValueError):
        return jsonify({"error": "id、qb_tag 和 suffix 不能为空"}), 400

    if not rule_id or not qb_tag or not suffix:
        return jsonify({"error": "id、qb_tag 和 suffix 不能为空"}), 400

    row = RenameRule.query.filter_by(id=rule_id).first()
    if not row:
        return jsonify({"error": "记录不存在"}), 404

    conflict = RenameRule.query.filter(
        RenameRule.qb_tag == qb_tag,
        RenameRule.id != rule_id,
    ).first()
    if conflict:
        return jsonify({"error": "该 qB 标签已存在"}), 400

    row.qb_tag = qb_tag
    row.suffix = suffix
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/rename_rules/<int:row_id>", methods=["DELETE"])
def delete_rename_rule(row_id):
    row = RenameRule.query.filter_by(id=row_id).first()
    if not row:
        return jsonify({"error": "记录不存在"}), 404
    db.session.delete(row)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/settings/system", methods=["GET"])
@app.route("/api/config", methods=["GET"])
def get_system_settings():
    auth_username, _ = get_auth_credentials()
    return jsonify(
        {
            "auth_username": auth_username,
            "upload_cron": get_upload_cron(),
            "clouddrive_path": get_clouddrive_path(),
            "qb_monitor_tag": get_qb_monitor_tag(),
            "delete_after_upload": get_delete_after_upload(),
            "notify_pack_start": get_notify_flag("notify_pack_start", True),
            "notify_pack_end": get_notify_flag("notify_pack_end", True),
            "notify_upload_start": get_notify_flag("notify_upload_start", True),
            "notify_upload_end": get_notify_flag("notify_upload_end", True),
            "notify_task_add": get_notify_flag("notify_task_add", True),
            "notify_task_cancel": get_notify_flag("notify_task_cancel", True),
            "agent_token": get_agent_token(),
            "global_auto_upload": get_global_auto_upload(),
            "global_proxy": get_global_proxy(),
            "enable_tmdb": get_enable_tmdb(),
            "tmdb_api_key": get_tmdb_api_key(),
            "rename_trigger_tags": get_rename_trigger_tags_raw(),
            "rename_finish_tag": get_rename_finish_tag(),
            "mp_staging_path": get_setting("mp_staging_path") or "/Downloads/MP-LINK缓存区",
            "mp_final_path": get_setting("mp_final_path") or "/Downloads/115-LINK",
            "rename_enabled": get_rename_enabled(),
            "rename_move_all": get_rename_move_all(),
        }
    )


@app.route("/api/settings/system", methods=["POST"])
@app.route("/api/config/update", methods=["POST"])
def save_system_settings():
    payload = request.get_json(force=True) or {}
    current_global_auto_upload = get_global_auto_upload()
    current_auth_username, _ = get_auth_credentials()
    auth_username = (
        (payload.get("auth_username") or "").strip()
        if "auth_username" in payload
        else (current_auth_username or DEFAULT_ADMIN_USERNAME)
    )
    auth_password = (payload.get("auth_password") or "").strip()
    auth_password_confirm = (payload.get("auth_password_confirm") or "").strip()
    upload_cron_expr = (payload.get("upload_cron") or "").strip() if "upload_cron" in payload else get_upload_cron()
    clouddrive_path = (
        (payload.get("clouddrive_path") or "").strip() if "clouddrive_path" in payload else get_clouddrive_path()
    )
    qb_monitor_tag = (payload.get("qb_monitor_tag") or "").strip() if "qb_monitor_tag" in payload else get_qb_monitor_tag()
    delete_after_upload = parse_bool(payload.get("delete_after_upload"), default=get_delete_after_upload())
    notify_pack_start = parse_bool(payload.get("notify_pack_start"), default=get_notify_flag("notify_pack_start", True))
    notify_pack_end = parse_bool(payload.get("notify_pack_end"), default=get_notify_flag("notify_pack_end", True))
    notify_upload_start = parse_bool(
        payload.get("notify_upload_start"), default=get_notify_flag("notify_upload_start", True)
    )
    notify_upload_end = parse_bool(payload.get("notify_upload_end"), default=get_notify_flag("notify_upload_end", True))
    notify_task_add = parse_bool(payload.get("notify_task_add"), default=get_notify_flag("notify_task_add", True))
    notify_task_cancel = parse_bool(payload.get("notify_task_cancel"), default=get_notify_flag("notify_task_cancel", True))
    agent_token = (payload.get("agent_token") or "").strip() if "agent_token" in payload else get_agent_token()
    global_auto_upload = parse_bool(payload.get("global_auto_upload"), default=get_global_auto_upload())
    global_proxy = (payload.get("global_proxy") or "").strip() if "global_proxy" in payload else get_global_proxy()
    enable_tmdb = parse_bool(payload.get("enable_tmdb"), default=get_enable_tmdb())
    tmdb_api_key = (payload.get("tmdb_api_key") or "").strip() if "tmdb_api_key" in payload else get_tmdb_api_key()
    rename_trigger_tags = (
        (payload.get("rename_trigger_tags") or "").strip()
        if "rename_trigger_tags" in payload
        else get_rename_trigger_tags_raw()
    )
    rename_finish_tag = (
        (payload.get("rename_finish_tag") or "").strip() if "rename_finish_tag" in payload else get_rename_finish_tag()
    )
    mp_staging_path = (
        (payload.get("mp_staging_path") or "").strip() if "mp_staging_path" in payload else get_mp_staging_path()
    )
    mp_final_path = (payload.get("mp_final_path") or "").strip() if "mp_final_path" in payload else get_mp_final_path()
    rename_enabled = parse_bool(payload.get("rename_enabled"), default=(get_rename_enabled() != "0"))
    rename_move_all = parse_bool(payload.get("rename_move_all"), default=(get_rename_move_all() == "1"))

    if not auth_username:
        return jsonify({"error": "账号不能为空"}), 400

    if upload_cron_expr:
        try:
            CronTrigger.from_crontab(upload_cron_expr)
        except Exception as exc:
            return jsonify({"error": f"Cron 表达式无效: {exc}"}), 400
    else:
        upload_cron_expr = get_upload_cron()

    if not clouddrive_path:
        clouddrive_path = get_clouddrive_path()
    if not clouddrive_path:
        clouddrive_path = DEFAULT_CLOUDDRIVE_PATH

    if not qb_monitor_tag:
        qb_monitor_tag = get_qb_monitor_tag()

    if auth_password or auth_password_confirm:
        if not auth_password or not auth_password_confirm:
            return jsonify({"error": "修改密码时必须填写并确认新密码"}), 400
        if auth_password != auth_password_confirm:
            return jsonify({"error": "两次密码输入不一致"}), 400
        set_setting("auth_password", auth_password)

    set_setting("auth_username", auth_username)
    set_setting("clouddrive_path", clouddrive_path)
    set_setting("qb_monitor_tag", qb_monitor_tag)
    set_setting("delete_after_upload", "1" if delete_after_upload else "0")
    set_setting("notify_pack_start", "1" if notify_pack_start else "0")
    set_setting("notify_pack_end", "1" if notify_pack_end else "0")
    set_setting("notify_upload_start", "1" if notify_upload_start else "0")
    set_setting("notify_upload_end", "1" if notify_upload_end else "0")
    set_setting("notify_task_add", "1" if notify_task_add else "0")
    set_setting("notify_task_cancel", "1" if notify_task_cancel else "0")
    set_setting("agent_token", agent_token or DEFAULT_AGENT_TOKEN)
    set_setting("global_auto_upload", "1" if global_auto_upload else "0")
    if "global_auto_upload" in payload and global_auto_upload != current_global_auto_upload:
        apply_global_auto_upload_status(global_auto_upload)
    set_setting("global_proxy", global_proxy)
    set_setting("enable_tmdb", "1" if enable_tmdb else "0")
    set_setting("tmdb_api_key", tmdb_api_key)
    set_setting("rename_trigger_tags", rename_trigger_tags or DEFAULT_RENAME_TRIGGER_TAGS)
    set_setting("rename_finish_tag", rename_finish_tag or DEFAULT_RENAME_FINISH_TAG)
    if "mp_staging_path" in payload:
        set_setting("mp_staging_path", mp_staging_path)
    if "mp_final_path" in payload:
        set_setting("mp_final_path", mp_final_path)
    set_setting("rename_enabled", "1" if rename_enabled else "0")
    set_setting("rename_move_all", "1" if rename_move_all else "0")
    normalized = apply_upload_scheduler(upload_cron_expr)
    return jsonify(
        {
            "ok": True,
            "upload_cron": normalized,
            "clouddrive_path": clouddrive_path,
            "qb_monitor_tag": qb_monitor_tag,
            "delete_after_upload": delete_after_upload,
            "notify_pack_start": notify_pack_start,
            "notify_pack_end": notify_pack_end,
            "notify_upload_start": notify_upload_start,
            "notify_upload_end": notify_upload_end,
            "notify_task_add": notify_task_add,
            "notify_task_cancel": notify_task_cancel,
            "agent_token": agent_token or DEFAULT_AGENT_TOKEN,
            "global_auto_upload": global_auto_upload,
            "global_proxy": global_proxy,
            "enable_tmdb": enable_tmdb,
            "tmdb_api_key": tmdb_api_key,
            "rename_trigger_tags": rename_trigger_tags or DEFAULT_RENAME_TRIGGER_TAGS,
            "rename_finish_tag": rename_finish_tag or DEFAULT_RENAME_FINISH_TAG,
            "mp_staging_path": mp_staging_path or DEFAULT_MP_STAGING_PATH,
            "mp_final_path": mp_final_path or DEFAULT_MP_FINAL_PATH,
            "rename_enabled": "1" if rename_enabled else "0",
            "rename_move_all": "1" if rename_move_all else "0",
        }
    )


@app.route("/api/settings/auth", methods=["GET"])
def get_auth_settings():
    auth_username, _ = get_auth_credentials()
    return jsonify(
        {
            "auth_username": auth_username,
            "upload_cron": get_upload_cron(),
            "clouddrive_path": get_clouddrive_path(),
            "qb_monitor_tag": get_qb_monitor_tag(),
            "delete_after_upload": get_delete_after_upload(),
            "notify_pack_start": get_notify_flag("notify_pack_start", True),
            "notify_pack_end": get_notify_flag("notify_pack_end", True),
            "notify_upload_start": get_notify_flag("notify_upload_start", True),
            "notify_upload_end": get_notify_flag("notify_upload_end", True),
            "agent_token": get_agent_token(),
            "global_auto_upload": get_global_auto_upload(),
            "global_proxy": get_global_proxy(),
            "enable_tmdb": get_enable_tmdb(),
            "tmdb_api_key": get_tmdb_api_key(),
        }
    )


@app.route("/api/settings/auth", methods=["POST"])
def save_auth_settings():
    payload = request.get_json(force=True) or {}
    current_global_auto_upload = get_global_auto_upload()
    auth_username = (payload.get("auth_username") or "").strip()
    auth_password = (payload.get("auth_password") or "").strip()
    auth_password_confirm = (payload.get("auth_password_confirm") or "").strip()
    upload_cron_expr = (payload.get("upload_cron") or "").strip() or get_upload_cron()
    clouddrive_path = (payload.get("clouddrive_path") or "").strip() or get_clouddrive_path()
    qb_monitor_tag = (payload.get("qb_monitor_tag") or "").strip() or get_qb_monitor_tag()
    delete_after_upload = parse_bool(payload.get("delete_after_upload"), default=get_delete_after_upload())
    notify_pack_start = parse_bool(payload.get("notify_pack_start"), default=get_notify_flag("notify_pack_start", True))
    notify_pack_end = parse_bool(payload.get("notify_pack_end"), default=get_notify_flag("notify_pack_end", True))
    notify_upload_start = parse_bool(
        payload.get("notify_upload_start"), default=get_notify_flag("notify_upload_start", True)
    )
    notify_upload_end = parse_bool(payload.get("notify_upload_end"), default=get_notify_flag("notify_upload_end", True))
    agent_token = (payload.get("agent_token") or "").strip() if "agent_token" in payload else get_agent_token()
    global_auto_upload = parse_bool(payload.get("global_auto_upload"), default=get_global_auto_upload())
    global_proxy = (payload.get("global_proxy") or "").strip() if "global_proxy" in payload else get_global_proxy()
    enable_tmdb = parse_bool(payload.get("enable_tmdb"), default=get_enable_tmdb())
    tmdb_api_key = (payload.get("tmdb_api_key") or "").strip() if "tmdb_api_key" in payload else get_tmdb_api_key()

    if not auth_username or not auth_password or not auth_password_confirm:
        return jsonify({"error": "账号和密码不能为空"}), 400
    if auth_password != auth_password_confirm:
        return jsonify({"error": "两次密码输入不一致"}), 400

    try:
        CronTrigger.from_crontab(upload_cron_expr)
    except Exception as exc:
        return jsonify({"error": f"Cron 表达式无效: {exc}"}), 400

    set_setting("auth_username", auth_username)
    set_setting("auth_password", auth_password)
    set_setting("clouddrive_path", clouddrive_path or DEFAULT_CLOUDDRIVE_PATH)
    set_setting("qb_monitor_tag", qb_monitor_tag)
    set_setting("delete_after_upload", "1" if delete_after_upload else "0")
    set_setting("notify_pack_start", "1" if notify_pack_start else "0")
    set_setting("notify_pack_end", "1" if notify_pack_end else "0")
    set_setting("notify_upload_start", "1" if notify_upload_start else "0")
    set_setting("notify_upload_end", "1" if notify_upload_end else "0")
    set_setting("agent_token", agent_token or DEFAULT_AGENT_TOKEN)
    set_setting("global_auto_upload", "1" if global_auto_upload else "0")
    if "global_auto_upload" in payload and global_auto_upload != current_global_auto_upload:
        apply_global_auto_upload_status(global_auto_upload)
    set_setting("global_proxy", global_proxy)
    set_setting("enable_tmdb", "1" if enable_tmdb else "0")
    set_setting("tmdb_api_key", tmdb_api_key)
    normalized = apply_upload_scheduler(upload_cron_expr)
    return jsonify(
        {
            "ok": True,
            "upload_cron": normalized,
            "clouddrive_path": clouddrive_path or DEFAULT_CLOUDDRIVE_PATH,
            "qb_monitor_tag": qb_monitor_tag,
            "delete_after_upload": delete_after_upload,
            "notify_pack_start": notify_pack_start,
            "notify_pack_end": notify_pack_end,
            "notify_upload_start": notify_upload_start,
            "notify_upload_end": notify_upload_end,
            "agent_token": agent_token or DEFAULT_AGENT_TOKEN,
            "global_auto_upload": global_auto_upload,
            "global_proxy": global_proxy,
            "enable_tmdb": enable_tmdb,
            "tmdb_api_key": tmdb_api_key,
        }
    )


@app.route("/api/history", methods=["GET"])
def list_history():
    limit = request.args.get("limit", default=0, type=int)
    query = PackHistory.query.order_by(PackHistory.id.desc())
    if limit and limit > 0:
        query = query.limit(limit)
    rows = query.all()
    data = []
    for row in rows:
        duration_seconds = None
        if row.start_time and row.end_time:
            duration_seconds = int((row.end_time - row.start_time).total_seconds())
        upload_start_dt = parse_db_time(row.upload_start_time)
        upload_end_dt = parse_db_time(row.upload_end_time)
        upload_duration_seconds = None
        if upload_start_dt and upload_end_dt:
            upload_duration_seconds = int((upload_end_dt - upload_start_dt).total_seconds())
        data.append(
            {
                "id": row.id,
                "task_name": row.task_name,
                "display_name": build_display_name(row.task_name),
                "qb_server_id": row.qb_server_id,
                "qb_server_name": resolve_pack_history_node_name(row),
                "status": row.status,
                "start_time": row.start_time.strftime("%Y-%m-%d %H:%M:%S") if row.start_time else "",
                "end_time": row.end_time.strftime("%Y-%m-%d %H:%M:%S") if row.end_time else "",
                "file_size_gb": row.file_size_gb or 0,
                "duration_seconds": duration_seconds,
                "message": row.message or "",
                "info": row.info or "",
                "upload_start_time": row.upload_start_time or "",
                "upload_end_time": row.upload_end_time or "",
                "upload_duration_seconds": upload_duration_seconds,
            }
        )
    return jsonify(data)


@app.route("/api/history/delete", methods=["POST"])
def delete_history_batch():
    payload = request.get_json(force=True) or {}
    ids = payload.get("ids", [])
    if not isinstance(ids, list):
        return jsonify({"error": "ids 必须是数组"}), 400

    clean_ids = []
    for x in ids:
        try:
            clean_ids.append(int(x))
        except (TypeError, ValueError):
            continue
    if not clean_ids:
        return jsonify({"error": "未提供有效的历史记录 ID"}), 400

    PackHistory.query.filter(PackHistory.id.in_(clean_ids)).delete(synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True, "deleted": len(clean_ids)})


@app.route("/api/logs", methods=["GET"])
def get_logs():
    try:
        if not os.path.exists(LOG_FILE):
            return jsonify({"logs": [], "message": "日志文件不存在，已返回空结果"})
        logs = parse_recent_log_entries(hours=24)
        return jsonify({"logs": logs})
    except (OSError, PermissionError) as exc:
        logger.exception("读取日志文件失败")
        return jsonify({"logs": [], "message": f"日志接口异常: {exc}"})
    except Exception as exc:
        logger.exception("日志接口异常")
        return jsonify({"logs": [], "message": f"日志接口异常: {exc}"})


@app.route("/api/logs/clear", methods=["POST"])
def clear_logs():
    try:
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        with open(LOG_FILE, "w", encoding="utf-8"):
            pass
        logger.info("日志已清空")
        return jsonify({"ok": True})
    except Exception as exc:
        logger.exception("日志接口异常")
        return jsonify({"error": f"清空日志失败: {exc}"}), 500


@app.route("/api/logs/export", methods=["GET"])
def export_logs():
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", encoding="utf-8"):
                pass
        return send_file(LOG_FILE, as_attachment=True, download_name="autoiso.log", mimetype="text/plain; charset=utf-8")
    except Exception as exc:
        logger.exception("日志接口异常")
        return jsonify({"error": f"导出日志失败: {exc}"}), 500


@app.route("/api/progress", methods=["GET"])
def get_progress():
    tasks = []
    now_dt = now_local()
    with ACTIVE_TASKS_LOCK:
        snapshot = dict(ACTIVE_TASKS)
    for key, task in snapshot.items():
        source_size = task.get("source_size_bytes", 0)
        iso_path = task.get("iso_path", "")
        iso_size = os.path.getsize(iso_path) if iso_path and os.path.exists(iso_path) else 0
        percent = 0.0
        if source_size > 0:
            percent = min(100.0, max(0.0, (iso_size / source_size) * 100))
        started = task.get("start_time")
        elapsed_seconds = int((now_local() - started).total_seconds()) if started else 0
        speed_mbps = 0.0
        eta_seconds = None
        try:
            if elapsed_seconds > 0 and percent > 0:
                ratio = percent / 100.0
                estimated_total_seconds = elapsed_seconds / ratio
                eta_seconds = max(0, int(estimated_total_seconds - elapsed_seconds))
                estimated_total_mb = (source_size / (1024 * 1024)) if source_size > 0 else 0.0
                if estimated_total_mb > 0:
                    speed_mbps = (estimated_total_mb * ratio) / elapsed_seconds
        except ZeroDivisionError:
            speed_mbps = 0.0
            eta_seconds = None
        tasks.append(
            {
                "id": key,
                "task_name": task.get("task_name", ""),
                "display_name": build_display_name(task.get("task_name", "")),
                "server_name": task.get("server_name", ""),
                "source_path": task.get("source_path", ""),
                "iso_path": iso_path,
                "source_size_bytes": source_size,
                "iso_size_bytes": iso_size,
                "progress": round(percent, 2),
                "elapsed_seconds": elapsed_seconds,
                "speed_mbps": round(float(speed_mbps), 2),
                "eta_seconds": eta_seconds,
                "eta_text": format_eta(eta_seconds) if eta_seconds is not None else "-",
            }
        )

    stale_agent_keys = []
    with AGENT_TASKS_LOCK:
        agent_snapshot = dict(AGENT_TASKS)
    for filename, task in agent_snapshot.items():
        last_update = task.get("last_update")
        try:
            if not isinstance(last_update, datetime):
                stale_agent_keys.append(filename)
                continue
            if (now_dt - last_update).total_seconds() > AGENT_TASK_TTL_SECONDS:
                stale_agent_keys.append(filename)
                continue
        except Exception:
            stale_agent_keys.append(filename)
            continue

        progress = max(0.0, min(100.0, float(task.get("progress", 0) or 0)))
        speed_mbps = float(task.get("speed_mbps", 0) or 0)
        eta_text = str(task.get("eta_text") or "-").strip() or "-"
        tasks.append(
            {
                "id": f"agent:{filename}",
                "task_name": filename,
                "display_name": build_display_name(filename),
                "server_name": task.get("node", "VPS"),
                "source_path": "",
                "iso_path": "",
                "source_size_bytes": 0,
                "iso_size_bytes": 0,
                "progress": round(progress, 2),
                "elapsed_seconds": 0,
                "speed_mbps": round(speed_mbps, 2),
                "eta_seconds": None,
                "eta_text": eta_text,
                "status": task.get("status", "packing"),
                "from_agent": True,
            }
        )

    if stale_agent_keys:
        with AGENT_TASKS_LOCK:
            for key in stale_agent_keys:
                AGENT_TASKS.pop(key, None)

    return jsonify({"active": len(tasks) > 0, "tasks": tasks})


@app.route("/api/pending", methods=["GET"])
def list_pending():
    rows = []
    monitor_tag = get_qb_monitor_tag()
    servers = QBServer.query.order_by(QBServer.id.asc()).all()
    for server in servers:
        if is_invalid_qb_server_url(getattr(server, "url", "")):
            logger.warning("待封装列表跳过无效节点: node=%s url=%s", server.name, server.url)
            continue
        try:
            client = make_qb_client(server)
            torrents = client.torrents_info()
        except Exception as exc:
            if is_qb_connection_exception(exc):
                logger.warning("获取待封装任务连接失败，已跳过节点=%s err=%s", server.name, exc)
            else:
                logger.exception("获取待封装任务失败，节点=%s", server.name)
            continue

        for torrent in torrents:
            if not has_waiting_tag(getattr(torrent, "tags", "")):
                continue
            original_name = getattr(torrent, "name", "")
            tags_str = getattr(torrent, "tags", "")
            rename_suffix = resolve_rename_suffix(tags_str)
            scraped_name = insert_suffix_smart(original_name, rename_suffix)
            size_bytes = int(getattr(torrent, "size", 0) or getattr(torrent, "total_size", 0) or 0)
            added_on = getattr(torrent, "added_on", 0) or 0
            try:
                added_dt = datetime.fromtimestamp(int(added_on), TZ).strftime("%Y-%m-%d %H:%M") if added_on else ""
            except Exception:
                added_dt = ""

            rows.append(
                {
                    "title": original_name,
                    "scraped_name": scraped_name,
                    "size_gb": round(size_bytes / GB, 3),
                    "node_alias": server.name,
                    "added_on": added_dt,
                    "display_name": build_display_name(scraped_name),
                }
            )

    with AGENT_PENDING_TASKS_LOCK:
        pending_snapshot = dict(AGENT_PENDING_TASKS)
    now_dt_pending = now_local()
    stale_pending_nodes = []
    for node_name, payload in pending_snapshot.items():
        if not isinstance(payload, dict):
            stale_pending_nodes.append(node_name)
            continue
        updated_at = payload.get("updated_at")
        try:
            if not isinstance(updated_at, datetime):
                stale_pending_nodes.append(node_name)
                continue
            if (now_dt_pending - updated_at).total_seconds() > AGENT_TASK_TTL_SECONDS:
                stale_pending_nodes.append(node_name)
                continue
        except Exception:
            stale_pending_nodes.append(node_name)
            continue
        for task in payload.get("tasks", []):
            if not isinstance(task, dict):
                continue
            title = str(task.get("title") or "").strip()
            tags_str = str(task.get("tags") or "").strip()
            rename_suffix = resolve_rename_suffix(tags_str)
            scraped_name = insert_suffix_smart(title, rename_suffix)
            rows.append(
                {
                    "title": title,
                    "size_gb": round(float(task.get("size_gb", 0) or 0), 3),
                    "node_alias": str(task.get("node_alias") or node_name).strip() or node_name,
                    "added_on": str(task.get("added_on") or "").strip(),
                    "display_name": build_display_name(scraped_name),
                    "scraped_name": scraped_name,
                }
            )

    if stale_pending_nodes:
        with AGENT_PENDING_TASKS_LOCK:
            for node_name in stale_pending_nodes:
                AGENT_PENDING_TASKS.pop(node_name, None)

    blocked_names = build_pending_history_block_set()
    filtered_rows = []
    for item in rows:
        title = str(item.get("title") or "").strip()
        scraped_name = str(item.get("scraped_name") or "").strip()
        keys = build_task_name_keys(title) | build_task_name_keys(scraped_name)
        if keys & blocked_names:
            continue
        filtered_rows.append(item)

    scrape_keys = set()
    for item in filtered_rows:
        title = str(item.get("title") or "").strip()
        scraped_name = str(item.get("scraped_name") or "").strip()
        scrape_keys |= build_task_name_keys(title)
        scrape_keys |= build_task_name_keys(scraped_name)

    if scrape_keys:
        records = (
            ScrapeRecord.query.filter(
                func.lower(ScrapeRecord.original_name).in_(list(scrape_keys)),
                ScrapeRecord.status == SCRAPE_STATUS_SUCCESS,
            )
            .order_by(ScrapeRecord.updated_at.desc(), ScrapeRecord.id.desc())
            .all()
        )
        record_map = {str(r.original_name or "").strip().lower(): r for r in records}
        for item in filtered_rows:
            title = str(item.get("title") or "").strip()
            scraped_name = str(item.get("scraped_name") or "").strip()
            if not title:
                continue
            record = None
            for key in (build_task_name_keys(title) | build_task_name_keys(scraped_name)):
                record = record_map.get(str(key or "").strip().lower())
                if record:
                    break
            if record and (record.title or "").strip():
                year = str(record.year or "").strip()
                display_title = str(record.title or "").strip()
                item["display_name"] = f"{display_title} ({year})" if year else display_title

    filtered_rows.sort(key=lambda x: x.get("added_on", ""), reverse=True)
    return jsonify(filtered_rows)


@app.route("/api/pending_uploads", methods=["GET"])
def list_pending_uploads():
    rows = []
    seen_filenames = set()
    pending_statuses = {"待上传", "待上传 (阻塞中)"}
    agent_policy_map = {
        row.node_name: normalize_agent_upload_policy(row.upload_policy)
        for row in AgentNode.query.all()
    }

    names = get_upload_list_files()
    for name in names:
        safe_filename = os.path.basename(str(name or "").strip())
        if not safe_filename:
            continue
        filename_key = safe_filename.lower()
        if filename_key in seen_filenames:
            continue
        file_path = os.path.join(OUTPUT_DIR, name)
        try:
            size_bytes = os.path.getsize(file_path)
        except OSError:
            size_bytes = 0
        size_gb = round(size_bytes / GB, 3)

        upload_row = UploadHistory.query.filter_by(filename=safe_filename).first()
        upload_status = (upload_row.status if upload_row else "").strip().lower()
        status_text = "uploaded" if upload_status == "uploaded" else "pending"

        task_name = os.path.splitext(safe_filename)[0]
        # 优先通过 task_id 精准查找原始封装记录
        history_row = PackHistory.query.get(upload_row.task_id) if upload_row and upload_row.task_id else None
        if not history_row:
            history_row = (
                PackHistory.query.filter(PackHistory.task_name.in_([safe_filename, task_name]))
                .order_by(PackHistory.id.desc())
                .first()
            )

        # 提取最原始的任务名（不带标签后缀的干净名字）用于获取刮削中文名
        original_task_name = history_row.task_name if history_row else task_name
        node_name = resolve_pack_history_node_name(history_row)
        node_policy = agent_policy_map.get(node_name, "")

        rows.append(
            {
                "id": history_row.id if history_row else 0,
                "filename": safe_filename,
                "size": size_gb,
                "size_gb": size_gb,
                "node": node_name,
                "node_policy": node_policy,
                "status": status_text,
                "auto_upload": bool(get_task_auto_upload(safe_filename)),
                "display_name": build_display_name(original_task_name),
            }
        )
        seen_filenames.add(filename_key)

    pending_rows = (
        PackHistory.query.outerjoin(QBServer, PackHistory.qb_server_id == QBServer.id)
        .filter(
            or_(
                PackHistory.status.in_(list(pending_statuses)),
                func.lower(PackHistory.status) == "pending_upload",
            )
        )
        .order_by(PackHistory.id.desc())
        .all()
    )
    for history_row in pending_rows:
        task_name = os.path.basename(str(history_row.task_name or "").strip())
        if not task_name:
            continue
        filename = task_name
        filename_key = filename.lower()
        if filename_key in seen_filenames:
            continue

        info_node = extract_node_name_from_history_info(history_row.info)
        qb_name = (history_row.qb_server.name if history_row.qb_server else "").strip()
        is_vps_task = bool(info_node) or (bool(qb_name) and qb_name != "NAS")
        if not is_vps_task:
            continue

        node_name = info_node or qb_name or "VPS"
        size_gb = round(max(0.0, float(history_row.file_size_gb or 0.0)), 3)
        node_policy = agent_policy_map.get(node_name, AGENT_UPLOAD_DEFAULT_POLICY)
        rows.append(
            {
                "id": history_row.id,
                "filename": filename,
                "size": size_gb,
                "size_gb": size_gb,
                "node": node_name,
                "node_policy": node_policy,
                "status": "待上传 (阻塞中)",
                "auto_upload": True,
                "display_name": build_display_name(task_name),
            }
        )
        seen_filenames.add(filename_key)

    # 按 ID 升序排列：ID 越小（时间越早）的任务排在越前面
    rows.sort(key=lambda x: x.get("id", 0))
    return jsonify(rows)


@app.route("/api/upload_now/<path:filename>", methods=["POST"])
def upload_now(filename):
    safe_name = os.path.basename((filename or "").strip())
    if not safe_name or safe_name != filename or safe_name.startswith("."):
        return jsonify({"error": "文件名不合法"}), 400

    with app.app_context():
        if not is_valid_upload_file(safe_name):
            return jsonify({"error": "文件不存在"}), 404

    thread = threading.Thread(target=process_single_upload, args=(safe_name,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "上传任务已启动", "file_name": safe_name})


@app.route("/api/upload/toggle_auto/<path:filename>", methods=["POST"])
def toggle_task_auto_upload(filename):
    safe_name = os.path.basename((filename or "").strip())
    if not safe_name or safe_name != filename or safe_name.startswith("."):
        return jsonify({"error": "文件名不合法"}), 400
    if not is_valid_upload_file(safe_name) and not UploadHistory.query.filter_by(filename=safe_name).first():
        return jsonify({"error": "文件不存在"}), 404

    current_state = bool(get_task_auto_upload(safe_name))
    next_state = not current_state
    try:
        set_task_auto_upload(safe_name, next_state)
        apply_auto_upload_status_for_task(safe_name, next_state)
        db.session.commit()
        return jsonify({"ok": True, "filename": safe_name, "auto_upload": next_state})
    except Exception:
        db.session.rollback()
        logger.exception("切换任务自动上传状态失败: %s", safe_name)
        return jsonify({"error": "切换失败"}), 500


@app.route("/api/delete_local/<path:filename>", methods=["POST"])
def delete_local_file(filename):
    safe_name = os.path.basename((filename or "").strip())
    if not safe_name or safe_name != filename or safe_name.startswith("."):
        return jsonify({"error": "文件名不合法"}), 400

    file_path = os.path.join(OUTPUT_DIR, safe_name)
    try:
        os.remove(file_path)
    except FileNotFoundError:
        return jsonify({"error": "文件不存在"}), 404
    except OSError as exc:
        return jsonify({"error": f"删除失败: {exc}"}), 500

    mark_upload_status(safe_name, "deleted_local", "local file deleted")
    row = find_pack_row_for_upload(safe_name)
    if row:
        row.status = "本地已删除"
        row.info = "local source deleted"
        db.session.commit()

    return jsonify({"ok": True, "file_name": safe_name})


@app.route("/api/upload/control", methods=["POST"])
def upload_control():
    payload = request.get_json(force=True) or {}
    action = (payload.get("action") or "").strip().lower()
    if action not in {"pause", "resume", "abort"}:
        return jsonify({"error": "无效的控制动作"}), 400

    snapshot = get_current_upload_status_snapshot()
    if action == "pause":
        logger.info("收到上传控制指令：暂停")
        set_upload_command("paused")
        update_current_upload_status(
            active=snapshot.get("active", False),
            file_name=snapshot.get("file_name", ""),
            status="paused",
        )
    elif action == "abort":
        logger.info("收到上传控制指令：中止")
        set_upload_command("aborted")
        update_current_upload_status(
            active=snapshot.get("active", False),
            file_name=snapshot.get("file_name", ""),
            status="aborted",
        )
    else:
        logger.info("收到上传控制指令：继续")
        set_upload_command("running")
        update_current_upload_status(
            active=snapshot.get("active", False),
            file_name=snapshot.get("file_name", ""),
            status="uploading" if snapshot.get("active") else "idle",
        )

    return jsonify({"ok": True, "status": get_current_upload_status_snapshot().get("status", "idle")})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    now_dt = now_local().replace(tzinfo=None)
    month_start = datetime(now_dt.year, now_dt.month, 1)
    month_start_text = month_start.strftime("%Y-%m-%d %H:%M:%S")

    def is_vps(row):
        info = (row.info or "").strip().lower()
        return info.startswith("node=")

    all_rows = PackHistory.query.all()

    pack_data = {
        "nas": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0},
        "vps": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0},
    }
    up_data = {
        "nas": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0},
        "vps": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0},
    }

    pack_success_statuses = {
        STATUS_PACKED_PENDING_UPLOAD,
        STATUS_UPLOADING,
        STATUS_UPLOADED,
        "已封装",
        "待上传",
        "待上传 (阻塞中)",
        "成功"
    }

    for r in all_rows:
        size_gb = float(r.file_size_gb or 0.0)
        cat = "vps" if is_vps(r) else "nas"

        pack_data[cat]["count"] += 1
        pack_data[cat]["size"] += size_gb
        if r.start_time and r.start_time >= month_start:
            pack_data[cat]["mc"] += 1
            pack_data[cat]["msize"] += size_gb

        if r.start_time and r.end_time and r.status in pack_success_statuses:
            delta = (r.end_time - r.start_time).total_seconds()
            if delta > 0:
                pack_data[cat]["vc"] += 1
                pack_data[cat]["dur"] += delta
                pack_data[cat]["mb"] += size_gb * 1024.0

        if r.status in {STATUS_UPLOADED, "已上传", "成功"}:
            up_data[cat]["count"] += 1
            up_data[cat]["size"] += size_gb
            if (r.upload_end_time or "") >= month_start_text:
                up_data[cat]["mc"] += 1
                up_data[cat]["msize"] += size_gb

            start_dt = parse_db_time(r.upload_start_time)
            end_dt = parse_db_time(r.upload_end_time)
            if start_dt and end_dt:
                delta = (end_dt - start_dt).total_seconds()
                if delta > 0:
                    up_data[cat]["vc"] += 1
                    up_data[cat]["dur"] += delta
                    up_data[cat]["mb"] += size_gb * 1024.0

    def compile_stats(d):
        res = {}
        for cat in ["nas", "vps"]:
            res[f"{cat}_count"] = d[cat]["count"]
            res[f"{cat}_size_text"] = format_data_size(d[cat]["size"])
            res[f"{cat}_month_count"] = d[cat]["mc"]
            res[f"{cat}_month_size_text"] = format_data_size(d[cat]["msize"])
            avg_dur = int(d[cat]["dur"] / d[cat]["vc"]) if d[cat]["vc"] > 0 else 0
            res[f"{cat}_avg_duration_text"] = format_seconds(avg_dur)
            avg_spd = (d[cat]["mb"] / d[cat]["dur"]) if d[cat]["dur"] > 0 else 0.0
            res[f"{cat}_avg_rate_text"] = f"{avg_spd:.0f} MB/s"

        res["total_count"] = d["nas"]["count"] + d["vps"]["count"]
        res["total_size_text"] = format_data_size(d["nas"]["size"] + d["vps"]["size"])
        res["month_count"] = d["nas"]["mc"] + d["vps"]["mc"]
        res["month_size_text"] = format_data_size(d["nas"]["msize"] + d["vps"]["msize"])
        tot_vc = d["nas"]["vc"] + d["vps"]["vc"]
        tot_dur = d["nas"]["dur"] + d["vps"]["dur"]
        tot_mb = d["nas"]["mb"] + d["vps"]["mb"]
        avg_dur = int(tot_dur / tot_vc) if tot_vc > 0 else 0
        res["avg_duration_text"] = format_seconds(avg_dur)
        avg_spd = (tot_mb / tot_dur) if tot_dur > 0 else 0.0
        res["avg_rate_text"] = f"{avg_spd:.0f} MB/s"
        return res

    now_dt_agent = now_local()
    stale_agent_keys = []
    agent_tasks = []
    with AGENT_TASKS_LOCK:
        agent_snapshot = dict(AGENT_TASKS)
    for filename, task in agent_snapshot.items():
        last_update = task.get("last_update")
        try:
            if not isinstance(last_update, datetime):
                stale_agent_keys.append(filename)
                continue
            if (now_dt_agent - last_update).total_seconds() > AGENT_TASK_TTL_SECONDS:
                stale_agent_keys.append(filename)
                continue
        except Exception:
            stale_agent_keys.append(filename)
            continue

        progress = max(0.0, min(100.0, float(task.get("progress", 0) or 0)))
        speed_mbps = float(task.get("speed_mbps", 0) or 0)
        eta_text = str(task.get("eta_text") or "-").strip() or "-"
        speed_text = str(task.get("speed_text") or "").strip() or format_speed_mbps(speed_mbps)
        agent_tasks.append(
            {
                "id": f"agent:{filename}",
                "task_name": filename,
                "server_name": task.get("node", "VPS"),
                "status": task.get("status", "packing"),
                "progress": round(progress, 2),
                "speed_mbps": round(speed_mbps, 2),
                "speed_text": speed_text,
                "eta_text": eta_text,
                "updated_at": format_db_time(last_update),
            }
        )

    if stale_agent_keys:
        with AGENT_TASKS_LOCK:
            for key in stale_agent_keys:
                AGENT_TASKS.pop(key, None)

    return jsonify(
        {
            "pack": compile_stats(pack_data),
            "upload": compile_stats(up_data),
            "current_uploading_file": current_uploading_file,
            "current_upload_status": get_current_upload_status_snapshot(),
            "upload_progress": get_upload_progress_snapshot(),
            "agent_progress": {
                "active": len(agent_tasks) > 0,
                "count": len(agent_tasks),
                "tasks": agent_tasks,
            },
        }
    )


with app.app_context():
    os.makedirs("/data", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_schema()
    created_default_admin = False

    auth_username = get_setting("auth_username")
    auth_password = get_setting("auth_password")
    if not auth_username and not auth_password:
        set_setting("auth_username", DEFAULT_ADMIN_USERNAME)
        set_setting("auth_password", DEFAULT_ADMIN_PASSWORD)
        created_default_admin = True
    else:
        if not auth_username:
            set_setting("auth_username", DEFAULT_ADMIN_USERNAME)
        if not auth_password:
            set_setting("auth_password", DEFAULT_ADMIN_PASSWORD)

    if not get_setting("upload_cron"):
        set_setting("upload_cron", DEFAULT_UPLOAD_CRON)
    if not get_setting("clouddrive_path"):
        set_setting("clouddrive_path", DEFAULT_CLOUDDRIVE_PATH)
    if not get_setting("delete_after_upload"):
        set_setting("delete_after_upload", "0")
    if not get_setting("qb_monitor_tag"):
        set_setting("qb_monitor_tag", DEFAULT_QB_MONITOR_TAG)
    if not get_setting("notify_pack_start"):
        set_setting("notify_pack_start", "1")
    if not get_setting("notify_pack_end"):
        set_setting("notify_pack_end", "1")
    if not get_setting("notify_upload_start"):
        set_setting("notify_upload_start", "1")
    if not get_setting("notify_upload_end"):
        set_setting("notify_upload_end", "1")
    if not get_setting("notify_task_add"):
        set_setting("notify_task_add", "1")
    if not get_setting("notify_task_cancel"):
        set_setting("notify_task_cancel", "1")
    if not get_setting("agent_token"):
        set_setting("agent_token", DEFAULT_AGENT_TOKEN)
    if not get_setting("global_auto_upload"):
        set_setting("global_auto_upload", "1")
    
    PackHistory.query.filter_by(status=STATUS_UPLOADING).update(
        {
            "status": STATUS_PACKED_PENDING_UPLOAD,
            "info": "recovered on startup",
        },
        synchronize_session=False,
    )
    db.session.commit()
    if created_default_admin:
        print(f"[AutoISO] 默认管理员已初始化: 用户名={DEFAULT_ADMIN_USERNAME} 密码={DEFAULT_ADMIN_PASSWORD}")

    init_upload_cron = get_upload_cron()
    upload_trigger, normalized = build_cron_trigger(init_upload_cron)
    if normalized != init_upload_cron:
        set_setting("upload_cron", normalized)
        db.session.commit()

scheduler.add_job(
    func=process_all_qbs,
    trigger="interval",
    minutes=PACK_POLL_INTERVAL_MINUTES,
    id="pack_poll_job",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
)

scheduler.add_job(
    func=process_uploads,
    trigger=upload_trigger,
    id="upload_job",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
)

if not scheduler.running:
    scheduler.start()

atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
