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

APP_VERSION = "v0.9.6"

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

ACTIVE_TASKS = {}
ACTIVE_TASKS_LOCK = threading.Lock()
SCRAPE_INFLIGHT = set()
SCRAPE_INFLIGHT_LOCK = threading.Lock()
AGENT_TASKS = {}
AGENT_TASKS_LOCK = threading.Lock()
AGENT_PENDING_TASKS = {}
AGENT_PENDING_TASKS_LOCK = threading.Lock()
AGENT_TASK_TTL_SECONDS = int(os.getenv("AGENT_TASK_TTL_SECONDS", "180"))
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
    updated_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(TZ), onupdate=lambda: datetime.now(TZ))

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
    if request.path in ["/api/agent/report", "/api/agent/pending", "/api/agent/config", "/api/agent/can_upload"]:
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

        conn.execute(text("""
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
        """))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_records_original_name ON scrape_records(original_name)"))
        
        conn.execute(text("""
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
        """))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_nodes_node_name ON agent_nodes(node_name)"))
        agent_result = conn.execute(text("PRAGMA table_info(agent_nodes)"))
        agent_columns = {row[1] for row in agent_result.fetchall()}
        if "auto_upload" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN auto_upload BOOLEAN NOT NULL DEFAULT 1"))
        if "upload_policy" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN upload_policy VARCHAR(16) NOT NULL DEFAULT 'instant'"))
        if "upload_cron" not in agent_columns:
            conn.execute(text("ALTER TABLE agent_nodes ADD COLUMN upload_cron VARCHAR(64) NOT NULL DEFAULT ''"))
        conn.execute(text("UPDATE agent_nodes SET upload_policy = 'instant' WHERE upload_policy IS NULL OR TRIM(upload_policy) = '' OR LOWER(TRIM(upload_policy)) NOT IN ('instant', 'pause', 'global', 'custom')"))
        conn.execute(text("UPDATE agent_nodes SET upload_cron = '' WHERE upload_cron IS NULL"))
        
        conn.execute(text("INSERT INTO qb_servers(name, url, username, password) SELECT 'NAS', 'local://nas', '-', '-' WHERE NOT EXISTS (SELECT 1 FROM qb_servers WHERE name = 'NAS')"))
        conn.execute(text("UPDATE pack_history SET qb_server_id = (SELECT id FROM qb_servers WHERE name = 'NAS' LIMIT 1) WHERE qb_server_id IN (SELECT id FROM qb_servers WHERE url LIKE 'agent://%')"))
        conn.execute(text("DELETE FROM qb_servers WHERE url LIKE 'agent://%'"))

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
        "封装中", "待上传", "待上传 (阻塞中)", "上传中", "成功", "已上传", "已封装",
        STATUS_PACKED_PENDING_UPLOAD, STATUS_PACKED_ONLY, STATUS_UPLOADED,
    }
    rows = db.session.query(PackHistory.task_name).filter(PackHistory.status.in_(list(blocking_statuses))).all()
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
        row = UploadHistory(filename=safe_name, task_id=task_id, status="pending" if global_enabled else "packed", message="", auto_upload=global_enabled)
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
        PackHistory.query.filter(PackHistory.status == STATUS_PACKED_ONLY).update({"status": STATUS_PACKED_PENDING_UPLOAD}, synchronize_session=False)
        UploadHistory.query.filter_by(status="packed").update({"status": "pending"}, synchronize_session=False)
    else:
        PackHistory.query.filter(PackHistory.status.in_(list(pending_statuses))).update({"status": STATUS_PACKED_ONLY}, synchronize_session=False)
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
    if not v: return ""
    if v.startswith(TMDB_IMAGE_BASE): return v[len(TMDB_IMAGE_BASE) :]
    return v if v.startswith("/") else ""

def build_poster_url(poster_path):
    p = normalize_poster_path(poster_path)
    return f"{TMDB_IMAGE_BASE}{p}" if p else ""

def tmdb_request(path, *, params=None, api_key=None, proxy_url=None, timeout=10):
    token = (api_key or "").strip() or get_tmdb_api_key()
    if not token: raise ValueError("TMDB API Key 未配置")
    req_params = {"api_key": token}
    if params: req_params.update(params)
    request_kwargs = {"params": req_params, "timeout": timeout}
    proxy = (proxy_url or "").strip()
    if proxy: request_kwargs["proxies"] = {"http": proxy, "https": proxy}
    return requests.get(f"{TMDB_API_BASE}{path}", **request_kwargs)

def format_tmdb_item(item):
    if not item or not item.get("id"): return None
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
    if not original: return ""
    record = ScrapeRecord.query.filter_by(original_name=original).first()
    if record and record.status == SCRAPE_STATUS_SUCCESS:
        title = (record.title or "").strip()
        year = (record.year or "").strip()
    else:
        title, year = clean_filename(original)
    if not title: title = original
    return f"{title} ({year})" if year else title

def search_tmdb_candidates(keyword, limit=10, year=None):
    kw = (keyword or "").strip()
    if not kw: return []
    proxy = get_global_proxy()
    year_token = extract_year(year)
    results = []

    if kw.isdigit():
        tmdb_id = int(kw)
        for media_type in ("movie", "tv"):
            try:
                resp = tmdb_request(f"/{media_type}/{tmdb_id}", params={"language": "zh-CN"}, proxy_url=proxy)
            except (ValueError, requests.exceptions.RequestException): continue
            if resp.status_code == 200:
                data = resp.json() or {}
                data["media_type"] = media_type
                item = format_tmdb_item(data)
                if item: results.append(item)
        return results[:limit]

    def append_results(items, default_media_type=""):
        seen = {(x.get("id"), x.get("title"), x.get("year")) for x in results}
        for item in items:
            if default_media_type and not item.get("media_type"): item["media_type"] = default_media_type
            media_type = (item.get("media_type") or "").lower()
            if media_type not in {"movie", "tv", ""}: continue
            formatted = format_tmdb_item(item)
            if not formatted: continue
            sig = (formatted.get("id"), formatted.get("title"), formatted.get("year"))
            if sig in seen: continue
            seen.add(sig)
            results.append(formatted)
            if len(results) >= limit: break

    if year_token:
        resp = tmdb_request("/search/movie", params={"query": kw, "year": year_token, "language": "zh-CN", "page": 1, "include_adult": "false"}, proxy_url=proxy)
        if resp.status_code == 200: append_results(resp.json().get("results", []), default_media_type="movie")
        if len(results) < limit:
            resp = tmdb_request("/search/tv", params={"query": kw, "first_air_date_year": year_token, "language": "zh-CN", "page": 1, "include_adult": "false"}, proxy_url=proxy)
            if resp.status_code == 200: append_results(resp.json().get("results", []), default_media_type="tv")

    if len(results) < limit:
        resp = tmdb_request("/search/multi", params={"query": kw, "language": "zh-CN", "page": 1, "include_adult": "false"}, proxy_url=proxy)
        if resp.status_code == 200: append_results(resp.json().get("results", []))
    return results

def upsert_scrape_record(original_name, tmdb_data=None, status=SCRAPE_STATUS_FAILED):
    name = (original_name or "").strip()
    if not name: return None
    row = ScrapeRecord.query.filter_by(original_name=name).first()
    if not row: row = ScrapeRecord(original_name=name)
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
    if not get_enable_tmdb(): return None
    name = (original_name or "").strip()
    if not name: return None
    cached = ScrapeRecord.query.filter_by(original_name=name).first()
    if cached and cached.status == SCRAPE_STATUS_SUCCESS: return cached
    clean_title, clean_year = clean_filename(name)
    forced_keyword = (preferred_keyword or "").strip()
    keyword = forced_keyword or clean_title or name
    search_year = extract_year(forced_keyword) or clean_year
    try:
        matches = search_tmdb_candidates(keyword, limit=1, year=search_year)
        if not matches and search_year: matches = search_tmdb_candidates(keyword, limit=1)
        if not matches and clean_title and clean_title != keyword: matches = search_tmdb_candidates(clean_title, limit=1, year=clean_year)
        if not matches and clean_title and clean_title != keyword and clean_year: matches = search_tmdb_candidates(clean_title, limit=1)
        if not matches and keyword != name: matches = search_tmdb_candidates(name, limit=1)
        if matches: return upsert_scrape_record(name, matches[0], SCRAPE_STATUS_SUCCESS)
        return upsert_scrape_record(name, {}, SCRAPE_STATUS_FAILED)
    except Exception as exc:
        logger.warning("TMDB 刮削异常: %s | 错误: %s", name, exc)
        return upsert_scrape_record(name, {}, SCRAPE_STATUS_FAILED)

def trigger_auto_scrape_async(original_name, search_keyword=""):
    if not get_enable_tmdb(): return
    name = (original_name or "").strip()
    if not name: return
    keyword = (search_keyword or "").strip()
    cached = ScrapeRecord.query.filter_by(original_name=name, status=SCRAPE_STATUS_SUCCESS).first()
    if cached: return
    with SCRAPE_INFLIGHT_LOCK:
        if name in SCRAPE_INFLIGHT: return
        SCRAPE_INFLIGHT.add(name)
    def _worker(task_name, task_keyword):
        with app.app_context():
            try: auto_scrape_for_original_name(task_name, preferred_keyword=task_keyword)
            except Exception: pass
            finally:
                with SCRAPE_INFLIGHT_LOCK: SCRAPE_INFLIGHT.discard(task_name)
    threading.Thread(target=_worker, args=(name, keyword), daemon=True).start()

def get_notify_flag(key, default=True):
    raw = get_setting(key)
    if raw == "": return default
    return parse_bool(raw, default=default)

def parse_bool(value, default=False):
    if isinstance(value, bool): return value
    if value is None: return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

def format_eta(seconds):
    if seconds is None or seconds < 0: return "-"
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def update_upload_progress(active=False, file_name="", status="uploading", percentage=0.0, speed_mbps=0.0, eta="-", bytes_done=0, total_bytes=0):
    with UPLOAD_PROGRESS_LOCK:
        UPLOAD_PROGRESS.update({
            "active": bool(active), "file_name": file_name or "", "status": status or "uploading",
            "percentage": round(float(percentage), 2), "speed_mbps": round(float(speed_mbps), 2),
            "eta": eta or "-", "bytes_done": int(bytes_done or 0), "total_bytes": int(total_bytes or 0),
        })

def get_upload_progress_snapshot():
    with UPLOAD_PROGRESS_LOCK: return dict(UPLOAD_PROGRESS)

def set_upload_command(command):
    global upload_command
    with UPLOAD_COMMAND_LOCK: upload_command = command

def get_upload_command():
    with UPLOAD_COMMAND_LOCK: return upload_command

def update_current_upload_status(active=False, file_name="", status="idle"):
    current_upload_status.update({"active": bool(active), "file_name": file_name or "", "status": status or "idle"})

def get_current_upload_status_snapshot(): return dict(current_upload_status)

def is_valid_upload_file(name):
    if not name or name.startswith(".") or name.endswith(PACKING_SUFFIX): return False
    return os.path.isfile(os.path.join(OUTPUT_DIR, name))

def get_pending_upload_files():
    try: names = sorted(os.listdir(OUTPUT_DIR))
    except FileNotFoundError: return []
    uploaded_names = {row.filename for row in UploadHistory.query.with_entities(UploadHistory.filename).filter_by(status="uploaded").all()}
    return [n for n in names if not n.endswith(PACKING_SUFFIX) and is_valid_upload_file(n) and n not in uploaded_names]

def get_upload_list_files():
    try: names = sorted(os.listdir(OUTPUT_DIR))
    except FileNotFoundError: return []
    return [n for n in names if is_valid_upload_file(n)]

def mark_upload_status(filename, status, message="", task_id=None):
    if not filename: return
    global_auto_upload = bool(get_global_auto_upload())
    row = UploadHistory.query.filter_by(task_id=task_id).first() if task_id else None
    if not row: row = UploadHistory.query.filter_by(filename=filename).first()
    if not row:
        row = UploadHistory(filename=filename, task_id=task_id, status=status, message=message or "", auto_upload=global_auto_upload)
        db.session.add(row)
    else:
        is_new_task_cycle = task_id is not None and row.task_id != task_id
        if task_id is not None: row.task_id = task_id
        if is_new_task_cycle: row.auto_upload = global_auto_upload
        row.filename = filename
        row.status = status
        row.message = message or ""
    if status == "uploaded": row.uploaded_at = now_local()
    db.session.commit()

def find_pack_row_for_upload(file_name):
    safe_name = os.path.basename(file_name or "")
    if not safe_name: return None
    task_name = os.path.splitext(safe_name)[0]
    iso_path = os.path.join(OUTPUT_DIR, safe_name)
    iso_msg, file_msg = f"ISO: {iso_path}", f"FILE: {iso_path}"
    row = PackHistory.query.filter(PackHistory.task_name.in_([safe_name, task_name]), PackHistory.message.in_([iso_msg, file_msg]), PackHistory.status.in_([STATUS_PACKED_PENDING_UPLOAD, STATUS_UPLOADING])).order_by(PackHistory.id.desc()).first()
    if row: return row
    return PackHistory.query.filter(PackHistory.task_name.in_([safe_name, task_name]), PackHistory.message.in_([iso_msg, file_msg])).order_by(PackHistory.id.desc()).first()

def get_or_create_external_server_id():
    server = QBServer.query.filter_by(name="NAS").first()
    if server: return server.id
    server = QBServer(name="NAS", url="local://nas", username="-", password="-")
    db.session.add(server)
    db.session.commit()
    return server.id

def upsert_agent_history_status(node_name, file_name, status, file_size_gb=0.0):
    safe_name = os.path.basename(str(file_name or "").strip())
    if not safe_name: return None
    safe_status = (status or "").strip().lower()
    status_map = {"packing": "封装中", "uploading": "上传中", "pending_upload": "待上传 (阻塞中)", "finished": "成功", "error": "失败"}
    mapped_status = status_map.get(safe_status)
    if not mapped_status: return None

    now_dt = now_local()
    normalized_size_gb = max(0.0, float(file_size_gb or 0.0))
    node_label = (node_name or "").strip() or "VPS"
    server_id = get_or_create_external_server_id()
    task_name_no_ext = os.path.splitext(safe_name)[0]
    
    row = PackHistory.query.filter(PackHistory.qb_server_id == server_id, PackHistory.task_name.in_([safe_name, task_name_no_ext]), PackHistory.message.like("Agent report:%"), PackHistory.info == f"node={node_label}").order_by(PackHistory.id.desc()).first()

    if safe_status == "packing":
        if not row:
            row = PackHistory(task_name=safe_name[:255], qb_server_id=server_id, status=mapped_status, start_time=now_dt, file_size_gb=normalized_size_gb, message="Agent report: packing", info=f"node={node_label}")
            db.session.add(row)
            db.session.commit()
            return row
        row.status = mapped_status
        if not row.start_time: row.start_time = now_dt
        if normalized_size_gb > 0 or not (row.file_size_gb and row.file_size_gb > 0): row.file_size_gb = normalized_size_gb
        row.message = "Agent report: packing"
        row.info = f"node={node_label}"
        db.session.commit()
        return row

    if not row:
        row = PackHistory(task_name=safe_name[:255], qb_server_id=server_id, status=mapped_status, start_time=now_dt, message=f"Agent report: {safe_status}", info=f"node={node_label}")
        db.session.add(row)

    row.status = mapped_status
    row.message = f"Agent report: {safe_status}"
    row.info = f"node={node_label}"
    if normalized_size_gb > 0 or not (row.file_size_gb and row.file_size_gb > 0): row.file_size_gb = normalized_size_gb
    
    # 补充VPS上传相关的时间戳
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
    if not has_app_context() or not file_name: return
    row = db.session.get(PackHistory, task_id) if task_id else None
    if row:
        row.status = STATUS_UPLOADED
        row.end_time = now_local()
        row.message = f"已上传: {dst_path}"
        row.info = ""
        if not row.upload_start_time: row.upload_start_time = format_db_time(now_local())
        row.upload_end_time = format_db_time(now_local())
        db.session.commit()
        return

    ext_server_id = get_or_create_external_server_id()
    now_dt = now_local()
    try: size_gb = round(os.path.getsize(dst_path) / GB, 3)
    except OSError: size_gb = 0.0
    new_row = PackHistory(task_name=file_name[:255], qb_server_id=ext_server_id, status=STATUS_UPLOADED, start_time=now_dt, end_time=now_dt, file_size_gb=size_gb, message=f"已上传: {dst_path}", info="外部文件", upload_start_time=format_db_time(now_dt), upload_end_time=format_db_time(now_dt))
    db.session.add(new_row)
    db.session.commit()

def upload_file_with_progress(src_path, dst_path, delete_after=False, task_id=None):
    global current_uploading_file
    chunk_size = 10 * 1024 * 1024
    total_bytes = os.path.getsize(src_path)
    bytes_done = 0
    start_ts = time.time()
    update_upload_timestamps(os.path.basename(src_path), start_time=now_local(), task_id=task_id)
    temp_dst_path = f"{dst_path}{UPLOADING_SUFFIX}"
    aborted, paused_logged = False, False

    try:
        if os.path.exists(temp_dst_path): os.remove(temp_dst_path)
    except OSError: pass

    update_upload_progress(active=True, file_name=os.path.basename(src_path), status="uploading", percentage=0.0, speed_mbps=0.0, eta="-", bytes_done=0, total_bytes=total_bytes)

    try:
        with open(src_path, "rb") as rf, open(temp_dst_path, "wb") as wf:
            while True:
                command = get_upload_command()
                if command == "aborted":
                    aborted = True
                    break
                if command == "paused":
                    if not paused_logged: paused_logged = True
                    percent = (bytes_done / total_bytes * 100) if total_bytes > 0 else 0.0
                    update_upload_progress(active=True, file_name=os.path.basename(src_path), status="paused", percentage=percent, speed_mbps=0.0, eta="paused", bytes_done=bytes_done, total_bytes=total_bytes)
                    update_current_upload_status(active=True, file_name=os.path.basename(src_path), status="paused")
                    time.sleep(1)
                    continue

                if paused_logged: paused_logged = False
                update_current_upload_status(active=True, file_name=os.path.basename(src_path), status="uploading")

                chunk = rf.read(chunk_size)
                if not chunk: break
                wf.write(chunk)
                bytes_done += len(chunk)
                elapsed = max(time.time() - start_ts, 1e-6)
                speed_bps = bytes_done / elapsed
                speed_mbps = speed_bps / (1024 * 1024)
                remaining = max(total_bytes - bytes_done, 0)
                eta_seconds = (remaining / speed_bps) if speed_bps > 0 else -1
                percent = (bytes_done / total_bytes * 100) if total_bytes > 0 else 100.0
                update_upload_progress(active=True, file_name=os.path.basename(src_path), status="uploading", percentage=percent, speed_mbps=speed_mbps, eta=format_eta(eta_seconds), bytes_done=bytes_done, total_bytes=total_bytes)
            wf.flush()

        if aborted:
            try:
                if os.path.isfile(temp_dst_path): os.remove(temp_dst_path)
            except OSError: pass
            current_uploading_file = None
            return False

        os.rename(temp_dst_path, dst_path)
        if delete_after: os.remove(src_path)
        update_upload_timestamps(os.path.basename(src_path), end_time=now_local(), task_id=task_id)
        finalize_pack_history_after_upload(os.path.basename(src_path), dst_path, task_id=task_id)
        return True
    except Exception:
        try:
            if os.path.isfile(temp_dst_path): os.remove(temp_dst_path)
        except OSError: pass
        raise

def build_cron_trigger(cron_expr):
    expr = (cron_expr or "").strip() or DEFAULT_UPLOAD_CRON
    try: return CronTrigger.from_crontab(expr), expr
    except Exception: return CronTrigger.from_crontab(DEFAULT_UPLOAD_CRON), DEFAULT_UPLOAD_CRON

def is_now_in_cron_window(cron_expr, now_dt=None, window_minutes=5):
    expr = (cron_expr or "").strip()
    if not expr: return False
    current = now_dt or now_local()
    try:
        if CRONITER_AVAILABLE: prev_fire = croniter(expr, current).get_prev(datetime)
        else: prev_fire = CronTrigger.from_crontab(expr, timezone=TZ).get_prev_fire_time(None, current)
    except Exception: return False
    if not prev_fire: return False
    return 0 <= (current - prev_fire).total_seconds() <= max(1, int(window_minutes)) * 60

def apply_upload_scheduler(cron_expr):
    trigger, normalized = build_cron_trigger(cron_expr)
    set_setting("upload_cron", normalized)
    db.session.commit()
    job = scheduler.get_job("upload_job")
    if job: scheduler.reschedule_job("upload_job", trigger=trigger)
    else: scheduler.add_job(func=process_uploads, trigger=trigger, id="upload_job", max_instances=1, coalesce=True, replace_existing=True)
    return normalized

def format_seconds(seconds):
    seconds = int(max(0, seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

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

def format_db_time(dt):
    if not dt: return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_db_time(raw):
    if not raw: return None
    try: return datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
    except Exception: return None

def update_upload_timestamps(file_name, start_time=None, end_time=None, task_id=None):
    if not has_app_context() or not file_name: return
    safe_name = os.path.basename(file_name)
    row = UploadHistory.query.filter_by(task_id=task_id).first() if task_id else UploadHistory.query.filter_by(filename=safe_name).first()
    if row:
        if start_time is not None:
            row.upload_start_time = format_db_time(start_time)
            row.upload_end_time = ""
        if end_time is not None: row.upload_end_time = format_db_time(end_time)
    pack_row = db.session.get(PackHistory, task_id) if task_id else None
    if pack_row:
        if start_time is not None:
            pack_row.upload_start_time = format_db_time(start_time)
            pack_row.upload_end_time = ""
        if end_time is not None: pack_row.upload_end_time = format_db_time(end_time)
    if row or pack_row: db.session.commit()

def get_path_size_bytes(path):
    if not path or not os.path.exists(path): return 0
    if os.path.isfile(path):
        try: return os.path.getsize(path)
        except OSError: return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try: total += os.path.getsize(os.path.join(root, name))
            except OSError: continue
    return total

def extract_error_tail(stderr, stdout):
    lines = [ln.strip() for ln in (stderr or "").splitlines() if ln.strip()]
    filtered = [ln for ln in lines if not ln.lower().startswith("genisoimage ")]
    target = filtered if filtered else lines
    if target: return "\n".join(target[-5:])
    out_lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip()]
    if out_lines: return "\n".join(out_lines[-3:])
    return "unknown error"

def send_tg_notification(text_msg):
    return send_tg_notification_with_config(text_msg, get_setting("tg_token"), get_setting("tg_chat_id"))

def send_tg_notification_with_config(text_msg, token, chat_id, proxy_url=None):
    if not token or not chat_id: return False, "未配置 TG"
    if proxy_url is None: proxy_url = get_global_proxy()
    proxy_url = (proxy_url or "").strip()
    request_kwargs = {"json": {"chat_id": chat_id, "text": text_msg}, "timeout": 15}
    if proxy_url: request_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", **request_kwargs)
        resp.raise_for_status()
        data = resp.json()
        return (True, "ok") if data.get("ok") else (False, str(data))
    except Exception as exc: return False, str(exc)

def make_qb_client(server: QBServer):
    client = qbittorrentapi.Client(host=server.url, username=server.username, password=server.password, REQUESTS_ARGS={"timeout": 20})
    client.auth_log_in()
    return client

def is_qb_connection_exception(exc):
    if exc is None: return False
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(k in text for k in ("nameresolutionerror", "connectionerror", "apiconnectionerror", "newconnectionerror", "maxretryerror", "failed to establish a new connection", "failed to resolve", "temporary failure in name resolution", "nodename nor servname provided", "getaddrinfo failed"))

def is_invalid_qb_server_url(raw_url):
    url = (raw_url or "").strip().lower()
    if not url or "agent://" in url or "://agent" in url: return True
    return False

def should_suppress_qb_alert(server, exc):
    url = (getattr(server, "url", "") or "").strip().lower()
    if "agent" in url or is_invalid_qb_server_url(url): return True
    return is_qb_connection_exception(exc)

def has_waiting_tag(tags_str):
    monitor_tag = get_qb_monitor_tag()
    return monitor_tag in [tag.strip() for tag in (tags_str or "").split(",") if tag.strip()]

def qb_remove_tags(client, torrent_hash, tags):
    if not torrent_hash or not tags: return
    tags_text = ",".join([x for x in tags if x])
    try: client.torrents_remove_tags(tags=tags_text, torrent_hashes=torrent_hash)
    except TypeError: client.torrents_remove_tags(tags=tags_text, hashes=torrent_hash)

def qb_add_tags(client, torrent_hash, tags):
    if not torrent_hash or not tags: return
    tags_text = ",".join([x for x in tags if x])
    try: client.torrents_add_tags(tags=tags_text, torrent_hashes=torrent_hash)
    except TypeError: client.torrents_add_tags(tags=tags_text, hashes=torrent_hash)

def resolve_source_path(torrent):
    content_path = getattr(torrent, "content_path", None)
    if content_path: return content_path
    return os.path.join(getattr(torrent, "save_path", ""), getattr(torrent, "name", ""))

def pack_to_iso(task_name, source_path, vol_id):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    iso_path = os.path.join(OUTPUT_DIR, f"{task_name}.iso")
    temp_iso_path = f"{iso_path}{PACKING_SUFFIX}"
    if os.path.exists(iso_path): os.remove(iso_path)
    if os.path.exists(temp_iso_path): os.remove(temp_iso_path)

    cmd = ["genisoimage", "-udf", "-iso-level", "3", "-allow-limited-size", "-V", vol_id, "-o", temp_iso_path, source_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0: os.rename(temp_iso_path, iso_path)
    else:
        try:
            if os.path.exists(temp_iso_path): os.remove(temp_iso_path)
        except OSError: pass
    return proc.returncode == 0, iso_path, proc.stdout, proc.stderr

def process_uploads():
    global current_uploading_file
    if not UPLOAD_ENGINE_LOCK.acquire(blocking=False): return
    with app.app_context():
        try:
            clouddrive_path = (get_clouddrive_path() or "").strip() or DEFAULT_CLOUDDRIVE_PATH
            delete_after_upload = get_delete_after_upload()
            try: os.makedirs(clouddrive_path, exist_ok=True)
            except Exception: return
            
            pending_names = get_pending_upload_files()
            global_auto_upload = get_global_auto_upload()

            for file_name in pending_names:
                src_path = os.path.join(OUTPUT_DIR, file_name)
                dst_path = os.path.join(clouddrive_path, file_name)
                row = find_pack_row_for_upload(file_name)
                task_id = row.id if row else None
                task_auto_upload = get_task_auto_upload(file_name)
                
                if not (global_auto_upload and task_auto_upload):
                    reason = "global auto upload disabled" if not global_auto_upload else "task auto upload disabled"
                    if row:
                        row.status = STATUS_PACKED_ONLY
                        row.info = reason
                        db.session.commit()
                    mark_upload_status(file_name, "packed", reason, task_id=task_id)
                    continue

                if row:
                    row.status = STATUS_UPLOADING
                    row.info = f"uploading: {src_path}"
                    db.session.commit()
                mark_upload_status(file_name, "uploading", f"uploading: {src_path}", task_id=task_id)

                try:
                    if not os.path.isfile(src_path): raise FileNotFoundError(f"file not found: {src_path}")
                    current_uploading_file = file_name
                    set_upload_command("running")
                    update_current_upload_status(active=True, file_name=file_name, status="uploading")
                    if get_notify_flag("notify_upload_start", True): send_tg_notification(f"[AutoISO] 📤 开始转移至网盘：{file_name}")

                    uploaded_ok = upload_file_with_progress(src_path, dst_path, delete_after=delete_after_upload, task_id=task_id)
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
                    if get_notify_flag("notify_upload_end", True): send_tg_notification(f"[AutoISO] 🎉 转移完成：{file_name} 已送达网盘。")
                except Exception as exc:
                    mark_upload_status(file_name, "failed", str(exc), task_id=task_id)
                    if row:
                        row.status = STATUS_PACKED_PENDING_UPLOAD
                        row.info = f"upload failed: {exc}"
                        db.session.commit()
                finally:
                    current_uploading_file = None
                    set_upload_command("running")
                    update_current_upload_status(active=False, file_name="", status="idle")
                    update_upload_progress(active=False, file_name="", status="uploading", percentage=0.0, speed_mbps=0.0, eta="-")
        finally:
            UPLOAD_ENGINE_LOCK.release()

def process_single_upload(file_name):
    global current_uploading_file
    safe_name = os.path.basename(file_name or "")
    if not safe_name or safe_name.startswith(".") or safe_name != file_name: return
    with UPLOAD_ENGINE_LOCK:
        with app.app_context():
            if not is_valid_upload_file(safe_name): return
            clouddrive_path = (get_clouddrive_path() or "").strip() or DEFAULT_CLOUDDRIVE_PATH
            delete_after_upload = get_delete_after_upload()
            try: os.makedirs(clouddrive_path, exist_ok=True)
            except Exception: return

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
                if get_notify_flag("notify_upload_start", True): send_tg_notification(f"[AutoISO] 📤 开始转移至网盘：{safe_name}")
                uploaded_ok = upload_file_with_progress(src_path, dst_path, delete_after=delete_after_upload, task_id=task_id)
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
                if get_notify_flag("notify_upload_end", True): send_tg_notification(f"[AutoISO] 🎉 转移完成：{safe_name} 已送达网盘。")
            except Exception as exc:
                mark_upload_status(safe_name, "failed", str(exc), task_id=task_id)
                if row:
                    row.status = STATUS_PACKED_PENDING_UPLOAD
                    row.info = f"upload failed: {exc}"
                    db.session.commit()
            finally:
                current_uploading_file = None
                set_upload_command("running")
                update_current_upload_status(active=False, file_name="", status="idle")
                update_upload_progress(active=False, file_name="", status="uploading", percentage=0.0, speed_mbps=0.0, eta="-")

def process_one_torrent(server: QBServer, client, torrent):
    if PackHistory.query.filter_by(task_name=torrent.name, qb_server_id=server.id, status=STATUS_PROCESSING).first(): return
    try: trigger_auto_scrape_async(getattr(torrent, "name", ""))
    except Exception: pass

    source_path = resolve_source_path(torrent)
    source_size_bytes = get_path_size_bytes(source_path)
    history = PackHistory(task_name=torrent.name, qb_server_id=server.id, status=STATUS_PROCESSING, start_time=now_local(), file_size_gb=round(source_size_bytes / GB, 3))
    db.session.add(history)
    db.session.commit()
    try:
        expected_output_name = os.path.basename(source_path) if os.path.isfile(source_path) else f"{torrent.name}.iso"
        init_task_auto_upload(expected_output_name, history.id)
    except Exception: pass

    started = now_local()
    torrent_hash = getattr(torrent, "hash", "")
    task_key = f"{server.id}:{torrent_hash or torrent.name}"
    with ACTIVE_TASKS_LOCK:
        ACTIVE_TASKS[task_key] = {"task_name": torrent.name, "server_name": server.name, "source_path": source_path, "source_size_bytes": source_size_bytes, "iso_path": os.path.join(OUTPUT_DIR, f"{torrent.name}.iso{PACKING_SUFFIX}"), "start_time": started, "task_id": history.id}

    try:
        if os.path.isfile(source_path):
            output_file = os.path.join(OUTPUT_DIR, os.path.basename(source_path))
            try:
                if os.path.abspath(source_path) != os.path.abspath(output_file): shutil.copy2(source_path, output_file)
            except Exception as exc:
                history.status = STATUS_FAILED
                history.end_time = now_local()
                history.message = f"single file copy failed: {exc}"
                history.info = str(exc)
                db.session.commit()
                qb_remove_tags(client, torrent_hash, [PACKING_TAG])
                qb_add_tags(client, torrent_hash, [FAILED_TAG])
                if get_notify_flag("notify_pack_end", True): send_tg_notification(f"[AutoISO] ❌ 单文件转存失败 | 节点: {server.name} | 任务: {torrent.name}")
                return
            finished = now_local()
            duration = format_seconds((finished - started).total_seconds())
            output_name = os.path.basename(output_file)
            auto_upload_enabled = init_task_auto_upload(output_name, history.id)
            history.status = STATUS_PACKED_PENDING_UPLOAD if auto_upload_enabled else STATUS_PACKED_ONLY
            history.end_time = finished
            history.message = f"FILE: {output_file}"
            history.info = "single file copied to output"
            if os.path.isfile(output_file): history.file_size_gb = round((os.path.getsize(output_file) / GB), 3)
            db.session.commit()
            mark_upload_status(output_name, "pending" if auto_upload_enabled else "packed", "single file ready", task_id=history.id)
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True): send_tg_notification(f"[AutoISO] ✅ 转存成功，等待上传 | 节点: {server.name} | 任务: {torrent.name}")
            return

        if not os.path.isdir(source_path):
            history.status = STATUS_FAILED
            history.end_time = now_local()
            history.message = f"source path not found: {source_path}"
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            if get_notify_flag("notify_pack_end", True): send_tg_notification(f"[AutoISO] ❌ 封装失败 (目录不存在) | 节点: {server.name} | 任务: {torrent.name}")
            return

        safe_vol_id = re.sub(r"[^a-zA-Z0-9_]", "_", torrent.name or "AUTOISO")[:30]
        ok, iso_path, out, err = pack_to_iso(torrent.name, source_path, safe_vol_id)
        finished = now_local()

        if ok:
            iso_name = os.path.basename(iso_path)
            auto_upload_enabled = init_task_auto_upload(iso_name, history.id)
            history.status = STATUS_PACKED_PENDING_UPLOAD if auto_upload_enabled else STATUS_PACKED_ONLY
            history.end_time = finished
            history.message = f"ISO: {iso_path}"
            history.info = ""
            db.session.commit()
            mark_upload_status(iso_name, "pending" if auto_upload_enabled else "packed", "iso ready", task_id=history.id)
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True): send_tg_notification(f"[AutoISO] ✅ 封装成功，等待上传 | 节点: {server.name} | 任务: {torrent.name}")
        else:
            history.status = STATUS_FAILED
            history.end_time = finished
            history.message = extract_error_tail(err, out)
            history.info = (err or "").strip()
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            if get_notify_flag("notify_pack_end", True): send_tg_notification(f"[AutoISO] ❌ 封装失败 | 节点: {server.name} | 任务: {torrent.name}")
    finally:
        with ACTIVE_TASKS_LOCK: ACTIVE_TASKS.pop(task_key, None)

def process_all_qbs():
    with app.app_context():
        servers = QBServer.query.all()
        monitor_tag = get_qb_monitor_tag()
        for server in servers:
            if is_invalid_qb_server_url(getattr(server, "url", "")): continue
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
            except Exception as exc:
                if not should_suppress_qb_alert(server, exc): send_tg_notification(f"[AutoISO] 节点连接失败 | 节点: {server.name} | 错误: {exc}")
                continue

            for torrent in torrents:
                if not has_waiting_tag(getattr(torrent, "tags", "")): continue
                try: trigger_auto_scrape_async(getattr(torrent, "name", ""))
                except Exception: pass
                if float(getattr(torrent, "progress", 0)) != 1.0: continue

                torrent_hash = getattr(torrent, "hash", "")
                try:
                    qb_remove_tags(client, torrent_hash, [monitor_tag])
                    qb_add_tags(client, torrent_hash, [PACKING_TAG])
                    if get_notify_flag("notify_pack_start", True): send_tg_notification(f"[AutoISO] 🚀 开始封装 | 节点: {server.name} | 任务: {torrent.name}")
                    process_one_torrent(server, client, torrent)
                except Exception as exc:
                    try:
                        qb_remove_tags(client, torrent_hash, [PACKING_TAG, monitor_tag])
                        qb_add_tags(client, torrent_hash, [FAILED_TAG])
                    except Exception: pass
                    failed_record = PackHistory(task_name=getattr(torrent, "name", "unknown"), qb_server_id=server.id, status=STATUS_FAILED, start_time=now_local(), end_time=now_local(), message=f"Unhandled error: {exc}")
                    db.session.add(failed_record)
                    db.session.commit()
                    if not should_suppress_qb_alert(server, exc): send_tg_notification(f"[AutoISO] ❌ 任务异常 | 节点: {server.name} | 任务: {getattr(torrent, 'name', 'unknown')}")

def parse_recent_log_entries(hours=24):
    if not os.path.exists(LOG_FILE): return []
    cutoff = datetime.now() - timedelta(hours=hours)
    entries, current_lines, current_dt = [], [], None
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = LOG_TS_RE.match(line)
            if m:
                if current_lines and current_dt: entries.append((current_dt, "\n".join(current_lines)))
                current_lines = [line]
                try: current_dt = datetime.strptime(m.group("dt"), "%Y-%m-%d %H:%M:%S")
                except Exception: current_dt = None
            else:
                if current_lines: current_lines.append(line)
    if current_lines and current_dt: entries.append((current_dt, "\n".join(current_lines)))
    recent = [item for item in entries if item[0] >= cutoff]
    recent.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in recent]

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if session.get("logged_in"): return redirect(url_for("index"))
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
def index(): return render_template("index.html", version=APP_VERSION)

@app.route("/api/qb/test", methods=["POST"])
def test_qb_connection():
    payload = request.get_json(force=True)
    try:
        client = qbittorrentapi.Client(host=(payload.get("url") or "").strip(), username=(payload.get("username") or "").strip(), password=(payload.get("password") or "").strip(), REQUESTS_ARGS={"timeout": 15})
        client.auth_log_in()
        return jsonify({"ok": True, "message": "连接成功"})
    except Exception as exc: return jsonify({"error": f"连接失败: {exc}"}), 400

@app.route("/api/qbservers", methods=["GET"])
def list_qbservers():
    return jsonify([{"id": x.id, "name": x.name, "url": x.url, "username": x.username} for x in QBServer.query.order_by(QBServer.id.desc()).all()])

@app.route("/api/qbservers", methods=["POST"])
def add_qbserver():
    payload = request.get_json(force=True)
    if QBServer.query.filter_by(name=(payload.get("name") or "").strip()).first(): return jsonify({"error": "节点别名已存在"}), 400
    db.session.add(QBServer(name=(payload.get("name") or "").strip(), url=(payload.get("url") or "").strip(), username=(payload.get("username") or "").strip(), password=(payload.get("password") or "").strip()))
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/qb/update", methods=["POST"])
def update_qbserver():
    payload = request.get_json(force=True) or {}
    server = db.session.get(QBServer, int(payload.get("id", 0)))
    if not server: return jsonify({"error": "节点不存在"}), 404
    server.name = (payload.get("alias") or "").strip()
    server.url = (payload.get("url") or "").strip()
    server.username = (payload.get("username") or "").strip()
    if (payload.get("password") or "").strip(): server.password = payload.get("password").strip()
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/qbservers/<int:server_id>", methods=["DELETE"])
def delete_qbserver(server_id):
    item = db.session.get(QBServer, server_id)
    if not item: return jsonify({"error": "节点不存在"}), 404
    fallback_id = get_or_create_external_server_id()
    if fallback_id == item.id:
        alt = QBServer.query.filter(QBServer.id != item.id).order_by(QBServer.id.asc()).first()
        fallback_id = alt.id if alt else get_or_create_external_server_id()
    PackHistory.query.filter_by(qb_server_id=item.id).update({"qb_server_id": fallback_id}, synchronize_session=False)
    db.session.delete(item)
    db.session.commit()
    return jsonify({"ok": True})

def serialize_agent_node(row):
    return {"id": row.id, "node_name": row.node_name, "qb_url": row.qb_url, "qb_user": row.qb_user, "qb_pass": row.qb_pass, "temp_path": row.temp_path, "cd2_path": row.cd2_path, "auto_upload": bool(getattr(row, "auto_upload", True)), "upload_policy": normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY)), "upload_cron": getattr(row, "upload_cron", "").strip()}

@app.route("/api/agent_nodes", methods=["GET"])
def list_agent_nodes():
    return jsonify([serialize_agent_node(row) for row in AgentNode.query.order_by(AgentNode.id.desc()).all()])

@app.route("/api/agent_nodes", methods=["POST"])
def add_agent_node():
    payload = request.get_json(force=True) or {}
    row = AgentNode(node_name=payload.get("node_name").strip(), qb_url=payload.get("qb_url").strip(), qb_user=payload.get("qb_user").strip(), qb_pass=payload.get("qb_pass").strip(), temp_path=payload.get("temp_path").strip(), cd2_path=payload.get("cd2_path").strip(), auto_upload=parse_bool(payload.get("auto_upload"), True), upload_policy=normalize_agent_upload_policy(payload.get("upload_policy")), upload_cron=payload.get("upload_cron", "").strip())
    db.session.add(row)
    db.session.commit()
    return jsonify({"ok": True, "node": serialize_agent_node(row)})

@app.route("/api/agent_nodes/update", methods=["POST"])
def update_agent_node():
    payload = request.get_json(force=True) or {}
    row = db.session.get(AgentNode, int(payload.get("id", 0)))
    row.node_name, row.qb_url, row.qb_user, row.qb_pass, row.temp_path, row.cd2_path = payload.get("node_name").strip(), payload.get("qb_url").strip(), payload.get("qb_user").strip(), payload.get("qb_pass").strip(), payload.get("temp_path").strip(), payload.get("cd2_path").strip()
    row.upload_policy, row.upload_cron = normalize_agent_upload_policy(payload.get("upload_policy")), payload.get("upload_cron", "").strip()
    db.session.commit()
    return jsonify({"ok": True, "node": serialize_agent_node(row)})

@app.route("/api/agent_nodes/toggle_policy", methods=["POST"])
def toggle_agent_node_policy():
    payload = request.get_json(force=True) or {}
    row = AgentNode.query.filter_by(node_name=payload.get("node", "").strip()).first()
    row.upload_policy = "custom" if normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY)) == "global" else "global"
    db.session.commit()
    return jsonify({"ok": True, "node": serialize_agent_node(row)})

@app.route("/api/agent_nodes/<int:node_id>", methods=["DELETE"])
def delete_agent_node(node_id):
    db.session.delete(db.session.get(AgentNode, node_id))
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/settings/telegram", methods=["GET", "POST", "DELETE"])
def telegram_settings():
    if request.method == "GET": return jsonify({"tg_token": get_setting("tg_token"), "tg_chat_id": get_setting("tg_chat_id")})
    if request.method == "DELETE":
        delete_setting("tg_token")
        delete_setting("tg_chat_id")
        db.session.commit()
        return jsonify({"ok": True})
    payload = request.get_json(force=True) or {}
    set_setting("tg_token", payload.get("tg_token", "").strip())
    set_setting("tg_chat_id", payload.get("tg_chat_id", "").strip())
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/telegram/test", methods=["POST"])
def test_telegram_settings():
    payload = request.get_json(force=True) or {}
    ok, msg = send_tg_notification_with_config("TG 通知测试成功！", payload.get("tg_token", "").strip(), payload.get("tg_chat_id", "").strip())
    return jsonify({"ok": True}) if ok else jsonify({"error": msg}), 400

@app.route("/api/tmdb/test", methods=["POST"])
def test_tmdb_connection():
    payload = request.get_json(force=True) or {}
    url = f"https://api.themoviedb.org/3/authentication?api_key={payload.get('tmdb_api_key', '').strip()}"
    proxy = payload.get("global_proxy", "").strip()
    try:
        resp = requests.get(url, timeout=10, proxies={"http": proxy, "https": proxy} if proxy else None)
        if resp.status_code == 200: return jsonify({"message": "TMDB API 连接成功！"}), 200
        return jsonify({"error": "API Key 无效"}), 400
    except Exception as exc: return jsonify({"error": f"TMDB 请求失败: {exc}"}), 400

@app.route("/api/agent/report", methods=["POST"])
def agent_report():
    payload = request.get_json(force=True) or {}
    token = str(payload.get("token") or "").strip()
    expected_token = (get_setting("agent_token") or "").strip()
    if not expected_token or not token or not hmac.compare_digest(token, expected_token): return jsonify({"error": "forbidden"}), 403

    node = str(payload.get("node") or "").strip() or "VPS"
    filename = os.path.basename(str(payload.get("filename") or "").strip())
    status = str(payload.get("status") or "").strip().lower()

    file_size_gb = float(payload.get("size_gb") or payload.get("file_size_gb") or (float(payload.get("file_size_bytes") or 0) / GB))
    
    server_id = get_or_create_external_server_id()
    old_row = PackHistory.query.filter(PackHistory.qb_server_id == server_id, PackHistory.task_name.in_([filename, os.path.splitext(filename)[0]]), PackHistory.info == f"node={node}").order_by(PackHistory.id.desc()).first()
    old_status = old_row.status if old_row else None

    updated_row = upsert_agent_history_status(node, filename, status, file_size_gb=file_size_gb)
    new_status = updated_row.status if updated_row else None

    # 精准发送通知逻辑
    if new_status and old_status != new_status:
        if new_status == "封装中" and get_notify_flag("notify_pack_start", True):
            send_tg_notification(f"[AutoISO] 🚀 开始封装 | 节点: {node} | 任务: {filename}")
        elif new_status == "待上传 (阻塞中)" and get_notify_flag("notify_pack_end", True):
            send_tg_notification(f"[AutoISO] ✅ 封装完成，等待放行 | 节点: {node} | 任务: {filename}")
        elif new_status == "上传中" and get_notify_flag("notify_upload_start", True):
            send_tg_notification(f"[AutoISO] 📤 开始转移至网盘 | 节点: {node} | 任务: {filename}")
        elif new_status == "成功" and get_notify_flag("notify_upload_end", True):
            send_tg_notification(f"[AutoISO] 🎉 转移完成 | 节点: {node} | 任务: {filename} 已送达网盘。")
        elif new_status == "失败" and (get_notify_flag("notify_pack_end", True) or get_notify_flag("notify_upload_end", True)):
            send_tg_notification(f"[AutoISO] ❌ 任务处理失败 | 节点: {node} | 任务: {filename}")

    if status in {"finished", "error"}:
        with AGENT_TASKS_LOCK: AGENT_TASKS.pop(filename, None)
    else:
        with AGENT_TASKS_LOCK: AGENT_TASKS[filename] = {"node": node, "filename": filename, "status": status, "progress": float(payload.get("progress", 0)), "speed_mbps": float(payload.get("speed_mbps", 0)), "eta_text": str(payload.get("eta_text", "-")), "last_update": now_local()}
    
    if status in {"pending_upload", "finished"}:
        try: trigger_auto_scrape_async(filename, search_keyword=clean_agent_report_filename(filename) or filename)
        except Exception: pass
    return jsonify({"ok": True, "status": "accepted"})

@app.route("/api/agent/pending", methods=["POST"])
def agent_pending_report():
    payload = request.get_json(force=True) or {}
    node = str(payload.get("node") or "").strip() or "VPS"
    normalized = [{"title": str(i.get("title")), "display_name": str(i.get("display_name") or i.get("title")), "size_gb": float(i.get("size_gb", 0)), "added_on": str(i.get("added_on")), "node_alias": node} for i in payload.get("tasks", []) if isinstance(i, dict)]
    with AGENT_PENDING_TASKS_LOCK: AGENT_PENDING_TASKS[node] = {"updated_at": now_local(), "tasks": normalized}
    return jsonify({"ok": True, "node": node, "count": len(normalized)})

@app.route("/api/agent/config", methods=["GET"])
def get_agent_config():
    row = AgentNode.query.filter_by(node_name=(request.args.get("node_name") or "").strip()).first()
    data = serialize_agent_node(row)
    data["delete_after_upload"] = bool(get_delete_after_upload())
    return jsonify(data)

@app.route("/api/agent/whitelist_upload", methods=["POST"])
def whitelist_agent_upload():
    safe_name = os.path.basename(str((request.get_json(force=True) or {}).get("filename") or "").strip())
    with UPLOAD_WHITELIST_LOCK: UPLOAD_WHITELIST.add(safe_name)
    return jsonify({"ok": True, "filename": safe_name})

@app.route("/api/agent/can_upload", methods=["GET"])
def agent_can_upload():
    filename = os.path.basename(str(request.args.get("filename") or "").strip())
    row = AgentNode.query.filter_by(node_name=(request.args.get("node_name") or "").strip()).first()
    if filename:
        with UPLOAD_WHITELIST_LOCK:
            if filename in UPLOAD_WHITELIST:
                UPLOAD_WHITELIST.discard(filename)
                return jsonify({"can_upload": True})
    policy = normalize_agent_upload_policy(getattr(row, "upload_policy", AGENT_UPLOAD_DEFAULT_POLICY))
    if policy == "instant": return jsonify({"can_upload": True})
    if policy == "pause": return jsonify({"can_upload": False})
    if policy == "global": return jsonify({"can_upload": bool(is_now_in_cron_window(get_upload_cron(), window_minutes=5))})
    return jsonify({"can_upload": bool(is_now_in_cron_window(getattr(row, "upload_cron", ""), window_minutes=5))})

@app.route("/api/scrape/records", methods=["GET", "DELETE"])
def scrape_records():
    if request.method == "GET":
        return jsonify([{"id": r.id, "original_name": r.original_name, "tmdb_id": r.tmdb_id, "title": r.title or "", "year": r.year or "", "poster_url": build_poster_url(r.poster_url), "overview": r.overview or "", "status": r.status, "updated_at": format_db_time(r.updated_at)} for r in ScrapeRecord.query.order_by(ScrapeRecord.updated_at.desc()).all()])
    ids = [int(v) for v in (request.get_json(force=True) or {}).get("ids", []) if str(v).isdigit()]
    ScrapeRecord.query.filter(ScrapeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()
    return jsonify({"message": "删除成功"})

@app.route("/api/scrape/search", methods=["POST"])
def scrape_search_tmdb():
    return jsonify({"results": search_tmdb_candidates((request.get_json(force=True) or {}).get("keyword", "").strip(), limit=10)})

@app.route("/api/scrape/bind", methods=["POST"])
def scrape_bind_tmdb():
    payload = request.get_json(force=True) or {}
    return jsonify({"ok": True, "record": {"id": upsert_scrape_record(payload.get("original_name").strip(), format_tmdb_item(payload.get("tmdb_data")), SCRAPE_STATUS_SUCCESS).id}})

@app.route("/api/settings/system", methods=["GET", "POST"])
def system_settings():
    if request.method == "GET":
        auth_username, _ = get_auth_credentials()
        return jsonify({"auth_username": auth_username, "upload_cron": get_upload_cron(), "clouddrive_path": get_clouddrive_path(), "qb_monitor_tag": get_qb_monitor_tag(), "delete_after_upload": get_delete_after_upload(), "notify_pack_start": get_notify_flag("notify_pack_start", True), "notify_pack_end": get_notify_flag("notify_pack_end", True), "notify_upload_start": get_notify_flag("notify_upload_start", True), "notify_upload_end": get_notify_flag("notify_upload_end", True), "agent_token": get_agent_token(), "global_auto_upload": get_global_auto_upload(), "global_proxy": get_global_proxy(), "enable_tmdb": get_enable_tmdb(), "tmdb_api_key": get_tmdb_api_key()})
    
    payload = request.get_json(force=True) or {}
    if "auth_password" in payload and payload["auth_password"]: set_setting("auth_password", payload["auth_password"])
    set_setting("auth_username", payload.get("auth_username", get_auth_credentials()[0]))
    set_setting("clouddrive_path", payload.get("clouddrive_path", get_clouddrive_path()))
    set_setting("qb_monitor_tag", payload.get("qb_monitor_tag", get_qb_monitor_tag()))
    set_setting("delete_after_upload", "1" if parse_bool(payload.get("delete_after_upload")) else "0")
    set_setting("notify_pack_start", "1" if parse_bool(payload.get("notify_pack_start")) else "0")
    set_setting("notify_pack_end", "1" if parse_bool(payload.get("notify_pack_end")) else "0")
    set_setting("notify_upload_start", "1" if parse_bool(payload.get("notify_upload_start")) else "0")
    set_setting("notify_upload_end", "1" if parse_bool(payload.get("notify_upload_end")) else "0")
    set_setting("global_auto_upload", "1" if parse_bool(payload.get("global_auto_upload")) else "0")
    set_setting("global_proxy", payload.get("global_proxy", get_global_proxy()))
    set_setting("enable_tmdb", "1" if parse_bool(payload.get("enable_tmdb")) else "0")
    set_setting("tmdb_api_key", payload.get("tmdb_api_key", get_tmdb_api_key()))
    apply_upload_scheduler(payload.get("upload_cron", get_upload_cron()))
    return jsonify({"ok": True})

@app.route("/api/history", methods=["GET"])
def list_history():
    rows = PackHistory.query.order_by(PackHistory.id.desc()).all()
    data = []
    for row in rows:
        ds, uds = None, None
        if row.start_time and row.end_time: ds = int((row.end_time - row.start_time).total_seconds())
        ust, uet = parse_db_time(row.upload_start_time), parse_db_time(row.upload_end_time)
        if ust and uet: uds = int((uet - ust).total_seconds())
        data.append({
            "id": row.id, 
            "task_name": row.task_name, 
            "display_name": build_display_name(row.task_name),
            "qb_server_name": resolve_pack_history_node_name(row), 
            "status": row.status, 
            "start_time": format_db_time(row.start_time), 
            "end_time": format_db_time(row.end_time), 
            "file_size_gb": row.file_size_gb or 0, 
            "duration_seconds": ds, 
            "upload_start_time": row.upload_start_time or "", 
            "upload_end_time": row.upload_end_time or "", 
            "upload_duration_seconds": uds
        })
    return jsonify(data)

@app.route("/api/history/delete", methods=["POST"])
def delete_history_batch():
    ids = [int(x) for x in (request.get_json(force=True) or {}).get("ids", []) if str(x).isdigit()]
    PackHistory.query.filter(PackHistory.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/logs", methods=["GET"])
def get_logs(): return jsonify({"logs": parse_recent_log_entries(24)})

@app.route("/api/logs/clear", methods=["POST"])
def clear_logs():
    open(LOG_FILE, "w", encoding="utf-8").close()
    return jsonify({"ok": True})

@app.route("/api/logs/export", methods=["GET"])
def export_logs(): return send_file(LOG_FILE, as_attachment=True, download_name="autoiso.log")

@app.route("/api/progress", methods=["GET"])
def get_progress():
    tasks = []
    now_dt = now_local()
    with ACTIVE_TASKS_LOCK: snapshot = dict(ACTIVE_TASKS)
    for key, task in snapshot.items():
        iso_size = os.path.getsize(task.get("iso_path", "")) if os.path.exists(task.get("iso_path", "")) else 0
        pct = min(100.0, max(0.0, (iso_size / task.get("source_size_bytes", 1)) * 100)) if task.get("source_size_bytes", 0) > 0 else 0.0
        tasks.append({"id": key, "task_name": task.get("task_name", ""), "display_name": build_display_name(task.get("task_name", "")), "server_name": task.get("server_name", ""), "progress": pct, "speed_mbps": 0.0, "eta_text": "-"})

    with AGENT_TASKS_LOCK: agent_snapshot = dict(AGENT_TASKS)
    for filename, task in agent_snapshot.items():
        tasks.append({"id": f"agent:{filename}", "task_name": filename, "display_name": build_display_name(filename), "server_name": task.get("node", "VPS"), "progress": task.get("progress", 0), "speed_mbps": task.get("speed_mbps", 0), "eta_text": task.get("eta_text", "-"), "status": task.get("status", "packing")})
    return jsonify({"active": len(tasks) > 0, "tasks": tasks})

@app.route("/api/pending", methods=["GET"])
def list_pending():
    rows = []
    monitor_tag = get_qb_monitor_tag()
    for server in QBServer.query.all():
        if is_invalid_qb_server_url(server.url): continue
        try:
            client = make_qb_client(server)
            for t in client.torrents_info():
                if has_waiting_tag(getattr(t, "tags", "")):
                    rows.append({"title": t.name, "display_name": build_display_name(t.name), "size_gb": round(getattr(t, "size", 0)/GB, 3), "node_alias": server.name, "added_on": ""})
        except: pass

    with AGENT_PENDING_TASKS_LOCK: pending_snapshot = dict(AGENT_PENDING_TASKS)
    for node_name, payload in pending_snapshot.items():
        for task in payload.get("tasks", []):
            task["display_name"] = build_display_name(task.get("title"))
            rows.append(task)
            
    blocked_names = build_pending_history_block_set()
    return jsonify([r for r in rows if not (build_task_name_keys(r["title"]) & blocked_names)])

@app.route("/api/pending_uploads", methods=["GET"])
def list_pending_uploads():
    rows, seen = [], set()
    agent_policy_map = {r.node_name: normalize_agent_upload_policy(r.upload_policy) for r in AgentNode.query.all()}
    
    for name in get_upload_list_files():
        if name.lower() in seen: continue
        task_name = os.path.splitext(name)[0]
        history_row = PackHistory.query.filter(PackHistory.task_name.in_([name, task_name])).order_by(PackHistory.id.desc()).first()
        node_name = resolve_pack_history_node_name(history_row)
        rows.append({"filename": name, "display_name": build_display_name(task_name), "size_gb": round(os.path.getsize(os.path.join(OUTPUT_DIR, name))/GB, 3), "node": node_name, "node_policy": agent_policy_map.get(node_name, ""), "status": "pending", "auto_upload": bool(get_task_auto_upload(name))})
        seen.add(name.lower())

    for history_row in PackHistory.query.filter(PackHistory.status.in_(["待上传", "待上传 (阻塞中)", "pending_upload"])).order_by(PackHistory.id.desc()).all():
        filename = f"{os.path.splitext(os.path.basename(history_row.task_name))[0]}.iso"
        if filename.lower() in seen: continue
        node_name = resolve_pack_history_node_name(history_row)
        if node_name != "NAS":
            rows.append({"filename": filename, "display_name": build_display_name(filename), "size_gb": round(history_row.file_size_gb or 0, 3), "node": node_name, "node_policy": agent_policy_map.get(node_name, AGENT_UPLOAD_DEFAULT_POLICY), "status": "待上传 (阻塞中)", "auto_upload": True})
            seen.add(filename.lower())
    return jsonify(rows)

@app.route("/api/upload_now/<path:filename>", methods=["POST"])
def upload_now(filename):
    threading.Thread(target=process_single_upload, args=(os.path.basename(filename),), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/upload/toggle_auto/<path:filename>", methods=["POST"])
def toggle_task_auto_upload(filename):
    safe_name = os.path.basename(filename)
    next_state = not bool(get_task_auto_upload(safe_name))
    set_task_auto_upload(safe_name, next_state)
    apply_auto_upload_status_for_task(safe_name, next_state)
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/delete_local/<path:filename>", methods=["POST"])
def delete_local_file(filename):
    safe_name = os.path.basename(filename)
    try: os.remove(os.path.join(OUTPUT_DIR, safe_name))
    except FileNotFoundError: pass
    mark_upload_status(safe_name, "deleted_local", "local file deleted")
    row = find_pack_row_for_upload(safe_name)
    if row:
        row.status = "本地已删除"
        db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/upload/control", methods=["POST"])
def upload_control():
    action = (request.get_json(force=True).get("action") or "").lower()
    if action == "pause": set_upload_command("paused")
    elif action == "abort": set_upload_command("aborted")
    else: set_upload_command("running")
    return jsonify({"ok": True})

@app.route("/api/stats", methods=["GET"])
def get_stats():
    now_dt = now_local().replace(tzinfo=None)
    month_start = datetime(now_dt.year, now_dt.month, 1)
    pack_data = {"nas": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0}, "vps": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0}}
    up_data = {"nas": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0}, "vps": {"count": 0, "size": 0, "dur": 0, "mb": 0, "mc": 0, "msize": 0, "vc": 0}}
    pack_success_statuses = {STATUS_PACKED_PENDING_UPLOAD, STATUS_UPLOADING, STATUS_UPLOADED, "已封装", "待上传", "待上传 (阻塞中)", "成功"}

    for r in PackHistory.query.all():
        size_gb = float(r.file_size_gb or 0.0)
        cat = "vps" if (r.info or "").strip().lower().startswith("node=") else "nas"
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
            if (r.upload_end_time or "") >= month_start.strftime("%Y-%m-%d %H:%M:%S"):
                up_data[cat]["mc"] += 1
                up_data[cat]["msize"] += size_gb
            ust, uet = parse_db_time(r.upload_start_time), parse_db_time(r.upload_end_time)
            if ust and uet:
                delta = (uet - ust).total_seconds()
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
            res[f"{cat}_avg_duration_text"] = format_seconds(int(d[cat]["dur"] / d[cat]["vc"])) if d[cat]["vc"] > 0 else "0s"
            res[f"{cat}_avg_rate_text"] = f"{(d[cat]['mb'] / d[cat]['dur']):.0f} MB/s" if d[cat]["dur"] > 0 else "0 MB/s"
        res["total_count"] = d["nas"]["count"] + d["vps"]["count"]
        res["total_size_text"] = format_data_size(d["nas"]["size"] + d["vps"]["size"])
        res["month_count"] = d["nas"]["mc"] + d["vps"]["mc"]
        res["month_size_text"] = format_data_size(d["nas"]["msize"] + d["vps"]["msize"])
        tot_vc, tot_dur, tot_mb = d["nas"]["vc"] + d["vps"]["vc"], d["nas"]["dur"] + d["vps"]["dur"], d["nas"]["mb"] + d["vps"]["mb"]
        res["avg_duration_text"] = format_seconds(int(tot_dur / tot_vc)) if tot_vc > 0 else "0s"
        res["avg_rate_text"] = f"{(tot_mb / tot_dur):.0f} MB/s" if tot_dur > 0 else "0 MB/s"
        return res

    return jsonify({"pack": compile_stats(pack_data), "upload": compile_stats(up_data), "current_uploading_file": current_uploading_file, "current_upload_status": get_current_upload_status_snapshot(), "upload_progress": get_upload_progress_snapshot()})

with app.app_context():
    os.makedirs("/data", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_schema()
    if not get_setting("auth_username"): set_setting("auth_username", DEFAULT_ADMIN_USERNAME)
    if not get_setting("auth_password"): set_setting("auth_password", DEFAULT_ADMIN_PASSWORD)
    db.session.commit()

scheduler.add_job(func=process_all_qbs, trigger="interval", minutes=PACK_POLL_INTERVAL_MINUTES, id="pack_poll_job", max_instances=1, coalesce=True, replace_existing=True)
scheduler.add_job(func=process_uploads, trigger=build_cron_trigger(get_upload_cron())[0], id="upload_job", max_instances=1, coalesce=True, replace_existing=True)
if not scheduler.running: scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)