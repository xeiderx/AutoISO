
import atexit
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
from sqlalchemy import func, text

APP_VERSION = "v0.6.6"

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
PACK_POLL_INTERVAL_MINUTES = int(os.getenv("PACK_POLL_INTERVAL_MINUTES", "2"))
LOG_FILE = os.getenv("LOG_FILE", "/data/autoiso.log")

STATUS_PROCESSING = "Processing"
STATUS_PACKED_PENDING_UPLOAD = "封装完成，待上传"
STATUS_UPLOADING = "上传中"
STATUS_UPLOADED = "已上传至网盘"
STATUS_FAILED = "Failed"
PACKING_SUFFIX = ".packing"
UPLOADING_SUFFIX = ".uploading"

ACTIVE_TASKS = {}
ACTIVE_TASKS_LOCK = threading.Lock()
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

        # Backward-compatible migration requirement for legacy schema.
        for sql in (
            "ALTER TABLE tasks ADD COLUMN upload_start_time TEXT",
            "ALTER TABLE tasks ADD COLUMN upload_end_time TEXT",
        ):
            try:
                conn.execute(text(sql))
            except Exception:
                pass


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


def get_delete_after_upload():
    value = (get_setting("delete_after_upload") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


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
    row = UploadHistory.query.filter_by(task_id=task_id).first() if task_id else None
    if not row:
        # upload_history.filename has a unique constraint in legacy schema.
        # If same filename is repacked, keep upload_history as "latest filename state"
        # while pack_history status updates stay strictly id-based.
        row = UploadHistory.query.filter_by(filename=filename).first()
    if not row:
        row = UploadHistory(filename=filename, task_id=task_id, status=status, message=message or "")
        db.session.add(row)
    else:
        if task_id is not None:
            row.task_id = task_id
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

    # 外部文件补录，确保历史页面可见
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


def format_data_size(gb_value):
    if gb_value >= 1024:
        return f"{gb_value / 1024:.2f} TB"
    return f"{gb_value:.2f} GB"


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
    proxy_url = get_setting("tg_proxy").strip()
    return send_tg_notification_with_config(text_msg, token, chat_id, proxy_url)


def send_tg_notification_with_config(text_msg, token, chat_id, proxy_url=""):
    if not token or not chat_id:
        return False, "tg_token 或 tg_chat_id 未配置"

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


def has_waiting_tag(tags_str):
    monitor_tag = get_qb_monitor_tag()
    tags = [tag.strip() for tag in (tags_str or "").split(",") if tag.strip()]
    return monitor_tag in tags


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
            logger.info("开始检查待上传队列，待处理=%s，目标路径=%s，模式=%s", len(pending_names), clouddrive_path, "上传后删除" if delete_after_upload else "上传后保留")

            for file_name in pending_names:
                src_path = os.path.join(OUTPUT_DIR, file_name)
                dst_path = os.path.join(clouddrive_path, file_name)
                task_name = os.path.splitext(file_name)[0]
                row = find_pack_row_for_upload(file_name)
                task_id = row.id if row else None

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
                        send_tg_notification(f"[AutoISO]  开始转移至网盘：{file_name}")

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
                    if get_notify_flag("notify_upload_end", True):
                        send_tg_notification(f"[AutoISO] 🎉 转移完成：{file_name} 已送达网盘挂载目录 (等待后台同步至云端)。")
                    logger.info("上传成功，任务=%s，源=%s，目标=%s", task_name, src_path, dst_path)
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
                    send_tg_notification(f"[AutoISO]  开始转移至网盘：{safe_name}")

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
                    send_tg_notification(f"[AutoISO] 🎉 转移完成：{safe_name} 已送达网盘挂载目录 (等待后台同步至云端)。")
                logger.info("手动上传成功: %s", safe_name)
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
    existing = PackHistory.query.filter_by(
        task_name=torrent.name,
        qb_server_id=server.id,
        status=STATUS_PROCESSING,
    ).first()
    if existing:
        return

    source_path = resolve_source_path(torrent)
    source_size_bytes = get_path_size_bytes(source_path)

    history = PackHistory(
        task_name=torrent.name,
        qb_server_id=server.id,
        status=STATUS_PROCESSING,
        start_time=now_local(),
        file_size_gb=round(source_size_bytes / GB, 3),
    )
    db.session.add(history)
    db.session.commit()

    started = now_local()
    torrent_hash = getattr(torrent, "hash", "")
    task_key = f"{server.id}:{torrent_hash or torrent.name}"
    with ACTIVE_TASKS_LOCK:
        ACTIVE_TASKS[task_key] = {
            "task_name": torrent.name,
            "server_name": server.name,
            "source_path": source_path,
            "source_size_bytes": source_size_bytes,
            "iso_path": os.path.join(OUTPUT_DIR, f"{torrent.name}.iso{PACKING_SUFFIX}"),
            "start_time": started,
            "task_id": history.id,
        }

    try:
        if os.path.isfile(source_path):
            output_file = os.path.join(OUTPUT_DIR, os.path.basename(source_path))
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
                    send_tg_notification(
                        f"[AutoISO] 单文件转存失败 | 节点: {server.name} | 任务: {torrent.name} | 原因: {exc}"
                    )
                logger.exception("单文件任务处理失败，[节点:%s] 任务=%s", server.name, torrent.name)
                return
            finished = now_local()
            duration = format_seconds((finished - started).total_seconds())
            history.status = STATUS_PACKED_PENDING_UPLOAD
            history.end_time = finished
            history.message = f"FILE: {output_file}"
            history.info = "single file copied to output"
            history.file_size_gb = round((os.path.getsize(output_file) / GB), 3) if os.path.isfile(output_file) else history.file_size_gb
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True):
                send_tg_notification(
                    f"[AutoISO] 单文件转存成功 | 节点: {server.name} | 任务: {torrent.name} | 耗时: {duration} | 输出: {output_file}"
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
                send_tg_notification(f"[AutoISO] 封装失败 | 节点: {server.name} | 任务: {torrent.name} | 原因: {history.message}")
            logger.error("封装失败：源目录不存在，任务=%s，路径=%s", torrent.name, source_path)
            return

        safe_vol_id = re.sub(r"[^a-zA-Z0-9_]", "_", torrent.name or "AUTOISO")[:30]
        ok, iso_path, out, err = pack_to_iso(torrent.name, source_path, safe_vol_id)
        finished = now_local()
        duration = format_seconds((finished - started).total_seconds())

        if ok:
            history.status = STATUS_PACKED_PENDING_UPLOAD
            history.end_time = finished
            history.message = f"ISO: {iso_path}"
            history.info = ""
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            if get_notify_flag("notify_pack_end", True):
                send_tg_notification(f"[AutoISO] 封装成功 | 节点: {server.name} | 任务: {torrent.name} | 耗时: {duration} | 输出: {iso_path}")
            logger.info("封装成功，[节点:%s] 任务=%s，耗时=%s", server.name, torrent.name, duration)
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
                send_tg_notification(f"[AutoISO] 封装失败 | 节点: {server.name} | 任务: {torrent.name} | 耗时: {duration} | 原因: {history.message}")
            logger.error("封装失败，[节点:%s] 任务=%s，耗时=%s，原因=%s", server.name, torrent.name, duration, history.message)
    finally:
        with ACTIVE_TASKS_LOCK:
            ACTIVE_TASKS.pop(task_key, None)

def process_all_qbs():
    with app.app_context():
        servers = QBServer.query.all()
        monitor_tag = get_qb_monitor_tag()
        logger.info("开始轮询 qB 节点，监控标签=%s", monitor_tag)
        for server in servers:
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
                logger.info("[节点:%s] 扫描 qB 任务完成，发现 %s 个种子", server.name, len(torrents))
            except Exception as exc:
                logger.exception("qB 节点连接失败，节点=%s", server.name)
                send_tg_notification(f"[AutoISO] 节点连接失败 | 节点: {server.name} | 错误: {exc}")
                continue

            for torrent in torrents:
                if float(getattr(torrent, "progress", 0)) != 1.0:
                    continue
                if not has_waiting_tag(getattr(torrent, "tags", "")):
                    continue

                torrent_hash = getattr(torrent, "hash", "")
                try:
                    qb_remove_tags(client, torrent_hash, [monitor_tag])
                    qb_add_tags(client, torrent_hash, [PACKING_TAG])
                    if get_notify_flag("notify_pack_start", True):
                        send_tg_notification(f"[AutoISO] 开始封装 | 节点: {server.name} | 任务: {torrent.name}")
                    logger.info("开始封装任务，[节点:%s] 任务=%s", server.name, getattr(torrent, "name", "unknown"))
                    process_one_torrent(server, client, torrent)
                except Exception as exc:
                    logger.exception("任务处理异常，[节点:%s] 任务=%s", server.name, getattr(torrent, "name", "unknown"))
                    qb_remove_tags(client, torrent_hash, [PACKING_TAG, monitor_tag])
                    qb_add_tags(client, torrent_hash, [FAILED_TAG])
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
                    send_tg_notification(f"[AutoISO] 任务处理异常 | 节点: {server.name} | 任务: {getattr(torrent, 'name', 'unknown')} | 错误: {exc}")

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
    db.session.delete(item)
    db.session.commit()
    return jsonify({"ok": True})

@app.route("/api/settings/telegram", methods=["GET"])
def get_telegram_settings():
    return jsonify(
        {
            "tg_token": get_setting("tg_token"),
            "tg_chat_id": get_setting("tg_chat_id"),
            "tg_proxy": get_setting("tg_proxy"),
        }
    )


@app.route("/api/settings/telegram", methods=["POST"])
def save_telegram_settings():
    payload = request.get_json(force=True) or {}
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()
    tg_proxy = (payload.get("tg_proxy") or "").strip()
    if not tg_token or not tg_chat_id:
        return jsonify({"error": "tg_token 和 tg_chat_id 不能为空"}), 400
    set_setting("tg_token", tg_token)
    set_setting("tg_chat_id", tg_chat_id)
    set_setting("tg_proxy", tg_proxy)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/telegram/test", methods=["POST"])
def test_telegram_settings():
    payload = request.get_json(force=True) or {}
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()
    tg_proxy = (payload.get("tg_proxy") or "").strip()
    ok, msg = send_tg_notification_with_config("TG 通知测试成功！", tg_token, tg_chat_id, tg_proxy)
    if not ok:
        return jsonify({"error": msg}), 400
    return jsonify({"ok": True})


@app.route("/api/settings/telegram", methods=["DELETE"])
def clear_telegram_settings():
    delete_setting("tg_token")
    delete_setting("tg_chat_id")
    delete_setting("tg_proxy")
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
        }
    )


@app.route("/api/settings/system", methods=["POST"])
@app.route("/api/config/update", methods=["POST"])
def save_system_settings():
    payload = request.get_json(force=True) or {}
    auth_username = (payload.get("auth_username") or "").strip()
    auth_password = (payload.get("auth_password") or "").strip()
    auth_password_confirm = (payload.get("auth_password_confirm") or "").strip()
    upload_cron_expr = (payload.get("upload_cron") or "").strip()
    clouddrive_path = (payload.get("clouddrive_path") or "").strip()
    qb_monitor_tag = (payload.get("qb_monitor_tag") or "").strip()
    delete_after_upload = parse_bool(payload.get("delete_after_upload"), default=False)
    notify_pack_start = parse_bool(payload.get("notify_pack_start"), default=get_notify_flag("notify_pack_start", True))
    notify_pack_end = parse_bool(payload.get("notify_pack_end"), default=get_notify_flag("notify_pack_end", True))
    notify_upload_start = parse_bool(
        payload.get("notify_upload_start"), default=get_notify_flag("notify_upload_start", True)
    )
    notify_upload_end = parse_bool(payload.get("notify_upload_end"), default=get_notify_flag("notify_upload_end", True))

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
        }
    )


@app.route("/api/settings/auth", methods=["POST"])
def save_auth_settings():
    payload = request.get_json(force=True)
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
                "qb_server_id": row.qb_server_id,
                "qb_server_name": row.qb_server.name if row.qb_server else "",
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
    return jsonify({"active": len(tasks) > 0, "tasks": tasks})


@app.route("/api/pending", methods=["GET"])
def list_pending():
    rows = []
    monitor_tag = get_qb_monitor_tag()
    logger.info("拉取待封装列表，监控标签=%s", monitor_tag)
    servers = QBServer.query.order_by(QBServer.id.asc()).all()
    for server in servers:
        try:
            client = make_qb_client(server)
            torrents = client.torrents_info()
        except Exception:
            logger.exception("获取待封装任务失败，节点=%s", server.name)
            continue

        for torrent in torrents:
            if not has_waiting_tag(getattr(torrent, "tags", "")):
                continue

            size_bytes = int(getattr(torrent, "size", 0) or getattr(torrent, "total_size", 0) or 0)
            added_on = getattr(torrent, "added_on", 0) or 0
            try:
                added_dt = datetime.fromtimestamp(int(added_on), TZ).strftime("%Y-%m-%d %H:%M") if added_on else ""
            except Exception:
                added_dt = ""

            rows.append(
                {
                    "title": getattr(torrent, "name", ""),
                    "size_gb": round(size_bytes / GB, 3),
                    "node_alias": server.name,
                    "added_on": added_dt,
                }
            )

    rows.sort(key=lambda x: x.get("added_on", ""), reverse=True)
    return jsonify(rows)


@app.route("/api/pending_uploads", methods=["GET"])
def list_pending_uploads():
    rows = []
    names = get_upload_list_files()
    for name in names:
        file_path = os.path.join(OUTPUT_DIR, name)
        try:
            size_bytes = os.path.getsize(file_path)
        except OSError:
            size_bytes = 0
        size_gb = round(size_bytes / GB, 3)

        upload_row = UploadHistory.query.filter_by(filename=name).first()
        upload_status = (upload_row.status if upload_row else "").strip().lower()
        status_text = "uploaded" if upload_status == "uploaded" else "pending"

        task_name = os.path.splitext(name)[0]
        history_row = (
            PackHistory.query.filter(PackHistory.task_name.in_([name, task_name]))
            .order_by(PackHistory.id.desc())
            .first()
        )
        node_name = history_row.qb_server.name if history_row and history_row.qb_server else "NAS"

        rows.append(
            {
                "filename": name,
                "size": size_gb,
                "size_gb": size_gb,
                "node": node_name,
                "status": status_text,
            }
        )
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
    total_count = db.session.query(func.count(PackHistory.id)).scalar() or 0
    total_size_gb = (
        db.session.query(func.coalesce(func.sum(PackHistory.file_size_gb), 0.0))
        .filter(PackHistory.file_size_gb.isnot(None))
        .scalar()
        or 0.0
    )

    now_dt = now_local().replace(tzinfo=None)
    month_start = datetime(now_dt.year, now_dt.month, 1)
    month_start_text = month_start.strftime("%Y-%m-%d %H:%M:%S")

    month_count = db.session.query(func.count(PackHistory.id)).filter(PackHistory.start_time >= month_start).scalar() or 0
    month_size_gb = (
        db.session.query(func.coalesce(func.sum(PackHistory.file_size_gb), 0.0))
        .filter(PackHistory.start_time >= month_start)
        .scalar()
        or 0.0
    )

    pack_success_statuses = {STATUS_PACKED_PENDING_UPLOAD, STATUS_UPLOADING, STATUS_UPLOADED}
    finished_pack_rows = PackHistory.query.filter(
        PackHistory.start_time.isnot(None),
        PackHistory.end_time.isnot(None),
        PackHistory.status.in_(pack_success_statuses),
    ).all()
    pack_total_seconds = 0.0
    pack_total_mb = 0.0
    valid_pack_count = 0
    for row in finished_pack_rows:
        delta = (row.end_time - row.start_time).total_seconds()
        if delta <= 0:
            continue
        valid_pack_count += 1
        pack_total_seconds += delta
        pack_total_mb += float((row.file_size_gb or 0.0) * 1024.0)

    pack_avg_seconds = int(pack_total_seconds / valid_pack_count) if valid_pack_count > 0 else 0
    try:
        pack_avg_rate_mbps = (pack_total_mb / pack_total_seconds) if pack_total_seconds > 0 else 0.0
    except ZeroDivisionError:
        pack_avg_rate_mbps = 0.0

    uploaded_rows = PackHistory.query.filter(
        PackHistory.status == STATUS_UPLOADED,
        PackHistory.upload_start_time.isnot(None),
        PackHistory.upload_start_time != "",
        PackHistory.upload_end_time.isnot(None),
        PackHistory.upload_end_time != "",
    ).all()

    total_upload_count = len(uploaded_rows)
    total_upload_size_gb = sum(float(r.file_size_gb or 0.0) for r in uploaded_rows)
    month_upload_rows = [r for r in uploaded_rows if (r.upload_end_time or "") >= month_start_text]
    month_upload_count = len(month_upload_rows)
    month_upload_size_gb = sum(float(r.file_size_gb or 0.0) for r in month_upload_rows)

    upload_total_seconds = 0.0
    upload_total_mb = 0.0
    valid_upload_count = 0
    for row in uploaded_rows:
        start_dt = parse_db_time(row.upload_start_time)
        end_dt = parse_db_time(row.upload_end_time)
        if not start_dt or not end_dt:
            continue
        delta = (end_dt - start_dt).total_seconds()
        if delta <= 0:
            continue
        valid_upload_count += 1
        upload_total_seconds += delta
        upload_total_mb += float((row.file_size_gb or 0.0) * 1024.0)

    upload_avg_seconds = int(upload_total_seconds / valid_upload_count) if valid_upload_count > 0 else 0
    try:
        upload_avg_rate_mbps = (upload_total_mb / upload_total_seconds) if upload_total_seconds > 0 else 0.0
    except ZeroDivisionError:
        upload_avg_rate_mbps = 0.0

    return jsonify(
        {
            "pack": {
                "total_count": int(total_count or 0),
                "total_size_gb": round(float(total_size_gb), 2),
                "total_size_text": format_data_size(float(total_size_gb)),
                "avg_duration_seconds": pack_avg_seconds,
                "avg_duration_text": format_seconds(pack_avg_seconds),
                "avg_rate_mbps": round(float(pack_avg_rate_mbps), 2),
                "avg_rate_text": f"{pack_avg_rate_mbps:.0f} MB/s",
                "month_count": int(month_count or 0),
                "month_size_gb": round(float(month_size_gb), 2),
                "month_size_text": format_data_size(float(month_size_gb)),
            },
            "upload": {
                "total_count": int(total_upload_count or 0),
                "total_size_gb": round(float(total_upload_size_gb), 2),
                "total_size_text": format_data_size(float(total_upload_size_gb)),
                "avg_duration_seconds": upload_avg_seconds,
                "avg_duration_text": format_seconds(upload_avg_seconds),
                "avg_rate_mbps": round(float(upload_avg_rate_mbps), 2),
                "avg_rate_text": f"{upload_avg_rate_mbps:.0f} MB/s",
                "month_count": int(month_upload_count or 0),
                "month_size_gb": round(float(month_upload_size_gb), 2),
                "month_size_text": format_data_size(float(month_upload_size_gb)),
            },
            "current_uploading_file": current_uploading_file,
            "current_upload_status": get_current_upload_status_snapshot(),
            "upload_progress": get_upload_progress_snapshot(),
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
    # 启动时清理僵尸上传状态，避免历史记录卡死
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
