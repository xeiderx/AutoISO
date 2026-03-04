
import atexit
import logging
import os
import re
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from functools import wraps

import qbittorrentapi
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, text

APP_VERSION = "v0.3.3"

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
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")
DEFAULT_ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
DEFAULT_CRON_SCHEDULE = os.getenv("DEFAULT_CRON_SCHEDULE", "* * * * *")
LOG_FILE = os.getenv("LOG_FILE", "/data/autoiso.log")

ACTIVE_TASKS = {}
ACTIVE_TASKS_LOCK = threading.Lock()
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

    qb_server = db.relationship("QBServer", backref=db.backref("histories", lazy=True))


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), nullable=False, unique=True)
    value = db.Column(db.Text, nullable=False)


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


def get_cron_schedule():
    return get_setting("cron_schedule") or DEFAULT_CRON_SCHEDULE


def build_cron_trigger(cron_expr):
    expr = (cron_expr or "").strip() or DEFAULT_CRON_SCHEDULE
    try:
        return CronTrigger.from_crontab(expr), expr
    except Exception:
        logger.exception("invalid cron expression: %s, fallback to default", expr)
        fallback = DEFAULT_CRON_SCHEDULE
        return CronTrigger.from_crontab(fallback), fallback


def apply_scheduler_cron(cron_expr):
    trigger, normalized = build_cron_trigger(cron_expr)
    set_setting("cron_schedule", normalized)
    db.session.commit()

    job = scheduler.get_job("pack_job")
    if job:
        scheduler.reschedule_job("pack_job", trigger=trigger)
    else:
        scheduler.add_job(
            func=process_all_qbs,
            trigger=trigger,
            id="pack_job",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
    logger.info("scheduler cron applied: %s", normalized)
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
    tags = [tag.strip() for tag in (tags_str or "").split(",") if tag.strip()]
    return WAITING_TAG in tags


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

    if os.path.exists(iso_path):
        os.remove(iso_path)

    cmd = [
        "genisoimage",
        "-udf",
        "-iso-level",
        "3",
        "-allow-limited-size",
        "-V",
        vol_id,
        "-o",
        iso_path,
        source_path,
    ]
    logger.info("genisoimage command start, task=%s, source=%s, iso=%s", task_name, source_path, iso_path)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    logger.info("genisoimage command finished, task=%s, returncode=%s", task_name, proc.returncode)
    if proc.returncode != 0 and proc.stderr:
        logger.error("genisoimage full stderr, task=%s:\n%s", task_name, proc.stderr)
    return proc.returncode == 0, iso_path, proc.stdout, proc.stderr


def process_one_torrent(server: QBServer, client, torrent):
    existing = PackHistory.query.filter_by(
        task_name=torrent.name,
        qb_server_id=server.id,
        status="Processing",
    ).first()
    if existing:
        return

    source_path = resolve_source_path(torrent)
    source_size_bytes = get_path_size_bytes(source_path)

    history = PackHistory(
        task_name=torrent.name,
        qb_server_id=server.id,
        status="Processing",
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
            "iso_path": os.path.join(OUTPUT_DIR, f"{torrent.name}.iso"),
            "start_time": started,
        }

    try:
        if not os.path.isdir(source_path):
            history.status = "Failed"
            history.end_time = now_local()
            history.message = f"源路径不是文件夹: {source_path}"
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            send_tg_notification(
                "[AutoISO] 封装失败\n"
                f"节点: {server.name}\n"
                f"任务: {torrent.name}\n"
                f"错误: {history.message}"
            )
            logger.error("task failed, reason=source not directory, task=%s, path=%s", torrent.name, source_path)
            return

        safe_vol_id = re.sub(r"[^a-zA-Z0-9_]", "_", torrent.name or "AUTOISO")[:30]
        ok, iso_path, out, err = pack_to_iso(torrent.name, source_path, safe_vol_id)
        finished = now_local()
        duration = format_seconds((finished - started).total_seconds())

        if ok:
            history.status = "Success"
            history.end_time = finished
            history.message = f"ISO: {iso_path}"
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [DONE_TAG])
            send_tg_notification(
                "[AutoISO] 封装成功\n"
                f"节点: {server.name}\n"
                f"任务: {torrent.name}\n"
                f"耗时: {duration}\n"
                f"输出: {iso_path}"
            )
            logger.info("task success, node=%s, task=%s, duration=%s", server.name, torrent.name, duration)
        else:
            history.status = "Failed"
            history.end_time = finished
            history.message = extract_error_tail(err, out)
            history.info = (err or "").strip()
            if history.info:
                logger.error("genisoimage full stderr, node=%s, task=%s:\n%s", server.name, torrent.name, history.info)
            db.session.commit()
            qb_remove_tags(client, torrent_hash, [PACKING_TAG])
            qb_add_tags(client, torrent_hash, [FAILED_TAG])
            send_tg_notification(
                "[AutoISO] 封装失败\n"
                f"节点: {server.name}\n"
                f"任务: {torrent.name}\n"
                f"耗时: {duration}\n"
                f"错误: {history.message}"
            )
            logger.error(
                "task failed, node=%s, task=%s, duration=%s, err=%s",
                server.name,
                torrent.name,
                duration,
                history.message,
            )
    finally:
        with ACTIVE_TASKS_LOCK:
            ACTIVE_TASKS.pop(task_key, None)

def process_all_qbs():
    with app.app_context():
        servers = QBServer.query.all()
        for server in servers:
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
                logger.info("poll node=%s, torrents=%s", server.name, len(torrents))
            except Exception as exc:
                logger.exception("qb connect failed, node=%s", server.name)
                send_tg_notification(f"[AutoISO] 节点连接失败\n节点: {server.name}\n错误: {exc}")
                continue

            for torrent in torrents:
                if float(getattr(torrent, "progress", 0)) != 1.0:
                    continue
                if not has_waiting_tag(getattr(torrent, "tags", "")):
                    continue

                torrent_hash = getattr(torrent, "hash", "")
                try:
                    qb_remove_tags(client, torrent_hash, [WAITING_TAG])
                    qb_add_tags(client, torrent_hash, [PACKING_TAG])
                    send_tg_notification(
                        "[AutoISO] 开始封装\n"
                        f"节点: {server.name}\n"
                        f"任务: {torrent.name}"
                    )
                    logger.info("task start, node=%s, task=%s", server.name, getattr(torrent, "name", "unknown"))
                    process_one_torrent(server, client, torrent)
                except Exception as exc:
                    logger.exception("task process exception, node=%s, task=%s", server.name, getattr(torrent, "name", "unknown"))
                    qb_remove_tags(client, torrent_hash, [PACKING_TAG, WAITING_TAG])
                    qb_add_tags(client, torrent_hash, [FAILED_TAG])
                    failed_record = PackHistory(
                        task_name=getattr(torrent, "name", "unknown"),
                        qb_server_id=server.id,
                        status="Failed",
                        start_time=now_local(),
                        end_time=now_local(),
                        message=f"Unhandled error: {exc}",
                    )
                    db.session.add(failed_record)
                    db.session.commit()
                    send_tg_notification(
                        "[AutoISO] 任务处理异常\n"
                        f"节点: {server.name}\n"
                        f"任务: {getattr(torrent, 'name', 'unknown')}\n"
                        f"错误: {exc}"
                    )


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
    payload = request.get_json(force=True)
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()
    tg_proxy = (payload.get("tg_proxy") or "").strip()
    if not tg_token or not tg_chat_id:
        return jsonify({"error": "tg_token 和 tg_chat_id 不能为空"}), 400
    set_setting("tg_token", tg_token)
    set_setting("tg_chat_id", tg_chat_id)
    set_setting("tg_proxy", tg_proxy)
    db.session.commit()
    ok, msg = send_tg_notification("[AutoISO] Telegram 配置已保存，测试消息发送成功。")
    if not ok:
        return jsonify({"error": f"已保存，但测试消息发送失败: {msg}"}), 400
    return jsonify({"ok": True})


@app.route("/api/settings/telegram", methods=["DELETE"])
def clear_telegram_settings():
    delete_setting("tg_token")
    delete_setting("tg_chat_id")
    delete_setting("tg_proxy")
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/settings/system", methods=["GET"])
def get_system_settings():
    auth_username, _ = get_auth_credentials()
    return jsonify({"auth_username": auth_username, "cron_schedule": get_cron_schedule()})


@app.route("/api/settings/system", methods=["POST"])
def save_system_settings():
    payload = request.get_json(force=True) or {}
    auth_username = (payload.get("auth_username") or "").strip()
    auth_password = (payload.get("auth_password") or "").strip()
    auth_password_confirm = (payload.get("auth_password_confirm") or "").strip()
    cron_expr = (payload.get("cron_schedule") or "").strip()

    if not auth_username:
        return jsonify({"error": "账号不能为空"}), 400

    if cron_expr:
        try:
            CronTrigger.from_crontab(cron_expr)
        except Exception as exc:
            return jsonify({"error": f"Cron 表达式无效: {exc}"}), 400
    else:
        cron_expr = get_cron_schedule()

    if auth_password or auth_password_confirm:
        if not auth_password or not auth_password_confirm:
            return jsonify({"error": "修改密码时必须填写并确认新密码"}), 400
        if auth_password != auth_password_confirm:
            return jsonify({"error": "两次密码输入不一致"}), 400
        set_setting("auth_password", auth_password)

    set_setting("auth_username", auth_username)
    normalized = apply_scheduler_cron(cron_expr)
    return jsonify({"ok": True, "cron_schedule": normalized})


@app.route("/api/settings/auth", methods=["GET"])
def get_auth_settings():
    auth_username, _ = get_auth_credentials()
    return jsonify({"auth_username": auth_username, "cron_schedule": get_cron_schedule()})


@app.route("/api/settings/auth", methods=["POST"])
def save_auth_settings():
    payload = request.get_json(force=True)
    auth_username = (payload.get("auth_username") or "").strip()
    auth_password = (payload.get("auth_password") or "").strip()
    auth_password_confirm = (payload.get("auth_password_confirm") or "").strip()
    cron_expr = (payload.get("cron_schedule") or "").strip() or get_cron_schedule()

    if not auth_username or not auth_password or not auth_password_confirm:
        return jsonify({"error": "账号和密码不能为空"}), 400
    if auth_password != auth_password_confirm:
        return jsonify({"error": "两次密码输入不一致"}), 400

    try:
        CronTrigger.from_crontab(cron_expr)
    except Exception as exc:
        return jsonify({"error": f"Cron 表达式无效: {exc}"}), 400

    set_setting("auth_username", auth_username)
    set_setting("auth_password", auth_password)
    normalized = apply_scheduler_cron(cron_expr)
    return jsonify({"ok": True, "cron_schedule": normalized})


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
        logs = parse_recent_log_entries(hours=24)
        return jsonify({"logs": logs})
    except Exception as exc:
        logger.exception("read logs failed")
        return jsonify({"error": f"读取日志失败: {exc}"}), 500


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
                "elapsed_seconds": int((now_local() - started).total_seconds()) if started else 0,
            }
        )
    return jsonify({"active": len(tasks) > 0, "tasks": tasks})


@app.route("/api/pending", methods=["GET"])
def list_pending():
    rows = []
    servers = QBServer.query.order_by(QBServer.id.asc()).all()
    for server in servers:
        try:
            client = make_qb_client(server)
            torrents = client.torrents_info()
        except Exception:
            logger.exception("pending fetch failed, node=%s", server.name)
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


@app.route("/api/stats", methods=["GET"])
def get_stats():
    total_count = db.session.query(func.count(PackHistory.id)).scalar() or 0
    success_count = db.session.query(func.count(PackHistory.id)).filter(PackHistory.status == "Success").scalar() or 0
    total_size_gb = (
        db.session.query(func.coalesce(func.sum(PackHistory.file_size_gb), 0.0))
        .filter(PackHistory.file_size_gb.isnot(None))
        .scalar()
        or 0.0
    )

    now_dt = now_local().replace(tzinfo=None)
    month_start = datetime(now_dt.year, now_dt.month, 1)
    month_count = db.session.query(func.count(PackHistory.id)).filter(PackHistory.start_time >= month_start).scalar() or 0
    month_size_gb = (
        db.session.query(func.coalesce(func.sum(PackHistory.file_size_gb), 0.0))
        .filter(PackHistory.start_time >= month_start)
        .scalar()
        or 0.0
    )

    finished_rows = PackHistory.query.filter(
        PackHistory.start_time.isnot(None), PackHistory.end_time.isnot(None)
    ).all()
    avg_seconds = 0
    if finished_rows:
        total_seconds = sum((row.end_time - row.start_time).total_seconds() for row in finished_rows)
        avg_seconds = int(total_seconds / len(finished_rows))
    success_rate = (success_count / total_count * 100) if total_count else 0
    return jsonify(
        {
            "total_count": total_count,
            "success_rate": round(success_rate, 2),
            "total_size_gb": round(float(total_size_gb), 2),
            "total_size_text": format_data_size(float(total_size_gb)),
            "avg_duration_seconds": avg_seconds,
            "avg_duration_text": format_seconds(avg_seconds),
            "month_count": month_count,
            "month_size_gb": round(float(month_size_gb), 2),
            "month_size_text": format_data_size(float(month_size_gb)),
        }
    )

with app.app_context():
    os.makedirs("/data", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_schema()

    if not get_setting("cron_schedule"):
        set_setting("cron_schedule", DEFAULT_CRON_SCHEDULE)
        db.session.commit()

    init_cron = get_cron_schedule()
    trigger, normalized = build_cron_trigger(init_cron)
    if normalized != init_cron:
        set_setting("cron_schedule", normalized)
        db.session.commit()

scheduler.add_job(
    func=process_all_qbs,
    trigger=trigger,
    id="pack_job",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
)

if not scheduler.running:
    scheduler.start()

atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
