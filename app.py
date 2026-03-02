import atexit
import os
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from functools import wraps

import qbittorrentapi
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, text

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "isoarr-v2-secret-key")
app.permanent_session_lifetime = timedelta(days=7)

app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{os.getenv('DB_PATH', '/data/autoiso.db')}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

TZ = timezone(timedelta(hours=8))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
WAITING_TAG = os.getenv("QB_WAITING_TAG", "待封装")
DONE_TAG = os.getenv("QB_DONE_TAG", "已封装")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

ACTIVE_TASKS = {}
ACTIVE_TASKS_LOCK = threading.Lock()
GB = 1024 ** 3


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

    qb_server = db.relationship("QBServer", backref=db.backref("histories", lazy=True))


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), nullable=False, unique=True)
    value = db.Column(db.Text, nullable=False)


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


def send_telegram_message(text_msg):
    token = get_setting("tg_token")
    chat_id = get_setting("tg_chat_id")
    if not token or not chat_id:
        return False, "tg_token 或 tg_chat_id 未配置"

    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(api_url, json={"chat_id": chat_id, "text": text_msg}, timeout=15)
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


def update_torrent_tags(client, torrent_hash):
    if not torrent_hash:
        return
    try:
        client.torrents_remove_tags(tags=WAITING_TAG, torrent_hashes=torrent_hash)
    except TypeError:
        client.torrents_remove_tags(tags=WAITING_TAG, hashes=torrent_hash)

    try:
        client.torrents_add_tags(tags=DONE_TAG, torrent_hashes=torrent_hash)
    except TypeError:
        client.torrents_add_tags(tags=DONE_TAG, hashes=torrent_hash)


def resolve_source_path(torrent):
    content_path = getattr(torrent, "content_path", None)
    if content_path:
        return content_path
    save_path = getattr(torrent, "save_path", "")
    name = getattr(torrent, "name", "")
    return os.path.join(save_path, name)


def pack_to_iso(task_name, source_path):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    iso_path = os.path.join(OUTPUT_DIR, f"{task_name}.iso")
    cmd = [
        "xorriso",
        "-as",
        "mkisofs",
        "-udf",
        "-V",
        task_name,
        "-o",
        iso_path,
        source_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
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
    task_key = f"{server.id}:{getattr(torrent, 'hash', torrent.name)}"
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
        ok, iso_path, out, err = pack_to_iso(torrent.name, source_path)
        finished = now_local()
        duration = format_seconds((finished - started).total_seconds())

        if ok:
            history.status = "Success"
            history.end_time = finished
            history.message = f"ISO: {iso_path}"
            db.session.commit()

            update_torrent_tags(client, getattr(torrent, "hash", ""))

            send_telegram_message(
                "[IsoArr V2] 封装成功\n"
                f"节点: {server.name}\n"
                f"任务: {torrent.name}\n"
                f"耗时: {duration}\n"
                f"输出: {iso_path}"
            )
        else:
            history.status = "Failed"
            history.end_time = finished
            history.message = (err or out or "unknown error")[:3000]
            db.session.commit()

            send_telegram_message(
                "[IsoArr V2] 封装失败\n"
                f"节点: {server.name}\n"
                f"任务: {torrent.name}\n"
                f"耗时: {duration}\n"
                f"错误: {history.message}"
            )
    finally:
        with ACTIVE_TASKS_LOCK:
            ACTIVE_TASKS.pop(task_key, None)


def poll_and_pack():
    with app.app_context():
        servers = QBServer.query.all()
        for server in servers:
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
            except Exception as exc:
                send_telegram_message(f"[IsoArr V2] 节点连接失败\n节点: {server.name}\n错误: {exc}")
                continue

            for torrent in torrents:
                if float(getattr(torrent, "progress", 0)) != 1.0:
                    continue
                if not has_waiting_tag(getattr(torrent, "tags", "")):
                    continue

                try:
                    process_one_torrent(server, client, torrent)
                except Exception as exc:
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
                    send_telegram_message(
                        "[IsoArr V2] 任务处理异常\n"
                        f"节点: {server.name}\n"
                        f"任务: {getattr(torrent, 'name', 'unknown')}\n"
                        f"错误: {exc}"
                    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if session.get("logged_in"):
            return redirect(url_for("index"))
        return render_template("login.html", error="")

    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        session["logged_in"] = True
        session.permanent = True
        return redirect(url_for("index"))

    return render_template("login.html", error="账号或密码错误")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@require_login
def index():
    return render_template("index.html")


@app.route("/api/qbservers", methods=["GET"])
def list_qbservers():
    items = QBServer.query.order_by(QBServer.id.desc()).all()
    return jsonify(
        [
            {
                "id": row.id,
                "name": row.name,
                "url": row.url,
                "username": row.username,
            }
            for row in items
        ]
    )


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
        }
    )


@app.route("/api/settings/telegram", methods=["POST"])
def save_telegram_settings():
    payload = request.get_json(force=True)
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()

    if not tg_token or not tg_chat_id:
        return jsonify({"error": "tg_token 和 tg_chat_id 不能为空"}), 400

    set_setting("tg_token", tg_token)
    set_setting("tg_chat_id", tg_chat_id)
    db.session.commit()

    ok, message = send_telegram_message("[IsoArr V2] Telegram 配置已保存，测试消息发送成功。")
    if not ok:
        return jsonify({"error": f"已保存，但测试消息发送失败: {message}"}), 400

    return jsonify({"ok": True})


@app.route("/api/settings/telegram", methods=["DELETE"])
def clear_telegram_settings():
    delete_setting("tg_token")
    delete_setting("tg_chat_id")
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/history", methods=["GET"])
def list_history():
    limit = max(1, min(request.args.get("limit", default=50, type=int), 200))
    rows = PackHistory.query.order_by(PackHistory.id.desc()).limit(limit).all()

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
        }
    )


scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
scheduler.add_job(
    func=poll_and_pack,
    trigger="interval",
    seconds=POLL_SECONDS,
    id="poll_pack_job",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
)

with app.app_context():
    os.makedirs("/data", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ensure_schema()

if not scheduler.running:
    scheduler.start()

atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
