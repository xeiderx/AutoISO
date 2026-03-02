import atexit
import os
import subprocess
from datetime import datetime, timezone, timedelta

import qbittorrentapi
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{os.getenv('DB_PATH', '/data/autoiso.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

TZ = timezone(timedelta(hours=8))
POLL_SECONDS = 60
WAITING_TAG = "待封装"
DONE_TAG = "已封装"
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")


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
    message = db.Column(db.Text, nullable=True)

    qb_server = db.relationship("QBServer", backref=db.backref("histories", lazy=True))


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), nullable=False, unique=True)
    value = db.Column(db.Text, nullable=False)


def now_local():
    return datetime.now(TZ)


def get_setting(key):
    item = SystemSetting.query.filter_by(key=key).first()
    return item.value if item else ""


def set_setting(key, value):
    item = SystemSetting.query.filter_by(key=key).first()
    if item:
        item.value = value
    else:
        item = SystemSetting(key=key, value=value)
        db.session.add(item)


def format_seconds(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def send_telegram_message(text):
    token = get_setting("tg_token")
    chat_id = get_setting("tg_chat_id")
    if not token or not chat_id:
        return False, "tg_token 或 tg_chat_id 未配置"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            return False, str(data)
        return True, "ok"
    except Exception as e:
        return False, str(e)


def make_qb_client(server: QBServer):
    client = qbittorrentapi.Client(
        host=server.url,
        username=server.username,
        password=server.password,
        REQUESTS_ARGS={"timeout": 20},
    )
    client.auth_log_in()
    return client


def torrent_has_waiting_tag(tags_str):
    tags = [t.strip() for t in (tags_str or "").split(",") if t.strip()]
    return WAITING_TAG in tags


def update_torrent_tags(client, torrent_hash):
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
    out_file = os.path.join(OUTPUT_DIR, f"{task_name}.iso")
    cmd = [
        "xorriso",
        "-as",
        "mkisofs",
        "-udf",
        "-V",
        task_name,
        "-o",
        out_file,
        source_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode == 0, out_file, proc.stdout, proc.stderr


def process_one_torrent(server: QBServer, client, torrent):
    existing = PackHistory.query.filter_by(
        task_name=torrent.name,
        qb_server_id=server.id,
        status="Processing",
    ).first()
    if existing:
        return

    history = PackHistory(
        task_name=torrent.name,
        qb_server_id=server.id,
        status="Processing",
        start_time=now_local(),
    )
    db.session.add(history)
    db.session.commit()

    source_path = resolve_source_path(torrent)
    started = now_local()
    ok, iso_path, out, err = pack_to_iso(torrent.name, source_path)
    finished = now_local()
    duration = format_seconds((finished - started).total_seconds())

    if ok:
        history.status = "Success"
        history.end_time = finished
        history.message = f"ISO: {iso_path}"
        update_torrent_tags(client, torrent.hash)
        db.session.commit()

        send_telegram_message(
            f"[AutoISO] 封装成功\n"
            f"节点: {server.name}\n"
            f"任务: {torrent.name}\n"
            f"耗时: {duration}\n"
            f"输出: {iso_path}"
        )
    else:
        history.status = "Failed"
        history.end_time = finished
        history.message = (err or out or "unknown error")[:2000]
        db.session.commit()

        send_telegram_message(
            f"[AutoISO] 封装失败\n"
            f"节点: {server.name}\n"
            f"任务: {torrent.name}\n"
            f"耗时: {duration}\n"
            f"错误: {history.message}"
        )


def poll_and_pack():
    with app.app_context():
        servers = QBServer.query.all()
        for server in servers:
            try:
                client = make_qb_client(server)
                torrents = client.torrents_info()
            except Exception as e:
                send_telegram_message(f"[AutoISO] 节点连接失败\n节点: {server.name}\n错误: {e}")
                continue

            for torrent in torrents:
                if float(getattr(torrent, "progress", 0)) != 1.0:
                    continue
                if not torrent_has_waiting_tag(getattr(torrent, "tags", "")):
                    continue

                try:
                    process_one_torrent(server, client, torrent)
                except Exception as e:
                    fail = PackHistory(
                        task_name=getattr(torrent, "name", "unknown"),
                        qb_server_id=server.id,
                        status="Failed",
                        start_time=now_local(),
                        end_time=now_local(),
                        message=f"Unhandled error: {e}",
                    )
                    db.session.add(fail)
                    db.session.commit()
                    send_telegram_message(
                        f"[AutoISO] 任务处理异常\n节点: {server.name}\n任务: {getattr(torrent, 'name', 'unknown')}\n错误: {e}"
                    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/qbservers", methods=["GET"])
def list_qbservers():
    data = [
        {
            "id": s.id,
            "name": s.name,
            "url": s.url,
            "username": s.username,
        }
        for s in QBServer.query.order_by(QBServer.id.desc()).all()
    ]
    return jsonify(data)


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

    item = QBServer(name=name, url=url, username=username, password=password)
    db.session.add(item)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/qbservers/<int:server_id>", methods=["DELETE"])
def delete_qbserver(server_id):
    item = QBServer.query.get(server_id)
    if not item:
        return jsonify({"error": "节点不存在"}), 404

    db.session.delete(item)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/settings/telegram", methods=["GET"])
def get_tg_settings():
    return jsonify(
        {
            "tg_token": get_setting("tg_token"),
            "tg_chat_id": get_setting("tg_chat_id"),
        }
    )


@app.route("/api/settings/telegram", methods=["POST"])
def save_tg_settings():
    payload = request.get_json(force=True)
    tg_token = (payload.get("tg_token") or "").strip()
    tg_chat_id = (payload.get("tg_chat_id") or "").strip()

    if not tg_token or not tg_chat_id:
        return jsonify({"error": "tg_token 和 tg_chat_id 不能为空"}), 400

    set_setting("tg_token", tg_token)
    set_setting("tg_chat_id", tg_chat_id)
    db.session.commit()

    ok, msg = send_telegram_message("[AutoISO] Telegram 配置已保存，测试消息发送成功。")
    if not ok:
        return jsonify({"error": f"已保存，但测试消息发送失败: {msg}"}), 400

    return jsonify({"ok": True})


@app.route("/api/history", methods=["GET"])
def list_history():
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(200, limit))

    rows = (
        PackHistory.query.order_by(PackHistory.id.desc())
        .limit(limit)
        .all()
    )

    return jsonify(
        [
            {
                "id": x.id,
                "task_name": x.task_name,
                "qb_server_id": x.qb_server_id,
                "qb_server_name": x.qb_server.name if x.qb_server else "",
                "status": x.status,
                "start_time": x.start_time.strftime("%Y-%m-%d %H:%M:%S") if x.start_time else "",
                "end_time": x.end_time.strftime("%Y-%m-%d %H:%M:%S") if x.end_time else "",
                "message": x.message or "",
            }
            for x in rows
        ]
    )


scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
scheduler.add_job(
    func=poll_and_pack,
    trigger="interval",
    seconds=POLL_SECONDS,
    id="poll_job",
    max_instances=1,
    coalesce=True,
    replace_existing=True,
)


with app.app_context():
    os.makedirs("/data", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db.create_all()

if not scheduler.running:
    scheduler.start()

atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
