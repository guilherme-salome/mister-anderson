#!/usr/bin/env python3

import logging
import os
import time
import json
import threading
import queue
import sqlite3
import functools

from flask import Flask, g, render_template_string, send_from_directory, abort, url_for, Response, request, redirect, session

from .config import read_basic_users, read_token
from .storage import PRODUCTS_DIR, DB_PATH, init_db, list_products, get_product



logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = Flask(__name__)

try:
    app.secret_key = os.environ.get("WEBUI_SECRET") or read_token('mister-anderson-webui', 'secret')
except Exception:
    app.secret_key = os.urandom(32).hex()  # fallback (ephemeral)

WEB_USERS = read_basic_users()


def login_required(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login", next=request.url))
        return fn(*args, **kwargs)
    return wrapper


LOGIN_TMPL = """
<!doctype html><meta charset="utf-8"><title>Login</title>
<style>body{font-family:sans-serif;margin:2rem;max-width:420px}</style>
<h1>Login</h1>
{% if error %}<p style="color:#b00">{{error}}</p>{% endif %}
<form method="post">
  <label>User<br><input name="username" required></label><br><br>
  <label>Password<br><input name="password" type="password" required></label><br><br>
  <button type="submit">Sign in</button>
</form>
"""

LIST_TMPL = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Products</title>
  <style>
    body { font-family: sans-serif; margin: 2rem; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; }
    th { background: #f4f4f4; text-align: left; }
    a { text-decoration: none; color: #0645ad; }
  </style>
</head>
<body>
  <div style="display:flex;justify-content:space-between;align-items:center;">
    <h1>Products</h1>
    <div>
      <span>{{ session.get('user') }}</span>
      <a href="{{ url_for('logout') }}" style="margin-left:12px;">Logout</a>
    </div>
  </div>
  <table>
    <thead>
      <tr>
        <th>ID</th><th>Asset Tag</th><th>Pickup</th><th>Qty</th>
        <th>Serial</th><th>Subcategory</th><th>Destination</th><th>Short Description</th>
      </tr>
    </thead>
    <tbody id="rows">
    {% for p in products %}
      <tr>
        <td>{{ p["id"] }}</td>
        <td><a href="{{ url_for('product_detail', asset_tag=p['asset_tag']) }}">{{ p["asset_tag"] }}</a></td>
        <td>{{ p["pickup"] or "" }}</td>
        <td>{{ p["quantity"] }}</td>
        <td>{{ p["serial_number"] or "" }}</td>
        <td>{{ p["subcategory"] or "" }}</td>
        <td>{{ p["destination"] or "" }}</td>
        <td>{{ p["short_description"] or "" }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>

  <script>
    // Auto-refresh table rows when products change
    const es = new EventSource("/events");
    es.addEventListener("products", async (evt) => {
      try {
        const res = await fetch("/table-rows");
        const html = await res.text();
        const tbody = document.getElementById("rows");
        tbody.innerHTML = html;
      } catch (e) {
        console.error("Failed to refresh rows:", e);
      }
    });
    // Optional: log keep-alives
    es.onerror = (e) => console.error("SSE error", e);
  </script>
</body>
</html>
"""

DETAIL_TMPL = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Product {{ p["asset_tag"] }}</title>
  <style>
    body { font-family: sans-serif; margin: 2rem; }
    .meta { margin-bottom: 1rem; }
    .grid { display: flex; flex-wrap: wrap; gap: 12px; }
    .grid img { max-height: 200px; border: 1px solid #ddd; padding: 4px; background: #fff; }
    a { text-decoration: none; color: #0645ad; }
  </style>
</head>
<body>
  <div style="display:flex;justify-content:space-between;align-items:center;">
    <p><a href="{{ url_for('index') }}">&larr; Back</a></p>
    <div>
      <span>{{ session.get('user') }}</span>
      <a href="{{ url_for('logout') }}" style="margin-left:12px;">Logout</a>
    </div>
  </div>
  <h1>Product {{ p["asset_tag"] }}</h1>
  <div class="meta">
    <div><strong>Pickup:</strong> {{ p["pickup"] or "" }}</div>
    <div><strong>Quantity:</strong> {{ p["quantity"] }}</div>
    <div><strong>Serial:</strong> {{ p["serial_number"] or "" }}</div>
    <div><strong>Subcategory:</strong> {{ p["subcategory"] or "" }}</div>
    <div><strong>Destination:</strong> {{ p["destination"] or "" }}</div>
    <div><strong>Short Description:</strong> {{ p["short_description"] or "" }}</div>
    <div><strong>Created By:</strong> {{ p["created_by"] }}</div>
    <div><strong>Created At:</strong> {{ p["created_at"] }}</div>
    <div><strong>Raw JSON:</strong></div>
    <pre style="white-space: pre-wrap">{{ p["description_raw"] or "" }}</pre>
  </div>
  <h2>Images</h2>
  <div class="grid">
    {% for rel in photos %}
      <a href="{{ url_for('serve_file', asset_tag=rel.split('/')[0], filename=rel.split('/')[1]) }}" target="_blank">
        <img src="{{ url_for('serve_file', asset_tag=rel.split('/')[0], filename=rel.split('/')[1]) }}" />
      </a>
    {% endfor %}
  </div>

  <script>
    // Optional: if this product gets updated (same asset tag), reload page
    const currentTag = "{{ p['asset_tag'] }}";
    const es = new EventSource("/events");
    es.addEventListener("products", (evt) => {
      try {
        const payload = JSON.parse(evt.data || "{}");
        const updated = payload.new || [];
        if (updated.includes(currentTag)) {
          location.reload();
        }
      } catch(e) {}
    });
  </script>
</body>
</html>
"""

ROWS_TMPL = """
{% for p in products %}
  <tr>
    <td>{{ p["id"] }}</td>
    <td><a href="{{ url_for('product_detail', asset_tag=p['asset_tag']) }}">{{ p["asset_tag"] }}</a></td>
    <td>{{ p["pickup"] or "" }}</td>
    <td>{{ p["quantity"] }}</td>
    <td>{{ p["serial_number"] or "" }}</td>
    <td>{{ p["subcategory"] or "" }}</td>
    <td>{{ p["destination"] or "" }}</td>
    <td>{{ p["short_description"] or "" }}</td>
  </tr>
{% endfor %}
"""
class Notifier:
    def __init__(self):
        self.subs = set()
        self.lock = threading.Lock()

    def subscribe(self):
        q = queue.Queue()
        with self.lock:
            self.subs.add(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            self.subs.discard(q)

    def publish(self, payload: dict):
        with self.lock:
            for q in list(self.subs):
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    pass

notifier = Notifier()

def _db_stats_since(last_id: int):
    logger.info("Querying DB for Updates...")
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        # New rows since last_id
        cur.execute("SELECT id, asset_tag FROM products WHERE id > ? ORDER BY id ASC", (last_id,))
        new_rows = cur.fetchall()
        # Total count (optional, useful if you want to display counts)
        cur.execute("SELECT COUNT(1), COALESCE(MAX(id), 0) FROM products")
        count, max_id = cur.fetchone()
        return new_rows, count or 0, max_id or 0
    finally:
        con.close()

def _watch_db(interval=1.0):
    # Initialize last_seen from DB
    _, _, last_seen = _db_stats_since(0)
    last_keepalive = time.time()
    while True:
        time.sleep(interval)
        try:
            new_rows, count, max_id = _db_stats_since(last_seen)
            if new_rows:
                logger.info(f"{len(new_rows)} new rows found in DB.")
                last_seen = max_id
                payload = {
                    "new": [r[1] for r in new_rows],  # asset_tags
                    "count": count,
                    "max_id": max_id,
                }
                logger.info(f"Payload: {repr(payload)}")
                notifier.publish(payload)
            # Keep-alive every 15s to prevent idle proxies from closing
            if time.time() - last_keepalive > 15:
                logger.info("Pinging.")
                notifier.publish({"ping": True})
                last_keepalive = time.time()
        except Exception:
            # Fail-safe: don't kill the thread on transient errors
            pass

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username","")
        p = request.form.get("password","")
        if WEB_USERS.get(u) == p:
            session["user"] = u
            return redirect(request.args.get("next") or url_for("index"))
        return render_template_string(LOGIN_TMPL, error="Invalid credentials.")
    return render_template_string(LOGIN_TMPL, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_required
def index():
    products = list_products(limit=500)
    return render_template_string(LIST_TMPL, products=products)

@app.route("/events")
@login_required
def events():
    q = notifier.subscribe()
    def stream():
        try:
            while True:
                try:
                    data = q.get(timeout=20)
                    if "ping" in data:
                        # SSE comment as keep-alive
                        yield ": keep-alive\n\n"
                    else:
                        yield f"event: products\ndata: {json.dumps(data)}\n\n"
                except queue.Empty:
                    # periodic keep-alive
                    yield ": keep-alive\n\n"
        finally:
            notifier.unsubscribe(q)
    return Response(stream(), mimetype="text/event-stream")

@app.route("/table-rows")
@login_required
def table_rows():
    products = list_products(limit=500)
    return render_template_string(ROWS_TMPL, products=products)

@app.route("/product/<asset_tag>")
@login_required
def product_detail(asset_tag):
    p = get_product(asset_tag)
    if not p:
        abort(404)
    photos = [x for x in (p.get("photos") or "").split(";") if x]
    return render_template_string(DETAIL_TMPL, p=p, photos=photos)

@app.route("/files/<asset_tag>/<filename>")
@login_required
def serve_file(asset_tag, filename):
    safe_dir = os.path.abspath(os.path.join(PRODUCTS_DIR, asset_tag))
    if not safe_dir.startswith(os.path.abspath(PRODUCTS_DIR)):
        abort(403)
    return send_from_directory(safe_dir, filename)

def main():
    init_db()
    # Start background DB watcher
    threading.Thread(target=_watch_db, daemon=True).start()
    # threaded=True so SSE can stream while other requests are served
    app.run(host="0.0.0.0", port=9090, debug=False, threaded=True)

if __name__ == "__main__":
    main()
