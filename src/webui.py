#!/usr/bin/env python3

import os
from flask import Flask, g, render_template_string, send_from_directory, abort, url_for
from .storage import PRODUCTS_DIR, init_db, list_products, get_product

app = Flask(__name__)

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
  <h1>Products</h1>
  <table>
    <thead>
      <tr>
        <th>ID</th><th>Asset Tag</th><th>Pickup</th><th>Qty</th>
        <th>Serial</th><th>Commodity</th><th>Destination</th><th>Short Description</th>
      </tr>
    </thead>
    <tbody>
    {% for p in products %}
      <tr>
        <td>{{ p["id"] }}</td>
        <td><a href="{{ url_for('product_detail', asset_tag=p['asset_tag']) }}">{{ p["asset_tag"] }}</a></td>
        <td>{{ p["pickup"] or "" }}</td>
        <td>{{ p["quantity"] }}</td>
        <td>{{ p["serial_number"] or "" }}</td>
        <td>{{ p["commodity"] or "" }}</td>
        <td>{{ p["destination"] or "" }}</td>
        <td>{{ p["short_description"] or "" }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
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
  <p><a href="{{ url_for('index') }}">&larr; Back</a></p>
  <h1>Product {{ p["asset_tag"] }}</h1>
  <div class="meta">
    <div><strong>Pickup:</strong> {{ p["pickup"] or "" }}</div>
    <div><strong>Quantity:</strong> {{ p["quantity"] }}</div>
    <div><strong>Serial:</strong> {{ p["serial_number"] or "" }}</div>
    <div><strong>Commodity:</strong> {{ p["commodity"] or "" }}</div>
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
</body>
</html>
"""

@app.route("/")
def index():
    products = list_products(limit=500)
    return render_template_string(LIST_TMPL, products=products)

@app.route("/product/<asset_tag>")
def product_detail(asset_tag):
    p = get_product(asset_tag)
    if not p:
        abort(404)
    photos = [x for x in (p.get("photos") or "").split(";") if x]
    return render_template_string(DETAIL_TMPL, p=p, photos=photos)

@app.route("/files/<asset_tag>/<filename>")
def serve_file(asset_tag, filename):
    safe_dir = os.path.abspath(os.path.join(PRODUCTS_DIR, asset_tag))
    if not safe_dir.startswith(os.path.abspath(PRODUCTS_DIR)):
        abort(403)
    return send_from_directory(safe_dir, filename)

def main():
    init_db() # creates products table if it doesn't already exist
    app.run(host="0.0.0.0", port=9090, debug=False)

if __name__ == "__main__":
    main()


