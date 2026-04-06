#!/usr/bin/env python3
"""
remote_receiver.py  —  Run this on the RPi5
============================================
Listens on port 5001 for production log rows pushed by the main OLS machine.
Stores them in its own SQLite database and keeps only the last 6 months of data.

Start:
    python3 remote_receiver.py

Or as a systemd service — see the comment at the bottom of this file.

Config (environment variables):
    RECEIVER_PORT   TCP port to listen on         default: 5001
    RECEIVER_DB     Path to SQLite database file   default: ./logs/production_remote.db
"""

import os, json, sqlite3, logging
from datetime import datetime
from flask import Flask, request, jsonify

# ── Config ─────────────────────────────────────────────────────────
PORT    = int(os.getenv("RECEIVER_PORT", "5002"))
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
DB_PATH = os.getenv("RECEIVER_DB", os.path.join(LOG_DIR, "production_remote.db"))

os.makedirs(LOG_DIR, exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("receiver")

# ── Database ────────────────────────────────────────────────────────
def _init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS production_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id       INTEGER,          -- original id from the sender machine
                timestamp       TEXT NOT NULL,
                product         TEXT,
                initial_weight  REAL,
                required_weight REAL,
                final_weight    REAL,
                status          TEXT,
                reason          TEXT,
                received_at     TEXT NOT NULL      -- when this RPi5 received the row
            )
        """)
        # Index speeds up the 6-month purge query
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp
            ON production_log (timestamp)
        """)
        con.commit()
    log.info("DB initialised at %s", DB_PATH)

_init_db()

def _purge_old_records(con):
    """Delete rows older than 6 months. Called on every sync batch."""
    cur = con.execute(
        "DELETE FROM production_log WHERE timestamp < datetime('now', '-6 months')"
    )
    if cur.rowcount:
        log.info("Purged %d records older than 6 months.", cur.rowcount)

# ── Flask app ───────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/api/sync", methods=["POST"])
def receive_sync():
    """
    Accepts a JSON array of production_log rows from the sender machine.
    Inserts them and purges anything older than 6 months.
    Returns 200 on success so the sender marks those rows as synced.
    """
    try:
        records = request.get_json(force=True)
        if not isinstance(records, list):
            return jsonify({"error": "expected a JSON array"}), 400

        received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with sqlite3.connect(DB_PATH) as con:
            inserted = 0
            for r in records:
                # Skip duplicates (same source_id already stored)
                exists = con.execute(
                    "SELECT 1 FROM production_log WHERE source_id = ?",
                    (r.get("id"),)
                ).fetchone()
                if exists:
                    continue

                con.execute(
                    """INSERT INTO production_log
                       (source_id, timestamp, product,
                        initial_weight, required_weight,
                        final_weight, status, reason, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        r.get("id"),
                        r.get("timestamp"),
                        r.get("product"),
                        r.get("initial_weight"),
                        r.get("required_weight"),
                        r.get("final_weight"),
                        r.get("status"),
                        r.get("reason"),
                        received_at,
                    ),
                )
                inserted += 1

            _purge_old_records(con)
            con.commit()

        log.info("Received %d rows, inserted %d (skipped %d duplicates).",
                 len(records), inserted, len(records) - inserted)
        return jsonify({"ok": True, "inserted": inserted}), 200

    except Exception as exc:
        log.error("Error processing sync: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/status", methods=["GET"])
def status():
    """Quick health-check — returns row count and DB size."""
    try:
        with sqlite3.connect(DB_PATH) as con:
            count = con.execute("SELECT COUNT(*) FROM production_log").fetchone()[0]
            oldest = con.execute(
                "SELECT MIN(timestamp) FROM production_log"
            ).fetchone()[0]
            newest = con.execute(
                "SELECT MAX(timestamp) FROM production_log"
            ).fetchone()[0]
        size_mb = round(os.path.getsize(DB_PATH) / 1024 / 1024, 2)
        return jsonify({
            "ok":       True,
            "rows":     count,
            "oldest":   oldest,
            "newest":   newest,
            "db_mb":    size_mb,
            "db_path":  DB_PATH,
        }), 200
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 55)
    print("  OLS Remote DB Receiver")
    print(f"  Listening on  http://0.0.0.0:{PORT}")
    print(f"  Database      {DB_PATH}")
    print(f"  Retention     6 months")
    print("=" * 55)
    # use_reloader=False so it doesn't fight with the DB thread
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ══════════════════════════════════════════════════════════════════
#  SYSTEMD SERVICE  (optional — save as /etc/systemd/system/ols-receiver.service)
# ══════════════════════════════════════════════════════════════════
# [Unit]
# Description=OLS Remote DB Receiver
# After=network.target
#
# [Service]
# ExecStart=/usr/bin/python3 /home/pi/remote_receiver.py
# WorkingDirectory=/home/pi
# Restart=always
# RestartSec=5
# Environment=REMOTE_RPI_IP=192.168.1.100
#
# [Install]
# WantedBy=multi-user.target