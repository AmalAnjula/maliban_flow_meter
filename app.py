"""
Oil Tank Monitor - Flask App
Requirements: pip install flask paho-mqtt

MQTT Broker : localhost:1883
MQTT Topic  : ols/tx/status
Payload     : {"ts": 1774372427, "weight": 48.0, "infeed": false, "outfeed": false}
DB          : oil_tank.db (SQLite, auto-created)

Session Logic:
  infeed  false → true  : infeed  STARTED   → INSERT new row  (status='active')
  infeed  true  → false : infeed  COMPLETE  → UPDATE row      (status='complete', end_weight=weight)
  outfeed false → true  : outfeed STARTED   → INSERT new row  (status='active')
  outfeed true  → false : outfeed COMPLETE  → UPDATE row      (status='complete', end_weight=weight)
  infeed/outfeed active  : UPDATE current_weight only  (no new row)
"""

from flask import Flask, render_template, jsonify, request, Response
import sqlite3, json, threading, io, csv, os, time
import paho.mqtt.client as mqtt
from datetime import datetime

app = Flask(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
DB_PATH     = os.environ.get('OIL_DB',       'oil_tank.db')
MQTT_BROKER = os.environ.get('MQTT_BROKER',  'localhost')
MQTT_PORT   = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC  = 'ols/tx/infeed_event'

# ── Shared live state ──────────────────────────────────────────────────────────
_state = {
    'infeed':  None,   # None = unknown (first message not yet received)
    'outfeed': None,
    'weight':  0.0,
    'ts':      None,
    'status':  'idle',   # idle | infeed | outfeed
    'mqtt_ok': False,
}
_lock = threading.Lock()


# ── Database ───────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS oil_sessions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                type           TEXT    NOT NULL,                  -- 'infeed' | 'outfeed'
                status         TEXT    NOT NULL DEFAULT 'active', -- 'active' | 'complete'
                start_dt       TEXT    NOT NULL,
                end_dt         TEXT,
                start_weight   REAL    NOT NULL,
                end_weight     REAL,
                current_weight REAL    NOT NULL
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_type   ON oil_sessions(type)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON oil_sessions(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sdt    ON oil_sessions(start_dt)')
        conn.commit()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── MQTT processing ────────────────────────────────────────────────────────────
def _process_message(payload_str: str):
    """
    Detect state transitions and manage DB sessions accordingly.
    Only inserts a new row on START; updates in-place during active & on COMPLETE.
    """
    try:
        p       = json.loads(payload_str)
        ts      = int(p.get('ts', time.time()))
        weight  = float(p.get('weight', 0))
        infeed  = bool(p.get('infeed',  False))
        outfeed = bool(p.get('outfeed', False))
        dt_str  = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        with _lock:
            prev_infeed  = _state['infeed']
            prev_outfeed = _state['outfeed']
            _state.update(
                weight=weight, ts=ts, infeed=infeed, outfeed=outfeed,
                status='infeed' if infeed else ('outfeed' if outfeed else 'idle')
            )

        print(f'[MQTT] {payload_str}')

        with get_db() as conn:
            # ── INFEED transitions ─────────────────────────────────────────
            if prev_infeed is False and infeed is True:
                # false → true : STARTED
                print(f'[DB] Infeed STARTED  weight={weight}')
                conn.execute(
                    "INSERT INTO oil_sessions (type, status, start_dt, start_weight, current_weight)"
                    " VALUES ('infeed', 'active', ?, ?, ?)",
                    (dt_str, weight, weight)
                )
            elif prev_infeed is True and infeed is False:
                # true → false : COMPLETE
                print(f'[DB] Infeed COMPLETE weight={weight}')
                conn.execute(
                    "UPDATE oil_sessions SET status='complete', end_dt=?, end_weight=?, current_weight=?"
                    " WHERE type='infeed' AND status='active'",
                    (dt_str, weight, weight)
                )
            elif infeed is True:
                # still active → live weight update only (no new row)
                conn.execute(
                    "UPDATE oil_sessions SET current_weight=?"
                    " WHERE type='infeed' AND status='active'",
                    (weight,)
                )

            # ── OUTFEED transitions ────────────────────────────────────────
            if prev_outfeed is False and outfeed is True:
                # false → true : STARTED
                print(f'[DB] Outfeed STARTED  weight={weight}')
                conn.execute(
                    "INSERT INTO oil_sessions (type, status, start_dt, start_weight, current_weight)"
                    " VALUES ('outfeed', 'active', ?, ?, ?)",
                    (dt_str, weight, weight)
                )
            elif prev_outfeed is True and outfeed is False:
                # true → false : COMPLETE
                print(f'[DB] Outfeed COMPLETE weight={weight}')
                conn.execute(
                    "UPDATE oil_sessions SET status='complete', end_dt=?, end_weight=?, current_weight=?"
                    " WHERE type='outfeed' AND status='active'",
                    (dt_str, weight, weight)
                )
            elif outfeed is True:
                # still active → live weight update
                conn.execute(
                    "UPDATE oil_sessions SET current_weight=?"
                    " WHERE type='outfeed' AND status='active'",
                    (weight,)
                )

            conn.commit()

    except Exception as exc:
        print(f'[MQTT] msg error: {exc}')


def _mqtt_thread():
    client = mqtt.Client()

    def on_connect(c, *_):
        _state['mqtt_ok'] = True
        print(f'[MQTT] connected → subscribed to {MQTT_TOPIC}')
        c.subscribe(MQTT_TOPIC)

    def on_disconnect(*_):
        _state['mqtt_ok'] = False
        print('[MQTT] disconnected')

    def on_message(c, u, msg):
        _process_message(msg.payload.decode('utf-8', errors='replace'))

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as exc:
            _state['mqtt_ok'] = False
            print(f'[MQTT] connection failed ({exc}), retry in 5s…')
            time.sleep(5)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _parse_filter(dt_local: str):
    """Convert datetime-local string → SQL-safe datetime string."""
    if not dt_local:
        return None
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(dt_local, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    return None


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/data')
def api_data():
    dt_str = _parse_filter(request.args.get('start_datetime', ''))

    with get_db() as conn:
        # --- Completed infeed sessions (after optional filter) ---
        where  = "WHERE type='infeed' AND status='complete'"
        params = []
        if dt_str:
            where  += ' AND start_dt >= ?'
            params.append(dt_str)

        row = conn.execute(
            f"SELECT COUNT(*) cnt, COALESCE(SUM(end_weight),0) total,"
            f" COALESCE(AVG(end_weight),0) avg FROM oil_sessions {where}",
            params
        ).fetchone()

        # --- Active infeed session (for live display) ---
        active_infeed = conn.execute(
            "SELECT current_weight, start_dt, start_weight FROM oil_sessions"
            " WHERE type='infeed' AND status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()

        # --- Active outfeed session (for live display) ---
        active_outfeed = conn.execute(
            "SELECT current_weight, start_dt, start_weight FROM oil_sessions"
            " WHERE type='outfeed' AND status='active' ORDER BY id DESC LIMIT 1"
        ).fetchone()

        # --- Date range for datetime picker ---
        rng = conn.execute(
            "SELECT MIN(start_dt) mn, MAX(start_dt) mx FROM oil_sessions"
        ).fetchone()

    with _lock:
        live = dict(status=_state['status'], weight=_state['weight'],
                    mqtt_ok=_state['mqtt_ok'])

    result = {
        'total_oil_in':      round(float(row['total'] or 0), 2),
        'count':             int(row['cnt'] or 0),
        'average_per_entry': round(float(row['avg']   or 0), 2),
        'min_datetime':      rng['mn'],
        'max_datetime':      rng['mx'],
        'active_infeed':     None,
        'active_outfeed':    None,
        **live,
    }

    if active_infeed:
        result['active_infeed'] = {
            'current_weight': round(float(active_infeed['current_weight']), 2),
            'start_dt':       active_infeed['start_dt'],
            'start_weight':   round(float(active_infeed['start_weight']), 2),
        }

    if active_outfeed:
        result['active_outfeed'] = {
            'current_weight': round(float(active_outfeed['current_weight']), 2),
            'start_dt':       active_outfeed['start_dt'],
            'start_weight':   round(float(active_outfeed['start_weight']), 2),
        }

    return jsonify(result)


@app.route('/api/status')
def api_status():
    with _lock:
        return jsonify(dict(_state))


@app.route('/api/download')
def api_download():
    dt_str = _parse_filter(request.args.get('start_datetime', ''))

    sql    = ("SELECT type, status, start_dt, end_dt,"
              " start_weight, end_weight, current_weight FROM oil_sessions")
    params = []
    if dt_str:
        sql   += ' WHERE start_dt >= ?'
        params = [dt_str]
    sql += ' ORDER BY start_dt'

    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(['Type', 'Status', 'Start DateTime', 'End DateTime',
                'Start Weight (L)', 'End Weight (L)', 'Current Weight (L)'])
    w.writerows(rows)
    buf.seek(0)

    fname = f'oil_sessions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    return Response(buf.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename={fname}'})


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    init_db()
    #threading.Thread(target=_mqtt_thread, daemon=True).start()
    app.run(host='0.0.0.0', port=5003, debug=True, use_reloader=False)