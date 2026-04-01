import os
import threading
from pathlib import Path
from flask import Flask, send_from_directory, jsonify

import synopticom_nps_watch_MB_codegen_based_NO_FSTRING_HTML_v10_READY as kiosk

app = Flask(__name__, static_folder='public', static_url_path='/public')
BASE_DIR = Path(__file__).parent
PUBLIC_DIR = BASE_DIR / 'public'
_worker_lock = threading.Lock()
_worker_started = False


def _worker_loop() -> None:
    kiosk.log('[WEB] background worker started')
    try:
        kiosk.main()
    except Exception as e:
        kiosk.log(f'[WEB] background worker stopped: {e}')


def ensure_worker_started() -> None:
    global _worker_started
    if os.getenv('RUN_WORKER', '1').strip() != '1':
        kiosk.log('[WEB] RUN_WORKER != 1, background worker disabled')
        return
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_worker_loop, name='synopticom-worker', daemon=True)
        t.start()
        _worker_started = True


@app.before_request
def _startup() -> None:
    ensure_worker_started()


@app.get('/')
def index():
    index_path = PUBLIC_DIR / 'index.html'
    if not index_path.exists():
        return (
            '<h1>Kiosk puslapis dar ruošiamas</h1>'
            '<p>Palauk kelias sekundes ir perkrauk puslapį.</p>',
            503,
        )
    return send_from_directory(PUBLIC_DIR, 'index.html')


@app.get('/data.json')
def data_json():
    data_path = PUBLIC_DIR / 'data.json'
    if not data_path.exists():
        return jsonify({'status': 'preparing', 'message': 'Duomenys dar ruošiami'}), 503
    return send_from_directory(PUBLIC_DIR, 'data.json', mimetype='application/json')


@app.get('/healthz')
def healthz():
    return jsonify({'ok': True, 'worker_started': _worker_started})


if __name__ == '__main__':
    ensure_worker_started()
    port = int(os.environ.get('PORT', '10000'))
    app.run(host='0.0.0.0', port=port, debug=False)
