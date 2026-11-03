import os
import secrets
import threading
from pathlib import Path
from functools import wraps
from flask import Flask, send_from_directory, jsonify, request, session, redirect, url_for

import synopticom_nps_watch_MB_codegen_based_NO_FSTRING_HTML_v10_READY as kiosk

app = Flask(__name__, static_folder='public', static_url_path='/public')
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))

ADMIN_USER = os.environ.get('KIOSK_USER', '')
ADMIN_PASS = os.environ.get('KIOSK_PASS', '')

KA_USER = os.environ.get('KA_USER', '')
KA_PASS = os.environ.get('KA_PASS', '')

BASE_DIR = Path(__file__).parent
PUBLIC_DIR = BASE_DIR / 'public'
_worker_lock = threading.Lock()
_worker_started = False


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def ka_login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('ka_logged_in'):
            return redirect(url_for('ka_login'))
        return f(*args, **kwargs)
    return decorated


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


LOGIN_PAGE_TEMPLATE = '''<!DOCTYPE html>
<html lang="lt">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Prisijungimas – {title}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #1a1a2e; display: flex; align-items: center; justify-content: center;
         min-height: 100vh; color: #e0e0e0; }}
  .login-box {{ background: #16213e; padding: 40px; border-radius: 12px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.4); width: 100%; max-width: 380px; }}
  .login-box h1 {{ text-align: center; margin-bottom: 30px; font-size: 1.5rem; color: #fff; }}
  .login-box input {{ width: 100%; padding: 12px 16px; margin-bottom: 16px; border: 1px solid #2a3a5c;
                      border-radius: 8px; background: #0f3460; color: #fff; font-size: 1rem;
                      outline: none; transition: border-color 0.2s; }}
  .login-box input:focus {{ border-color: #e94560; }}
  .login-box button {{ width: 100%; padding: 12px; background: #e94560; color: #fff; border: none;
                       border-radius: 8px; font-size: 1rem; cursor: pointer; transition: background 0.2s; }}
  .login-box button:hover {{ background: #c73652; }}
  .error {{ color: #e94560; text-align: center; margin-bottom: 16px; font-size: 0.9rem; }}
</style>
</head>
<body>
<div class="login-box">
  <h1>{title}</h1>
  {error}
  <form method="POST">
    <input type="text" name="username" placeholder="Vartotojo vardas" autocomplete="username" required autofocus>
    <input type="password" name="password" placeholder="Slaptažodis" autocomplete="current-password" required>
    <button type="submit">Prisijungti</button>
  </form>
</div>
</body>
</html>'''


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = ''
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if ADMIN_USER and ADMIN_PASS and username == ADMIN_USER and password == ADMIN_PASS:
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = 'Neteisingas prisijungimo vardas arba slaptažodis'
    return LOGIN_PAGE_TEMPLATE.format(
        title='Kioskelis',
        error=f'<p class="error">{error}</p>' if error else '',
    )


@app.route('/ka/login', methods=['GET', 'POST'])
def ka_login():
    error = ''
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if KA_USER and KA_PASS and username == KA_USER and password == KA_PASS:
            session['ka_logged_in'] = True
            return redirect(url_for('ka_index'))
        error = 'Neteisingas prisijungimo vardas arba slaptažodis'
    return LOGIN_PAGE_TEMPLATE.format(
        title='KA Kioskelis',
        error=f'<p class="error">{error}</p>' if error else '',
    )


@app.get('/')
@login_required
def index():
    index_path = PUBLIC_DIR / 'index.html'
    if not index_path.exists():
        return (
            '<h1>Kiosk puslapis dar ruošiamas</h1>'
            '<p>Palauk kelias sekundes ir perkrauk puslapį.</p>',
            503,
        )
    return send_from_directory(PUBLIC_DIR, 'index.html')


@app.get('/ka')
@ka_login_required
def ka_index():
    ka_path = PUBLIC_DIR / 'ka.html'
    if not ka_path.exists():
        return (
            '<h1>KA puslapis dar ruošiamas</h1>'
            '<p>Palauk kelias sekundes ir perkrauk puslapį.</p>',
            503,
        )
    return send_from_directory(PUBLIC_DIR, 'ka.html')


@app.get('/ka/atstovai')
@ka_login_required
def ka_atstovai():
    path = PUBLIC_DIR / 'ka_atstovai.html'
    if not path.exists():
        return (
            '<h1>KA atstovai puslapis dar ruošiamas</h1>'
            '<p>Palauk kelias sekundes ir perkrauk puslapį.</p>',
            503,
        )
    return send_from_directory(PUBLIC_DIR, 'ka_atstovai.html')


@app.get('/data.json')
@login_required
def data_json():
    data_path = PUBLIC_DIR / 'data.json'
    if not data_path.exists():
        return jsonify({'status': 'preparing', 'message': 'Duomenys dar ruošiami'}), 503
    return send_from_directory(PUBLIC_DIR, 'data.json', mimetype='application/json')


@app.get('/ka/data.json')
@ka_login_required
def ka_data_json():
    data_path = PUBLIC_DIR / 'ka_data.json'
    if not data_path.exists():
        return jsonify({'status': 'preparing', 'message': 'Duomenys dar ruošiami'}), 503
    return send_from_directory(PUBLIC_DIR, 'ka_data.json', mimetype='application/json')


@app.get('/healthz')
def healthz():
    return jsonify({'ok': True, 'worker_started': _worker_started})


@app.get('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.get('/ka/logout')
def ka_logout():
    session.pop('ka_logged_in', None)
    return redirect(url_for('ka_login'))


if __name__ == '__main__':
    ensure_worker_started()
    port = int(os.environ.get('PORT', '10000'))
    app.run(host='0.0.0.0', port=port, debug=False)
