import os
from synopticom_nps_watch_MB_codegen_based_NO_FSTRING_HTML_v10_READY import render_kiosk_html as good_render_kiosk_html
import json
import time
import hashlib
import re
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

SYN_USER = (os.getenv("SYN_USER") or "").strip()
SYN_PASS = (os.getenv("SYN_PASS") or "").strip()
POLL_SECONDS = int((os.getenv("POLL_SECONDS") or "120").strip())
DEBUG = (os.getenv("DEBUG") or "0").strip() == "1"
HEADLESS = (os.getenv("HEADLESS") or "1").strip() == "1"

PUBLIC_DIR = Path(os.getenv("PUBLIC_DIR") or (Path(__file__).parent / "public"))
PUBLIC_DATA_JSON = PUBLIC_DIR / "data.json"
PUBLIC_INDEX_HTML = PUBLIC_DIR / "index.html"

SYNOPTICOM_LOGIN_URL = "https://synopticom.com/in/en/users/login"
REPORTS_INDEX_URL = "https://reports.synopticom.com/index.php?"
REPORTS_ALLDATA_URL = "https://reports.synopticom.com/alldata.php"
ACTIVITY_URL = "https://reports.synopticom.com/dashboard.php?tab=activity"

REPORT_PROJECT_DIV_ID = (os.getenv("REPORT_PROJECT_DIV_ID") or "5720").strip()
REPORT_PROJECT_TEXT = (os.getenv("REPORT_PROJECT_TEXT") or "Mano Būstas rezultatai nuo").strip()
ACTIVITY_PROJECT_SELECTOR = (os.getenv("ACTIVITY_PROJECT_SELECTOR") or '[id="5723"] > .project_link > .front_project_field1 > .front_project_text_field_img > .imgSet').strip()

TARGET_EXECUTOR = (os.getenv("TARGET_EXECUTOR") or "Mindaugas Bukauskas").strip()
REG_PREFIX = (os.getenv("REG_PREFIX") or "V").strip()

STATE_DIR = Path(os.getenv("STATE_DIR") or tempfile.gettempdir())
NPS_STATE_FILE = STATE_DIR / "synopticom_last_row.json"
ALERT_STATE_FILE = STATE_DIR / "synopticom_alert_state.json"

DEBUG_DIR = Path(os.getenv("DEBUG_DIR") or "/root/Kioskelis/debug_synopticom")
SAVE_HTML = (os.getenv("SAVE_HTML") or "0").strip() == "1"
SAVE_SCREENSHOTS = (os.getenv("SAVE_SCREENSHOTS") or "0").strip() == "1"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

SENDGRID_API_KEY = (os.getenv("SENDGRID_API_KEY") or "").strip()
FROM_EMAIL = (os.getenv("FROM_EMAIL") or "").strip()
ALERT_EMAIL_TO = (os.getenv("ALERT_EMAIL_TO") or "").strip()


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def save_text(name: str, text: str) -> None:
    if not SAVE_HTML:
        return
    path = DEBUG_DIR / name
    path.write_text(text, encoding="utf-8", errors="ignore")
    log(f"[DEBUG] saved text: {path}")


def save_bytes(name: str, data: bytes) -> None:
    path = DEBUG_DIR / name
    path.write_bytes(data)
    log(f"[DEBUG] saved bytes: {path}")


def safe_screenshot(page, name: str) -> None:
    if not SAVE_SCREENSHOTS:
        return
    try:
        path = DEBUG_DIR / name
        page.screenshot(path=str(path), full_page=True)
        log(f"[DEBUG] screenshot saved: {path}")
    except Exception as e:
        log(f"[DEBUG] screenshot failed: {e}")


def decode_export(data: bytes) -> str:
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        return data.decode("utf-16", errors="ignore")
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return data.decode("latin-1", errors="ignore")


def load_nps_state() -> Optional[str]:
    if not NPS_STATE_FILE.exists():
        return None
    try:
        obj = json.loads(NPS_STATE_FILE.read_text(encoding="utf-8"))
        return obj.get("last_hash_mb") or obj.get("last_hash")
    except Exception:
        return None


def save_nps_state(last_hash_mb: str) -> None:
    NPS_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    NPS_STATE_FILE.write_text(json.dumps({"last_hash_mb": last_hash_mb}, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_all_rows(data: bytes) -> List[Dict[str, str]]:
    text = decode_export(data)
    lines = [ln.rstrip("\n\r") for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    header = [h.lstrip("\ufeff").strip() for h in lines[0].split("\t")]
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}")
    out: List[Dict[str, str]] = []
    for ln in lines[1:]:
        ln = ln.strip()
        if not ln or not date_pattern.match(ln):
            continue
        parts = ln.split("\t")
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        row = {header[i]: (parts[i] if i < len(header) else "") for i in range(len(header))}
        row["_ts"] = parts[0].strip() if parts else ""
        out.append(row)
    return out


def latest_executor_row_hash(rows: List[Dict[str, str]], executor_name: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    ex_norm = executor_name.strip().lower()
    candidates = []
    for r in rows:
        ex = str(r.get("Darbų vykdytojas", "")).strip().lower()
        if ex != ex_norm:
            continue
        ts = str(r.get("_ts", "")).strip()
        candidates.append((ts, r))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0])
    latest = candidates[-1][1]
    raw = json.dumps(latest, ensure_ascii=False, sort_keys=True)
    return latest, sha(raw)


def normalize_score_zero(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return s
    low = s.lower()
    if "neturiu nuomon" in low or "nežinau" in low or "nežinau" in low:
        return "0"
    return s


def pick_description(row: Dict[str, str]) -> str:
    for k in ["Description", "Aprašymas", "Pranešimo aprašymas", "Pranesimo aprasymas"]:
        if k in row and str(row.get(k, "")).strip():
            return str(row.get(k, "")).strip()
    for key, val in row.items():
        lk = str(key).strip().lower()
        if "description" in lk or "apraš" in lk or "apras" in lk:
            v = str(val or "").strip()
            if v:
                return v
    return ""


def normalize_satisfaction_score(row: Dict[str, str]) -> Optional[int]:
    key = "Bendras pasitenkinimas MB suteiktomis paslaugomis"
    raw = str(row.get(key, "") or "").strip()
    if not raw:
        return None
    low = raw.lower()
    if "neturiu nuomon" in low or "nežinau" in low or "nežinau" in low:
        return 0
    m = re.search(r"-?\d+", raw)
    if not m:
        return None
    try:
        n = int(m.group(0))
    except Exception:
        return None
    if n < 0 or n > 10:
        return None
    return n


KIOSK_KEYS = [
    "Kontaktinis telefonas",
    "Registracijos nr.",
    "Adresas",
    "Darbo aprašas",
    "Bendras pasitenkinimas MB suteiktomis paslaugomis",
    "Bendras pasitenkinimas MB suteiktomis paslaugomis - Kas paskatino šitaip vertinti? (balai 8-10)",
    "Technikas",
    "Mano Būstas Rekomendacija (NPS)",
    "Mano Būstas Rekomendacija (NPS) - Kas paskatino šitaip įvertinti? Jūsų pasiūlymai; rekomendacijos",
    "Darbų vykdytojas",
]


def build_kiosk_payload(rows: List[Dict[str, str]], keep_last: int = 200, min_top_count: int = 1) -> Dict[str, Any]:
    now = datetime.now()
    cur_y, cur_m = now.year, now.month
    prev_y, prev_m = (cur_y - 1, 12) if cur_m == 1 else (cur_y, cur_m - 1)

    def in_prev_month(ts: str) -> bool:
        try:
            dt = datetime.strptime((ts or "").strip(), "%Y-%m-%d %H:%M:%S")
            return (dt.year == prev_y) and (dt.month == prev_m)
        except Exception:
            return False

    def in_current_month(ts: str) -> bool:
        try:
            dt = datetime.strptime((ts or "").strip(), "%Y-%m-%d %H:%M:%S")
            return (dt.year == cur_y) and (dt.month == cur_m)
        except Exception:
            return False

    filt = [r for r in rows if str(r.get("Darbų vykdytojas", "")).strip().lower() == TARGET_EXECUTOR.lower() and in_current_month(str(r.get("_ts", "")))]
    prev_filt = [r for r in rows if str(r.get("Darbų vykdytojas", "")).strip().lower() == TARGET_EXECUTOR.lower() and in_prev_month(str(r.get("_ts", "")))]
    recent = filt[-keep_last:]
    reviews = []
    for r in reversed(recent):
        item = {k: normalize_score_zero(str(r.get(k, "")).strip()) for k in KIOSK_KEYS}
        item["Description"] = pick_description(r)
        item["_ts"] = str(r.get("_ts", "")).strip()
        reviews.append(item)

    def tech_table(src_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        stats: Dict[str, Dict[str, float]] = {}
        for r in src_rows:
            tech = str(r.get("Technikas", "")).strip()
            if not tech:
                continue
            score = normalize_satisfaction_score(r)
            if score is None:
                continue
            st = stats.setdefault(tech, {"sum": 0.0, "cnt": 0.0})
            st["sum"] += float(score)
            st["cnt"] += 1.0
        out = []
        for tech, st in stats.items():
            cnt = int(st["cnt"])
            if cnt < min_top_count:
                continue
            out.append({"technikas": tech, "avg": round(st["sum"] / st["cnt"], 2), "count": cnt})
        out.sort(key=lambda x: (x["avg"], x["count"]), reverse=True)
        return out

    prev_top = tech_table(prev_filt)
    top = tech_table(filt)
    prev_map = {x["technikas"]: x["avg"] for x in prev_top}
    for x in top:
        prev_avg = prev_map.get(x["technikas"])
        x["prev_avg"] = prev_avg
        x["delta"] = round(x["avg"] - prev_avg, 2) if prev_avg is not None else None

    return {
        "executor": TARGET_EXECUTOR,
        "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "month": f"{cur_y:04d}-{cur_m:02d}",
        "topTechnicians": top,
        "prevMonthLabel": f"{prev_y:04d}-{prev_m:02d}",
        "prevTopTechnicians": prev_top,
        "reviews": reviews,
    }


def render_kiosk_html(payload, refresh_seconds: int = 20, slide_seconds: int = 60) -> str:
    return good_render_kiosk_html(payload, refresh_seconds=refresh_seconds, slide_seconds=slide_seconds)


def update_public_outputs(nps_data: bytes) -> None:
    rows = parse_all_rows(nps_data)
    payload = build_kiosk_payload(rows)
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DATA_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    PUBLIC_INDEX_HTML.write_text(render_kiosk_html(payload), encoding="utf-8")
    log(f"[INFO] kiosk updated -> {PUBLIC_INDEX_HTML}")


def normalize_alert_state(raw):
    if raw is None:
        return None
    if isinstance(raw, dict):
        if "total" in raw and "uzpildyta" in raw:
            if "registration_numbers" not in raw:
                raw["registration_numbers"] = []
            return raw
        if "value" in raw:
            value = int(raw.get("value", 0))
            return {"name": TARGET_EXECUTOR, "uzpildyta": 0, "neuzpildyta": value, "pradeta": 0, "total": value, "registration_numbers": []}
    if isinstance(raw, int):
        return {"name": TARGET_EXECUTOR, "uzpildyta": 0, "neuzpildyta": raw, "pradeta": 0, "total": raw, "registration_numbers": []}
    return None


def load_alert_state():
    if not ALERT_STATE_FILE.exists():
        return None
    try:
        raw = json.loads(ALERT_STATE_FILE.read_text(encoding="utf-8"))
        return normalize_alert_state(raw)
    except Exception:
        return None


def save_alert_state(row_data: dict) -> None:
    ALERT_STATE_FILE.write_text(json.dumps(row_data, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_row_data(worker_data: bytes) -> dict:
    text = decode_export(worker_data)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    header_found = False
    for line in lines:
        cols = re.split(r"\t+", line)
        if len(cols) >= 4 and cols[0].strip() == "Label" and cols[1].strip() == "Užpildyta":
            header_found = True
            continue
        if header_found and len(cols) >= 4 and cols[0].strip() == TARGET_EXECUTOR:
            uzp = int((cols[1].strip() or "0"))
            neuzp = int((cols[2].strip() or "0"))
            prad = int((cols[3].strip() or "0"))
            return {"name": TARGET_EXECUTOR, "uzpildyta": uzp, "neuzpildyta": neuzp, "pradeta": prad, "total": uzp + neuzp + prad}
    raise RuntimeError(f"Nerasta eilutė: {TARGET_EXECUTOR}")


def extract_registration_numbers(reg_data: bytes) -> list[str]:
    text = decode_export(reg_data)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    numbers = []
    seen = set()
    table_started = False
    for line in lines:
        cols = re.split(r"\t+", line)
        if len(cols) >= 4 and cols[0].strip() == "Label" and cols[1].strip() == "Užpildyta" and cols[2].strip() == "Neužpildyta":
            table_started = True
            continue
        if not table_started or not cols:
            continue
        first = cols[0].strip()
        if re.match(rf"^{re.escape(REG_PREFIX)}\d+", first):
            if first not in seen:
                seen.add(first)
                numbers.append(first)
        elif numbers:
            break
    return numbers


def send_email_alert(subject: str, body: str) -> None:
    if not SENDGRID_API_KEY or not FROM_EMAIL or not ALERT_EMAIL_TO:
        log("[INFO] Email alert skipped: missing SENDGRID_API_KEY / FROM_EMAIL / ALERT_EMAIL_TO")
        return
    payload = {"personalizations": [{"to": [{"email": ALERT_EMAIL_TO}], "subject": subject}], "from": {"email": FROM_EMAIL}, "content": [{"type": "text/plain", "value": body}]}
    response = requests.post("https://api.sendgrid.com/v3/mail/send", headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"}, json=payload, timeout=30)
    if response.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error: {response.status_code} {response.text}")


def process_alerts(worker_data: bytes, reg_data: bytes) -> None:
    current = extract_row_data(worker_data)
    current["registration_numbers"] = extract_registration_numbers(reg_data)
    previous = load_alert_state()
    if previous is None:
        save_alert_state(current)
        log("[INFO] alert initial state saved")
        return
    prev_total = int(previous.get("total", 0))
    curr_total = int(current["total"])
    prev_uzp = int(previous.get("uzpildyta", 0))
    curr_uzp = int(current["uzpildyta"])
    new_regs = sorted(set(current.get("registration_numbers", [])) - set(previous.get("registration_numbers", [])))
    if curr_total > prev_total:
        send_email_alert(f"Synopticom alert: {TARGET_EXECUTOR} suma padidėjo {prev_total} -> {curr_total}", f"Darbuotojas: {TARGET_EXECUTOR}\nBuvo total: {prev_total}\nDabar total: {curr_total}\n")
        if new_regs:
            send_email_alert(f"Synopticom alert: nauji registracijos numeriai ({len(new_regs)})", "Nauji registracijos numeriai:\n" + "\n".join(f"- {x}" for x in new_regs))
    if curr_uzp > prev_uzp:
        send_email_alert(f"Synopticom alert: užpildyta padidėjo {prev_uzp} -> {curr_uzp}", f"Darbuotojas: {TARGET_EXECUTOR}\nUžpildyta buvo: {prev_uzp}\nUžpildyta dabar: {curr_uzp}\n")
    save_alert_state(current)


def click_locator(locator, label: str, timeout: int = 10000) -> None:
    for idx, fn in enumerate([
        lambda: locator.click(timeout=timeout),
        lambda: locator.click(timeout=timeout, force=True),
        lambda: locator.evaluate("(el) => el.click()"),
    ], start=1):
        try:
            fn()
            log(f"[DEBUG] {label} clicked with method #{idx}")
            return
        except Exception as e:
            last = e
    raise last


def fill_input(locator, value: str, name: str) -> None:
    for idx, fn in enumerate([
        lambda: (locator.click(timeout=10000), locator.fill(value, timeout=10000)),
        lambda: locator.fill(value, timeout=10000),
        lambda: (locator.click(timeout=10000), locator.press("Control+A"), locator.type(value, delay=30, timeout=10000)),
    ], start=1):
        try:
            fn()
            log(f"[DEBUG] {name} filled with method #{idx}")
            return
        except Exception as e:
            last = e
    raise last


def do_login(page) -> None:
    log(f"[DEBUG] opening login page: {SYNOPTICOM_LOGIN_URL}")
    page.goto(SYNOPTICOM_LOGIN_URL, wait_until="commit", timeout=90000)
    page.wait_for_timeout(3000)
    user_locator = page.locator("input[name='data[username]']:visible").first
    pass_locator = page.locator("input[name='data[password]']:visible").first
    user_locator.wait_for(state="visible", timeout=30000)
    pass_locator.wait_for(state="visible", timeout=30000)
    fill_input(user_locator, SYN_USER, "username")
    fill_input(pass_locator, SYN_PASS, "password")
    click_locator(page.locator("input[type='submit']:visible").first, "login", timeout=10000)
    page.wait_for_timeout(5000)
    log(f"[DEBUG] after login URL: {page.url}")


def open_nps_exports(page) -> bytes:
    tile = page.locator(f'[id="{REPORT_PROJECT_DIV_ID}"]').first
    clickable = tile.locator("div").filter(has_text=REPORT_PROJECT_TEXT).first
    if clickable.count() == 0:
        clickable = tile.locator("a.project_link").first
    click_locator(clickable, "NPS project tile", timeout=30000)
    page.wait_for_timeout(1500)
    if "reports.synopticom.com/index.php" not in page.url:
        page.goto(REPORTS_INDEX_URL, wait_until="commit", timeout=90000)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=60000)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    try:
        page.get_by_role("listitem").first.click(timeout=15000)
    except Exception:
        pass
    page.get_by_role("link", name=re.compile(r"Visi klientai", re.I)).click(timeout=15000)
    page.goto(REPORTS_ALLDATA_URL, wait_until="commit", timeout=90000)
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    with page.expect_download(timeout=60000) as dl_info:
        export_btn = page.locator('a.export-to-xls-button[href*="export=1"]').first
        if export_btn.count() > 0:
            export_btn.click()
        else:
            page.get_by_role("link", name=re.compile(r"XLS", re.I)).click()
    dl = dl_info.value
    path = dl.path()
    data = Path(path).read_bytes() if path else b""
    if not data:
        tmp = Path(tempfile.gettempdir()) / "synopticom_nps_export.bin"
        dl.save_as(str(tmp))
        data = tmp.read_bytes()
    return data


def wait_for_slice_button(page):
    for locator in [
        page.get_by_role("button", name=re.compile(r"Pasirinkite pjūvį:", re.I)),
        page.get_by_role("button", name=re.compile(r"Pasirinkite pjūvį", re.I)),
        page.get_by_role("button", name=re.compile(r"Regionas", re.I)),
        page.locator("button").filter(has_text=re.compile(r"pjūv|Regionas", re.I)).first,
    ]:
        try:
            locator.wait_for(state="visible", timeout=20000)
            return locator
        except Exception:
            pass
    raise RuntimeError("Nerastas pjūvio mygtukas")


def wait_after_slice_change(page, wait_text: Optional[str] = None) -> None:
    try:
        page.locator("#loadOverlay").wait_for(state="hidden", timeout=60000)
    except Exception:
        pass
    if wait_text:
        try:
            page.wait_for_function(f"""() => {{ const t = document.body.innerText || ''; return t.includes({json.dumps(wait_text)}); }}""", timeout=60000)
        except Exception:
            pass
    page.wait_for_timeout(3000)


def click_xls_and_download(page, export_label: str) -> bytes:
    xls = page.locator(".export-to-xls-button").first
    xls.wait_for(state="visible", timeout=15000)
    with page.expect_download(timeout=60000) as download_info:
        xls.click(timeout=10000, force=True)
    download = download_info.value
    path = download.path()
    data = Path(path).read_bytes() if path else b""
    if not data:
        tmp = Path(tempfile.gettempdir()) / f"synopticom_{export_label}.xls"
        download.save_as(str(tmp))
        data = tmp.read_bytes()
    return data


def open_activity_dashboard(page) -> None:
    page.goto(ACTIVITY_URL, wait_until="commit", timeout=90000)
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    page.wait_for_timeout(3000)


def choose_slice_and_export(page, slice_name: str, wait_text: Optional[str], export_label: str) -> bytes:
    open_activity_dashboard(page)
    slice_btn = wait_for_slice_button(page)
    click_locator(slice_btn, f"slice button ({slice_name})", timeout=15000)
    page.wait_for_timeout(1500)
    slice_locator = page.get_by_role("link", name=slice_name)
    click_locator(slice_locator, slice_name, timeout=15000)
    wait_after_slice_change(page, wait_text=wait_text)
    return click_xls_and_download(page, export_label=export_label)


def download_all_exports_once() -> Tuple[bytes, bytes, bytes]:
    if not SYN_USER or not SYN_PASS:
        raise RuntimeError("Trūksta SYN_USER arba SYN_PASS")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"], slow_mo=800 if DEBUG else 250)
        context = browser.new_context(accept_downloads=True, ignore_https_errors=True, viewport={"width": 1440, "height": 1200})
        page = context.new_page()
        try:
            do_login(page)
            nps_data = open_nps_exports(page)
            page.goto("https://synopticom.com/in/en/users/surveys", wait_until="commit", timeout=90000)
            page.wait_for_timeout(3000)
            project_tile = page.locator(ACTIVITY_PROJECT_SELECTOR)
            click_locator(project_tile, "activity project tile", timeout=15000)
            page.wait_for_timeout(2000)
            worker_data = choose_slice_and_export(page, "Darbų vykdytojas", TARGET_EXECUTOR, "worker")
            reg_data = choose_slice_and_export(page, "Registracijos numeris", REG_PREFIX, "registration")
            return nps_data, worker_data, reg_data
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


def run_once(nps_last_hash: Optional[str]) -> Optional[str]:
    nps_data, worker_data, reg_data = download_all_exports_once()
    update_public_outputs(nps_data)
    rows = parse_all_rows(nps_data)
    latest_row, h = latest_executor_row_hash(rows, TARGET_EXECUTOR)
    if h is None or latest_row is None:
        log(f"[WARN] Nerasta įrašų su 'Darbų vykdytojas' = {TARGET_EXECUTOR}.")
    elif nps_last_hash is None:
        save_nps_state(h)
        log("[INFO] initial NPS state saved")
        nps_last_hash = h
    elif h != nps_last_hash:
        log(f"[INFO] new {TARGET_EXECUTOR} NPS row detected")
        save_nps_state(h)
        nps_last_hash = h
    else:
        log("[INFO] NPS no changes")
    process_alerts(worker_data, reg_data)
    return nps_last_hash


def main() -> None:
    nps_last_hash = load_nps_state()
    log(f"[INFO] combined worker start | poll={POLL_SECONDS}s")
    while True:
        try:
            nps_last_hash = run_once(nps_last_hash)
        except PlaywrightTimeoutError as e:
            log(f"[ERROR] Timeout: {e}")
        except Exception as e:
            log(f"[ERROR] {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
