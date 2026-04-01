import os
import json
import time
import hashlib
import csv
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

# -----------------------------
# Config (.env)
# -----------------------------
SYN_USER = (os.getenv("SYN_USER") or "").strip()
SYN_PASS = (os.getenv("SYN_PASS") or "").strip()


POLL_SECONDS = int((os.getenv("POLL_SECONDS") or "60").strip())
DEBUG = (os.getenv("DEBUG") or "0").strip() == "1"

# Project tile on synopticom surveys page (from your codegen)
PROJECT_DIV_ID = (os.getenv("REPORT_PROJECT_DIV_ID") or "5720").strip()
PROJECT_TEXT_SNIPPET = (os.getenv("REPORT_PROJECT_TEXT") or "Mano Būstas rezultatai nuo").strip()

# URLs
SYNOPTICOM_LOGIN_URL = "https://synopticom.com/in/en/users/login"
REPORTS_INDEX_URL = "https://reports.synopticom.com/index.php?"
REPORTS_ALLDATA_URL = "https://reports.synopticom.com/alldata.php"

# State file in TEMP (avoid OneDrive permission issues)
STATE_FILE = Path(os.getenv("TEMP", r"C:\temp")) / "synopticom_last_row.json"


# -----------------------------
# Helpers
# -----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def send_telegram(text: str) -> None:
    pass


def load_state() -> Optional[str]:
    """Load last notified hash for Mindaugas Bukauskas.
    Backward compatible with legacy key 'last_hash'.
    """
    if not STATE_FILE.exists():
        return None
    try:
        obj = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return obj.get("last_hash_mb") or obj.get("last_hash")
    except Exception:
        return None


def save_state(last_hash_mb: str) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps({"last_hash_mb": last_hash_mb}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def decode_export(data: bytes) -> str:
    # Exports are often UTF-16 with BOM
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        return data.decode("utf-16", errors="ignore")
    return data.decode("utf-8", errors="ignore")


def parse_last_row(data: bytes) -> Tuple[Dict[str, Any], str]:
    """
    Robust parser:
      - header is first line (TAB-separated)
      - data lines start with timestamp: YYYY-MM-DD HH:MM:SS
    """
    text = decode_export(data)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    if not lines:
        raise RuntimeError("Eksporte nėra eilučių")

    header = [h.lstrip("\ufeff").strip() for h in lines[0].split("\t")]
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}")

    rows = []
    for ln in lines[1:]:
        if date_pattern.match(ln):
            rows.append(ln.split("\t"))

    if not rows:
        # Helpful debug
        sample = "\n".join(lines[:25])
        raise RuntimeError("Nerasta duomenų eilučių su data. Pavyzdys:\n" + sample)

    last = rows[-1]
    if len(last) < len(header):
        last += [""] * (len(header) - len(last))

    row_dict = {header[i]: (last[i] if i < len(last) else "") for i in range(len(header))}
    raw = json.dumps(row_dict, ensure_ascii=False, sort_keys=True)
    return row_dict, sha(raw)


def is_executor_mindaugas(row: Dict[str, Any]) -> bool:
    val = row.get("Darbų vykdytojas", "")
    return str(val).strip().lower() == "mindaugas bukauskas"



def latest_executor_row_hash(rows: list[Dict[str, str]], executor_name: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """Return latest row (by _ts) for executor and its stable hash; (None, None) if missing."""
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
    # Sort by timestamp string (ISO-like) then take last
    candidates.sort(key=lambda x: x[0])
    latest = candidates[-1][1]
    raw = json.dumps(latest, ensure_ascii=False, sort_keys=True)
    return latest, sha(raw)


def format_message(row: Dict[str, Any]) -> str:
    keys = [
        "Kontaktinis telefonas",
        "Registracijos nr.",
        "Adresas",
        "Darbo aprašas",
        "Bendras pasitenkinimas MB suteiktomis paslaugomis",
        "Bendras pasitenkinimas MB suteiktomis paslaugomis - Kas paskatino šitaip vertinti? (balai 8-10)",
        "Technikas",
        "Mano Būstas Rekomendacija (NPS) (balais)",
    "Mano Būstas Rekomendacija (NPS) - Kas paskatino šitaip įvertinti? Jūsų pasiūlymai; rekomendacijos",
    ]
    lines = [f"{k}: {str(row.get(k, '')).strip()}" for k in keys]
    return "Naujas NPS irasas (Mindaugas Bukauskas)\n\n" + "\n".join(lines)


# -----------------------------
# Playwright flow (based on your codegen)
# -----------------------------
def do_export_download() -> bytes:
    if not SYN_USER or not SYN_PASS:
        raise RuntimeError("Trūksta SYN_USER arba SYN_PASS (.env).")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not DEBUG))
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Login
        page.goto(SYNOPTICOM_LOGIN_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass

        # Use codegen-style role selectors
        page.get_by_role("textbox", name=re.compile(r"E-?mail", re.I)).fill(SYN_USER)
        page.get_by_role("textbox", name=re.compile(r"Password", re.I)).fill(SYN_PASS)
        page.get_by_role("button", name=re.compile(r"Login", re.I)).click()

        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(800)

        log(f"[DEBUG] after synopticom login URL: {page.url}")

        # Click project tile/link (numeric id -> attribute selector)
        tile = page.locator(f'[id="{PROJECT_DIV_ID}"]').first
        if tile.count() == 0:
            raise RuntimeError(f"Nerandu projekto tile su id={PROJECT_DIV_ID} (surveys puslapyje). URL: {page.url}")

        # In codegen you clicked: [id="5720"] div (with text snippet)
        clickable = tile.locator("div").filter(has_text=PROJECT_TEXT_SNIPPET).first
        if clickable.count() == 0:
            # Fallback: any project_link inside the tile
            clickable = tile.locator("a.project_link").first

        try:
            clickable.scroll_into_view_if_needed(timeout=10_000)
        except Exception:
            pass

        try:
            clickable.click(timeout=30_000)
        except Exception:
            clickable.click(timeout=30_000, force=True)

        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(800)
        log(f"[DEBUG] after project click URL: {page.url}")

        # Navigate to reports index and then "Visi klientai"
        page.goto(REPORTS_INDEX_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(500)
        log(f"[DEBUG] opened reports index URL: {page.url}")

        # codegen: first listitem click (keeps same behavior)
        try:
            page.get_by_role("listitem").first.click(timeout=15_000)
        except Exception:
            pass

        page.get_by_role("link", name=re.compile(r"Visi klientai", re.I)).click()
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass

        page.goto(REPORTS_ALLDATA_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(800)
        log(f"[DEBUG] opened alldata URL: {page.url}")

        # Export download
        with page.expect_download(timeout=60_000) as dl_info:
            # Prefer the exact class, fallback to link name "XLS"
            export_btn = page.locator('a.export-to-xls-button[href*="export=1"]').first
            if export_btn.count() > 0:
                export_btn.click()
            else:
                page.get_by_role("link", name=re.compile(r"XLS", re.I)).click()

        dl = dl_info.value
        path = dl.path()
        if not path:
            tmp = Path(os.getenv("TEMP", r"C:\temp")) / "synopticom_export.bin"
            dl.save_as(str(tmp))
            data = tmp.read_bytes()
        else:
            data = Path(path).read_bytes()

        context.close()
        browser.close()
        return data



# -----------------------------
# Public kiosk output (NO server / NO admin)
# Generates:
#   public/index.html  (self-contained slideshow; works via file://)
#   public/data.json   (optional; for troubleshooting)
# The kiosk page auto-refreshes every N seconds to pick up changes.
# -----------------------------
PUBLIC_DIR = Path(__file__).parent / "public"
PUBLIC_DATA_JSON = PUBLIC_DIR / "data.json"
PUBLIC_INDEX_HTML = PUBLIC_DIR / "index.html"

# Keep the same info that is sent to Telegram (plus executor)
KIOSK_KEYS = [
    "Kontaktinis telefonas",
    "Registracijos nr.",
    "Adresas",
    "Darbo aprašas",
    "Bendras pasitenkinimas MB suteiktomis paslaugomis",
    "Bendras pasitenkinimas MB suteiktomis paslaugomis - Kas paskatino šitaip vertinti? (balai 8-10)",
    "Technikas",
    "Mano Būstas Rekomendacija (NPS) (balais)",
    "Mano Būstas Rekomendacija (NPS) - Kas paskatino šitaip įvertinti? Jūsų pasiūlymai; rekomendacijos",
    "Darbų vykdytojas",
]

def normalize_score_zero(v: str) -> str:
    s = (v or '').strip()
    if not s:
        return s
    low = s.lower()
    if 'neturiu nuomon' in low or 'nežinau' in low or 'nežinau' in low:
        return '0'
    return s



def pick_description(row: Dict[str, str]) -> str:
    """Return description from row using flexible header matching."""
    # Exact matches first
    for k in ["Description", "Aprašymas", "Pranešimo aprašymas", "Pranesimo aprasymas"]:
        if k in row and str(row.get(k, "")).strip():
            return str(row.get(k, "")).strip()
    # Fuzzy match
    for key, val in row.items():
        lk = str(key).strip().lower()
        if "description" in lk or "apraš" in lk or "apras" in lk:
            v = str(val or "").strip()
            if v:
                return v
    return ""

def _to_int_safe(x: object) -> Optional[int]:
    s = str(x or "").strip()
    if not s:
        return None
    m = re.search(r"-?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None

def normalize_satisfaction_score(row: Dict[str, str]) -> Optional[int]:
    """
    Parse 'Bendras pasitenkinimas MB suteiktomis paslaugomis' as integer 0..10.
    - 'neturiu nuomonės / nežinau' -> 0
    - If value looks like non-rating (e.g., phone), ignore (None)
    """
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


def parse_all_rows(data: bytes) -> list[Dict[str, str]]:
    """
    Parse export (TAB-separated). Return list of dict rows.
    Assumes first column is timestamp like 'YYYY-MM-DD HH:MM:SS'.
    Stores that timestamp into '_ts' key.
    """
    text = decode_export(data)
    lines = [ln.rstrip("\n\r") for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []

    header = [h.lstrip("\ufeff").strip() for h in lines[0].split("\t")]
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}")

    out: list[Dict[str, str]] = []
    for ln in lines[1:]:
        ln = ln.strip()
        if not ln:
            continue
        if not date_pattern.match(ln):
            continue
        parts = ln.split("\t")
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        row = {header[i]: (parts[i] if i < len(header) else "") for i in range(len(header))}
        row["_ts"] = parts[0].strip() if parts else ""
        out.append(row)

    return out


def build_kiosk_payload(rows: list[Dict[str, str]], keep_last: int = 200, min_top_count: int = 1) -> Dict[str, Any]:
    """Build payload for kiosk.
    - Only current month evaluations (based on export timestamp in first column).
    - Technician ranking criterion: 'Bendras pasitenkinimas MB suteiktomis paslaugomis'
    """
    from datetime import datetime

    now = datetime.now()
    cur_y, cur_m = now.year, now.month
    # Previous month (for sidebar bottom)
    if cur_m == 1:
        prev_y, prev_m = cur_y - 1, 12
    else:
        prev_y, prev_m = cur_y, cur_m - 1

    def in_prev_month(ts: str) -> bool:
        ts = (ts or "").strip()
        if not ts:
            return False
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            return (dt.year == prev_y) and (dt.month == prev_m)
        except Exception:
            return False


    def in_current_month(ts: str) -> bool:
        ts = (ts or "").strip()
        if not ts:
            return False
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            return (dt.year == cur_y) and (dt.month == cur_m)
        except Exception:
            return False

    # Filter: executor + current month
    filt = []
    for r in rows:
        ex = str(r.get("Darbų vykdytojas", "")).strip().lower()
        if ex != "mindaugas bukauskas":
            continue
        if not in_current_month(str(r.get("_ts", ""))):
            continue
        filt.append(r)


    # Filter: executor + previous month
    prev_filt = []
    for r in rows:
        ex = str(r.get("Darbų vykdytojas", "")).strip().lower()
        if ex != "mindaugas bukauskas":
            continue
        if not in_prev_month(str(r.get("_ts", ""))):
            continue
        prev_filt.append(r)

    # Recent reviews (newest first)
    recent = filt[-keep_last:]
    reviews = []
    for r in reversed(recent):
        item = {k: normalize_score_zero(str(r.get(k, "")).strip()) for k in KIOSK_KEYS}
        item["Description"] = pick_description(r)
        item["_ts"] = str(r.get("_ts", "")).strip()
        reviews.append(item)

    # TOP technicians (avg satisfaction score)
    tech_stats: Dict[str, Dict[str, float]] = {}
    for r in filt:
        tech = str(r.get("Technikas", "")).strip()
        if not tech:
            continue
        score = normalize_satisfaction_score(r)
        if score is None:
            continue
        st = tech_stats.setdefault(tech, {"sum": 0.0, "cnt": 0.0})
        st["sum"] += float(score)
        st["cnt"] += 1.0

    top = []
    for tech, st in tech_stats.items():
        cnt = int(st["cnt"])
        if cnt < min_top_count:
            continue
        avg = st["sum"] / st["cnt"]
        top.append({"technikas": tech, "avg": round(avg, 2), "count": cnt})

    top.sort(key=lambda x: (x["avg"], x["count"]), reverse=True)


    # Previous month TOP technicians
    prev_tech_stats: Dict[str, Dict[str, float]] = {}
    for r in prev_filt:
        tech = str(r.get("Technikas", "")).strip()
        if not tech:
            continue
        score = normalize_satisfaction_score(r)
        if score is None:
            continue
        st = prev_tech_stats.setdefault(tech, {"sum": 0.0, "cnt": 0.0})
        st["sum"] += float(score)
        st["cnt"] += 1.0

    prev_top = []
    for tech, st in prev_tech_stats.items():
        cnt = int(st["cnt"])
        if cnt < min_top_count:
            continue
        avg = st["sum"] / st["cnt"]
        prev_top.append({"technikas": tech, "avg": round(avg, 2), "count": cnt})

    prev_top.sort(key=lambda x: (x["avg"], x["count"]), reverse=True)

    return {
        "executor": "Mindaugas Bukauskas",
        "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "month": f"{cur_y:04d}-{cur_m:02d}",
        "topTechnicians": top,
        "prevMonthLabel": f"{prev_y:04d}-{prev_m:02d}",
        "prevTopTechnicians": prev_top,
        "reviews": reviews,
    }



def render_kiosk_html(payload: Dict[str, Any], refresh_seconds: int = 20, slide_seconds: int = 60) -> str:

    # Self-contained kiosk page: embed payload JSON directly (works with file:// without server).
    # IMPORTANT: Do NOT use f-strings here (JS contains {} which breaks f-strings).
    payload_json = json.dumps(payload, ensure_ascii=False)

    template = """<!doctype html>
<html lang="lt">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Atsiliepimai – Technikų TOP</title>
  <style>
    html, body { height: 100%; margin: 0; background:#0b0f14; color:#e8eef6; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; }
    .wrap { height: 100%; display:flex; flex-direction:column; }
    header { padding: 16px 20px; display:flex; justify-content:space-between; align-items:baseline; border-bottom:1px solid rgba(255,255,255,.08); }
    header h1 { margin:0; font-size: 20px; font-weight: 900; letter-spacing:.2px; }
    header .meta { opacity:.85; font-size: 13px; }

    .stage { flex:1; display:flex; padding: 16px 20px; gap: 16px; }
    .panel { border:1px solid rgba(255,255,255,.10); border-radius: 18px; padding: 16px; box-shadow: 0 12px 30px rgba(0,0,0,.35); background: rgba(255,255,255,.03); }
    .top-panel { width: 34%; min-width: 360px; overflow:auto; }
    .slide-panel { flex: 1; }

    .k { opacity:.75; font-size: 12px; }
    .k-title { opacity:.92; font-size: 18px; font-weight: 900; letter-spacing:.2px; }
    .k-title-sub { opacity:.9; font-size: 16px; font-weight: 900; letter-spacing:.2px; }
    .v { font-size: 16px; font-weight: 650; margin-top: 4px; word-break: break-word; line-height: 1.35; }
    .big { font-size: 44px; font-weight: 950; letter-spacing: .2px; }

    .pill { display:inline-block; padding: 6px 10px; border-radius: 999px; border:1px solid rgba(255,255,255,.14); font-size: 12px; opacity:.95; background: rgba(255,255,255,.06); }

    .badge { display:inline-block; padding: 6px 10px; border-radius: 12px; font-weight: 900; letter-spacing:.2px; }
    .b-good { background: rgba(34,197,94,.22); border:1px solid rgba(34,197,94,.35); }
    .b-mid  { background: rgba(234,179,8,.22); border:1px solid rgba(234,179,8,.35); }
    .b-bad  { background: rgba(239,68,68,.22); border:1px solid rgba(239,68,68,.35); }
    .b-na   { background: rgba(148,163,184,.18); border:1px solid rgba(148,163,184,.30); }

    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding: 10px 8px; border-bottom:1px solid rgba(255,255,255,.08); }
    th { opacity:.7; font-weight:900; font-size: 12px; letter-spacing:.2px; }
    td { font-size: 15px; }
    .muted { opacity:.75; }

    .row { display:flex; gap: 12px; align-items:baseline; flex-wrap:wrap; }
    .divider { height: 12px; }

    .footer { padding: 10px 20px; opacity:.75; font-size: 12px; border-top:1px solid rgba(255,255,255,.08); display:flex; justify-content:space-between; }

    .navbtn {
      cursor: pointer;
      border-radius: 14px;
      padding: 10px 14px;
      font-weight: 900;
      letter-spacing:.2px;
      border:1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.06);
      color:#e8eef6;
      transition: transform .06s ease, background .12s ease;
      user-select:none;
    }
    .navbtn:hover { background: rgba(255,255,255,.10); }
    .navbtn:active { transform: translateY(1px); }
    .navbtn:focus { outline: 2px solid rgba(255,255,255,.25); outline-offset: 2px; }


    @media (min-width: 1600px) {
      header h1 { font-size: 22px; }
      td { font-size: 16px; }
      .v { font-size: 18px; }
    }
  </style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Atsiliepimai (kiosk) <span class="pill" id="executorPill"></span></h1>
    <div class="meta" id="status"></div>
  </header>

  <div class="stage">
    <div class="panel top-panel">
      <div class="row">
        <div style="font-weight:950">TOP technikai</div>
        <div class="pill">kriterijus: Pasitenkinimas suteiktomis paslaugomis</div>
      </div>
      <div class="divider"></div>
      <div id="topTable"></div>
      <div class="divider"></div>
      <div class="row">
        <div style="font-weight:900">Praeitas mėnuo</div>
        <div class="pill" id="prevMonthPill"></div>
      </div>
      <div class="divider"></div>
      <div id="prevTopTable"></div>
      <div class="divider"></div>
      <div class="k">Atnaujinta</div>
      <div class="v" id="updatedInline"></div>
    </div>

    <div class="panel slide-panel">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;">
        <div class="pill" id="slideHint">⬅️ ➡️ arba mygtukai</div>
        <div style="display:flex;gap:10px;">
          <button id="prevBtn" class="navbtn" type="button">◀ Atgal</button>
          <button id="nextBtn" class="navbtn" type="button">Sekanti ▶</button>
        </div>
      </div>
      <div id="slide"></div>
    </div>
  </div>

  <div class="footer">
    <div>F11 → fullscreen</div>
    <div id="updated"></div>
  </div>
</div>

<script id="payload" type="application/json">__PAYLOAD__</script>
<script>
  const SLIDE_SECONDS = __SLIDE_SECONDS__;
  const payload = JSON.parse(document.getElementById("payload").textContent);
  let slideIdx = 0;

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }

  function scoreBadge(score) {
    const n = Number(score);
    if (!Number.isFinite(n)) return '<span class="badge b-na">—</span>';
    if (n >= 9) return '<span class="badge b-good">'+esc(n)+'</span>';
    if (n >= 7) return '<span class="badge b-mid">'+esc(n)+'</span>';
    return '<span class="badge b-bad">'+esc(n)+'</span>';
  }

  function medal(i) {
    if (i === 0) return "🥇";
    if (i === 1) return "🥈";
    if (i === 2) return "🥉";
    return String(i+1);
  }

  function renderTop(p) {
    const top = p.topTechnicians || [];
    const rows = top.map((t, i) => `
      <tr>
        <td style="width:44px">${medal(i)}</td>
        <td><strong>${esc(t.technikas)}</strong></td>
        <td style="width:84px">${esc(t.avg)}</td>
        <td class="muted" style="width:70px">${esc(t.count)}</td>
      </tr>
    `).join("");

    const table = `
      <table>
        <thead><tr><th></th><th>Technikas</th><th>Vid.</th><th>Kiekis</th></tr></thead>
        <tbody>${rows || `<tr><td colspan="4" class="muted">Kol kas nėra duomenų.</td></tr>`}</tbody>
      </table>
    `;
    document.getElementById("topTable").innerHTML = table;

    const prev = p.prevTopTechnicians || [];
    const prevRows = prev.map((t, i) => `
      <tr>
        <td style="width:44px">${medal(i)}</td>
        <td><strong>${esc(t.technikas)}</strong></td>
        <td style="width:84px">${esc(t.avg)}</td>
        <td class="muted" style="width:70px">${esc(t.count)}</td>
      </tr>
    `).join("");

    const prevTable = `
      <table>
        <thead><tr><th>#</th><th>Technikas</th><th>Vid.</th><th>Kiekis</th></tr></thead>
        <tbody>${prevRows || `<tr><td colspan="4" class="muted">Praeitą mėnesį duomenų nėra.</td></tr>`}</tbody>
      </table>
    `;
    const pill = document.getElementById("prevMonthPill");
    if (pill) pill.textContent = (p.prevMonthLabel || "");
    const prevDiv = document.getElementById("prevTopTable");
    if (prevDiv) prevDiv.innerHTML = prevTable;
  }

  function renderReview(r){
    const satKey = "Bendras pasitenkinimas MB suteiktomis paslaugomis";
    const npsKey = "Mano Būstas Rekomendacija (NPS) (balais)";
    const npsCommentKey = "Mano Būstas Rekomendacija (NPS) - Kas paskatino šitaip įvertinti? Jūsų pasiūlymai; rekomendacijos";

    const satScore = r[satKey] ?? "";
    const npsScore = r[npsKey] ?? "";
    const npsComment = r[npsCommentKey] ?? "";

    return `
      <div class="k-title">🔧 Pasitenkinimas suteiktomis paslaugomis</div>
      <div class="divider"></div>
      <div class="big">${scoreBadge(satScore)} <span style="font-size:20px;opacity:.8;font-weight:900">/ 10</span></div>

      <div class="divider"></div>
      <div class="k-title-sub">Mano Būstas Rekomendacija (NPS)</div>
      <div class="divider"></div>
      <div class="big">${scoreBadge(npsScore)} <span style="font-size:20px;opacity:.8;font-weight:900">/ 10</span></div>

      <div class="divider"></div>
      <div class="k">NPS komentaras / pasiūlymai</div>
      <div class="v">${esc(npsComment) || "—"}</div>

      <div class="divider"></div>
      <div style="border-top:1px solid rgba(255,255,255,.08); padding-top:12px;">
        <div class="k">Technikas</div>
        <div class="v">${esc(r["Technikas"]) || "—"}</div>

        <div class="divider"></div>
        <div class="k">Adresas</div>
        <div class="v">${esc(r["Adresas"]) || "—"}</div>

        <div class="divider"></div>
        <div class="k">Registracijos nr.</div>
        <div class="v">${esc(r["Registracijos nr."]) || "—"}</div>

        <div class="divider"></div>
        <div class="k">Pranešimo aprašymas</div>
        <div class="v">${esc(r["Description"]) || "—"}</div>

        <div class="divider"></div>
        <div class="k">Kontaktinis telefonas</div>
        <div class="v">${esc(r["Kontaktinis telefonas"]) || "—"}</div>
      </div>
    `;
  }

  function render(){
    const slide = document.getElementById("slide");
    const status = document.getElementById("status");
    const updated = document.getElementById("updated");
    const updatedInline = document.getElementById("updatedInline");
    const executorPill = document.getElementById("executorPill");

    executorPill.textContent = payload.executor || "";
    updated.textContent = "lastUpdated: " + (payload.lastUpdated || "");
    updatedInline.textContent = payload.lastUpdated || "";

    renderTop(payload);

    const reviews = payload.reviews || [];
    const totalSlides = Math.max(reviews.length, 1);

    if (reviews.length === 0) {
      slide.innerHTML = '<div class="muted">Einamą mėnesį atsiliepimų nėra.</div>';
      status.textContent = "Atsiliepimai • 0";
      return;
    }

    slideIdx = slideIdx % totalSlides;
    slide.innerHTML = renderReview(reviews[slideIdx]);
    status.textContent = "Atsiliepimai • skaidrė " + (slideIdx+1) + "/" + totalSlides;
  }

  function nextSlide(){
    const reviews = payload.reviews || [];
    const totalSlides = Math.max(reviews.length, 1);
    if (totalSlides <= 1) return;
    slideIdx = (slideIdx + 1) % totalSlides;
    render();
  }

  function prevSlide(){
    const reviews = payload.reviews || [];
    const totalSlides = Math.max(reviews.length, 1);
    if (totalSlides <= 1) return;
    slideIdx = (slideIdx - 1 + totalSlides) % totalSlides;
    render();
  }

  const nextBtn = document.getElementById("nextBtn");
  const prevBtn = document.getElementById("prevBtn");
  if (nextBtn) nextBtn.addEventListener("click", nextSlide);
  if (prevBtn) prevBtn.addEventListener("click", prevSlide);

  // Keyboard navigation
  document.addEventListener("keydown", function(ev){
    if (ev.key === "ArrowRight") { nextSlide(); }
    if (ev.key === "ArrowLeft") { prevSlide(); }
  });

  render();

  // Auto-advance
  const intervalId = setInterval(function(){ nextSlide(); }, SLIDE_SECONDS * 1000);

  const totalSlides = Math.max((payload.reviews || []).length, 1);
  const cycleMs = totalSlides * SLIDE_SECONDS * 1000;
  setTimeout(function(){ location.reload(); }, cycleMs);
</script>
</body>
</html>"""

    return (template
            .replace("__PAYLOAD__", payload_json)
            .replace("__SLIDE_SECONDS__", str(slide_seconds))
            )



def update_public_outputs(data: bytes) -> None:
    """
    Always try to refresh kiosk outputs from the latest export bytes.
    This is independent from the "new row" / telegram notification logic.
    """
    try:
        rows = parse_all_rows(data)
        payload = build_kiosk_payload(rows)
        PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

        # Write JSON (useful for debugging)
        PUBLIC_DATA_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        # Write self-contained HTML
        PUBLIC_INDEX_HTML.write_text(render_kiosk_html(payload), encoding="utf-8")
        log(f"[INFO] kiosk updated -> {PUBLIC_INDEX_HTML}")
    except Exception as e:
        log(f"[WARN] kiosk output failed: {e}")

def run_once(last_hash: Optional[str]) -> Optional[str]:
    data = do_export_download()
    update_public_outputs(data)

    # Determine the latest entry for Mindaugas (not the global last row),
    # so we don't miss it when other executors submit newer rows.
    rows = parse_all_rows(data)
    latest_row, h = latest_executor_row_hash(rows, "Mindaugas Bukauskas")

    if h is None or latest_row is None:
        log("[WARN] Nerasta įrašų su 'Darbų vykdytojas' = Mindaugas Bukauskas.")
        return last_hash

    if last_hash is None:
        save_state(h)
        log("[INFO] initial state saved")
        return h

    if h == last_hash:
        log("[INFO] no changes")
        return last_hash

    # New Mindaugas row detected
    send_telegram(format_message(latest_row))  # format_message uses flexible get()
    log("[INFO] new Mindaugas row detected -> notified")

    save_state(h)
    return h


def main() -> None:
    last_hash = load_state()
    log(f"[INFO] start | poll={POLL_SECONDS}s | state_file={STATE_FILE}")

    while True:
        try:
            last_hash = run_once(last_hash)
        except PlaywrightTimeoutError as e:
            log(f"[ERROR] Timeout: {e}")
        except Exception as e:
            log(f"[ERROR] {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[FATAL] {e}")
        input("Spausk Enter, kad uzdaryti...")
