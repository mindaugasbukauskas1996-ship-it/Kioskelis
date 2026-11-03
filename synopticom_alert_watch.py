import os
import json
import time
import re
import tempfile
import requests
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

SYN_USER = (os.getenv("SYN_USER") or "").strip()
SYN_PASS = (os.getenv("SYN_PASS") or "").strip()

LOGIN_URL = "https://synopticom.com/in/en/users/login"
ACTIVITY_URL = "https://reports.synopticom.com/dashboard.php?tab=activity"

POLL_SECONDS = int((os.getenv("POLL_SECONDS") or "600").strip())
DEBUG = (os.getenv("DEBUG") or "1").strip() == "1"
HEADLESS = (os.getenv("HEADLESS") or "1").strip() == "1"

PROJECT_SELECTOR = '[id="5723"] > .project_link > .front_project_field1 > .front_project_text_field_img > .imgSet'
TARGET_NAME = (os.getenv("TARGET_NAME") or "Mindaugas Bukauskas").strip()

STATE_FILE = Path(os.getenv("STATE_FILE") or (Path(tempfile.gettempdir()) / "synopticom_alert_state.json"))
DEBUG_DIR = Path(os.getenv("DEBUG_DIR") or "/root/Kioskelis/debug_synopticom")
SAVE_SCREENSHOTS = (os.getenv("SAVE_SCREENSHOTS") or "0").strip() == "1"
SAVE_HTML = (os.getenv("SAVE_HTML") or "1").strip() == "1"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

SENDGRID_API_KEY = (os.getenv("SENDGRID_API_KEY") or "").strip()
FROM_EMAIL = (os.getenv("FROM_EMAIL") or "").strip()
ALERT_EMAIL_TO = (os.getenv("ALERT_EMAIL_TO") or "").strip()


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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


def safe_html_dump(page, name: str) -> None:
    if not SAVE_HTML:
        return
    try:
        html = page.content()
        save_text(name, html)
    except Exception as e:
        log(f"[DEBUG] html dump failed: {e}")


def normalize_state(raw):
    if raw is None:
        return None

    if isinstance(raw, dict):
        if "total" in raw and "uzpildyta" in raw:
            if "registration_numbers" not in raw:
                raw["registration_numbers"] = []
            return raw

        if "value" in raw:
            value = int(raw.get("value", 0))
            return {
                "name": TARGET_NAME,
                "uzpildyta": 0,
                "neuzpildyta": value,
                "pradeta": 0,
                "total": value,
                "registration_numbers": [],
            }

    if isinstance(raw, int):
        return {
            "name": TARGET_NAME,
            "uzpildyta": 0,
            "neuzpildyta": raw,
            "pradeta": 0,
            "total": raw,
            "registration_numbers": [],
        }

    return None


def load_state():
    if not STATE_FILE.exists():
        return None
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return normalize_state(raw)
    except Exception:
        return None


def save_state(row_data: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(row_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def decode_export(data: bytes) -> str:
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        return data.decode("utf-16", errors="ignore")
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return data.decode("latin-1", errors="ignore")


def extract_row_data(data: bytes) -> dict:
    text = decode_export(data)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    log(f"[DEBUG] total parsed lines: {len(lines)}")
    save_text("last_export_preview.txt", "\n".join(lines[:300]))

    header_found = False

    for line in lines:
        cols = re.split(r"\t+", line)

        if len(cols) >= 4 and cols[0].strip() == "Label" and cols[1].strip() == "Užpildyta":
            header_found = True
            log("[DEBUG] header found")
            continue

        if header_found and len(cols) >= 4:
            label = cols[0].strip()
            if label == TARGET_NAME:
                uzpildyta = int((cols[1].strip() or "0"))
                neuzpildyta = int((cols[2].strip() or "0"))
                pradeta = int((cols[3].strip() or "0"))
                total = uzpildyta + neuzpildyta + pradeta

                log(f"[DEBUG] found target row: {cols[:4]}")
                log(f"[DEBUG] total = {uzpildyta} + {neuzpildyta} + {pradeta} = {total}")

                return {
                    "name": TARGET_NAME,
                    "uzpildyta": uzpildyta,
                    "neuzpildyta": neuzpildyta,
                    "pradeta": pradeta,
                    "total": total,
                }

    raise RuntimeError(f"Nerasta eilutė: {TARGET_NAME}")


def extract_registration_numbers(data: bytes) -> list[str]:
    text = decode_export(data)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    numbers = []
    seen = set()
    table_started = False

    for line in lines:
        cols = re.split(r"\t+", line)

        if len(cols) >= 4 and cols[0].strip() == "Label" and cols[1].strip() == "Užpildyta" and cols[2].strip() == "Neužpildyta":
            table_started = True
            continue

        if not table_started:
            continue

        if not cols:
            continue

        first = cols[0].strip()
        if re.match(r"^V\d{2}-\d+", first):
            if first not in seen:
                seen.add(first)
                numbers.append(first)
        else:
            # jei lentelė prasidėjo ir baigėsi registracijos numeriai
            if numbers:
                break

    log(f"[DEBUG] registration numbers found: {len(numbers)}")
    save_text("last_registration_numbers.txt", "\n".join(numbers))
    return numbers



def detect_login_failure(page) -> str | None:
    try:
        current_url = page.url or ""
    except Exception:
        current_url = ""

    if "/users/login" not in current_url:
        return None

    try:
        body_text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body_text = ""

    normalized = re.sub(r"\s+", " ", body_text).strip()
    excerpt = normalized[:1200] if normalized else "be teksto"
    return f"Login nepavyko, puslapis liko prisijungimo lange. URL={current_url}. Body: {excerpt}"


def debug_login_page(page) -> None:
    log("[DEBUG] inspecting login page")
    try:
        username_inputs_all = page.locator("input[name='data[username]']").count()
        password_inputs_all = page.locator("input[name='data[password]']").count()
        username_inputs_visible = page.locator("input[name='data[username]']:visible").count()
        password_inputs_visible = page.locator("input[name='data[password]']:visible").count()
        submit_inputs_visible = page.locator("input[type='submit']:visible").count()
        submit_buttons_visible = page.locator("button[type='submit']:visible").count()

        log(f"[DEBUG] input[name='data[username]'] all = {username_inputs_all}")
        log(f"[DEBUG] input[name='data[password]'] all = {password_inputs_all}")
        log(f"[DEBUG] input[name='data[username]']:visible = {username_inputs_visible}")
        log(f"[DEBUG] input[name='data[password]']:visible = {password_inputs_visible}")
        log(f"[DEBUG] input[type='submit']:visible = {submit_inputs_visible}")
        log(f"[DEBUG] button[type='submit']:visible = {submit_buttons_visible}")
    except Exception as e:
        log(f"[DEBUG] login page inspection failed: {e}")

    safe_screenshot(page, "01_login_page.png")
    safe_html_dump(page, "01_login_page.html")


def fill_input(locator, value: str, name: str) -> None:
    methods = [
        ("click_fill", lambda: (locator.click(timeout=10000), locator.fill(value, timeout=10000))),
        ("fill_only", lambda: locator.fill(value, timeout=10000)),
        ("type", lambda: (locator.click(timeout=10000), locator.press("Control+A"), locator.type(value, delay=30, timeout=10000))),
    ]

    last_err = None
    for idx, (method_name, fn) in enumerate(methods, start=1):
        try:
            fn()
            log(f"[DEBUG] {name} filled with method #{idx}: {method_name}")
            return
        except Exception as e:
            last_err = e
            log(f"[DEBUG] {name} fill method #{idx} failed: {e}")

    raise last_err if last_err else RuntimeError(f"Nepavyko užpildyti lauko: {name}")


def click_locator(locator, label: str, timeout: int = 10000) -> None:
    methods = [
        ("normal click", lambda: locator.click(timeout=timeout)),
        ("force click", lambda: locator.click(timeout=timeout, force=True)),
        ("js click", lambda: locator.evaluate("(el) => el.click()")),
    ]

    last_err = None
    for idx, (method_name, fn) in enumerate(methods, start=1):
        try:
            fn()
            log(f"[DEBUG] {label} clicked with method #{idx}: {method_name}")
            return
        except Exception as e:
            last_err = e
            log(f"[DEBUG] {label} click method #{idx} failed: {e}")

    raise last_err if last_err else RuntimeError(f"Nepavyko paspausti: {label}")


def do_login(page) -> None:
    log(f"[DEBUG] opening login page: {LOGIN_URL}")
    page.goto(LOGIN_URL, wait_until="commit", timeout=90000)
    page.wait_for_timeout(3000)

    debug_login_page(page)

    user_locator = page.locator("input[name='data[username]']:visible").first
    pass_locator = page.locator("input[name='data[password]']:visible").first

    user_locator.wait_for(state="visible", timeout=30000)
    pass_locator.wait_for(state="visible", timeout=30000)
    log("[STEP] login fields resolved")

    log("[STEP] filling username")
    fill_input(user_locator, SYN_USER, "username")

    log("[STEP] filling password")
    fill_input(pass_locator, SYN_PASS, "password")

    safe_screenshot(page, "02_login_filled.png")

    log("[STEP] clicking login")
    click_candidates = [
        page.locator("input[type='submit']:visible").first,
        page.locator("button:visible").filter(has_text=re.compile(r"Login", re.I)).first,
        page.locator("button[type='submit']:visible").first,
        page.get_by_role("button", name=re.compile(r"Login", re.I)),
    ]

    last_err = None
    for idx, locator in enumerate(click_candidates, start=1):
        try:
            click_locator(locator, f"login candidate #{idx}", timeout=10000)
            break
        except Exception as e:
            last_err = e
    else:
        raise last_err if last_err else RuntimeError("Nepavyko paspausti Login mygtuko")

    page.wait_for_timeout(5000)
    safe_screenshot(page, "03_after_login_click.png")
    safe_html_dump(page, "03_after_login_click.html")

    log(f"[DEBUG] after login URL: {page.url}")

    login_error = detect_login_failure(page)
    if login_error:
        safe_screenshot(page, "03_login_failed.png")
        safe_html_dump(page, "03_login_failed.html")
        raise RuntimeError(f"LOGIN_FAILED: {login_error}")


def wait_for_slice_button(page):
    candidates = [
        page.get_by_role("button", name=re.compile(r"Pasirinkite pjūvį:", re.I)),
        page.get_by_role("button", name=re.compile(r"Pasirinkite pjūvį", re.I)),
        page.get_by_role("button", name=re.compile(r"Regionas", re.I)),
        page.locator("button").filter(has_text=re.compile(r"pjūv|Regionas", re.I)).first,
    ]

    last_err = None
    for i, locator in enumerate(candidates, start=1):
        try:
            locator.wait_for(state="visible", timeout=20000)
            log(f"[DEBUG] slice button found with candidate #{i}")
            return locator
        except Exception as e:
            last_err = e

    raise last_err if last_err else RuntimeError("Nerastas pjūvio mygtukas")


def wait_overlay_hidden(page, label: str, timeout: int = 30000) -> None:
    try:
        page.locator("#loadOverlay").wait_for(state="hidden", timeout=timeout)
        log(f"[DEBUG] {label}")
    except Exception as e:
        log(f"[DEBUG] overlay wait skipped/failed: {e}")


def wait_after_slice_change(page, wait_text: str | None = None) -> None:
    try:
        page.locator("#loadOverlay").wait_for(state="hidden", timeout=60000)
        log("[DEBUG] overlay hidden")
    except Exception as e:
        log(f"[DEBUG] loadOverlay wait skipped/failed: {e}")

    if wait_text:
        try:
            page.wait_for_function(
                f"""() => {{
                    const t = document.body.innerText || '';
                    return t.includes({json.dumps(wait_text)});
                }}""",
                timeout=60000,
            )
            log(f"[DEBUG] {wait_text} visible on page")
        except Exception as e:
            log(f"[DEBUG] wait text failed ({wait_text}): {e}")

    page.wait_for_timeout(3000)


def click_xls_and_download(page, export_label: str) -> bytes:
    log(f"[STEP] downloading XLS ({export_label})")
    xls = page.locator(".export-to-xls-button").first
    xls.wait_for(state="visible", timeout=15000)

    try:
        page.locator("#loadOverlay").wait_for(state="hidden", timeout=15000)
        log("[DEBUG] overlay hidden before XLS click")
    except Exception as e:
        log(f"[DEBUG] overlay before XLS click skipped/failed: {e}")

    methods = [
        ("normal click", lambda: xls.click(timeout=10000)),
        ("force click", lambda: xls.click(timeout=10000, force=True)),
        ("js click", lambda: page.evaluate("""
            () => {
                const el = document.querySelector('.export-to-xls-button');
                if (!el) throw new Error('XLS button not found');
                el.click();
            }
        """)),
    ]

    last_err = None
    for idx, (method_name, fn) in enumerate(methods, start=1):
        try:
            with page.expect_download(timeout=60000) as download_info:
                fn()
            download = download_info.value
            log(f"[DEBUG] XLS clicked with method #{idx}: {method_name}")
            break
        except Exception as e:
            last_err = e
            log(f"[DEBUG] XLS click method #{idx} failed: {e}")
    else:
        raise last_err if last_err else RuntimeError("Nepavyko paspausti XLS mygtuko")

    path = download.path()
    if not path:
        tmp = Path(tempfile.gettempdir()) / f"synopticom_{export_label}.xls"
        download.save_as(str(tmp))
        data = tmp.read_bytes()
    else:
        data = Path(path).read_bytes()

    save_bytes(f"last_{export_label}.xls", data)
    log(f"[DEBUG] export downloaded OK ({export_label})")
    return data


def open_activity_dashboard(page) -> None:
    log("[DEBUG] opening activity dashboard...")
    page.goto(ACTIVITY_URL, wait_until="commit", timeout=90000)
    log(f"[DEBUG] activity goto done, current URL: {page.url}")

    try:
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=20000)
        log("[DEBUG] activity networkidle reached")
    except Exception as e:
        log(f"[DEBUG] activity networkidle skipped: {e}")

    page.wait_for_timeout(3000)
    log(f"[DEBUG] activity URL: {page.url}")
    safe_screenshot(page, "05_activity_dashboard.png")
    safe_html_dump(page, "05_activity_dashboard.html")


def choose_slice_and_export(page, slice_name: str, wait_text: str | None, export_label: str) -> bytes:
    open_activity_dashboard(page)

    slice_btn = wait_for_slice_button(page)
    click_locator(slice_btn, f"slice button ({slice_name})", timeout=15000)
    page.wait_for_timeout(1500)
    safe_screenshot(page, f"06_slice_menu_open_{export_label}.png")

    slice_locator = page.get_by_role("link", name=slice_name)
    click_locator(slice_locator, slice_name, timeout=15000)
    log(f"[DEBUG] selected: {slice_name}")

    wait_after_slice_change(page, wait_text=wait_text)

    safe_screenshot(page, f"07_after_slice_selected_{export_label}.png")
    safe_html_dump(page, f"07_after_slice_selected_{export_label}.html")

    return click_xls_and_download(page, export_label=export_label)


def download_exports_once() -> tuple[bytes, bytes]:
    if not SYN_USER or not SYN_PASS:
        raise RuntimeError("Trūksta SYN_USER arba SYN_PASS .env faile")

    log("[STEP] before sync_playwright")

    with sync_playwright() as p:
        log("[STEP] after sync_playwright")

        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
            slow_mo=800 if DEBUG else 250,
        )
        log("[STEP] after browser.launch")

        context = browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True,
            viewport={"width": 1440, "height": 1200},
        )
        log("[STEP] after browser.new_context")

        page = context.new_page()
        log("[STEP] after context.new_page")

        try:
            do_login(page)

            log("[STEP] clicking project tile")
            project_tile = page.locator(PROJECT_SELECTOR)
            click_locator(project_tile, "project tile", timeout=15000)

            wait_overlay_hidden(page, "overlay hidden after project click", timeout=30000)
            page.wait_for_timeout(2000)
            safe_screenshot(page, "04_after_project_click.png")

            worker_data = choose_slice_and_export(
                page=page,
                slice_name="Darbų vykdytojas",
                wait_text=TARGET_NAME,
                export_label="worker",
            )

            reg_data = choose_slice_and_export(
                page=page,
                slice_name="Registracijos numeris",
                wait_text="V26-",
                export_label="registration",
            )

            return worker_data, reg_data

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


def download_and_extract_with_retry() -> dict:
    worker_data, reg_data = download_exports_once()
    try:
        row = extract_row_data(worker_data)
        row["registration_numbers"] = extract_registration_numbers(reg_data)
        return row
    except Exception as e:
        log(f"[DEBUG] first parse failed: {e}")
        log("[DEBUG] waiting 15s before retry...")
        time.sleep(15)

        worker_data, reg_data = download_exports_once()
        row = extract_row_data(worker_data)
        row["registration_numbers"] = extract_registration_numbers(reg_data)
        return row


TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()


def send_email_alert(subject: str, body: str) -> None:
    text = f"<b>{subject}</b>\n\n{body}"
    try:
        import urllib.request
        import urllib.parse
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data
        )
        log(f"[ALERT] Telegram žinutė išsiųsta")
    except Exception as e:
        log(f"[ERROR] Nepavyko išsiųsti Telegram alert: {e}")


def send_login_failed_alert(error_message: str) -> None:
    subject = f"Synopticom alert: nepavyko prisijungti prie {TARGET_NAME}"
    body = (
        f"Nepavyko prisijungti prie Synopticom. Skriptas sustabdytas po pirmo nesėkmingo bandymo.\n\n"
        f"Darbuotojas: {TARGET_NAME}\n"
        f"Login URL: {LOGIN_URL}\n\n"
        f"Klaida:\n"
        f"{error_message}\n\n"
        f"Laikas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    send_email_alert(subject, body)


def send_total_alert(previous: dict, current: dict) -> None:
    prev_total = int(previous.get("total", 0))
    curr_total = int(current.get("total", 0))

    subject = f"Synopticom alert: {TARGET_NAME} suma padidėjo {prev_total} -> {curr_total}"
    body = (
        f"Synopticom alert\n\n"
        f"Darbuotojas: {TARGET_NAME}\n\n"
        f"Buvo:\n"
        f"  Užpildyta: {previous.get('uzpildyta', 0)}\n"
        f"  Neužpildyta: {previous.get('neuzpildyta', 0)}\n"
        f"  Pradėta: {previous.get('pradeta', 0)}\n"
        f"  Total: {prev_total}\n\n"
        f"Dabar:\n"
        f"  Užpildyta: {current.get('uzpildyta', 0)}\n"
        f"  Neužpildyta: {current.get('neuzpildyta', 0)}\n"
        f"  Pradėta: {current.get('pradeta', 0)}\n"
        f"  Total: {curr_total}\n\n"
        f"Laikas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    send_email_alert(subject, body)


def send_uzpildyta_alert(previous: dict, current: dict) -> None:
    prev_uzp = int(previous.get("uzpildyta", 0))
    curr_uzp = int(current.get("uzpildyta", 0))
    diff = curr_uzp - prev_uzp

    subject = f"Synopticom alert: užpildyta padidėjo {prev_uzp} -> {curr_uzp}"
    body = (
        f"Užpildytas naujas pranešimas\n\n"
        f"Darbuotojas: {TARGET_NAME}\n\n"
        f"Užpildyta:\n"
        f"  Buvo: {prev_uzp}\n"
        f"  Dabar: {curr_uzp}\n"
        f"  Pokytis: +{diff}\n\n"
        f"Pilna būsena dabar:\n"
        f"  Užpildyta: {current.get('uzpildyta', 0)}\n"
        f"  Neužpildyta: {current.get('neuzpildyta', 0)}\n"
        f"  Pradėta: {current.get('pradeta', 0)}\n"
        f"  Total: {current.get('total', 0)}\n\n"
        f"Laikas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    send_email_alert(subject, body)


def send_new_registration_numbers_alert(previous: dict, current: dict, new_numbers: list[str]) -> None:
    prev_total = int(previous.get("total", 0))
    curr_total = int(current.get("total", 0))

    subject = f"Synopticom alert: nauji registracijos numeriai ({len(new_numbers)})"
    body = (
        f"Rasti nauji registracijos numeriai po bendro skaičiaus padidėjimo.\n\n"
        f"Darbuotojas: {TARGET_NAME}\n"
        f"Total: {prev_total} -> {curr_total}\n\n"
        f"Nauji registracijos numeriai:\n"
        + "\n".join(f"  - {num}" for num in new_numbers)
        + "\n\n"
        f"Laikas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    send_email_alert(subject, body)


def run_once():
    log("[STEP] run_once start")
    current = download_and_extract_with_retry()
    previous = load_state()

    log(
        f"[INFO] Current row: "
        f"užpildyta={current['uzpildyta']}, "
        f"neužpildyta={current['neuzpildyta']}, "
        f"pradėta={current['pradeta']}, "
        f"total={current['total']}, "
        f"registrations={len(current.get('registration_numbers', []))}"
    )
    log(f"[INFO] Previous state: {previous}")

    if previous is None:
        save_state(current)
        log("[INFO] initial state saved")
        return

    previous_total = int(previous.get("total", 0))
    current_total = int(current["total"])
    previous_uzpildyta = int(previous.get("uzpildyta", 0))
    current_uzpildyta = int(current["uzpildyta"])

    prev_regs = set(previous.get("registration_numbers", []))
    curr_regs = set(current.get("registration_numbers", []))
    new_regs = sorted(curr_regs - prev_regs)

    if current_total > previous_total:
        log(f"[ALERT] Suma padidėjo: {previous_total} -> {current_total}")
        send_total_alert(previous, current)

        if new_regs:
            log(f"[ALERT] Nauji registracijos numeriai: {', '.join(new_regs)}")
            send_new_registration_numbers_alert(previous, current, new_regs)
        else:
            log("[INFO] total padidėjo, bet naujų registracijos numerių nerasta")

    elif current_total < previous_total:
        log(f"[INFO] Suma sumažėjo: {previous_total} -> {current_total} | total alert nesiunčiam")
    else:
        log("[INFO] total nepasikeitė | no total alert")

    if current_uzpildyta > previous_uzpildyta:
        log(f"[ALERT] Užpildyta padidėjo: {previous_uzpildyta} -> {current_uzpildyta}")
        send_uzpildyta_alert(previous, current)
    elif current_uzpildyta < previous_uzpildyta:
        log(f"[INFO] Užpildyta sumažėjo: {previous_uzpildyta} -> {current_uzpildyta} | alert nesiunčiam")
    else:
        log("[INFO] užpildyta nepasikeitė | no alert")

    save_state(current)


def main():
    log(f"[INFO] start | poll={POLL_SECONDS}s | state_file={STATE_FILE}")

    while True:
        try:
            run_once()
        except PlaywrightTimeoutError as e:
            log(f"[ERROR] Timeout: {e}")
        except Exception as e:
            message = str(e)
            log(f"[ERROR] {message}")

            if "LOGIN_FAILED:" in message:
                send_login_failed_alert(message)
                log("[ERROR] Login nepavyko. Skriptas stabdomas po pirmo nesėkmingo bandymo.")
                raise SystemExit(0)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
