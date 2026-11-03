import time
import synopticom_nps_watch_MB_codegen_based_NO_FSTRING_HTML_v10_READY as kiosk
import synopticom_alert_watch as survey


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [COMBINED] {msg}", flush=True)


def main() -> None:
    kiosk_last_hash = None
    poll_seconds = kiosk.POLL_SECONDS

    log(f"start | poll={poll_seconds}s")

    while True:
        try:
            log("starting kiosk cycle")
            kiosk_last_hash = kiosk.run_once(kiosk_last_hash)
            log("finished kiosk cycle")
        except Exception as e:
            log(f"kiosk cycle error: {e}")

        try:
            log("starting survey-alert cycle")
            survey.run_once()
            log("finished survey-alert cycle")
        except Exception as e:
            log(f"survey-alert cycle error: {e}")

        log(f"sleeping {poll_seconds}s")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
