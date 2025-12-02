"""
Flask API server for open scraping - NO AUTHORIZATION
Accessible to everyone including Telegram bots and any external services
"""

import asyncio
import io
import json
import os
import time
import uuid
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from threading import Lock, Thread
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request, send_file

from credential_manager import CredentialManager
from gsheets_writer import month_tab_title, open_sheet
from scrape_orders import scrape_month, scrape_orders_month

scrape_locks = defaultdict(Lock)
app = Flask(__name__)

# Initialize credential manager
cred_manager = CredentialManager()


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "Unifi Scraper API (Open Access)",
        }
    )


# ---- Minimal in-memory job registry ----
JOBS = (
    {}
)  # job_id -> {status, created_at, started_at, finished_at, params, result, error, log_path}
JOBS_LOCK = Lock()


def _jobs_dir():
    d = os.path.join(os.getcwd(), "jobs")
    os.makedirs(d, exist_ok=True)
    return d


def _logs_dir():
    d = os.path.join(os.getcwd(), "logs")
    os.makedirs(d, exist_ok=True)
    return d


def _run_job(job_id: str, params: dict):
    """
    Background runner: captures stdout/stderr to a log file and
    calls the existing synchronous scrape_month().
    """
    log_path = os.path.join(_logs_dir(), f"{job_id}.log")
    with open(log_path, "w", buffering=1) as lf, redirect_stdout(lf), redirect_stderr(
        lf
    ):
        print(
            f"[{datetime.utcnow().isoformat()}] Job {job_id} started with params: {json.dumps(params)}"
        )
        with JOBS_LOCK:
            job = JOBS.get(job_id, {})
            job["status"] = "running"
            job["started_at"] = datetime.utcnow().isoformat()
            job["log_path"] = log_path
            JOBS[job_id] = job
        try:
            # Required params
            month_text = (
                params.get("month_text")
                or params.get("month")
                or params.get("monthText")
            )
            year = params.get("year")
            if not month_text or not year:
                raise ValueError("month_text and year are required")

            # Optional param: full_sync (default True to capture everything)
            full_sync = bool(params.get("full_sync", True))

            print(
                f"→ Calling scrape_month(month_text={month_text!r}, year={year!r}, full_sync={full_sync})"
            )
            result = scrape_month(
                month_text=str(month_text), year=int(year), full_sync=full_sync
            )
            print(
                f"[{datetime.utcnow().isoformat()}] scrape_month finished, result: {result}"
            )

            with JOBS_LOCK:
                job = JOBS.get(job_id, {})
                job["status"] = "done"
                job["finished_at"] = datetime.utcnow().isoformat()
                job["result"] = result
                job["log_path"] = log_path
                JOBS[job_id] = job
        except Exception as e:
            print(f"[{datetime.utcnow().isoformat()}] ERROR: {e!r}")
            with JOBS_LOCK:
                job = JOBS.get(job_id, {})
                job["status"] = "error"
                job["finished_at"] = datetime.utcnow().isoformat()
                job["error"] = str(e)
                job["log_path"] = log_path
                JOBS[job_id] = job


@app.post("/jobs")
def create_job():
    """
    Start a long-running scrape without blocking the HTTP connection.
    GLOBAL LOCK ENFORCED: Rejects new jobs if ANY job is running.
    """
    data = request.get_json(silent=True) or {}

    # --- NEW: GLOBAL LOCK CHECK ---
    with JOBS_LOCK:
        for jid, job in JOBS.items():
            # If any job is currently active, REJECT the new one
            if job.get("status") in ["queued", "running"]:

                # Get info about the running job to show in the error
                params = job.get("params", {})
                running_month = params.get("month_text") or params.get("month") or "?"
                running_year = params.get("year") or "?"

                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "JOB_IN_PROGRESS",
                            "message": f"⚠️ System is busy processing {running_month} {running_year}.\n\nThe server can only run one scraper at a time. Please wait until it finishes.",
                        }
                    ),
                    409,
                )
    # ------------------------------

    # If no jobs are running, proceed to create the new one
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "queued",
            "created_at": datetime.utcnow().isoformat(),
            "params": data,
        }

    t = Thread(target=_run_job, args=(job_id, data), daemon=True)
    t.start()

    return jsonify({"job_id": job_id, "status": "queued"}), 202


@app.get("/jobs/<job_id>")
def job_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown_job"}), 404
    # Don't dump large results; return a summary
    resp = {k: v for k, v in job.items() if k not in {"result"}}
    # Include success shorthand if available
    if (
        "result" in job
        and isinstance(job["result"], dict)
        and "success" in job["result"]
    ):
        resp["success"] = job["result"]["success"]
        # small message if present
        if "message" in job["result"]:
            resp["message"] = job["result"]["message"]
    return jsonify({"job_id": job_id, **resp}), 200


@app.get("/jobs/<job_id>/log")
def job_log(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown_job"}), 404
    log_path = job.get("log_path")
    if not log_path or not os.path.exists(log_path):
        return jsonify({"log": ""}), 200
    # Stream back as text/plain
    with open(log_path, "r") as f:
        return app.response_class(f.read(), mimetype="text/plain")


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Main scraping endpoint - OPEN ACCESS
    Body: {
        "chat_id": "123456789",  // Optional - for logging/tracking only
        "month": "Oct",
        "year": 2025,
        "full_sync": true,  // true = capture all, false = incremental
        "output_format": "sheets"  // or "csv"
    }
    """
    try:
        data = request.get_json() or {}

        # Get parameters (chat_id is optional, just for logging)
        chat_id = data.get("chat_id", "anonymous")
        month_text = data.get("month", "Oct")
        year = int(data.get("year", 2025))
        full_sync = data.get("full_sync", False)
        output_format = data.get("output_format", "sheets")

        print(
            f"🔍 Scrape request from: {chat_id} | Mode: {'Full' if full_sync else 'Incremental'}"
        )

        # Check if credentials exist
        if not cred_manager.credentials_exist():
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_CREDENTIALS",
                        "message": "No credentials saved. Please save credentials first.",
                    }
                ),
                400,
            )

        # Get credentials
        creds = cred_manager.get_credentials()
        if not creds:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "INVALID_CREDENTIALS",
                        "message": "Could not decrypt credentials.",
                    }
                ),
                400,
            )

        username = creds["username"]
        password = creds["password"]

        if not month_text or not year:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "MONTH_YEAR_REQUIRED",
                        "message": "Month and year are required.",
                    }
                ),
                400,
            )

        # Run scraper
        result = asyncio.run(
            scrape_orders_month(
                username,
                password,
                month_text,
                year,
                output_format,
                None,
                full_sync,
            )
        )

        return jsonify(result)

    except Exception as e:
        return (
            jsonify({"success": False, "error": str(e), "message": "Scraping failed"}),
            500,
        )


from collections import defaultdict
from threading import Lock

# Change from single lock to per-month locks
scrape_locks = defaultdict(Lock)  # Instead of: scrape_lock = Lock()


@app.route("/scrape_full", methods=["POST"])
def scrape_full():
    """
    Scrape full month (OPEN ACCESS)
    Per-month locking to allow concurrent scrapes of different months
    """
    data = request.json or {}
    month = data.get("month")
    year = data.get("year")

    if not month or not year:
        return jsonify({"success": False, "error": "month and year required"}), 400

    # Use per-month lock
    lock_key = f"{month}_{year}"
    acquired = scrape_locks[lock_key].acquire(blocking=False)

    if not acquired:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "SCRAPE_IN_PROGRESS",
                    "message": f"Scrape for {month} {year} is already in progress. Please wait.",
                    "month": month,
                    "year": year,
                }
            ),
            429,
        )

    try:
        full_sync = data.get("full_sync", False)

        # Import here to avoid circular imports
        from scrape_orders import scrape_month

        # Run the scrape
        result = scrape_month(month, year, full_sync=full_sync)

        # Brief cleanup pause
        time.sleep(2)

        return jsonify(result)

    except Exception as e:
        print(f"❌ Error in scrape_full: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        # Always release the lock for this specific month
        scrape_locks[lock_key].release()


@app.route("/scrape_incremental", methods=["POST"])
def scrape_incremental():
    """
    Incremental sync mode - only new/updated orders (OPEN ACCESS)
    Body: {
        "chat_id": "123456789",  // Optional
        "month": "Oct",
        "year": 2025
    }
    """
    try:
        data = request.get_json() or {}
        data["full_sync"] = False  # Force incremental sync

        # Extract parameters
        chat_id = data.get("chat_id", "anonymous")
        month_text = data.get("month", "Oct")
        year = int(data.get("year", 2025))
        output_format = data.get("output_format", "sheets")

        print(
            f"🔍 Incremental scrape request from: {chat_id} | Month: {month_text} {year}"
        )

        # Check credentials
        if not cred_manager.credentials_exist():
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_CREDENTIALS",
                        "message": "No credentials saved. Please save credentials first.",
                    }
                ),
                400,
            )

        creds = cred_manager.get_credentials()
        if not creds:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "INVALID_CREDENTIALS",
                        "message": "Could not decrypt credentials.",
                    }
                ),
                400,
            )

        username = creds["username"]
        password = creds["password"]

        # Run scraper with full_sync=False
        result = asyncio.run(
            scrape_orders_month(
                username,
                password,
                month_text,
                year,
                output_format,
                None,
                False,  # full_sync=False
            )
        )

        return jsonify(result)

    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "message": "Incremental scraping failed",
                }
            ),
            500,
        )


@app.route("/save_credentials", methods=["POST"])
def save_credentials():
    """
    Save login credentials (OPEN ACCESS)
    Body: {
        "chat_id": "123456789",  // Optional - for logging only
        "username": "TMRS00517",
        "password": "your_password"
    }
    """
    try:
        data = request.get_json()

        chat_id = data.get("chat_id", "anonymous")
        username = data.get("username")
        password = data.get("password")

        print(f"💾 Credentials save request from: {chat_id}")

        if not username or not password:
            return (
                jsonify({"success": False, "error": "USERNAME_PASSWORD_REQUIRED"}),
                400,
            )

        # Save credentials
        cred_manager.save_credentials(username, password)

        return jsonify({"success": True, "message": "Credentials saved securely"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/download_csv", methods=["GET"])
def download_csv():
    """
    Download CSV file (OPEN ACCESS)
    Query params: filename=unifi_orders_20251020_123456.csv
    """
    filename = request.args.get("filename")

    if not filename:
        return jsonify({"error": "filename required"}), 400

    filepath = f"outputs/{filename}"

    if not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404

    return send_file(filepath, as_attachment=True)


@app.get("/get_current_summary")
def get_current_summary():
    """
    Returns ONLY the current month summary (Asia/Kuala_Lumpur local time).
    {
      "month": "Nov", "year": 2025,
      "total": 123, "completed": 100, "cancelled": 10, "other": 13
    }
    """
    try:
        # Resolve current month in KL time
        now = datetime.now(ZoneInfo("Asia/Kuala_Lumpur"))
        month_short = now.strftime("%b")  # e.g., "Nov"
        year = now.year

        # Open Google Sheet and select "<Mon> <Year>" tab
        spread = open_sheet()
        tab_title = month_tab_title(month_short, year)
        ws = spread.worksheet(tab_title)

        # Count statuses (same logic you use post-scrape)
        completed = cancelled = other = total = 0
        rows = ws.get_all_values()
        for row in rows[1:]:  # skip header
            if not row or len(row) < 2:
                continue
            order_number = (row[0] or "").strip().lstrip("'")
            status = (row[1] or "").strip()
            if not order_number:
                continue
            total += 1
            s = status.lower()
            if s.startswith("completed"):
                completed += 1
            elif s.startswith("cancelled") or s.startswith("canceled"):
                cancelled += 1
            else:
                other += 1

        return jsonify(
            {
                "month": month_short,
                "year": year,
                "total": total,
                "completed": completed,
                "cancelled": cancelled,
                "other": other,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/status", methods=["GET"])
def status():
    """Get system status (OPEN ACCESS)"""

    creds_exist = cred_manager.credentials_exist()
    cookies_exist = os.path.exists("sessions/session_cache.json")

    # Get latest CSV info
    latest_csv = None
    if os.path.exists("outputs"):
        csv_files = [f for f in os.listdir("outputs") if f.endswith(".csv")]
        if csv_files:
            csv_files.sort(reverse=True)
            latest_csv = csv_files[0]

    return jsonify(
        {
            "credentials_saved": creds_exist,
            "session_active": cookies_exist,
            "latest_scrape": latest_csv,
            "timestamp": datetime.now().isoformat(),
            "version": "open_access_dual_mode",
            "authorization": "disabled - open to everyone",
        }
    )


@app.route("/test_date_comparison", methods=["GET"])
def test_date_comparison():
    """Test endpoint to check date comparison logic (OPEN ACCESS)"""
    try:
        from scrape_orders import (
            parse_last_synced,
            parse_ui_date,
            should_rescrape_order,
        )

        # Test data
        ui_date = "29 Oct 2025 11:27:31"
        last_synced = "2025-10-29T10:00:00"

        ui_dt = parse_ui_date(ui_date)
        last_synced_dt = parse_last_synced(last_synced)

        should_rescrape = (
            should_rescrape_order("test_order", ui_date, {"test_order": last_synced_dt})
            if last_synced_dt
            else True
        )

        return jsonify(
            {
                "ui_date": ui_date,
                "ui_parsed": ui_dt.isoformat() if ui_dt else None,
                "last_synced": last_synced,
                "last_synced_parsed": (
                    last_synced_dt.isoformat() if last_synced_dt else None
                ),
                "should_rescrape": should_rescrape,
                "comparison": (
                    "UI is newer"
                    if ui_dt and last_synced_dt and ui_dt > last_synced_dt
                    else "Last synced is newer or equal"
                ),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/health/browser")
def health_browser():
    import asyncio

    from playwright.async_api import async_playwright

    async def _probe():
        async with async_playwright() as p:
            b = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            c = await b.new_context()
            pg = await c.new_page()
            await pg.goto("https://example.com", timeout=20000)
            await b.close()

    try:
        asyncio.run(_probe())
        return {"ok": True, "message": "browser runnable"}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@app.route("/get_months", methods=["GET"])
def get_months():
    """
    Get all month tabs from Google Sheets (OPEN ACCESS)
    Returns list of months that exist in the sheet
    """
    try:
        from gsheets_writer import get_all_month_tabs, open_sheet

        spread = open_sheet()
        month_tabs = get_all_month_tabs(spread)

        # Parse month tabs into structured data
        months = []
        for tab in month_tabs:
            # Tab format: "Nov 2025" or "Nov"
            parts = tab.split()
            month = parts[0]
            year = int(parts[1]) if len(parts) > 1 else 2025
            months.append({"month": month, "year": year, "tab": tab})

        return jsonify({"success": True, "months": months, "count": len(months)})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/get_latest_summary", methods=["GET"])
def get_latest_summary():
    """
    Get summary for the latest month from today's scrape (OPEN ACCESS)
    Returns only the most recent month (e.g., Nov 2025)
    """
    try:
        import glob
        from datetime import datetime

        summary_dir = "outputs/summaries"

        # Get all summary files from today
        today_str = datetime.now().strftime("%Y%m%d")
        pattern = f"{summary_dir}/summary_{today_str}_*.json"
        files = glob.glob(pattern)

        if not files:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_SUMMARY_TODAY",
                        "message": "No summary file found for today. Scraping may not have run yet.",
                    }
                ),
                404,
            )

        # Read all summaries and find the latest month
        latest_summary = None
        latest_date = (0, 0)  # (year, month)

        for file in files:
            try:
                with open(file, "r", encoding="utf-8") as f:
                    summary = json.load(f)

                    year = summary.get("year", 0)
                    month_name = summary.get("month", "")

                    # Convert month name to number
                    month_map = {
                        "Jan": 1,
                        "Feb": 2,
                        "Mar": 3,
                        "Apr": 4,
                        "May": 5,
                        "Jun": 6,
                        "Jul": 7,
                        "Aug": 8,
                        "Sep": 9,
                        "Oct": 10,
                        "Nov": 11,
                        "Dec": 12,
                    }
                    month_num = month_map.get(month_name, 0)

                    # Check if this is the latest
                    if (year, month_num) > latest_date:
                        latest_date = (year, month_num)
                        latest_summary = summary
            except:
                continue

        if not latest_summary:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_VALID_SUMMARY",
                        "message": "Could not read summary files.",
                    }
                ),
                500,
            )

        return jsonify({"success": True, "summary": latest_summary})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("config", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("jobs", exist_ok=True)

    print("🌍 Starting Unifi Scraper API (OPEN ACCESS)")
    print("🔓 No authorization required - accessible to everyone")
    print("📱 Telegram bots, external services, and local calls all welcome")

    # Run server
    app.run(host="0.0.0.0", port=5000, debug=False)
