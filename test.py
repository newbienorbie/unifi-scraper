"""
Scraper that clicks Details and captures API response
"""

import asyncio
import json
import os
import time
from datetime import datetime

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

from credential_manager import CredentialManager

# Configuration
COOKIES_FILE = "config/cookies.json"
OUTPUT_DIR = "outputs"
GSHEET_CREDS_FILE = "config/google_sheets_credentials.json"
GSHEET_NAME = "Unifi Orders"
WORKSHEET_NAME = "Orders"
PROGRESS_FILE = "outputs/scraping_progress.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("config", exist_ok=True)
cred_manager = CredentialManager()


def init_google_sheets():
    """Initialize Google Sheets connection"""
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        creds = Credentials.from_service_account_file(GSHEET_CREDS_FILE, scopes=scopes)

        import warnings

        warnings.filterwarnings("ignore", category=DeprecationWarning)
        client = gspread.authorize(creds)
        spreadsheet = client.open(GSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        return worksheet
    except Exception as e:
        print(f"⚠ Could not connect to Google Sheets: {e}")
        return None


def append_to_sheet(worksheet, order_data):
    """Append a single order to Google Sheets with retry"""
    if not worksheet:
        return False

    for attempt in range(3):
        try:
            row = [
                order_data.get("order_number", ""),
                order_data.get("name", ""),
                order_data.get("email", ""),
                order_data.get("phone_number", ""),
                order_data.get("appointment_date", ""),
                order_data.get("address", ""),
                order_data.get("package", ""),
                order_data.get("ic_number", ""),
                order_data.get("creator", ""),
                order_data.get("db_created", ""),
            ]

            worksheet.append_row(row)
            return True
        except Exception as e:
            if attempt == 2:
                return False
            time.sleep(2**attempt)

    return False


def format_datetime(datetime_str):
    """Convert 20251022093000 to '22 Oct 2025 09:30'"""
    if not datetime_str or len(datetime_str) != 14:
        return ""

    try:
        dt = datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
        return dt.strftime("%d %b %Y %H:%M")
    except:
        return datetime_str


async def auto_login_with_gmail_otp(page, chat_id):
    """Auto-login using Gmail OTP"""
    try:
        from gmail_otp_reader import get_latest_otp

        staff_code = cred_manager.get_credential("UNIFI_STAFF_CODE")
        password = cred_manager.get_credential("UNIFI_PASSWORD")

        await page.fill("input#login-form_staffCode", staff_code)
        await page.fill("input#login-form_password", password)
        await page.click('button[type="submit"]')
        await page.wait_for_timeout(3000)

        otp_input = await page.locator('input[placeholder="Please enter OTP"]').count()

        if otp_input > 0:
            print("Waiting for OTP from Gmail...")

            max_attempts = 12
            for attempt in range(max_attempts):
                otp_code = get_latest_otp(max_age_seconds=120)

                if otp_code:
                    print(f"✓ Got OTP: {otp_code}")
                    await page.fill('input[placeholder="Please enter OTP"]', otp_code)
                    await page.click('button:has-text("Login")')
                    await page.wait_for_timeout(3000)

                    error = await page.locator("div.ant-message-error").count()
                    if error > 0:
                        print("✗ OTP failed, waiting for new one...")
                        continue
                    else:
                        print("✓ Login successful!")
                        return True, "Login successful"

                await asyncio.sleep(10)

            return False, "OTP timeout"
        else:
            print("✓ Login successful (no OTP needed)")
            return True, "Login successful"

    except Exception as e:
        return False, f"Login error: {e}"


async def scrape_capture_api(month_filter="Oct 2025", update_sheets=True):
    """Scrape by clicking Details and capturing API response"""

    print("\n" + "=" * 70)
    print("UNIFI API CAPTURE SCRAPER")
    print("=" * 70)
    print(f"Month: {month_filter}")
    print(f"Google Sheets: {'Enabled' if update_sheets else 'Disabled'}")
    print("=" * 70)

    # Initialize Google Sheets
    worksheet = None
    if update_sheets:
        print("\nConnecting to Google Sheets...")
        worksheet = init_google_sheets()

        if worksheet:
            print("✓ Connected to Google Sheets")
            try:
                first_row = worksheet.row_values(1)
                if not first_row or first_row[0] != "Order Number":
                    headers = [
                        "Order Number",
                        "Name",
                        "Email",
                        "Phone Number",
                        "Appointment Date",
                        "Address",
                        "Package",
                        "IC Number",
                        "Creator",
                        "Scraped At",
                    ]
                    worksheet.insert_row(headers, 1)
            except:
                pass

    # Load progress
    already_scraped = set()
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                progress = json.load(f)
                already_scraped = set(progress.get("completed", []))
            print(f"📋 Already scraped: {len(already_scraped)} orders")
        except:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        # Load cookies
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, "r") as f:
                cookies = json.load(f)
            await context.add_cookies(cookies)

        page = await context.new_page()

        # Set up global API response listener
        captured_details = {}

        async def handle_response(response):
            """Capture getCeeOrderDetail responses"""
            if "getCeeOrderDetail" in response.url:
                try:
                    data = await response.json()
                    if data.get("code") == "200":
                        order_id = data.get("data", {}).get("custOrderNbr", "")
                        if order_id:
                            captured_details[order_id] = data
                except:
                    pass

        page.on("response", handle_response)

        try:
            # Login
            print("\n🔐 Logging in...")
            await page.goto(
                "https://dealer.unifi.com.my/esales/retailHistory",
                wait_until="networkidle",
            )
            await page.wait_for_timeout(3000)

            is_logged_in = False
            try:
                login_form = await page.locator("input#login-form_staffCode").count()
                if login_form == 0:
                    history_tab = await page.locator(
                        'div.item___1xee2:has-text("History")'
                    ).count()
                    if history_tab > 0:
                        is_logged_in = True
            except:
                pass

            if not is_logged_in:
                print("🔒 Logging in with OTP...")
                success, message = await auto_login_with_gmail_otp(page, None)
                if not success:
                    await browser.close()
                    return {"success": False, "error": "LOGIN_FAILED"}

                await page.goto(
                    "https://dealer.unifi.com.my/esales/retailHistory",
                    wait_until="networkidle",
                )
                await page.wait_for_timeout(3000)

            print("✓ Logged in")

            # Save cookies
            cookies = await context.cookies()
            with open(COOKIES_FILE, "w") as f:
                json.dump(cookies, f, indent=2)

        finally:
            print("success")


if __name__ == "__main__":
    import sys

    month = sys.argv[1] if len(sys.argv) > 1 else "Oct 2025"
    use_sheets = sys.argv[2] != "no-sheets" if len(sys.argv) > 2 else True

    result = asyncio.run(scrape_capture_api(month, use_sheets))

    if result.get("success"):
        print("\n✅ Scraping completed!")
    else:
        print(f"\n❌ Failed: {result.get('error')}")
