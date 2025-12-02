"""
login_manager.py - Login with Telegram OTP and Gmail Fallback
"""

import json
import os
import time

from playwright.async_api import async_playwright

from gmail_otp_reader import get_latest_otp as get_gmail_otp

# Import BOTH readers with alias names to avoid conflict
from telegram_otp_reader import get_latest_otp as get_telegram_otp

LOGIN_URL = "https://dealer.unifi.com.my/esales/login"
HISTORY_URL = "https://dealer.unifi.com.my/esales/retailHistory"
SESSION_PATH = "sessions/session_cache.json"


async def _launch_browser_safe():
    pw = await async_playwright().start()
    try:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--max-old-space-size=512",
            ],
        )
    except Exception as e:
        raise RuntimeError(f"PLAYWRIGHT_BROWSER_LAUNCH_FAILED: {e}") from e
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_timeout(45000)
    page.set_default_navigation_timeout(60000)
    print("[BROWSER] engine=playwright chromium (no channel)")
    return pw, browser, context, page


async def load_session(context):
    """Load cached cookies if <1 day old"""
    if not os.path.exists(SESSION_PATH):
        return None
    try:
        with open(SESSION_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        return None

    age_seconds = time.time() - data.get("last_login", 0)
    age_days = age_seconds / 86400

    if age_seconds > 86400:
        print(f"⏰ Session expired ({age_days:.1f} days old, max 1 day)")
        return None

    print(f"Session age: {age_days:.1f} days (valid for {1 - age_days:.2f} more days)")

    cookies = data.get("cookies", [])
    if cookies:
        page = await context.new_page()
        try:
            await page.goto(
                "https://dealer.unifi.com.my/esales/login",
                timeout=30000,
                wait_until="domcontentloaded",
            )
            await page.evaluate(
                "() => { try { localStorage.clear(); sessionStorage.clear(); } catch (e) { /* ignore */ } }"
            )
        except Exception:
            pass
        finally:
            await page.close()

        await context.add_cookies(cookies)
        print("Loaded existing session cookies")
        return data

    return None


async def save_session(context):
    """Save cookies to disk"""
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    cookies = await context.cookies()
    payload = {
        "cookies": cookies,
        "last_login": time.time(),
    }
    with open(SESSION_PATH, "w") as f:
        json.dump(payload, f)
    print("Session cookies saved")


async def login_and_get_context(username: str, password: str):
    pw, browser, context, page = await _launch_browser_safe()

    # Try reuse session
    session = await load_session(context)
    if session:
        try:
            print("Testing cached session...")
            await page.goto(HISTORY_URL, timeout=30000)
            await page.wait_for_timeout(3000)

            current_url = page.url

            if "login" in current_url.lower():
                print("Redirected to login - session invalid")
            else:
                login_form_present = (
                    await page.locator("input#login-form_staffCode").count() > 0
                )
                if login_form_present:
                    print("❌ Login form detected - session invalid")
                else:
                    try:
                        await page.wait_for_selector(
                            'div.item___1xee2:has-text("History")', timeout=5000
                        )
                        await page.click(
                            'div.item___1xee2:has-text("History")', timeout=3000
                        )
                        await page.wait_for_timeout(1000)

                        month_picker = await page.locator(
                            ".ant-picker.select___38REx"
                        ).count()
                        if month_picker > 0:
                            print("Cached session valid - using it!")
                            return browser, context, pw, page
                        else:
                            print("Month picker not found - session invalid")
                    except:
                        print("Cannot interact with History tab - session invalid")
        except Exception as e:
            print(f"⚠️ Session test failed: {e}")

    # Fresh login
    print("Opening login page...")
    await page.goto(LOGIN_URL, timeout=30000)

    # Fill credentials
    await page.fill("#login-form_staffCode", username)
    await page.fill("#login-form_password", password)

    # Accept checkboxes and request OTP
    await page.locator('span.ant-checkbox input[type="checkbox"]').nth(0).check()
    await page.locator('span.ant-checkbox input[type="checkbox"]').nth(1).check()
    await page.click("text=GET")

    # === HYBRID OTP LOGIC ===
    print("📨 OTP Requested. Attempting to fetch code...")

    otp = None

    # 1. Try Telegram First (Wait max 120s)
    print("🔹 Primary: Listening to Telegram...")
    try:
        otp = await get_telegram_otp(max_wait=120)  # <--- NEW (Async with 'await')
    except Exception as e:
        print(f"⚠️ Telegram reader error: {e}")

    # 2. If Telegram failed, try Gmail Fallback (Wait max 180s)
    if not otp:
        print("🔸 Fallback: Telegram timed out. Checking Gmail...")
        try:
            # Note: Gmail reader uses 'max_age_seconds' for wait time
            otp = get_gmail_otp(max_age_seconds=180)
        except Exception as e:
            print(f"⚠️ Gmail reader error: {e}")

    # 3. If both failed
    if not otp:
        await browser.close()
        raise RuntimeError(
            "❌ CRITICAL: Failed to receive OTP from both Telegram and Gmail."
        )

    print(f"✅ Using OTP: {otp}")
    await page.fill("input#login-form_smsCode", otp)

    await page.click('button:has-text("Sign In")')
    print("Sign In clicked, waiting for dashboard...")
    await page.wait_for_timeout(5000)

    print("Navigating to Retail History...")
    await page.goto(HISTORY_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)
    print("On Retail History page")

    # Wait for app to initialize
    print("Waiting for app initialization...")
    await page.wait_for_load_state("networkidle", timeout=30000)
    await page.wait_for_timeout(2000)

    await save_session(context)
    return browser, context, pw, page
