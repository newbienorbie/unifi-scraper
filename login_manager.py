"""
login_manager.py - Simplified login with session cache
"""

import json
import os
import time

from playwright.async_api import async_playwright

# CHANGED: Import from your existing telegram reader
from telegram_otp_reader import get_latest_otp

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
        except Exception as e:
            print(f"⚠️ Skipped storage clear: {e}")
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

    # --- NEW: Select OTP Channel (SMS) ---
    print("Selecting OTP Channel (SMS)...")
    try:
        # The input has opacity:0, so we must use force=True to click it
        await page.click("#login-form_channel", force=True)
        await page.wait_for_timeout(1000)  # Wait for dropdown animation

        # Select 'SMS' from the dropdown list
        await page.click("div.ant-select-item-option-content:has-text('SMS')")
        print("✅ Selected SMS channel")
        await page.wait_for_timeout(500)
    except Exception as e:
        print(f"⚠️ Error selecting OTP channel (continuing anyway): {e}")
    # -------------------------------------

    # Accept checkboxes and request OTP
    await page.locator('span.ant-checkbox input[type="checkbox"]').nth(0).check()
    await page.locator('span.ant-checkbox input[type="checkbox"]').nth(1).check()
    await page.click("text=GET")

    print("Waiting for OTP from Telegram...")

    # CHANGED: 'await' added because your telegram_otp_reader.py is async
    otp = await get_latest_otp()

    if otp:
        print(f"Using OTP: {otp}")
        await page.fill("input#login-form_smsCode", otp)
        await page.click('button:has-text("Sign In")')

        print("Sign In clicked, waiting for dashboard...")
        await page.wait_for_timeout(10000)

        print("Navigating to Retail History...")
        await page.goto(HISTORY_URL, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        # CHECK POPUP SECOND (It likely appears here)
        # ---------------------------------------------------------
        try:
            print("Checking for 'Later' popup on History page...")
            later_btn = page.locator('button.ant-btn:has-text("Later")')
            if await later_btn.is_visible(timeout=3000):
                print("✅ Found 'Later' popup. Clicking it...")
                await later_btn.click()
                await page.wait_for_timeout(1000)
            else:
                print("ℹ️ No 'Later' popup appeared.")
        except Exception:
            pass

        # Wait for app to initialize
        print("Waiting for app initialization...")
        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        await save_session(context)
        return browser, context, pw, page
    else:
        await browser.close()
        raise RuntimeError("Failed to retrieve OTP from Telegram")
