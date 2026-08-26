"""
login_manager.py - Resilient login with session cache and anti-bot stealth
"""

import json
import os
import re
import time

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from gmail_otp_reader import get_latest_otp

LOGIN_URL = "https://dealer.unifi.com.my/esales/login"
HISTORY_URL = "https://dealer.unifi.com.my/esales/retailHistory"
SESSION_PATH = "sessions/session_cache.json"

STEALTH_SCRIPT = """
(function() {
    // fishx references a global trackSensors() (sensor telemetry) that is never
    // defined in our automated context, so its axios response interceptor throws
    // on every FishModule API call and the Order Entry form never renders. Its
    // return value is unused, so a no-op stub fully satisfies it.
    if (typeof window.trackSensors === 'undefined') {
        window.trackSensors = function() {};
    }
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [
        { name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }
    ]});
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
    window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
    Object.defineProperty(window, 'outerWidth',  { get: () => 1280 });
    Object.defineProperty(window, 'outerHeight', { get: () => 800 });
    Object.defineProperty(window, 'innerWidth',  { get: () => 1280 });
    Object.defineProperty(window, 'innerHeight', { get: () => 800 });

    window.location.reload = function() {
        console.warn('[STEALTH] Blocked location.reload()');
    };

    function noop() { return { start: noop, stop: noop }; }
    try {
        Object.defineProperty(window, 'DisableDevtool', {
            get: () => noop, set: () => {}, configurable: false
        });
    } catch(e) { window.DisableDevtool = noop; }

    const _realSetInterval = window.setInterval;
    window.setInterval = function(fn, delay) {
        return _realSetInterval(function() {
            try { fn.apply(this, arguments); } catch(e) {}
        }, delay);
    };
    const _realSetTimeout = window.setTimeout;
    window.setTimeout = function(fn, delay) {
        return _realSetTimeout(function() {
            try { fn.apply(this, arguments); } catch(e) {}
        }, delay);
    };

    const BLOCK = ['no-devtool', 'disable-devtool'];
    function isBlocked(url) {
        return BLOCK.some(p => String(url || '').toLowerCase().includes(p));
    }
    const _assign  = window.location.assign.bind(window.location);
    const _replace = window.location.replace.bind(window.location);
    window.location.assign  = function(url) { if (isBlocked(url)) return; return _assign(url); };
    window.location.replace = function(url) { if (isBlocked(url)) return; return _replace(url); };
    try {
        Object.defineProperty(window.location, 'href', {
            get: () => window.location.toString(),
            set: function(val) { if (isBlocked(val)) return; _assign(val); }
        });
    } catch(e) {}
    const _push     = history.pushState.bind(history);
    const _replace2 = history.replaceState.bind(history);
    history.pushState    = function(s,t,u) { if (isBlocked(u)) return; return _push(s,t,u); };
    history.replaceState = function(s,t,u) { if (isBlocked(u)) return; return _replace2(s,t,u); };

    ['_phantom','__phantom','callPhantom','_selenium','awesomium'].forEach(k => {
        try { Object.defineProperty(window, k, { get: () => undefined }); } catch(e) {}
    });
})();
"""


def _patch_script(body: str, url: str) -> str:
    original = body
    fname = url.split("/")[-1]

    # 1. Kill the disable-devtool initialization block:
    #    function() { window.location.href = "..." } OR () => { window.location.href = "..." }
    body = re.sub(
        r'ondevtoolopen:\s*(?:function\s*\(\)|(?:\(\)\s*=>))\s*\{\s*window\.location\.href\s*=\s*["\'][^"\']*["\']\s*;?\s*\}',
        "ondevtoolopen: function() { /* patched */ }",
        body,
    )

    # 2. Kill the default ondevtoolopen handler (d function that does history.back + redirect)
    body = re.sub(
        r"(ondevtoolopen:\s*)d,",
        r"\1function(){},",
        body,
    )

    # 3. Neuter clearLog
    body = re.sub(
        r"clearLog:\s*!0",
        "clearLog: !1",
        body,
    )

    # 4. Kill window.location.reload calls
    body = body.replace("window.location.reload(!0)", "void 0/* reload blocked */")
    body = body.replace("window.location.reload()", "void 0/* reload blocked */")
    body = body.replace("location.reload()", "void 0/* reload blocked */")

    # 5. Block no-devtool redirects
    body = body.replace("/esales/no-devtool.html", "/esales/login")
    body = body.replace('"no-devtool.html"', '"login"')
    body = body.replace("'no-devtool.html'", "'login'")

    # 6. Kill the disable-devtool auto-init attribute check
    body = body.replace("[disable-devtool-auto]", "[disable-devtool-disabled]")

    # 7. Legacy patterns (in case old format still exists)
    body = re.sub(
        r"Hy\(\)\(\{[^}]*ondevtoolopen:function\(\)\{[^}]*\}\}\)",
        "void 0/* DisableDevtool removed */",
        body,
    )

    if body != original:
        print(f"    ↳ Patched: {fname}")
    else:
        print(f"    ↳ ⚠️  No pattern matched in: {fname}")

    return body


async def _launch_browser_safe():
    pw = await async_playwright().start()
    try:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--start-maximized",
                "--disable-gpu",
                "--disable-extensions",
                "--single-process",
            ],
            ignore_default_args=["--enable-automation"],
        )
    except Exception as e:
        raise RuntimeError(f"PLAYWRIGHT_BROWSER_LAUNCH_FAILED: {e}") from e

    context = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "sec-ch-ua": '"Not=A?Brand";v="24", "Chromium";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        },
    )

    await context.add_init_script(STEALTH_SCRIPT)
    stealth = Stealth()
    await stealth.apply_stealth_async(context)

    fishx_cache = {}

    async def handle_fishx_route(route):
        """Intercept fishx scripts. Patch once, serve from cache after."""
        url = route.request.url
        cache_key = url.split("?")[0]

        if cache_key in fishx_cache:
            await route.fulfill(
                status=200,
                headers={"Content-Type": "application/javascript"},
                body=fishx_cache[cache_key],
            )
            return

        try:
            response = await route.fetch()
            body = await response.text()
            patched = _patch_script(body, url)
            fishx_cache[cache_key] = patched
            await route.fulfill(
                status=response.status,
                headers=dict(response.headers),
                body=patched,
            )
        except Exception as e:
            print(f"⚠️ fishx intercept failed for {url}: {e}")
            await route.continue_()

    async def block_anti_bot_urls(route):
        """Block known anti-bot redirect URLs."""
        print(f"🛡️ Blocked URL: {route.request.url}")
        await route.abort()

    async def fix_fishmodule_module_path(route):
        """FishModule's loader resolves module scripts to a bad doubled path
        (.../FishModule/esales/modules/... → 404), which breaks the Order Entry
        form. Refetch from the correct .../FishModule/modules/... path."""
        url = route.request.url
        fixed = url.replace("/FishModule/esales/modules/", "/FishModule/modules/")
        try:
            resp = await route.fetch(url=fixed)
            body = await resp.body()
            await route.fulfill(status=resp.status, headers=dict(resp.headers), body=body)
        except Exception as e:
            print(f"⚠️ FishModule path fix failed for {url}: {e}")
            await route.continue_()

    await context.route("**/fishx*.js", handle_fishx_route)
    await context.route("**/FishModule/esales/modules/**", fix_fishmodule_module_path)
    await context.route("**/*no-devtool*", block_anti_bot_urls)
    await context.route("**/*disable-devtool*", block_anti_bot_urls)

    page = await context.new_page()
    page.set_default_timeout(45000)
    page.set_default_navigation_timeout(90000)

    async def on_frame_navigated(frame):
        try:
            if any(k in frame.url.lower() for k in ["no-devtool", "disable-devtool"]):
                print(f"⚠️ Frame navigated to blocked page — going back: {frame.url}")
                await page.go_back()
        except Exception:
            pass

    page.on("framenavigated", on_frame_navigated)

    print("[BROWSER] engine=playwright chromium (Headed + Stealth + surgical patch)")
    return pw, browser, context, page


async def load_session(context):
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
            await page.goto(LOGIN_URL, timeout=30000, wait_until="domcontentloaded")
            await page.evaluate(
                "() => { try { localStorage.clear(); sessionStorage.clear(); } catch(e) {} }"
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
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    cookies = await context.cookies()
    payload = {"cookies": cookies, "last_login": time.time()}
    with open(SESSION_PATH, "w") as f:
        json.dump(payload, f)
    print("Session cookies saved")


async def login_and_get_context(username: str, password: str):
    pw, browser, context, page = await _launch_browser_safe()

    session = await load_session(context)
    if session:
        try:
            print("Testing cached session...")
            await page.goto(HISTORY_URL, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            if (
                "login" in page.url.lower()
                or "no-devtool" in page.url.lower()
                or await page.locator("input#login-form_staffCode").count() > 0
            ):
                print("❌ Session invalid - proceeding to fresh login")
            else:
                try:
                    await page.locator('text="History"').last.click(timeout=5000)
                    await page.wait_for_timeout(1000)
                    if await page.locator(".ant-picker").count() > 0:
                        print("✅ Cached session valid - using it!")
                        return browser, context, pw, page
                except Exception:
                    pass
        except Exception as e:
            print(f"⚠️ Session test failed: {e}")

    print("Opening login page...")
    await page.goto(LOGIN_URL, timeout=45000, wait_until="domcontentloaded")

    if "no-devtool" in page.url.lower():
        raise RuntimeError("❌ Redirected to no-devtool on login load.")

    await page.wait_for_selector(
        "#login-form_staffCode", state="visible", timeout=30000
    )
    # Wait for the full form to render (OTP field, channel dropdown)
    try:
        await page.wait_for_selector(
            "#login-form_smsCode", state="visible", timeout=15000
        )
        print("  ✅ OTP field loaded")
    except Exception:
        print("  ⚠️ OTP field not found, waiting longer...")
        await page.wait_for_timeout(5000)
    await page.wait_for_timeout(1500)

    await page.fill("#login-form_staffCode", username)
    await page.fill("#login-form_password", password)

    # --- Select OTP Channel (Email) ---
    # The portal renders this dropdown in Chinese on some loads (邮箱 = Email,
    # 短信 = SMS) regardless of the En toggle, so match both languages.
    # #login-form_channel is a hidden antd input — click the visible
    # .ant-select-selector to actually open the menu.
    print("Selecting OTP Channel (Email)...")
    try:
        selector = page.locator("#login-form_channel").locator(
            "xpath=ancestor::div[contains(@class,'ant-select-selector')][1]"
        )
        if await selector.count() == 0:
            selector = page.locator(".ant-select-selector").last
        await selector.click(force=True, timeout=5000)
        await page.wait_for_selector(
            ".ant-select-dropdown:not(.ant-select-dropdown-hidden)",
            state="visible",
            timeout=8000,
        )
        await page.wait_for_timeout(500)
        await page.locator(".ant-select-item-option-content").filter(
            has_text=re.compile(r"Email|邮箱", re.I)
        ).first.click(force=True, timeout=8000)
        print("✅ Selected Email channel")
        await page.wait_for_timeout(1000)
    except Exception as e:
        print(f"⚠️ Error selecting OTP channel: {e}")

    # --- Accept Checkboxes ---
    print("Accepting Terms and Conditions...")
    try:
        await page.locator("input#login-form_rememerMe").check(force=True, timeout=3000)
        print("  ✅ Remember Me checked")
    except Exception as e:
        print(f"  ⚠️ Remember Me checkbox: {e}")
    try:
        # Try multiple selectors for T&C checkbox (CSS hash may change)
        tc_checked = False
        tc_selectors = [
            '.policy___1uV3w input[type="checkbox"]',
            'div[class*="policy"] input[type="checkbox"]',
            'input[type="checkbox"]:not(#login-form_rememerMe)',
        ]
        for sel in tc_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.check(force=True, timeout=3000)
                    tc_checked = True
                    print(f"  ✅ T&C checked via: {sel}")
                    break
            except Exception:
                continue
        if not tc_checked:
            print("  ⚠️ Could not find T&C checkbox")
        await page.wait_for_timeout(1000)
    except Exception as e:
        print(f"  ⚠️ Warning clicking checkboxes: {e}")

    # --- Request OTP ---
    print("Requesting OTP...")
    try:
        await page.click("text=GET", timeout=5000)
        print("✅ Clicked GET button")
    except Exception as e:
        print(f"⚠️ Warning clicking GET: {e}")

    # Screenshot to confirm GET was clicked and OTP field appeared
    os.makedirs("logs", exist_ok=True)
    await page.wait_for_timeout(2000)
    await page.screenshot(path="logs/after_get_click.png")
    print("📸 Screenshot saved to logs/after_get_click.png")

    print("Waiting for OTP from Email...")
    import asyncio

    # Runs the synchronous Gmail reader in a separate thread so Playwright doesn't freeze
    otp = await asyncio.to_thread(get_latest_otp)

    if otp:
        print(f"Using OTP: {otp}")

        # Page may have reloaded after GET click — wait for form to be ready
        print("Waiting for login form to stabilize...")
        try:
            await page.wait_for_selector(
                "#login-form_staffCode", state="visible", timeout=15000
            )
        except Exception:
            print("  ⚠️ Form not found, reloading login page...")
            await page.goto(LOGIN_URL, timeout=45000, wait_until="domcontentloaded")
            await page.wait_for_selector(
                "#login-form_staffCode", state="visible", timeout=30000
            )

        # Wait for OTP field to appear
        try:
            await page.wait_for_selector(
                "#login-form_smsCode", state="visible", timeout=15000
            )
        except Exception:
            print("  ⚠️ OTP field not visible, waiting longer...")
            await page.wait_for_timeout(5000)

        # Re-fill form fields — page reload may clear some or all
        staff_val = await page.input_value("#login-form_staffCode")
        if not staff_val:
            print("  Page was reloaded — re-filling username & password...")
            await page.fill("#login-form_staffCode", username)
        await page.fill("#login-form_password", password)

        # Always re-select Email channel (dropdown resets on reload even if text fields survive).
        # Match English "Email" or Chinese "邮箱" — the portal's language is inconsistent.
        try:
            item = page.locator(".ant-select-selection-item").last
            channel_text = await item.inner_text() if await item.count() > 0 else ""
            if "Email" not in channel_text and "邮箱" not in channel_text:
                print("  Re-selecting Email channel...")
                selector = page.locator("#login-form_channel").locator(
                    "xpath=ancestor::div[contains(@class,'ant-select-selector')][1]"
                )
                if await selector.count() == 0:
                    selector = page.locator(".ant-select-selector").last
                await selector.click(force=True, timeout=5000)
                await page.wait_for_selector(
                    ".ant-select-dropdown:not(.ant-select-dropdown-hidden)",
                    state="visible",
                    timeout=8000,
                )
                await page.wait_for_timeout(500)
                await page.locator(".ant-select-item-option-content").filter(
                    has_text=re.compile(r"Email|邮箱", re.I)
                ).first.click(force=True, timeout=8000)
                await page.wait_for_timeout(500)
        except Exception as e:
            print(f"  ⚠️ Re-select channel: {e}")

        # Always re-check checkboxes (they reset on reload)
        try:
            rm = page.locator("input#login-form_rememerMe")
            if not await rm.is_checked():
                await rm.check(force=True, timeout=3000)
        except Exception:
            pass
        for sel in [
            '.policy___1uV3w input[type="checkbox"]',
            'div[class*="policy"] input[type="checkbox"]',
            'input[type="checkbox"]:not(#login-form_rememerMe)',
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    if not await loc.is_checked():
                        await loc.check(force=True, timeout=3000)
                        print(f"  ✅ T&C re-checked")
                    break
            except Exception:
                continue

        try:
            otp_field = page.locator("input#login-form_smsCode")
            await otp_field.wait_for(state="visible", timeout=15000)
            await otp_field.fill(otp, force=True)
            print("✅ Filled OTP")
        except Exception as e:
            os.makedirs("logs", exist_ok=True)
            await page.screenshot(path="logs/otp_fill_failed.png")
            raise RuntimeError(
                f"Could not fill OTP field. Check logs/otp_fill_failed.png. Error: {e}"
            )

        # Final verification — ensure all fields are filled before Sign In
        final_staff = await page.input_value("#login-form_staffCode")
        final_pass = await page.input_value("#login-form_password")
        final_otp = await page.input_value("#login-form_smsCode")

        tc_checked = False
        for sel in [
            '.policy___1uV3w input[type="checkbox"]',
            'div[class*="policy"] input[type="checkbox"]',
            'input[type="checkbox"]:not(#login-form_rememerMe)',
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    tc_checked = await loc.is_checked()
                    break
            except Exception:
                continue

        print(
            f"  Pre-submit check: staff={'✅' if final_staff else '❌'} pass={'✅' if final_pass else '❌'} otp={'✅' if final_otp else '❌'} T&C={'✅' if tc_checked else '❌'}"
        )
        if not final_staff:
            await page.fill("#login-form_staffCode", username)
        if not final_pass:
            await page.fill("#login-form_password", password)
        if not final_otp:
            await page.locator("input#login-form_smsCode").fill(otp, force=True)
        if not tc_checked:
            for sel in [
                '.policy___1uV3w input[type="checkbox"]',
                'div[class*="policy"] input[type="checkbox"]',
                'input[type="checkbox"]:not(#login-form_rememerMe)',
            ]:
                try:
                    loc = page.locator(sel).first
                    if await loc.count() > 0:
                        await loc.check(force=True, timeout=3000)
                        break
                except Exception:
                    continue

        await page.screenshot(path="logs/before_sign_in.png")
        print("📸 Screenshot saved to logs/before_sign_in.png")

        print("Clicking Sign In...")
        try:
            await page.click('button:has-text("Sign In")', force=True, timeout=5000)
        except Exception:
            await page.locator('button[type="submit"]').click(force=True)

        print("Sign In clicked, waiting for dashboard...")
        await page.wait_for_timeout(10000)

        # Check if login succeeded
        await page.screenshot(path="logs/after_sign_in.png")
        print(f"  📍 URL after sign in: {page.url}")
        if "login" in page.url.lower():
            print("  ⚠️ Still on login page after Sign In, waiting longer...")
            await page.wait_for_timeout(15000)
            await page.screenshot(path="logs/after_sign_in_extra_wait.png")
            print(f"  📍 URL after extra wait: {page.url}")

        print("Navigating to Retail History...")
        await page.goto(HISTORY_URL, wait_until="networkidle", timeout=90000)
        await page.wait_for_timeout(3000)

        print(f"  📍 URL after navigation: {page.url}")
        if "login" in page.url.lower():
            await page.screenshot(path="logs/login_redirect.png")
            print("  ❌ Redirected back to login — authentication failed")

        try:
            later_btn = page.locator('button.ant-btn:has-text("Later")')
            if await later_btn.is_visible(timeout=3000):
                print("✅ Found 'Later' popup. Clicking it...")
                await later_btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        print("Waiting for app initialization...")
        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        await save_session(context)
        return browser, context, pw, page
    else:
        await browser.close()
        raise RuntimeError("Failed to retrieve OTP from Email")
