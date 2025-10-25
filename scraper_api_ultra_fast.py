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

            # Apply filters
            print(f"\n📅 Setting filters for {month_filter}...")
            await page.wait_for_timeout(2000)

            try:
                await page.click('div.item___1xee2:has-text("History")', timeout=5000)
            except:
                pass
            await page.wait_for_timeout(2000)

            # Set month
            month_name = month_filter.split()[0]
            try:
                await page.click(
                    ".ant-picker.select___38REx .ant-picker-input", timeout=5000
                )
                await page.wait_for_timeout(2000)
                await page.click(
                    f'td.ant-picker-cell:has-text("{month_name}")', timeout=5000
                )
                await page.wait_for_timeout(1000)
            except:
                print("⚠ Month filter failed")

            await page.wait_for_timeout(2000)

            # Open filter and select channels
            await page.click(
                'button.operateBtn___13GXb:has-text("Filter")', timeout=5000
            )
            await page.wait_for_timeout(2000)
            await page.click("span.icon-ic_nav_expand", timeout=5000)
            await page.wait_for_timeout(1500)
            await page.click('img[src*="chooseChannel"]', timeout=5000)
            await page.wait_for_timeout(2000)

            # Set channel selector pagination to 50
            await page.click('.ant-select-selection--single[role="combobox"]')
            await page.wait_for_timeout(1500)
            await page.click('.ant-select-dropdown-menu-item:has-text("50 / page")')
            await page.wait_for_timeout(2000)

            # Select all channels
            await page.wait_for_selector("tr.ant-table-row[data-row-key]")
            channel_rows = await page.locator("tr.ant-table-row[data-row-key]").all()
            for row in channel_rows:
                try:
                    await row.click()
                    await page.wait_for_timeout(100)
                except:
                    pass

            print(f"✓ Selected {len(channel_rows)} channels")
            await page.click('button:has-text("Select"):not(:has-text("Select All"))')
            await page.wait_for_timeout(2000)

            # Click Query
            print("Clicking Query...")
            await page.click('button:has-text("Query")')
            await page.wait_for_timeout(5000)
            print("✓ Query clicked")

            # Wait for table
            await page.wait_for_selector("table tbody tr", timeout=10000)
            await page.wait_for_timeout(3000)

            # Set main table pagination to 50/page
            print("\n⚙️ Setting table pagination to 50/page...")
            for attempt in range(3):
                try:
                    all_pag = page.locator(
                        '.ant-select-selection--single[role="combobox"]'
                    )
                    count = await all_pag.count()

                    if count > 0:
                        last_pag = all_pag.last
                        current_text = await last_pag.text_content()

                        if "50" not in current_text:
                            await last_pag.click(timeout=5000)
                            await page.wait_for_timeout(1500)
                            await page.click(
                                '.ant-select-dropdown-menu-item:has-text("50 / page")',
                                timeout=5000,
                            )
                            await page.wait_for_timeout(3000)

                            new_text = await last_pag.text_content()
                            if "50" in new_text:
                                print("✓ Set to 50/page")
                                break
                        else:
                            print("✓ Already 50/page")
                            break
                except Exception as e:
                    await page.wait_for_timeout(2000)

            await page.wait_for_timeout(2000)

            # Process page by page
            print("\n" + "=" * 70)
            print("PROCESSING ORDERS PAGE BY PAGE")
            print("=" * 70)

            page_number = 1
            total_processed = 0
            success_count = 0
            error_count = 0
            start_time = time.time()
            all_orders = []

            while True:
                print(f"\n{'='*60}")
                print(f"PAGE {page_number}")
                print(f"{'='*60}")

                # Wait for table
                await page.wait_for_selector("table tbody tr", timeout=10000)
                await page.wait_for_timeout(1500)

                # Get order rows
                order_rows = await page.locator("table tbody tr[index]").all()

                if len(order_rows) == 0:
                    print("No rows found")
                    break

                print(f"Found {len(order_rows)} orders on this page")

                # Process each row
                for row_idx, row in enumerate(order_rows, 1):
                    try:
                        # Get order ID
                        cells = await row.locator("td").all()
                        if len(cells) < 2:
                            continue

                        order_id_elem = cells[0].locator("a")
                        if await order_id_elem.count() > 0:
                            order_id = (await order_id_elem.text_content()).strip()
                        else:
                            order_id = (await cells[0].text_content()).strip()

                        if not order_id or len(order_id) < 10:
                            continue

                        # Skip if already scraped
                        if order_id in already_scraped:
                            print(
                                f"  [{row_idx}/{len(order_rows)}] {order_id} - Already scraped"
                            )
                            continue

                        print(f"  [{row_idx}/{len(order_rows)}] {order_id}...")

                        # Get the last cell (should contain Details link)
                        last_cell = cells[-1]

                        # Try multiple methods to click Details
                        details_clicked = False

                        # Method 1: JavaScript click on last cell's Details link
                        try:
                            print(f"      Method 1: JavaScript click")
                            details_link = last_cell.locator('a:has-text("Details")')
                            await details_link.evaluate("element => element.click()")
                            details_clicked = True
                            print(f"      ✓ Clicked!")
                        except Exception as e1:
                            # Method 2: Force click
                            try:
                                print(f"      Method 2: Force click")
                                details_link = last_cell.locator(
                                    'a:has-text("Details")'
                                )
                                await details_link.click(force=True, timeout=3000)
                                details_clicked = True
                                print(f"      ✓ Clicked!")
                            except Exception as e2:
                                # Method 3: Try from row directly
                                try:
                                    print(f"      Method 3: Row click")
                                    details_link = row.locator(
                                        'a:has-text("Details")'
                                    ).last
                                    await details_link.click(force=True, timeout=3000)
                                    details_clicked = True
                                    print(f"      ✓ Clicked!")
                                except Exception as e3:
                                    # Method 4: Try exact text match
                                    try:
                                        print(f"      Method 4: Text exact match")
                                        await row.locator('text="Details"').click(
                                            force=True, timeout=3000
                                        )
                                        details_clicked = True
                                        print(f"      ✓ Clicked!")
                                    except Exception as e4:
                                        print(f"      ✗ All methods failed")
                                        print(f"         E1: {str(e1)[:30]}")
                                        print(f"         E2: {str(e2)[:30]}")
                                        print(f"         E3: {str(e3)[:30]}")
                                        print(f"         E4: {str(e4)[:30]}")
                                        error_count += 1
                                        continue

                        # Wait for API response (up to 5 seconds)
                        for wait_attempt in range(10):
                            if order_id in captured_details:
                                break
                            await asyncio.sleep(0.5)

                        # Check if we captured the response
                        if order_id in captured_details:
                            result = captured_details[order_id]
                            data = result["data"]

                            # Extract data
                            installation_list = data.get("installationInfoList", [])
                            installation_info = (
                                installation_list[0] if installation_list else {}
                            )
                            contact_dto = installation_info.get("custContactDto", {})
                            appointment_info = installation_info.get(
                                "appointmentInfo", {}
                            )
                            cust_info = data.get("custInfo", {})
                            order_items = data.get("orderItemList", [])

                            phone1 = contact_dto.get("contactNbr", "")
                            phone2 = contact_dto.get("alternateContactNbr", "")
                            phone_number = f"{phone1} / {phone2}" if phone2 else phone1

                            appt_start = appointment_info.get(
                                "appointmentStartTime", ""
                            )
                            appt_end = appointment_info.get("appointmentEndTime", "")
                            appointment_date = ""
                            if appt_start and appt_end:
                                appointment_date = f"{format_datetime(appt_start)} - {format_datetime(appt_end)}"
                            elif appt_start:
                                appointment_date = format_datetime(appt_start)

                            package = (
                                order_items[0].get("accNbr", "") if order_items else ""
                            )

                            cert_nbr = cust_info.get("certNbr", "")
                            cert_type = cust_info.get("certTypeName", "")
                            ic_number = (
                                f"{cert_nbr} ({cert_type})" if cert_type else cert_nbr
                            )

                            party_name = data.get("partyName", "")
                            party_code = data.get("partyStaffCode", "")
                            creator = (
                                f"{party_name} ({party_code})"
                                if party_code
                                else party_name
                            )

                            order_data = {
                                "order_number": data.get("custOrderNbr", ""),
                                "name": contact_dto.get("contactName", ""),
                                "email": contact_dto.get("email", ""),
                                "phone_number": phone_number,
                                "appointment_date": appointment_date,
                                "address": installation_info.get("displayAddress", ""),
                                "package": package,
                                "ic_number": ic_number,
                                "creator": creator,
                                "db_created": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }

                            all_orders.append(order_data)

                            # Update Sheet immediately
                            if worksheet:
                                if append_to_sheet(worksheet, order_data):
                                    print(
                                        f"✓ {contact_dto.get('contactName', 'N/A')[:20]} → Sheet"
                                    )
                                else:
                                    print(
                                        f"✓ {contact_dto.get('contactName', 'N/A')[:20]}"
                                    )
                            else:
                                print(f"✓ {contact_dto.get('contactName', 'N/A')[:20]}")

                            success_count += 1
                            already_scraped.add(order_id)

                            # Clear from cache
                            del captured_details[order_id]

                        else:
                            print(f"✗ No API response captured")
                            error_count += 1

                        # Close modal
                        try:
                            await page.click("button.ant-modal-close", timeout=2000)
                            await page.wait_for_timeout(300)
                        except:
                            try:
                                await page.keyboard.press("Escape")
                                await page.wait_for_timeout(300)
                            except:
                                pass

                        # Save progress every 10 orders
                        if success_count % 10 == 0:
                            with open(PROGRESS_FILE, "w") as f:
                                json.dump({"completed": list(already_scraped)}, f)

                    except Exception as e:
                        print(f"✗ {str(e)[:40]}")
                        error_count += 1

                        # Try to close any open modals
                        try:
                            await page.click("button.ant-modal-close", timeout=1000)
                        except:
                            try:
                                await page.keyboard.press("Escape")
                            except:
                                pass

                total_processed += len(order_rows)
                print(f"\nPage {page_number} complete - Total: {total_processed}")

                # Next page
                try:
                    next_button = page.locator("li.ant-pagination-next").first
                    await next_button.wait_for(state="visible", timeout=5000)

                    is_disabled = await next_button.get_attribute("aria-disabled")
                    if is_disabled == "true":
                        print(f"\n✓ Reached last page")
                        break

                    print(f"\nGoing to page {page_number + 1}...")
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                    page_number += 1

                except:
                    print(f"\n✓ No more pages")
                    break

            await browser.close()

            # Save final progress
            with open(PROGRESS_FILE, "w") as f:
                json.dump({"completed": list(already_scraped)}, f)

            # Save CSV
            if all_orders:
                import csv

                output_csv = f'{OUTPUT_DIR}/unifi_orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                fieldnames = [
                    "order_number",
                    "name",
                    "email",
                    "phone_number",
                    "appointment_date",
                    "address",
                    "package",
                    "ic_number",
                    "creator",
                    "db_created",
                ]
                with open(output_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_orders)
                print(f"\n✓ Saved to: {output_csv}")

            elapsed = int(time.time() - start_time)
            print("\n" + "=" * 70)
            print("📊 SUMMARY")
            print("=" * 70)
            print(f"Pages: {page_number}")
            print(f"Total orders: {total_processed}")
            print(f"Successfully scraped: {success_count}")
            print(f"Failed: {error_count}")
            if success_count > 0:
                print(
                    f"Success rate: {(success_count/(success_count+error_count)*100):.1f}%"
                )
            print(f"Time: {elapsed//60}m {elapsed%60}s")
            if success_count > 0:
                print(f"Speed: {elapsed/success_count:.1f}s per order")
            print("=" * 70)

            return {
                "success": True,
                "total": total_processed,
                "successful": success_count,
                "failed": error_count,
                "orders": all_orders,
            }

        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback

            traceback.print_exc()
            await browser.close()
            return {"success": False, "error": str(e)}


if __name__ == "__main__":
    import sys

    month = sys.argv[1] if len(sys.argv) > 1 else "Oct 2025"
    use_sheets = sys.argv[2] != "no-sheets" if len(sys.argv) > 2 else True

    result = asyncio.run(scrape_capture_api(month, use_sheets))

    if result.get("success"):
        print("\n✅ Scraping completed!")
    else:
        print(f"\n❌ Failed: {result.get('error')}")
