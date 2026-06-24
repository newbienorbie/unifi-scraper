"""
backfill_company.py - Fill missing Company Name (col H) for business orders.
Reads orders from Google Sheet where IC Number contains "Company" or "Business"
but Company Name is empty, then fetches the order detail API to get custInfo.custName.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()

from playwright.async_api import Page

from gsheets_writer import HEADERS, open_sheet, month_tab_title
from login_manager import login_and_get_context

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

LOCAL_TZ = ZoneInfo("Asia/Kuala_Lumpur") if ZoneInfo else None


def get_orders_missing_company(ws) -> List[Dict]:
    """
    Find orders where IC Number indicates a business but Company Name is empty.
    """
    orders = []
    records = ws.get_all_values()
    if not records or len(records) <= 1:
        return orders

    headers = records[0]

    def col_idx(name):
        try:
            return headers.index(name)
        except ValueError:
            return -1

    idx_order = col_idx("Order Number")
    idx_company = col_idx("Company Name")
    idx_ic = col_idx("IC Number")

    if idx_order == -1 or idx_ic == -1 or idx_company == -1:
        print("  Missing required columns")
        return orders

    business_keywords = ["company", "business", "registration"]

    for row_num, row in enumerate(records[1:], start=2):
        if not row or len(row) <= idx_order:
            continue

        order_number = row[idx_order].strip().lstrip("'")
        if not order_number:
            continue

        # Pad row
        while len(row) <= max(idx_company, idx_ic):
            row.append("")

        ic_field = row[idx_ic].strip()
        company_name = row[idx_company].strip()

        # Only process if IC type indicates business AND company name is empty
        if not ic_field:
            continue

        is_business = any(kw in ic_field.lower() for kw in business_keywords)
        if is_business and not company_name:
            orders.append({
                "row_index": row_num,
                "order_number": order_number,
            })

    return orders


async def fetch_company_name(page: Page, context, order_id: str) -> str:
    """
    Open order detail in a new tab and capture the custInfo.custName.
    """
    captured = {}

    async def intercept(response):
        try:
            if "getCeeOrderDetail" in response.url and response.status == 200:
                data = await response.json()
                if isinstance(data, dict) and data.get("data"):
                    cust_info = data["data"].get("custInfo", {})
                    cust_type = cust_info.get("custType", "")
                    cert_type = cust_info.get("certTypeName", "").lower()
                    if cust_type == "B" or "business" in cert_type or "company" in cert_type:
                        captured["company"] = cust_info.get("custName", "")
        except Exception:
            pass

    p = await context.new_page()
    p.on("response", intercept)

    try:
        url = f"https://dealer.unifi.com.my/esales/h5/onBoarding/OrderDetails?custOrderId={order_id}&custOrderNbr={order_id}"
        await p.goto(url, wait_until="networkidle", timeout=60000)

        for _ in range(20):
            if "company" in captured:
                break
            await p.wait_for_timeout(500)

        await p.wait_for_timeout(500)
    except Exception as e:
        print(f"    Error fetching {order_id}: {e}")
    finally:
        try:
            await p.close()
        except Exception:
            pass

    return captured.get("company", "")


async def backfill_company_names(month_text: str, year: int):
    """
    Main function: find orders with missing company names and fill them.
    """
    print(f"\n{'=' * 70}")
    print(f"BACKFILL COMPANY NAMES - {month_text} {year}")
    print(f"{'=' * 70}")

    spread = open_sheet()
    tab_title = month_tab_title(month_text, year)

    try:
        ws = spread.worksheet(tab_title)
    except Exception:
        print(f"  Tab '{tab_title}' not found — skipping")
        return

    orders = get_orders_missing_company(ws)
    print(f"  Orders missing company name: {len(orders)}")

    if not orders:
        print("  Nothing to backfill")
        return

    from credential_manager import CredentialManager
    creds = CredentialManager().get_credentials()

    browser, context, pw, page = await login_and_get_context(
        creds["username"], creds["password"]
    )

    headers = ws.row_values(1)
    company_col = headers.index("Company Name") + 1  # 1-based

    filled = 0
    try:
        for i, order in enumerate(orders, 1):
            order_id = order["order_number"]
            row_idx = order["row_index"]

            print(f"  [{i}/{len(orders)}] {order_id}...", end=" ")

            company = await fetch_company_name(page, context, order_id)

            if company:
                ws.update_cell(row_idx, company_col, company)
                print(f"-> {company}")
                filled += 1
            else:
                print("-> not found")

    finally:
        await context.close()
        await browser.close()
        await pw.stop()

    print(f"\n  Filled {filled}/{len(orders)} company names")


if __name__ == "__main__":
    import sys

    month = sys.argv[1] if len(sys.argv) > 1 else "Jun"
    year = int(sys.argv[2]) if len(sys.argv) > 2 else 2026

    asyncio.run(backfill_company_names(month, year))
