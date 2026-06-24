"""
backfill_custid.py - Fill missing Cust ID column from order detail API.
Opens each order detail to get custInfo.custId and writes it back.
"""

import asyncio
import os
import sys
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()

from gsheets_writer import open_sheet, month_tab_title
from login_manager import login_and_get_context
from credential_manager import CredentialManager


def get_orders_missing_custid(ws, force: bool = False) -> List[Dict]:
    """Find orders where Cust ID column is empty (or all orders if force=True)."""
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
    idx_cust_id = col_idx("Cust ID")

    if idx_order == -1 or idx_cust_id == -1:
        print("  Missing required columns (Order Number or Cust ID)")
        return orders

    for row_num, row in enumerate(records[1:], start=2):
        if not row or len(row) <= idx_order:
            continue

        order_number = row[idx_order].strip().lstrip("'")
        if not order_number:
            continue

        # Pad row
        while len(row) <= idx_cust_id:
            row.append("")

        cust_id = row[idx_cust_id].strip()
        if not cust_id or force:
            orders.append({
                "row_index": row_num,
                "order_number": order_number,
            })

    return orders


async def fetch_custid(context, order_id: str) -> str:
    """Open order detail in a new tab and capture custInfo.custId."""
    captured = {}

    async def intercept(response):
        try:
            if "getCeeOrderDetail" in response.url and response.status == 200:
                data = await response.json()
                if isinstance(data, dict) and data.get("data"):
                    cust_info = data["data"].get("custInfo", {}) or {}
                    cid = cust_info.get("custId", "")
                    if cid:
                        captured["custId"] = str(cid)
        except Exception:
            pass

    p = await context.new_page()
    p.on("response", intercept)

    try:
        url = f"https://dealer.unifi.com.my/esales/h5/onBoarding/OrderDetails?custOrderId={order_id}&custOrderNbr={order_id}"
        await p.goto(url, wait_until="networkidle", timeout=60000)

        for _ in range(20):
            if "custId" in captured:
                break
            await p.wait_for_timeout(500)

        await p.wait_for_timeout(500)
    except Exception as e:
        print(f"    Error: {e}")
    finally:
        try:
            await p.close()
        except Exception:
            pass

    return captured.get("custId", "")


async def backfill_custids(month_text: str, year: int):
    print(f"\n{'=' * 70}")
    print(f"BACKFILL CUST ID — {month_text} {year}")
    print(f"{'=' * 70}")

    spread = open_sheet()
    tab_title = month_tab_title(month_text, year)

    try:
        ws = spread.worksheet(tab_title)
    except Exception:
        print(f"  Tab '{tab_title}' not found — skipping")
        return

    # Ensure headers are up to date
    from gsheets_writer import HEADERS
    first_row = ws.row_values(1)
    if first_row != HEADERS:
        ws.update([HEADERS], "A1")
        print("  Updated headers")

    force = "--force" in sys.argv
    orders = get_orders_missing_custid(ws, force=force)
    label = "all" if force else "missing Cust ID"
    print(f"  Orders ({label}): {len(orders)}")

    if not orders:
        print("  Nothing to backfill")
        return

    creds = CredentialManager().get_credentials()
    browser, context, pw, page = await login_and_get_context(
        creds["username"], creds["password"]
    )

    headers = ws.row_values(1)
    custid_col = headers.index("Cust ID") + 1  # 1-based

    # Batch writes
    from gspread.utils import rowcol_to_a1
    batch = []
    filled = 0

    try:
        for i, order in enumerate(orders, 1):
            order_id = order["order_number"]
            row_idx = order["row_index"]

            print(f"  [{i}/{len(orders)}] {order_id}...", end=" ")

            cust_id = await fetch_custid(context, order_id)

            if cust_id:
                batch.append({
                    "range": rowcol_to_a1(row_idx, custid_col),
                    "values": [[cust_id]],
                })
                print(f"-> {cust_id}")
                filled += 1
            else:
                print("-> not found")

            # Flush every 100
            if len(batch) >= 100:
                ws.batch_update(batch, value_input_option="USER_ENTERED")
                batch = []

        # Flush remaining
        if batch:
            ws.batch_update(batch, value_input_option="USER_ENTERED")

    finally:
        await context.close()
        await browser.close()
        await pw.stop()

    print(f"\n  Filled {filled}/{len(orders)} Cust IDs")


if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv) > 1 else "Jun"
    year = int(sys.argv[2]) if len(sys.argv) > 2 else 2026

    asyncio.run(backfill_custids(month, year))
