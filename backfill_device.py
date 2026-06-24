"""
backfill_device.py - Fill missing Device (col N) for orders with "device" in Package.
Opens each order detail to extract the SMART_DEVICE offer name.
"""

import asyncio
import os
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()

from playwright.async_api import Page

from gsheets_writer import open_sheet, month_tab_title
from login_manager import login_and_get_context


def get_orders_missing_device(ws) -> List[Dict]:
    """
    Find orders where Package contains 'device' but Device column is empty.
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
    idx_package = col_idx("Package")
    idx_device = col_idx("Device")

    if idx_order == -1 or idx_package == -1 or idx_device == -1:
        print("  Missing required columns")
        return orders

    for row_num, row in enumerate(records[1:], start=2):
        if not row or len(row) <= idx_order:
            continue

        order_number = row[idx_order].strip().lstrip("'")
        if not order_number:
            continue

        while len(row) <= max(idx_package, idx_device):
            row.append("")

        package = row[idx_package].strip()
        device = row[idx_device].strip()

        if "device" in package.lower() and not device:
            orders.append({
                "row_index": row_num,
                "order_number": order_number,
                "package": package,
            })

    return orders


async def fetch_device_name(context, order_id: str) -> str:
    """
    Open order detail in a new tab and extract the SMART_DEVICE offer name.
    """
    captured = {}

    async def intercept(response):
        try:
            if "getCeeOrderDetail" in response.url and response.status == 200:
                data = await response.json()
                if isinstance(data, dict) and data.get("data"):
                    order_items = data["data"].get("orderItemList", []) or []
                    for item in order_items:
                        for offer in item.get("offerInstList", []):
                            offer_name = offer.get("offerName", "")
                            attrs = {a.get("attrCode"): a.get("value") for a in offer.get("attrValueList", [])}
                            # Match by SMART_DEVICE tag, device serial, or goods delivery with device-like name
                            catg = attrs.get("TM_ADDITIONAL_OFFER_CATG", "")
                            is_device = (
                                catg == "SMART_DEVICE"
                                or "EXP_DEVICE_ESN" in attrs
                                or (
                                    "EXP_GOODS_DELIVERY_METHOD" in attrs
                                    and catg not in ("COMBOX", "")
                                )
                                or (
                                    "EXP_GOODS_DELIVERY_METHOD" in attrs
                                    and any(kw in offer_name.lower() for kw in ["ipad", "tablet", "phone", "watch", "galaxy", "iphone", "samsung", "device", "premium value"])
                                )
                            )
                            if is_device and offer_name:
                                captured["device"] = offer_name
                                return
        except Exception:
            pass

    p = await context.new_page()
    p.on("response", intercept)

    try:
        url = f"https://dealer.unifi.com.my/esales/h5/onBoarding/OrderDetails?custOrderId={order_id}&custOrderNbr={order_id}"
        await p.goto(url, wait_until="networkidle", timeout=60000)

        for _ in range(20):
            if "device" in captured:
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

    return captured.get("device", "")


async def backfill_devices(month_text: str, year: int):
    """
    Find orders with missing device names and fill them.
    """
    print(f"\n{'=' * 70}")
    print(f"BACKFILL DEVICE NAMES - {month_text} {year}")
    print(f"{'=' * 70}")

    spread = open_sheet()
    tab_title = month_tab_title(month_text, year)

    try:
        ws = spread.worksheet(tab_title)
    except Exception:
        print(f"  Tab '{tab_title}' not found — skipping")
        return

    orders = get_orders_missing_device(ws)
    print(f"  Orders missing device name: {len(orders)}")

    if not orders:
        print("  Nothing to backfill")
        return

    from credential_manager import CredentialManager
    creds = CredentialManager().get_credentials()

    browser, context, pw, page = await login_and_get_context(
        creds["username"], creds["password"]
    )

    headers = ws.row_values(1)
    device_col = headers.index("Device") + 1  # 1-based

    filled = 0
    try:
        for i, order in enumerate(orders, 1):
            order_id = order["order_number"]
            row_idx = order["row_index"]

            print(f"  [{i}/{len(orders)}] {order_id} ({order['package']})...", end=" ")

            device = await fetch_device_name(context, order_id)

            if device:
                ws.update_cell(row_idx, device_col, device)
                print(f"-> {device}")
                filled += 1
            else:
                print("-> not found")

    finally:
        await context.close()
        await browser.close()
        await pw.stop()

    print(f"\n  Filled {filled}/{len(orders)} device names")


if __name__ == "__main__":
    import sys

    month = sys.argv[1] if len(sys.argv) > 1 else "Jun"
    year = int(sys.argv[2]) if len(sys.argv) > 2 else 2026

    asyncio.run(backfill_devices(month, year))
