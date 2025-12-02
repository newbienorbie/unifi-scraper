"""
date_utils.py
Shared date utilities for Unifi Scraper.
Handles month ranges and date format standardization.
"""

from datetime import datetime, timedelta


def month_range_yyyymmddhhmmss(month_text, year):
    """
    Convert month name and year to start/end timestamps.
    Returns: (start_str, end_str) in YYYYMMDDHHMMSS format.
    Example: ("20251001000000", "20251031235959")
    """
    months = {
        "Jan": 1,
        "January": 1,
        "Feb": 2,
        "February": 2,
        "Mar": 3,
        "March": 3,
        "Apr": 4,
        "April": 4,
        "May": 5,
        "May": 5,
        "Jun": 6,
        "June": 6,
        "Jul": 7,
        "July": 7,
        "Aug": 8,
        "August": 8,
        "Sep": 9,
        "September": 9,
        "Oct": 10,
        "October": 10,
        "Nov": 11,
        "November": 11,
        "Dec": 12,
        "December": 12,
    }

    month_num = months.get(month_text, 1)
    year = int(year)

    # First day of month
    start_date = datetime(year, month_num, 1, 0, 0, 0)

    # First day of NEXT month
    if month_num == 12:
        next_month = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        next_month = datetime(year, month_num + 1, 1, 0, 0, 0)

    # Last second of current month
    end_date = next_month - timedelta(seconds=1)

    return start_date.strftime("%Y%m%d%H%M%S"), end_date.strftime("%Y%m%d%H%M%S")


def standardize_date(date_str):
    """
    Clean messy dates into a strict, zero-padded format for sorting.

    Input:  "1 Dec 2025 1:05"  OR "01 Dec 2025 01:05"
    Output: "01 Dec 2025 01:05"

    This ensures '1 Dec' doesn't get sorted after '10 Dec'.
    """
    if not date_str:
        return ""

    date_str = date_str.strip()

    # Formats to try parsing FROM
    input_formats = [
        "%d %b %Y %H:%M:%S",  # 01 Dec 2025 13:00:00
        "%d %b %Y %H:%M",  # 01 Dec 2025 13:00
        "%d-%m-%Y %H:%M:%S",  # 01-12-2025 13:00:00
        "%d-%m-%Y %H:%M",  # 01-12-2025 13:00
        "%Y%m%d%H%M%S",  # 20251201130000 (API format)
    ]

    # The clean output format we want
    # %d = 01 (padded), %b = Jan, %H:%M = 09:05 (padded)
    output_format = "%d %b %Y %H:%M"

    for fmt in input_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime(output_format)
        except ValueError:
            continue

    # If parsing fails, return original text so we don't lose data
    return date_str
