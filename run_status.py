"""
Run status check for a specific month.
Usage:
  python run_status.py Jun 2026
  python run_status.py May 2026 --empty   (only fill empty ones)
"""
import asyncio
import sys

from dotenv import load_dotenv

load_dotenv()

from credential_manager import CredentialManager
from check_status import check_status_standalone, check_status_standalone_empty

month = sys.argv[1] if len(sys.argv) > 1 else "Jun"
year = int(sys.argv[2]) if len(sys.argv) > 2 else 2026
only_empty = "--empty" in sys.argv

creds = CredentialManager().get_credentials()

if only_empty:
    print(f"Checking empty statuses for {month} {year}")
    asyncio.run(check_status_standalone_empty(creds["username"], creds["password"], month, year))
else:
    print(f"Checking all statuses for {month} {year}")
    asyncio.run(check_status_standalone(creds["username"], creds["password"], month, year))
