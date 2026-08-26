"""Update the stored login password.

Usage:
    python update_password.py                # prompts (no echo, not in shell history)
    python update_password.py <new_password>
"""

import getpass
import sys

from credential_manager import CredentialManager

new_password = sys.argv[1] if len(sys.argv) > 1 else getpass.getpass("New password: ")

if not new_password:
    print("✗ No password given")
    sys.exit(1)

if not CredentialManager().update_password(new_password):
    print("✗ No existing credentials to update — use save_credentials() first")
    sys.exit(1)
