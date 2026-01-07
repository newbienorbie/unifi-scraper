"""
telegram_otp_reader.py
(Userbot Version) - Logs in as YOU to see messages from other bots.
Async-native version to fix "Event Loop" errors.
"""

import asyncio
import re
import time
from datetime import datetime

from telethon import TelegramClient

# ================= CONFIGURATION =================
# ⚠️ Ensure these match what you used locally to generate the session
API_ID = 27295590  # Replace with your actual API ID
API_HASH = "da8f63a02b7f9bb6e567e06b9e7524d4"  # Replace with your actual Hash

# Your Group Chat ID
CHAT_ID = -1003215620011
SESSION_NAME = "sessions/unifi_user_session"
# =================================================


async def get_latest_otp(wait_seconds=None, max_wait=1200):
    """
    Waits for a NEW OTP.
    max_wait = 1200 seconds (20 minutes) to handle very slow SMS.
    """
    print(f"👤 Userbot: Connecting to Telegram...")

    # Initialize Client
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    # Connect (using existing session file)
    await client.start()

    print(f"👤 Userbot: Connected! Scanning group {CHAT_ID}...")

    # -----------------------------------------------------------
    # THE FIX: Define a "Cutoff Time"
    # We only accept messages that arrive AFTER we start looking.
    # We subtract 15 seconds just in case the PC clock is slightly ahead of Telegram's server.
    # -----------------------------------------------------------
    search_start_time = time.time()
    cutoff_timestamp = search_start_time - 15

    print(f"🕒 Waiting for OTP (timeout: {max_wait/60:.0f} mins)...")

    try:
        while time.time() - search_start_time < max_wait:
            # Get last 10 messages
            messages = await client.get_messages(CHAT_ID, limit=10)

            for message in messages:
                if not message.date:
                    continue

                # Get message timestamp (UTC)
                msg_timestamp = message.date.timestamp()

                # ------------------------------------------------
                # LOGIC: Is this message OLDER than our start time?
                # ------------------------------------------------
                if msg_timestamp < cutoff_timestamp:
                    # This message existed before we clicked "GET". Ignore it.
                    continue

                # If we get here, the message is NEW (arrived after we started)
                text = message.text or ""
                otp = _extract_otp(text)

                if otp:
                    arrival_time = datetime.fromtimestamp(msg_timestamp).strftime(
                        "%H:%M:%S"
                    )
                    print(
                        f"✅ Userbot Found NEW OTP: {otp} (Arrived at {arrival_time})"
                    )
                    return otp

            # Wait 5 seconds before checking again (save CPU)
            await asyncio.sleep(5)

            # Optional: Print a dot every 30 seconds to show it's still alive
            if int(time.time()) % 30 == 0:
                print(".", end="", flush=True)

    except Exception as e:
        print(f"⚠️ Userbot Error: {e}")
    finally:
        await client.disconnect()

    print("\n❌ Userbot Timeout: No NEW OTP received within limit.")
    return None


def _extract_otp(text):
    if not text:
        return None
    # Look for "OTP is XXXXXX" or "OTP: XXXXXX"
    patterns = [r"OTP is (\d{6})", r"OTP:\s*(\d{6})", r"\b(\d{6})\b"]  # Fallback
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None
