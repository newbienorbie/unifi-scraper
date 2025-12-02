"""
telegram_otp_reader.py
(Userbot Version) - Logs in as YOU to see messages from other bots.
Async-native version to fix "Event Loop" errors.
"""

import asyncio
import re
import time

from telethon import TelegramClient

# ================= CONFIGURATION =================
# ⚠️ Ensure these match what you used locally to generate the session
API_ID = 27295590  # Replace with your actual API ID
API_HASH = "da8f63a02b7f9bb6e567e06b9e7524d4"  # Replace with your actual Hash

# Your Group Chat ID
CHAT_ID = -1003215620011
SESSION_NAME = "sessions/unifi_user_session"
# =================================================


# NOTE: We defined this as 'async' now to share the loop with login_manager
async def get_latest_otp(wait_seconds=60, max_wait=120):
    print(f"👤 Userbot: Connecting to Telegram...")

    # Initialize Client
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    # Connect (using existing session file)
    await client.start()

    print(f"👤 Userbot: Connected! Scanning group {CHAT_ID}...")

    start_time = time.time()

    try:
        while time.time() - start_time < max_wait:
            # Get last 10 messages
            # Userbots can see messages from other Bots (like IFTTT)
            messages = await client.get_messages(CHAT_ID, limit=10)

            for message in messages:
                # Check if message is recent (within last 2 minutes)
                # Note: message.date is timezone-aware, time.time() is UTC timestamp usually
                # We compare timestamps to be safe
                msg_time = message.date.timestamp()

                # Check age (allow 2 mins slack)
                if (time.time() - msg_time) > 120:
                    continue

                text = message.text or ""
                otp = _extract_otp(text)

                if otp:
                    print(f"✅ Userbot Found OTP: {otp}")
                    return otp

            # Non-blocking sleep
            await asyncio.sleep(3)

    except Exception as e:
        print(f"⚠️ Userbot Error: {e}")
    finally:
        await client.disconnect()

    print("❌ Userbot Timeout: No OTP found.")
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
