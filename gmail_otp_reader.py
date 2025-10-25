"""
Read OTP from Gmail using Gmail API
Handles forwarded SMS like: "Unifi: Your OTP is 641776"
"""

import base64
import os
import re
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GmailOTPReader:
    def __init__(self):
        self.service = self._get_gmail_service()

    def _get_gmail_service(self):
        """Authenticate and return Gmail service"""
        creds = None

        if os.path.exists("config/gmail_token.json"):
            creds = Credentials.from_authorized_user_file(
                "config/gmail_token.json", SCOPES
            )

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists("config/gmail_credentials.json"):
                    print("ERROR: config/gmail_credentials.json not found!")
                    print("Please set up Gmail API credentials first.")
                    return None

                flow = InstalledAppFlow.from_client_secrets_file(
                    "config/gmail_credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open("config/gmail_token.json", "w") as token:
                token.write(creds.to_json())

        return build("gmail", "v1", credentials=creds)

    def get_latest_otp(
        self, sender_filter="@forward-sms.com", wait_seconds=60, max_wait=1800
    ):
        """
        Read latest OTP from email

        Args:
            sender_filter: Email sender pattern (e.g. '@forward-sms.com')
            wait_seconds: Initial wait time
            max_wait: Maximum wait time (30 minutes = 1800 seconds)

        Returns:
            OTP code as string, or None if not found
        """
        print(f"Waiting for OTP email from {sender_filter}...")
        print(f"Will check for up to {max_wait} seconds (timeout for delays)...")

        start_time = time.time()
        check_count = 0

        while time.time() - start_time < max_wait:
            try:
                check_count += 1

                # Search for forwarded SMS from any forward-sms.com mailer
                query = f"from:({sender_filter}) subject:(Forward SMS) newer_than:2m"

                results = (
                    self.service.users()
                    .messages()
                    .list(userId="me", q=query, maxResults=5)
                    .execute()
                )

                messages = results.get("messages", [])

                if messages:
                    # Check each message for OTP
                    for msg_data in messages:
                        msg_id = msg_data["id"]
                        message = (
                            self.service.users()
                            .messages()
                            .get(userId="me", id=msg_id, format="full")
                            .execute()
                        )

                        # Check message timestamp
                        msg_timestamp = int(message.get("internalDate", 0)) / 1000

                        # Only accept emails received AFTER we started waiting
                        if msg_timestamp < start_time:
                            print(
                                f"  Skipping old email (from {int(time.time() - msg_timestamp)}s ago)"
                            )
                            continue

                        # Get subject and body
                        subject = self._get_header(message, "Subject")
                        body = self._get_message_body(message)

                        # Debug: Print what we found
                        print(f"  Checking email - Subject: {subject[:50]}...")
                        print(f"  Body preview: {body[:100]}...")

                        full_text = f"{subject} {body}"

                        # Extract OTP
                        otp = self._extract_otp(full_text)

                        if otp:
                            print(
                                f"✓ Found OTP: {otp} (after {int(time.time() - start_time)}s, check #{check_count})"
                            )
                            return otp

                # Progress indicator
                elapsed = int(time.time() - start_time)
                if elapsed % 30 == 0 and elapsed > 0:
                    print(
                        f"Still waiting... ({elapsed}s elapsed, check #{check_count})"
                    )

                # Wait before next check (exponential backoff)
                if elapsed < 60:
                    time.sleep(3)  # Check every 3 seconds for first minute
                elif elapsed < 300:
                    time.sleep(10)  # Every 10 seconds for first 5 minutes
                else:
                    time.sleep(30)  # Every 30 seconds after that

            except Exception as e:
                print(f"Error reading email: {e}")
                time.sleep(5)

        print(f"✗ OTP not found after {max_wait} seconds")
        return None

    def _get_header(self, message, header_name):
        """Extract header value from email"""
        headers = message["payload"].get("headers", [])
        for header in headers:
            if header["name"].lower() == header_name.lower():
                return header["value"]
        return ""

    def _get_message_body(self, message):
        """Extract text from email message"""
        try:
            # Handle multipart messages
            if "parts" in message["payload"]:
                parts = message["payload"]["parts"]
                for part in parts:
                    if part["mimeType"] == "text/plain":
                        data = part["body"].get("data", "")
                        if data:
                            return base64.urlsafe_b64decode(data).decode(
                                "utf-8", errors="ignore"
                            )

            # Handle simple messages
            data = message["payload"]["body"].get("data", "")
            if data:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")

        except Exception as e:
            print(f"Error extracting body: {e}")

        return ""

    def _extract_otp(self, text):
        """
        Extract OTP code from email text
        Handles format: "Unifi: Your OTP is 641776"
        """
        # Specific pattern for your SMS format
        patterns = [
            r"OTP is (\d{6})",  # "Your OTP is 641776"
            r"OTP:\s*(\d{6})",  # "OTP: 641776"
            r"code is (\d{6})",  # "code is 641776"
            r"verification code:\s*(\d{6})",
            r"\b(\d{6})\b",  # Any 6-digit number (last resort)
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                otp = match.group(1)
                # Verify it's actually 6 digits
                if len(otp) == 6 and otp.isdigit():
                    return otp

        return None


# Test function
if __name__ == "__main__":
    reader = GmailOTPReader()
    otp = reader.get_latest_otp(
        sender_filter="@forward-sms.com", wait_seconds=60, max_wait=120
    )
    if otp:
        print(f"SUCCESS: OTP = {otp}")
    else:
        print("FAILED: No OTP found")
