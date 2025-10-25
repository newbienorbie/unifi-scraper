"""
Flask API server for n8n to interact with scraper
"""

import asyncio
import json
import os
from datetime import datetime

from flask import Flask, jsonify, request, send_file

from scraper_api import cred_manager, scrape_orders

app = Flask(__name__)

# Load authorized users
AUTHORIZED_USERS_FILE = "config/authorized_users.json"


def load_authorized_users():
    """Load list of authorized chat IDs"""
    if os.path.exists(AUTHORIZED_USERS_FILE):
        with open(AUTHORIZED_USERS_FILE, "r") as f:
            data = json.load(f)
            return data.get("authorized_chat_ids", [])
    return []


def is_authorized(chat_id):
    """Check if user is authorized"""
    authorized = load_authorized_users()
    return int(chat_id) in authorized


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "Unifi Scraper API",
        }
    )


@app.route("/is_authorized", methods=["GET"])
def check_authorization():
    """Check if a chat_id is authorized"""
    chat_id = request.args.get("chat_id")

    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400

    authorized = is_authorized(chat_id)

    return jsonify({"chat_id": chat_id, "authorized": authorized})


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Trigger scraping
    Body: {
        "chat_id": "123456789",
        "month": "Oct 2025"
    }
    """
    try:
        data = request.get_json()

        chat_id = data.get("chat_id")
        month = data.get("month", "Oct 2025")

        # Check authorization
        if not is_authorized(chat_id):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "UNAUTHORIZED",
                        "message": "You are not authorized to use this bot",
                    }
                ),
                403,
            )

        # Check if credentials exist
        if not cred_manager.credentials_exist():
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_CREDENTIALS",
                        "message": "No credentials saved. Please login first.",
                    }
                ),
                400,
            )

        # Run scraper
        result = asyncio.run(scrape_orders(month, chat_id))

        return jsonify(result)

    except Exception as e:
        return (
            jsonify({"success": False, "error": str(e), "message": "Scraping failed"}),
            500,
        )


# ADD THIS NEW ENDPOINT HERE ⬇️⬇️⬇️
@app.route("/scrape_fast", methods=["POST"])
def scrape_fast():
    """
    Fast API-based scraping with optional Google Sheets integration
    Body: {
        "chat_id": "123456789",
        "month": "Oct 2025",
        "use_sheets": true
    }
    """
    try:
        data = request.get_json()

        chat_id = data.get("chat_id")
        month = data.get("month", "Oct 2025")
        use_sheets = data.get("use_sheets", True)

        # Check authorization
        if not is_authorized(chat_id):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "UNAUTHORIZED",
                        "message": "You are not authorized to use this bot",
                    }
                ),
                403,
            )

        # Check if credentials exist
        if not cred_manager.credentials_exist():
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "NO_CREDENTIALS",
                        "message": "No credentials saved. Please login first.",
                    }
                ),
                400,
            )

        # Run fast scraper
        from scraper_api_fast import scrape_orders_fast

        result = asyncio.run(scrape_orders_fast(month, use_sheets))

        return jsonify(result)

    except Exception as e:
        return (
            jsonify(
                {"success": False, "error": str(e), "message": "Fast scraping failed"}
            ),
            500,
        )


@app.route("/scrape_fast", methods=["POST"])
def scrape_fast():
    """Fast API-based scraping with Google Sheets"""
    try:
        data = request.get_json()
        chat_id = data.get("chat_id")
        month = data.get("month", "Oct 2025")
        use_sheets = data.get("use_sheets", True)

        if not is_authorized(chat_id):
            return jsonify({"success": False, "error": "UNAUTHORIZED"}), 403

        if not cred_manager.credentials_exist():
            return jsonify({"success": False, "error": "NO_CREDENTIALS"}), 400

        # Import the fast scraper
        from scraper_api_fast import scrape_orders_fast

        result = asyncio.run(scrape_orders_fast(month, use_sheets))

        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/save_credentials", methods=["POST"])
def save_credentials():
    """
    Save login credentials
    Body: {
        "chat_id": "123456789",
        "username": "TMRS00517",
        "password": "Nexion2-"
    }
    """
    try:
        data = request.get_json()

        chat_id = data.get("chat_id")
        username = data.get("username")
        password = data.get("password")

        # Check authorization
        if not is_authorized(chat_id):
            return jsonify({"success": False, "error": "UNAUTHORIZED"}), 403

        if not username or not password:
            return (
                jsonify({"success": False, "error": "USERNAME_PASSWORD_REQUIRED"}),
                400,
            )

        # Save credentials
        cred_manager.save_credentials(username, password)

        return jsonify({"success": True, "message": "Credentials saved securely"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/download_csv", methods=["GET"])
def download_csv():
    """
    Download CSV file
    Query params: filename=unifi_orders_20251020_123456.csv
    """
    filename = request.args.get("filename")

    if not filename:
        return jsonify({"error": "filename required"}), 400

    filepath = f"outputs/{filename}"

    if not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404

    return send_file(filepath, as_attachment=True)


@app.route("/status", methods=["GET"])
def status():
    """Get system status"""

    creds_exist = cred_manager.credentials_exist()
    cookies_exist = os.path.exists("config/cookies.json")

    # Get latest CSV info
    latest_csv = None
    if os.path.exists("outputs"):
        csv_files = [f for f in os.listdir("outputs") if f.endswith(".csv")]
        if csv_files:
            csv_files.sort(reverse=True)
            latest_csv = csv_files[0]

    return jsonify(
        {
            "credentials_saved": creds_exist,
            "session_active": cookies_exist,
            "latest_scrape": latest_csv,
            "timestamp": datetime.now().isoformat(),
        }
    )


if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("config", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # Run server
    app.run(host="0.0.0.0", port=5000, debug=False)
