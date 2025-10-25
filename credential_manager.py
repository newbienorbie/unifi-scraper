"""
Secure credential management with encryption
"""

import json
import os

from cryptography.fernet import Fernet


class CredentialManager:
    def __init__(
        self, key_file="config/secret.key", creds_file="config/credentials.enc"
    ):
        self.key_file = key_file
        self.creds_file = creds_file

        os.makedirs("config", exist_ok=True)

        self.key = self._load_or_create_key()
        self.cipher = Fernet(self.key)

    def _load_or_create_key(self):
        """Load existing encryption key or create new one"""
        if os.path.exists(self.key_file):
            with open(self.key_file, "rb") as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(self.key_file, "wb") as f:
                f.write(key)
            print(f"✓ Created encryption key: {self.key_file}")
            return key

    def save_credentials(self, username, password):
        """Save encrypted credentials"""
        data = json.dumps({"username": username, "password": password})
        encrypted = self.cipher.encrypt(data.encode())

        with open(self.creds_file, "wb") as f:
            f.write(encrypted)

        print("✓ Credentials saved securely (encrypted)")
        return True

    def get_credentials(self):
        """Retrieve decrypted credentials"""
        if not os.path.exists(self.creds_file):
            return None

        try:
            with open(self.creds_file, "rb") as f:
                encrypted = f.read()

            decrypted = self.cipher.decrypt(encrypted)
            return json.loads(decrypted.decode())
        except Exception as e:
            print(f"Error decrypting credentials: {e}")
            return None

    def update_password(self, new_password):
        """Update password only"""
        creds = self.get_credentials()
        if creds:
            creds["password"] = new_password
            self.save_credentials(creds["username"], creds["password"])
            print("✓ Password updated")
            return True
        return False

    def credentials_exist(self):
        """Check if credentials are saved"""
        return os.path.exists(self.creds_file)

    def delete_credentials(self):
        """Delete stored credentials"""
        if os.path.exists(self.creds_file):
            os.remove(self.creds_file)
            print("✓ Credentials deleted")
            return True
        return False
