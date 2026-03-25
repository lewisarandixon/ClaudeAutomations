"""
One-time OAuth setup: generates token.json from google_credentials.json.
Run this once: python execution/google_auth_setup.py
It will open your browser to log in, then save the token locally.
"""

from google_auth_oauthlib.flow import InstalledAppFlow
import json
import os

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/gmail.send",
]

CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "..", "google_credentials.json")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "token.json")

flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
creds = flow.run_local_server(port=0)

token_data = {
    "token": creds.token,
    "refresh_token": creds.refresh_token,
    "token_uri": creds.token_uri,
    "client_id": creds.client_id,
    "client_secret": creds.client_secret,
    "scopes": list(creds.scopes),
}

with open(TOKEN_FILE, "w") as f:
    json.dump(token_data, f, indent=2)

print(f"Token saved to {TOKEN_FILE}")
print("You're authenticated. The tech radar script can now access Google Sheets/Docs/Drive/Gmail.")
