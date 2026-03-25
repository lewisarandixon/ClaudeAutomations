#!/usr/bin/env python3
"""
Backfill the Airtable Communications table with outbound draft emails
that were already sent but not logged.

Fetches all leads where Messaged At is set, Industry != Accountant,
and creates an Outbound row in the Communications table for each.
"""

import os
import re
import time
import requests
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
LEADS_TABLE_ID = os.environ["AIRTABLE_LEADS_ID"]
COMMS_TABLE_ID = os.environ["AIRTABLE_COMMUNICATIONS_ID"]

FIXED_TIMESTAMP = "2026-03-25T17:00:00.000Z"
DELAY = 0.25


def html_to_plain(html: str) -> str:
    """Strip HTML tags and collapse whitespace to get plain text."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>|</li>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>", "- ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_messaged_leads() -> list:
    """Fetch all leads that have been messaged, excluding Accountants."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{LEADS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    formula = "AND({Messaged At} != BLANK(), {Contact Email} != '', {Industry} != 'Accountant')"

    all_records = []
    offset = None

    while True:
        params = {
            "filterByFormula": formula,
            "fields[]": [
                "Company / Business Name",
                "Client Name",
                "Contact Email",
                "Industry",
            ],
        }
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(DELAY)

    print(f"Found {len(all_records)} messaged leads (excl. Accountants)")
    return all_records


def log_to_communications(email: str, company: str, subject: str, plain_body: str):
    """Create one outbound row in the Communications table."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{COMMS_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "fields": {
            "Contact Email": email,
            "Lead Name": company,
            "Direction": "Outbound",
            "Subject": subject,
            "Message": plain_body,
            "Status": "Messaged",
            "Date/time": FIXED_TIMESTAMP,
        }
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=10)
    resp.raise_for_status()


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from lead_outreach import load_templates, personalise_template

    leads = fetch_messaged_leads()
    templates = load_templates()
    default_template = templates.get("Default")

    created = 0
    errors = 0

    for record in leads:
        fields = record.get("fields", {})
        email = fields.get("Contact Email", "").strip()
        company = fields.get("Company / Business Name", "").strip()
        industry = fields.get("Industry", "")

        if not email:
            continue

        template = templates.get(industry, default_template)
        try:
            personalised = personalise_template(template, fields)
            subject = personalised["subject"]
            plain_body = html_to_plain(personalised["body_html"])

            log_to_communications(email, company, subject, plain_body)
            created += 1
            if created % 25 == 0:
                print(f"  {created}/{len(leads)} logged...")
        except Exception as e:
            errors += 1
            print(f"  Error for {email}: {e}")

        time.sleep(DELAY)

    print(f"\nDone. {created} rows created, {errors} errors.")


if __name__ == "__main__":
    main()
