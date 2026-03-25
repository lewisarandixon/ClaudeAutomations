#!/usr/bin/env python3
"""
Lead Outreach — Create draft emails in Outlook via Microsoft Graph API.

Fetches un-emailed leads from Airtable, loads industry-based templates from
Google Sheets, personalises them, and creates draft emails in the sender's
Outlook mailbox. Does NOT send — drafts are reviewed manually before sending.

Usage:
    # Test Microsoft Graph authentication
    python execution/lead_outreach.py --test-auth

    # Dry run — preview personalised emails without creating drafts
    python execution/lead_outreach.py --industry "Dentist" --limit 5 --dry-run

    # Create drafts for one industry
    python execution/lead_outreach.py --industry "Dentist" --limit 10

    # Create drafts for all industries
    python execution/lead_outreach.py --all --limit 50
"""

import os
import sys
import json
import time
import argparse
import logging
import requests as http_requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lead-outreach")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Microsoft Graph API
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Rate limiting
EMAIL_DELAY_SECONDS = 2       # Delay between draft creations
AIRTABLE_BATCH_SIZE = 10      # Airtable batch update limit
AIRTABLE_DELAY = 0.2          # Airtable rate limit delay

# Shared positioning line (used across all templates)
LOCAL_SERVICE_LINE = (
    "A lot of the automation systems I build are designed around common admin "
    "challenges in local service-based businesses, such as managing enquiries, "
    "follow-ups, scheduling, and day-to-day inbox admin."
)


# ---------------------------------------------------------------------------
# Microsoft Graph API — OAuth2 client credentials
# ---------------------------------------------------------------------------

def get_microsoft_token(tenant_id: str = None, client_id: str = None,
                        client_secret: str = None) -> str:
    """
    Get an access token from Azure AD using client credentials flow.
    Returns the access token string.
    Requires: Mail.Send (or Mail.ReadWrite) application permission.
    """
    tenant_id = tenant_id or os.getenv("MICROSOFT_TENANT_ID")
    client_id = client_id or os.getenv("MICROSOFT_CLIENT_ID")
    client_secret = client_secret or os.getenv("MICROSOFT_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "MICROSOFT_TENANT_ID, MICROSOFT_CLIENT_ID, MICROSOFT_CLIENT_SECRET "
            "must be set"
        )

    url = GRAPH_TOKEN_URL.format(tenant_id=tenant_id)
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    resp = http_requests.post(url, data=data, timeout=15)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in response")
    return token


# ---------------------------------------------------------------------------
# Microsoft Graph API — Draft email creation
# ---------------------------------------------------------------------------

def create_draft_email(token: str, sender: str, to_email: str,
                       subject: str, html_body: str) -> dict:
    """
    Create a draft email in the sender's mailbox via Microsoft Graph API.

    POST /users/{sender}/messages
    Creates a draft (not sent). Returns the draft message metadata.
    """
    url = f"{GRAPH_BASE}/users/{sender}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "subject": subject,
        "body": {
            "contentType": "HTML",
            "content": html_body,
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "address": to_email,
                }
            }
        ],
        "from": {
            "emailAddress": {
                "address": sender,
            }
        },
    }

    resp = http_requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    result = resp.json()
    logger.info(f"Draft created for {to_email}: {result.get('id', 'unknown')}")
    return result


# ---------------------------------------------------------------------------
# Airtable helpers
# ---------------------------------------------------------------------------

def fetch_unemailed_leads(api_key: str, base_id: str, table_id: str,
                          industry: str = None, limit: int = None) -> list:
    """
    Fetch leads from Airtable that haven't been emailed yet.

    Filters: Contact Email not empty AND Messaged At is blank.
    Optionally filters by Industry.
    Returns list of Airtable record dicts (with 'id' and 'fields').
    """
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    # Build filterByFormula
    conditions = [
        "{Contact Email} != ''",
        "{Messaged At} = BLANK()",
    ]
    if industry:
        conditions.append(f"{{Industry}} = '{industry}'")

    formula = "AND(" + ", ".join(conditions) + ")"

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
                "City/County",
                "Country/State",
                "Company Website URL",
                "Messaged At",
            ],
        }
        if offset:
            params["offset"] = offset

        resp = http_requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("records", [])
        all_records.extend(records)

        offset = data.get("offset")
        if not offset:
            break

        time.sleep(AIRTABLE_DELAY)

    logger.info(f"Found {len(all_records)} unemailed leads"
                + (f" in {industry}" if industry else ""))

    if limit and len(all_records) > limit:
        all_records = all_records[:limit]
        logger.info(f"Limited to {limit} leads")

    return all_records


def update_airtable_records_batch(api_key: str, base_id: str, table_id: str,
                                  updates: list) -> int:
    """
    Batch update Airtable records (PATCH).
    Each update: {"id": "recXXX", "fields": {"Field Name": value}}
    Returns count of updated records.
    """
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    updated = 0
    for i in range(0, len(updates), AIRTABLE_BATCH_SIZE):
        batch = updates[i:i + AIRTABLE_BATCH_SIZE]
        payload = {"records": batch}

        resp = http_requests.patch(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        updated += len(batch)
        logger.info(f"Updated Airtable batch {i // AIRTABLE_BATCH_SIZE + 1}: "
                    f"{len(batch)} records")
        time.sleep(AIRTABLE_DELAY)

    return updated


# ---------------------------------------------------------------------------
# Template loading from Google Sheets
# ---------------------------------------------------------------------------

def load_templates(token_data: dict = None) -> dict:
    """
    Load outreach templates from the "Outreach Templates" tab in the
    Automation Config spreadsheet.

    Expected columns:
      Industry | Subject | Bullets | Deck File ID | Deck Filename

    - Industry: must match Airtable Industry field. "Default" is the fallback.
    - Subject: email subject line (no variables needed, but supported).
    - Bullets: newline-separated list of automation examples for this industry.
    - Deck File ID: Google Drive file ID for the industry presentation (optional).
    - Deck Filename: display name for the deck (optional).

    Returns: {"Default": {"subject": ..., "bullets": [...], "deck_file_id": ..., ...}, ...}
    """
    import gspread
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from dotenv import load_dotenv
    load_dotenv()

    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
    if not config_sheet_id:
        raise ValueError("AUTOMATION_CONFIG_SHEET_ID must be set")

    # Auth
    if token_data:
        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=token_data.get("scopes", SCOPES),
        )
        if creds.expired:
            creds.refresh(Request())
    else:
        token_json = os.getenv("GOOGLE_TOKEN_JSON")
        if token_json:
            td = json.loads(token_json)
            creds = Credentials(
                token=td.get("token"),
                refresh_token=td.get("refresh_token"),
                token_uri=td.get("token_uri"),
                client_id=td.get("client_id"),
                client_secret=td.get("client_secret"),
                scopes=td.get("scopes", SCOPES),
            )
            if creds.expired:
                creds.refresh(Request())
        elif os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
            if creds.expired:
                creds.refresh(Request())
        else:
            raise RuntimeError("No Google credentials found")

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(config_sheet_id)

    # Find "Outreach Templates" tab
    try:
        ws = spreadsheet.worksheet("Outreach Templates")
    except gspread.exceptions.WorksheetNotFound:
        raise RuntimeError(
            "No 'Outreach Templates' tab found in the Automation Config "
            f"spreadsheet ({config_sheet_id}). "
            "Create it with columns: Industry | Subject | Bullets | "
            "Deck File ID | Deck Filename"
        )

    rows = ws.get_all_records()
    templates = {}

    for row in rows:
        industry = str(row.get("Industry", "")).strip()
        subject = str(row.get("Subject", "")).strip()
        bullets_raw = str(row.get("Bullets", "")).strip()
        deck_file_id = str(row.get("Deck File ID", "")).strip()
        deck_filename = str(row.get("Deck Filename", "")).strip()

        if not industry or not subject:
            continue

        # Parse bullets (newline-separated in the cell)
        bullets = [b.strip() for b in bullets_raw.split("\n") if b.strip()]

        templates[industry] = {
            "subject": subject,
            "bullets": bullets,
            "deck_file_id": deck_file_id,
            "deck_filename": deck_filename,
        }

    if "Default" not in templates:
        raise RuntimeError(
            "Outreach Templates tab must have a 'Default' row. "
            "This is used when no industry-specific template exists."
        )

    logger.info(f"Loaded {len(templates)} templates: {list(templates.keys())}")
    return templates


# ---------------------------------------------------------------------------
# Template personalisation
# ---------------------------------------------------------------------------

def _get_greeting() -> str:
    """Return time-of-day greeting based on current UK time (UTC)."""
    hour = datetime.now(timezone.utc).hour
    if hour < 12:
        return "Good morning,"
    if hour < 18:
        return "Good afternoon,"
    return "Good evening,"


def personalise_template(template: dict, lead_fields: dict, greeting_override: str = None) -> dict:
    """
    Build a personalised HTML email from the shared template structure
    and industry-specific parts (subject, bullets, deck link).

    The email structure is shared across all industries:
      1. Time-of-day greeting + name
      2. Lewis intro
      3. Local service positioning line + company mention
      4. Industry-specific bullet points
      5. Optional deck link (Google Drive presentation)
      6. Call to action
      7. Sign-off

    Fallbacks:
      No client name -> greeting without name
      No company name -> omit company mention
      No deck -> omit deck block
    """
    company = (lead_fields.get("Company / Business Name") or "").strip()
    client_name = (lead_fields.get("Client Name") or "").strip()

    # Greeting
    greeting = greeting_override or _get_greeting()
    opener_name = f" {client_name}," if client_name else ""

    # Bullets HTML
    bullets = template.get("bullets", [])
    bullets_html = "".join(f"<li>{b.lstrip('- ')}</li>" for b in bullets)

    # Company mention
    company_mention = ""
    if company:
        company_mention = (
            f' I came across <strong>{company}</strong> while researching '
            f'local businesses that could benefit from automation.'
        )

    # Deck block (only if deck file ID exists)
    deck_file_id = template.get("deck_file_id", "")
    deck_block = ""
    if deck_file_id:
        deck_share_link = (
            f"https://drive.google.com/file/d/{deck_file_id}/view?usp=sharing"
        )
        deck_for = (
            f'for <strong>{company}</strong>' if company else "for you"
        )
        deck_block = f"""
  <p style="margin: 0 0 12px 0;">
    I put together a short presentation for you:
    <br/>
    <a href="{deck_share_link}" target="_blank"
       style="display:inline-block; margin-top:8px; padding:10px 14px; background:#111827; color:#ffffff; text-decoration:none; border-radius:6px; font-weight:600;">
      View the presentation
    </a>
  </p>"""

    # Build full HTML email
    html_body = f"""<div style="font-family: Arial, Helvetica, sans-serif; font-size: 14px; color: #111; line-height: 1.6;">
  <p style="margin: 0 0 12px 0;">{greeting}{opener_name}</p>

  <p style="margin: 0 0 12px 0;">I hope you're well.</p>

  <p style="margin: 0 0 12px 0;">
    I'm <strong>Lewis</strong>, a <strong>22-year-old developer</strong> who's spent the last few years immersed in AI and automation and how it can genuinely transform the way businesses operate.
    I'm not here to talk about replacing anyone. AI and automation aren't about taking jobs, they're about taking the <strong>repetitive, time-consuming admin off people's plates</strong> so they can focus on the work that actually moves the business forward.
    It's not about automating everything, it's about automating the <strong>60-70% of small tasks that quietly eat hours every week</strong>.
    Alongside building these systems, I also enjoy helping managers and teams understand how to use AI effectively so it becomes a <strong>real asset to the business, not just a buzzword</strong>.
  </p>

  <p style="margin: 0 0 12px 0;">
    {LOCAL_SERVICE_LINE}{company_mention}
  </p>

  <p style="margin: 0 0 8px 0;"><strong>Common automations I build:</strong></p>
  <ul style="margin: 0 0 12px 20px; padding: 0;">
    {bullets_html}
  </ul>

  {deck_block}

  <p style="margin: 0 0 12px 0;">
    If you're open to it, we could have a <strong>quick 10-15 minute chat</strong>, just to hear a bit about you and your business and see
    <strong>whether there's anything I may be able to help with</strong>.
  </p>

  <p style="margin: 0;">
    Best,<br/>
    Lewis
  </p>
</div>"""

    return {
        "subject": template["subject"],
        "body_html": html_body,
    }


# ---------------------------------------------------------------------------
# Airtable Communications table logging
# ---------------------------------------------------------------------------

def log_to_communications(airtable_key: str, base_id: str, comms_table_id: str,
                          email: str, company: str, subject: str, now_iso: str,
                          html_body: str = ""):
    """Log an outbound draft to the Airtable Communications table."""
    if not comms_table_id:
        return
    url = f"https://api.airtable.com/v0/{base_id}/{comms_table_id}"
    headers = {"Authorization": f"Bearer {airtable_key}", "Content-Type": "application/json"}
    import re

    def _html_to_plain(html: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        text = re.sub(r"</p>|</div>|</li>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<li[^>]*>", "- ", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        return re.sub(r"\n{3,}", "\n\n", text).strip()

    payload = {
        "fields": {
            "Contact Email": email,
            "Lead Name": company,
            "Direction": "Outbound",
            "Subject": subject,
            "Message": _html_to_plain(html_body) if html_body else subject,
            "Status": "Messaged",
            "Date/time": now_iso,
        }
    }
    try:
        resp = http_requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Communications log failed for {email}: {e}")


# ---------------------------------------------------------------------------
# Core outreach logic
# ---------------------------------------------------------------------------

def send_outreach_batch(leads: list, templates: dict, dry_run: bool = False,
                        token_data: dict = None, greeting_override: str = None) -> dict:
    """
    Process a batch of leads: personalise templates and create draft emails.

    Args:
        leads: List of Airtable record dicts
        templates: Dict of industry -> {subject, body} from load_templates()
        dry_run: If True, preview without creating drafts
        token_data: Google token data (for webhook mode)

    Returns:
        Summary dict with counts and details
    """
    from dotenv import load_dotenv
    load_dotenv()

    sender = os.getenv("MICROSOFT_SENDER_EMAIL")
    if not sender and not dry_run:
        raise ValueError("MICROSOFT_SENDER_EMAIL must be set")

    # Get Microsoft token (only if not dry run)
    ms_token = None
    if not dry_run:
        ms_token = get_microsoft_token()

    # Airtable config (for updating Messaged At + Communications log)
    airtable_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    leads_table_id = os.getenv("AIRTABLE_LEADS_ID")
    comms_table_id = os.getenv("AIRTABLE_COMMUNICATIONS_ID")

    results = {
        "total": len(leads),
        "drafted": 0,
        "skipped": 0,
        "errors": 0,
        "dry_run": dry_run,
        "details": [],
    }

    airtable_updates = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for i, record in enumerate(leads):
        fields = record.get("fields", {})
        record_id = record.get("id", "")
        email = (fields.get("Contact Email") or "").strip()
        company = (fields.get("Company / Business Name") or "").strip()
        industry = (fields.get("Industry") or "").strip()

        if not email:
            results["skipped"] += 1
            results["details"].append({
                "company": company, "status": "skipped", "reason": "no email"
            })
            continue

        # Pick template: industry-specific or Default
        template = templates.get(industry, templates["Default"])

        # Personalise
        personalised = personalise_template(template, fields, greeting_override=greeting_override)

        if dry_run:
            results["drafted"] += 1
            results["details"].append({
                "company": company,
                "email": email,
                "industry": industry,
                "subject": personalised["subject"],
                "template_used": industry if industry in templates else "Default",
                "status": "preview",
            })
            logger.info(f"[DRY RUN] {email} | {personalised['subject']}")
            continue

        # Create draft in Outlook
        try:
            create_draft_email(
                token=ms_token,
                sender=sender,
                to_email=email,
                subject=personalised["subject"],
                html_body=personalised["body_html"],
            )
            results["drafted"] += 1
            results["details"].append({
                "company": company,
                "email": email,
                "subject": personalised["subject"],
                "status": "drafted",
            })

            # Queue Airtable update
            airtable_updates.append({
                "id": record_id,
                "fields": {"Messaged At": now_iso},
            })


        except Exception as e:
            results["errors"] += 1
            results["details"].append({
                "company": company,
                "email": email,
                "status": "error",
                "error": str(e),
            })
            logger.error(f"Failed to create draft for {email}: {e}")

        # Rate limit between emails
        if i < len(leads) - 1:
            time.sleep(EMAIL_DELAY_SECONDS)

    # Batch update Airtable with Messaged At
    if airtable_updates and all([airtable_key, base_id, leads_table_id]):
        try:
            update_airtable_records_batch(
                airtable_key, base_id, leads_table_id, airtable_updates
            )
            logger.info(f"Updated {len(airtable_updates)} Airtable records "
                        "with Messaged At")
        except Exception as e:
            logger.error(f"Airtable update failed: {e}")
            results["airtable_error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# Telegram notification
# ---------------------------------------------------------------------------

def send_completion_telegram(results: dict) -> bool:
    """Send a summary of the outreach batch to Telegram."""
    from dotenv import load_dotenv
    load_dotenv()

    bot_token = (os.getenv("LEAD_ANALYTICS_BOT_TOKEN")
                 or os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = (os.getenv("LEAD_ANALYTICS_CHAT_ID")
               or os.getenv("TELEGRAM_CHAT_ID"))

    if not bot_token or not chat_id:
        logger.warning("No Telegram credentials - skipping notification")
        return False

    mode = "DRY RUN" if results.get("dry_run") else "DRAFTS CREATED"
    msg = (
        f"*Lead Outreach - {mode}*\n\n"
        f"Total leads: {results['total']}\n"
        f"Drafts: {results['drafted']}\n"
        f"Skipped: {results['skipped']}\n"
        f"Errors: {results['errors']}\n"
    )

    if results.get("errors") > 0:
        error_details = [d for d in results.get("details", [])
                         if d.get("status") == "error"]
        if error_details:
            msg += "\n*Errors:*\n"
            for ed in error_details[:5]:
                msg += f"- {ed.get('company', '?')}: {ed.get('error', '?')}\n"

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        resp = http_requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram notification sent")
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Webhook entry point (Modal)
# ---------------------------------------------------------------------------

def run(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Entry point for webhook execution from modal_webhook.py.

    Payload options:
      {"action": "send", "industry": "Dentist", "limit": 10}
      {"action": "send", "all": true, "limit": 50}
      {"action": "send", "industry": "Dentist", "dry_run": true}
      {"action": "test_auth"}
    """
    action = payload.get("action", "")

    if not action:
        return {"error": "action required: 'send' or 'test_auth'"}

    if action == "test_auth":
        try:
            token = get_microsoft_token()
            # Verify by checking mailbox
            sender = os.getenv("MICROSOFT_SENDER_EMAIL", "")
            if sender:
                url = f"{GRAPH_BASE}/users/{sender}/mailFolders/Drafts"
                headers = {"Authorization": f"Bearer {token}"}
                resp = http_requests.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                drafts_count = resp.json().get("totalItemCount", 0)
                return {
                    "status": "ok",
                    "message": f"Auth successful. Sender: {sender}. "
                               f"Current drafts: {drafts_count}",
                }
            return {"status": "ok", "message": "Auth successful (no sender configured)"}
        except Exception as e:
            return {"status": "error", "message": f"Auth failed: {e}"}

    if action == "send":
        industry = payload.get("industry")
        limit = payload.get("limit")
        dry_run = payload.get("dry_run", False)
        send_all = payload.get("all", False)
        exclude = payload.get("exclude", [])
        greeting_override = payload.get("greeting")

        if not industry and not send_all:
            return {"error": "Specify 'industry' or set 'all': true"}

        # Load templates
        templates = load_templates(token_data)

        # Fetch leads
        airtable_key = os.getenv("AIRTABLE_API_KEY")
        base_id = os.getenv("AIRTABLE_BASE_ID")
        leads_table_id = os.getenv("AIRTABLE_LEADS_ID")

        if not all([airtable_key, base_id, leads_table_id]):
            return {"error": "Airtable env vars not set"}

        leads = fetch_unemailed_leads(
            airtable_key, base_id, leads_table_id,
            industry=industry if not send_all else None,
            limit=limit,
        )

        if exclude:
            leads = [l for l in leads if l.get("fields", {}).get("Industry", "") not in exclude]

        if not leads:
            msg = "No unemailed leads found"
            if industry:
                msg += f" for {industry}"
            return {"status": "ok", "message": msg, "total": 0}

        # Send batch
        results = send_outreach_batch(
            leads, templates, dry_run=dry_run, token_data=token_data,
            greeting_override=greeting_override,
        )

        # Telegram notification
        if not dry_run:
            send_completion_telegram(results)

        return results

    return {"error": f"Unknown action: {action}"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Create draft outreach emails in Outlook via Microsoft Graph API"
    )
    parser.add_argument("--test-auth", action="store_true",
                        help="Test Microsoft Graph authentication")
    parser.add_argument("--industry", help="Target industry (e.g. 'Dentist')")
    parser.add_argument("--all", action="store_true",
                        help="Target all industries")
    parser.add_argument("--limit", type=int,
                        help="Max number of leads to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview emails without creating drafts")
    parser.add_argument("--exclude", nargs="+", metavar="INDUSTRY",
                        help="Industries to skip (e.g. --exclude Accountant)")
    parser.add_argument("--greeting", metavar="TEXT",
                        help="Override greeting (e.g. 'Good morning,')")

    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    if args.test_auth:
        result = run({"action": "test_auth"}, {})
        print(json.dumps(result, indent=2))
        sys.exit(0 if result.get("status") == "ok" else 1)

    if not args.industry and not args.all:
        print("Error: specify --industry or --all")
        parser.print_help()
        sys.exit(1)

    payload = {
        "action": "send",
        "dry_run": args.dry_run,
    }
    if args.industry:
        payload["industry"] = args.industry
    if args.all:
        payload["all"] = True
    if args.limit:
        payload["limit"] = args.limit
    if args.exclude:
        payload["exclude"] = args.exclude
    if args.greeting:
        payload["greeting"] = args.greeting

    result = run(payload, {})
    print(json.dumps(result, indent=2, default=str))

    if result.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
