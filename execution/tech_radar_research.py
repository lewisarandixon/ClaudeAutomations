#!/usr/bin/env python3
"""
Tech Radar Research - Weekly AI/Automation Scanner

Triggers a Manus AI agent to scan AI/automation sources for new developments
from the last 7 days, produces a structured Markdown report, generates a
NotebookLM-style podcast via Google Cloud Podcast API, and delivers both
via Telegram and email.

Usage:
    # Triggered automatically by Modal cron (Friday 9am GMT)
    # Can also be run manually:
    python3 execution/tech_radar_research.py

    # Or with custom date range:
    python3 execution/tech_radar_research.py --from-date 2026-01-27 --to-date 2026-02-03

    # Register your Manus webhook (one-time setup):
    python3 execution/tech_radar_research.py --register-webhook "https://your-modal-url/d/tech-radar-complete"
"""

import os
import sys
import json
import time
import argparse
import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tech-radar")

# ---------------------------------------------------------------------------
# Google Sheets helpers (mirrors patterns from gmaps_lead_pipeline.py)
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]


def get_gspread_client(token_data: dict = None):
    """Get authenticated gspread client."""
    import gspread
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    if token_data:
        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=token_data.get("scopes", SCOPES),
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token
    else:
        # Local mode: use token.json or GOOGLE_TOKEN_JSON env var
        from dotenv import load_dotenv
        load_dotenv()

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

    return gspread.authorize(creds)


def get_google_creds(token_data: dict = None):
    """Get raw Google credentials object (for non-gspread APIs)."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    if token_data:
        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=token_data.get("scopes", SCOPES),
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token
        return creds
    else:
        from dotenv import load_dotenv
        load_dotenv()
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
        elif os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        else:
            raise RuntimeError("No Google credentials found")

    if creds.expired:
        creds.refresh(Request())
    return creds


def get_last_scan_date(gc, config_sheet_id: str) -> str:
    """Read last_tech_scan_date from Automation Config sheet."""
    try:
        sheet = gc.open_by_key(config_sheet_id).sheet1
        records = sheet.get_all_records()
        for row in records:
            if row.get("setting_name") == "last_tech_scan_date":
                return row.get("setting_value", "")
    except Exception as e:
        logger.warning(f"Could not read last scan date: {e}")
    return ""


def update_last_scan_date(gc, config_sheet_id: str, date_str: str):
    """Update last_tech_scan_date in Automation Config sheet."""
    try:
        sheet = gc.open_by_key(config_sheet_id).sheet1
        records = sheet.get_all_records()
        for i, row in enumerate(records):
            if row.get("setting_name") == "last_tech_scan_date":
                sheet.update_cell(i + 2, 2, date_str)  # +2: 1-indexed + header
                return
        # If not found, append it
        sheet.append_row(["last_tech_scan_date", date_str])
    except Exception as e:
        logger.error(f"Could not update last scan date: {e}")


def log_to_tracker(gc, tracker_sheet_id: str, row_data: dict):
    """Append a row to the Tech Radar Tracker sheet."""
    try:
        sheet = gc.open_by_key(tracker_sheet_id).sheet1
        headers = sheet.row_values(1)
        if not headers:
            headers = [
                "scan_date", "scan_period_start", "scan_period_end",
                "manus_task_id", "status", "doc_url", "podcast_url", "completed_at"
            ]
            sheet.append_row(headers)

        row = [row_data.get(h, "") for h in headers]
        sheet.append_row(row)
    except Exception as e:
        logger.error(f"Could not log to tracker: {e}")


def update_tracker_row(gc, tracker_sheet_id: str, task_id: str, updates: dict):
    """Update a row in the tracker by manus_task_id."""
    try:
        sheet = gc.open_by_key(tracker_sheet_id).sheet1
        headers = sheet.row_values(1)
        records = sheet.get_all_records()

        for i, row in enumerate(records):
            if str(row.get("manus_task_id")) == str(task_id):
                row_num = i + 2  # 1-indexed + header
                for key, value in updates.items():
                    if key in headers:
                        col = headers.index(key) + 1
                        sheet.update_cell(row_num, col, value)
                return
    except Exception as e:
        logger.error(f"Could not update tracker row: {e}")


# ---------------------------------------------------------------------------
# Manus API (REST - https://api.manus.ai/v1)
# ---------------------------------------------------------------------------

MANUS_API_BASE = "https://api.manus.ai/v1"


def build_manus_prompt(scan_start: str, scan_end: str) -> str:
    """Build the comprehensive research prompt for Manus."""
    return f"""You are a research analyst. Scan the following AI and automation sources for NEW developments published between {scan_start} and {scan_end}. Only include items from this date range.

## Sources to Scan

### AI Platforms (Priority - check these first)
- OpenAI blog (https://openai.com/blog) and API changelog (https://platform.openai.com/docs/changelog)
- Anthropic blog (https://www.anthropic.com/news) and Claude release notes
- Google AI blog (https://blog.google/technology/ai/) for Gemini updates
- Manus AI updates (https://manus.im/blog)
- Mistral AI blog (https://mistral.ai/news/)
- Cohere blog (https://cohere.com/blog)
- xAI announcements (https://x.ai)

### Automation Platforms
- Make.com blog (https://www.make.com/en/blog)
- n8n blog (https://blog.n8n.io/)
- Zapier blog (https://zapier.com/blog)
- Pipedream blog (https://pipedream.com/blog)

### News & Aggregators
- Product Hunt - AI, Automation, and Dev Tools categories (https://www.producthunt.com)
- Hacker News - search for: automation, AI tool, LLM, agent (https://hn.algolia.com)
- Ben's Bites (https://bensbites.com)
- The Rundown AI (https://www.therundown.ai)
- TLDR AI (https://tldr.tech/ai)

### YouTube (check for recent uploads in this date range)
- Nick Saraev, Nate Herk, Matt Wolfe, AI Advantage
- Liam Ottley, AI Jason, Skill Leap AI, WorldofAI

### Reddit (posts from this date range)
- r/automation, r/nocode, r/artificial, r/ChatGPT
- r/LocalLLaMA, r/ClaudeAI, r/OpenAI

### Twitter/X
- Search terms: "new AI model", "automation update", "API release"
- Accounts: @OpenAI, @AnthropicAI, @GoogleAI

## What to Extract
For each source, extract:
- AI model updates (releases, API changes, new features, pricing changes)
- Automation platform updates (new features, integrations, pricing)
- New tools launched (name, category, what it does, integration potential, URL, date)
- Integration announcements (Platform A + Platform B, what it enables)
- Trending topics (what's being discussed, where, why it matters)
- Industry news (funding rounds, acquisitions, partnerships)
- Notable YouTube videos (title, channel, key takeaway, link)

## Output Format
Return a complete Markdown report with this EXACT structure:

# Weekly AI/Automation Tech Radar
Week of {scan_end}

## Executive Summary
[2-3 sentences covering the most significant developments this week]

## AI Model Updates
### OpenAI
[Updates or "No significant updates this week"]
### Anthropic (Claude)
[Updates or "No significant updates this week"]
### Google AI (Gemini)
[Updates or "No significant updates this week"]
### Manus AI
[Updates or "No significant updates this week"]
### Other Models
[Mistral, Cohere, xAI, others]

## Automation Platform Updates
### Make.com
[Updates or "No significant updates this week"]
### n8n
[Updates or "No significant updates this week"]
### Zapier
[Updates or "No significant updates this week"]

## New Tools Launched
For each new tool found:
### [Tool Name]
- Category: [AI/Automation/Dev Tool/etc]
- What it does: [1-2 sentences]
- Why it matters: [1 sentence on relevance]
- Integration potential: [How it could fit into existing workflows]
- Link: [URL]
- Released: [Date]

## Integration Announcements
[Any new platform integrations, API partnerships, etc.]

## Trending Discussions
[What the community is talking about on Reddit, HN, Twitter]

## Industry News
[Funding, acquisitions, partnerships, major hires]

## Notable YouTube Videos
For each relevant video:
- **[Video Title]** by [Channel Name] - [Key takeaway in 1 sentence] - [Link]

## Opportunities for You
### High Priority
[Developments that directly affect your DOE framework or current automations]
### Medium Priority
[Interesting developments worth exploring soon]
### Low Priority
[Nice-to-know items for future reference]

## Research Methodology
Sources scanned: [list all sources checked]
Date range: {scan_start} to {scan_end}
Sources unavailable: [list any that couldn't be accessed]

IMPORTANT: Only include developments from the specified date range ({scan_start} to {scan_end}). Do not include older news. If a source has no new content in this period, note "No significant updates this week" for that source."""


def create_manus_task(api_key: str, prompt: str) -> dict:
    """Create a Manus research task via REST API."""
    headers = {
        "API_KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "agentProfile": "manus-1.6",
    }

    response = requests.post(
        f"{MANUS_API_BASE}/tasks",
        headers=headers,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_manus_task(api_key: str, task_id: str) -> dict:
    """Get task status from Manus API."""
    headers = {"API_KEY": api_key}
    response = requests.get(
        f"{MANUS_API_BASE}/tasks/{task_id}",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def register_manus_webhook(api_key: str, callback_url: str) -> dict:
    """Register a webhook with Manus for task completion notifications."""
    headers = {
        "API_KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "webhook": {
            "url": callback_url,
        }
    }
    response = requests.post(
        f"{MANUS_API_BASE}/webhooks",
        headers=headers,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Google Docs creation
# ---------------------------------------------------------------------------

def _sanitize_doc_text(text: str) -> str:
    """
    Strip characters that cause the Google Docs API batchUpdate to fail.
    Removes null bytes and non-printable control characters (keeps newlines/tabs).
    Normalises Unicode to avoid encoding issues from Manus output.
    """
    import unicodedata
    # Normalise unicode (handles smart quotes, em-dashes etc.)
    text = unicodedata.normalize("NFKC", text)
    # Strip null bytes and control chars except \n (0x0A) and \t (0x09)
    cleaned = []
    for ch in text:
        cp = ord(ch)
        if cp == 0x0A or cp == 0x09:
            cleaned.append(ch)
        elif cp >= 0x20:  # Normal printable range
            cleaned.append(ch)
        # Everything else (null bytes, control chars) is silently dropped
    return "".join(cleaned)


def create_google_doc(token_data: dict, title: str, markdown_content: str) -> str:
    """Create a Google Doc with the report content. Returns the doc URL."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri"),
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes", SCOPES),
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    docs_service = build("docs", "v1", credentials=creds)

    # Create blank doc — URL is valid from this point regardless of content insertion
    doc = docs_service.documents().create(body={"title": title}).execute()
    doc_id = doc["documentId"]
    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"

    # Sanitize content and insert in chunks (Docs API can reject long/dirty text)
    text = _sanitize_doc_text(markdown_content)
    CHUNK_SIZE = 50_000  # Safe limit per insertText request
    chunks = [text[i:i + CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]

    try:
        # Insert chunks in reverse order at index 1 so final order is correct
        for chunk in reversed(chunks):
            docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={"requests": [{"insertText": {"location": {"index": 1}, "text": chunk}}]},
            ).execute()
        logger.info(f"Created Google Doc: {doc_url}")
    except Exception as e:
        # Doc exists and link is valid — content insertion failed but URL still works
        logger.error(f"Google Doc content insertion failed: {e}. Doc created at {doc_url}")

    return doc_url


# ---------------------------------------------------------------------------
# Google Cloud Podcast API (NotebookLM-style audio)
# ---------------------------------------------------------------------------

PODCAST_API_BASE = "https://discoveryengine.googleapis.com/v1"


def generate_podcast(token_data: dict, report_text: str, title: str, notify_fn=None) -> str:
    """
    Generate a podcast from the research report using Google Cloud Podcast API.
    Returns a Google Drive shareable URL for the MP3, or empty string on failure.

    API: POST /v1/projects/{PROJECT_ID}/locations/global/podcasts
    Requires: Discovery Engine API enabled + Podcast API User IAM role
    """
    notify = notify_fn or (lambda msg: logger.info(msg))
    gcp_project_id = os.getenv("GCP_PROJECT_ID")

    if not gcp_project_id:
        notify("*Tech Radar* Podcast skipped: GCP_PROJECT_ID not set")
        return ""

    creds = get_google_creds(token_data)

    # Truncate report to stay under 100k token limit (~400k chars is safe)
    context_text = report_text[:350000]

    # Step 1: Create podcast
    create_url = f"{PODCAST_API_BASE}/projects/{gcp_project_id}/locations/global/podcasts"
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }
    payload = {
        "title": title,
        "description": f"AI-generated podcast overview of {title}",
        "podcastConfig": {
            "focus": (
                "Summarize the most important AI and automation developments this week. "
                "Highlight model releases, new tools, platform updates, and opportunities. "
                "Keep it conversational and actionable for someone building AI automations."
            ),
            "length": "STANDARD",
        },
        "contexts": [
            {"text": context_text}
        ],
    }

    try:
        resp = requests.post(create_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        operation = resp.json()
        operation_name = operation.get("name", "")
        logger.info(f"Podcast operation started: {operation_name}")
        notify(f"*Tech Radar* Podcast generation started: `{operation_name}`")
    except Exception as e:
        logger.error(f"Podcast API create failed: {e}")
        notify(f"*Tech Radar* Podcast creation failed: {e}")
        return ""

    if not operation_name:
        notify("*Tech Radar* Podcast API returned no operation name")
        return ""

    # Step 2: Poll for completion (max 10 min)
    poll_url = f"{PODCAST_API_BASE}/{operation_name}"
    max_polls = 60  # 60 x 10s = 10 min
    for i in range(max_polls):
        time.sleep(10)
        try:
            # Refresh token if needed
            if creds.expired:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            poll_headers = {
                "Authorization": f"Bearer {creds.token}",
            }
            poll_resp = requests.get(poll_url, headers=poll_headers, timeout=30)
            poll_resp.raise_for_status()
            poll_data = poll_resp.json()

            if poll_data.get("done"):
                logger.info(f"Podcast generation complete after {(i + 1) * 10}s")
                notify(f"*Tech Radar* Podcast ready after {(i + 1) * 10}s")
                break
        except Exception as e:
            logger.warning(f"Podcast poll attempt {i + 1} failed: {e}")
            continue
    else:
        notify("*Tech Radar* Podcast generation timed out after 10 minutes")
        return ""

    # Step 3: Download the MP3
    download_url = f"{PODCAST_API_BASE}/{operation_name}:download?alt=media"
    try:
        if creds.expired:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
        dl_headers = {"Authorization": f"Bearer {creds.token}"}
        dl_resp = requests.get(download_url, headers=dl_headers, timeout=120, stream=True)
        dl_resp.raise_for_status()
        mp3_data = dl_resp.content
        logger.info(f"Downloaded podcast MP3: {len(mp3_data)} bytes")
    except Exception as e:
        logger.error(f"Podcast download failed: {e}")
        notify(f"*Tech Radar* Podcast download failed: {e}")
        return ""

    # Step 4: Upload MP3 to Google Drive and get shareable link
    try:
        podcast_url = upload_mp3_to_drive(token_data, mp3_data, f"{title}.mp3")
        notify(f"*Tech Radar* Podcast uploaded to Drive: {podcast_url}")
        return podcast_url
    except Exception as e:
        logger.error(f"Podcast Drive upload failed: {e}")
        notify(f"*Tech Radar* Podcast Drive upload failed: {e}")
        return ""


def upload_mp3_to_drive(token_data: dict, mp3_bytes: bytes, filename: str) -> str:
    """Upload MP3 to Google Drive and return a shareable link."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaInMemoryUpload
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri"),
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes", SCOPES),
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    drive_service = build("drive", "v3", credentials=creds)

    # Upload file
    file_metadata = {"name": filename, "mimeType": "audio/mpeg"}
    media = MediaInMemoryUpload(mp3_bytes, mimetype="audio/mpeg")
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink",
    ).execute()

    file_id = file.get("id")
    web_link = file.get("webViewLink", "")

    # Make it accessible via link
    drive_service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()

    # Get the direct link
    if not web_link:
        web_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"

    logger.info(f"Uploaded MP3 to Drive: {web_link}")
    return web_link


# ---------------------------------------------------------------------------
# Telegram notification
# ---------------------------------------------------------------------------

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> bool:
    """Send a message via Telegram Bot API."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Telegram message sent to {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Email backup (uses Gmail API pattern from modal_webhook.py)
# ---------------------------------------------------------------------------

def send_email_backup(token_data: dict, to_email: str, subject: str, body: str):
    """Send email backup via Gmail API."""
    import base64
    from email.mime.text import MIMEText
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri"),
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes", SCOPES),
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    service = build("gmail", "v1", credentials=creds)
    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    logger.info(f"Email sent to {to_email} | ID: {result['id']}")
    return result


# ---------------------------------------------------------------------------
# Main trigger function (called by Modal cron or manually)
# ---------------------------------------------------------------------------

def trigger_scan(token_data: dict = None, slack_notify_fn=None) -> dict:
    """
    Main entry point: trigger a Tech Radar scan via Manus.

    Called by:
    - Modal cron (tech_radar_trigger) with token_data
    - Manual CLI run (uses local .env)
    """
    from dotenv import load_dotenv
    load_dotenv()

    manus_api_key = os.getenv("MANUS_API_KEY")
    tracker_sheet_id = os.getenv("TECH_RADAR_TRACKER_SHEET_ID")
    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")

    if not manus_api_key:
        raise ValueError("MANUS_API_KEY not set in environment")
    if not tracker_sheet_id:
        raise ValueError("TECH_RADAR_TRACKER_SHEET_ID not set in environment")
    if not config_sheet_id:
        raise ValueError("AUTOMATION_CONFIG_SHEET_ID not set in environment")

    notify = slack_notify_fn or (lambda msg: logger.info(msg))

    # Get Google Sheets client
    gc = get_gspread_client(token_data)

    # Get last scan date (or default to 7 days ago)
    last_scan = get_last_scan_date(gc, config_sheet_id)
    if not last_scan:
        last_scan = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        logger.info(f"No last scan date found, defaulting to {last_scan}")

    # Allow CLI override
    last_scan = os.getenv("_OVERRIDE_FROM_DATE", last_scan)
    scan_end = os.getenv("_OVERRIDE_TO_DATE", datetime.utcnow().strftime("%Y-%m-%d"))

    notify(f"*Tech Radar* Starting scan: {last_scan} to {scan_end}")

    # Build prompt and create Manus task
    prompt = build_manus_prompt(last_scan, scan_end)

    try:
        result = create_manus_task(manus_api_key, prompt)
        task_id = result.get("id") or result.get("task_id", "unknown")
        logger.info(f"Manus task created: {task_id}")
        notify(f"*Tech Radar* Manus task created: `{task_id}`")
    except Exception as e:
        logger.error(f"Failed to create Manus task: {e}")
        notify(f"*Tech Radar* ERROR creating Manus task: {e}")
        return {"status": "error", "error": str(e)}

    # Log to tracker sheet
    log_to_tracker(gc, tracker_sheet_id, {
        "scan_date": scan_end,
        "scan_period_start": last_scan,
        "scan_period_end": scan_end,
        "manus_task_id": str(task_id),
        "status": "running",
        "doc_url": "",
        "podcast_url": "",
        "completed_at": "",
    })

    return {
        "status": "running",
        "task_id": str(task_id),
        "scan_period": f"{last_scan} to {scan_end}",
    }


def handle_completion(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Handle Manus task completion webhook.

    Called by the tech-radar-complete webhook in modal_webhook.py.
    Receives the Manus webhook payload, extracts the report,
    creates Google Doc, generates podcast, sends notifications.
    """
    from dotenv import load_dotenv
    load_dotenv()

    tracker_sheet_id = os.getenv("TECH_RADAR_TRACKER_SHEET_ID")
    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    your_email = os.getenv("YOUR_EMAIL")

    notify = slack_notify_fn or (lambda msg: logger.info(msg))

    # Extract task details from Manus webhook payload
    event_type = payload.get("event_type", "")
    task_detail = payload.get("task_detail", {})
    task_id = task_detail.get("task_id", payload.get("task_id", "unknown"))
    stop_reason = task_detail.get("stop_reason", "")

    notify(f"*Tech Radar* Webhook received: {event_type} for task `{task_id}`")

    # Only process completed tasks
    if event_type != "task_stopped" or stop_reason != "finish":
        if event_type == "task_stopped" and stop_reason != "finish":
            gc = get_gspread_client(token_data)
            update_tracker_row(gc, tracker_sheet_id, str(task_id), {
                "status": "error",
                "completed_at": datetime.utcnow().isoformat(),
            })
            notify(f"*Tech Radar* Task `{task_id}` stopped with reason: {stop_reason}")
        return {"status": "skipped", "reason": f"event_type={event_type}, stop_reason={stop_reason}"}

    # Extract the markdown report from the payload
    message = task_detail.get("message", "")
    attachments = task_detail.get("attachments", [])

    # Try to get report from attachments first (Manus may output a .md file)
    report_content = ""
    for att in attachments:
        fname = att.get("file_name", "")
        if fname.endswith(".md") or fname.endswith(".txt") or fname.endswith(".markdown"):
            try:
                file_url = att.get("url", "")
                if file_url:
                    resp = requests.get(file_url, timeout=30)
                    resp.raise_for_status()
                    report_content = resp.text
                    break
            except Exception as e:
                logger.warning(f"Could not download attachment {fname}: {e}")

    # Fallback to message content
    if not report_content:
        report_content = message

    if not report_content:
        notify("*Tech Radar* ERROR: No report content found in Manus output")
        return {"status": "error", "error": "No report content in Manus output"}

    today = datetime.utcnow().strftime("%Y-%m-%d")
    doc_title = f"Tech Radar - Week of {today}"

    # --- Step 1: Create Google Doc ---
    doc_url = ""
    try:
        doc_url = create_google_doc(token_data, doc_title, report_content)
        notify(f"*Tech Radar* Google Doc created: {doc_url}")
    except Exception as e:
        logger.error(f"Google Doc creation failed: {e}")
        notify(f"*Tech Radar* Google Doc failed: {e}")

    # --- Step 2: Generate podcast from the research report ---
    podcast_url = ""
    try:
        podcast_url = generate_podcast(
            token_data=token_data,
            report_text=report_content,
            title=doc_title,
            notify_fn=notify,
        )
    except Exception as e:
        logger.error(f"Podcast generation failed: {e}")
        notify(f"*Tech Radar* Podcast failed (non-blocking): {e}")

    # --- Step 3: Send Telegram notification ---
    telegram_sent = False
    if telegram_token and telegram_chat_id:
        tg_lines = ["*Weekly Tech Radar Ready*\n"]
        if doc_url:
            tg_lines.append(f"[Read the Report]({doc_url})")
        if podcast_url:
            tg_lines.append(f"[Listen to the Podcast]({podcast_url})")
        if not doc_url and not podcast_url:
            tg_lines.append("Report generation had issues - check email for details.")
        tg_message = "\n".join(tg_lines)
        telegram_sent = send_telegram_message(telegram_token, telegram_chat_id, tg_message)

    # --- Step 4: Send email backup ---
    email_sent = False
    if your_email:
        try:
            email_subject = f"Weekly Tech Radar - {today}"
            email_body = f"Your weekly Tech Radar report is ready.\n\n"
            if doc_url:
                email_body += f"Google Doc: {doc_url}\n"
            if podcast_url:
                email_body += f"Podcast: {podcast_url}\n"
            email_body += "\n---\n\n"
            email_body += report_content[:50000]  # Truncate if massive
            send_email_backup(token_data, your_email, email_subject, email_body)
            email_sent = True
            notify(f"*Tech Radar* Email sent to {your_email}")
        except Exception as e:
            logger.error(f"Email backup failed: {e}")
            notify(f"*Tech Radar* Email failed: {e}")

    # --- Step 5: Update tracker sheet ---
    gc = get_gspread_client(token_data)
    update_tracker_row(gc, tracker_sheet_id, str(task_id), {
        "status": "completed",
        "doc_url": doc_url,
        "podcast_url": podcast_url,
        "completed_at": datetime.utcnow().isoformat(),
    })

    # Update last scan date
    update_last_scan_date(gc, config_sheet_id, today)

    return {
        "status": "success",
        "task_id": str(task_id),
        "doc_url": doc_url,
        "podcast_url": podcast_url,
        "telegram_sent": telegram_sent,
        "email_sent": email_sent,
    }


# ---------------------------------------------------------------------------
# run() function for procedural webhook execution
# (same pattern as instantly_autoreply.py - called by modal_webhook.py)
# ---------------------------------------------------------------------------

def run(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Entry point for procedural webhook execution from modal_webhook.py.
    Called when tech-radar-complete webhook fires with Manus payload.
    """
    return handle_completion(payload, token_data, slack_notify_fn)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Tech Radar Research Scanner")
    parser.add_argument("--from-date", help="Scan start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="Scan end date (YYYY-MM-DD)")
    parser.add_argument("--register-webhook", help="Register Manus webhook with this callback URL")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    # Register webhook mode
    if args.register_webhook:
        api_key = os.getenv("MANUS_API_KEY")
        if not api_key:
            print("Error: MANUS_API_KEY not set in .env")
            sys.exit(1)
        result = register_manus_webhook(api_key, args.register_webhook)
        print(json.dumps(result, indent=2))
        return

    # Override dates if provided
    if args.from_date:
        os.environ["_OVERRIDE_FROM_DATE"] = args.from_date
    if args.to_date:
        os.environ["_OVERRIDE_TO_DATE"] = args.to_date

    result = trigger_scan()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
