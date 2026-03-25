#!/usr/bin/env python3
"""
Investment Research — Monthly ISA investment analysis via Manus AI.

Triggers a comprehensive Manus research task covering macroeconomics, asset
classes, market cycles, geopolitics, policy, tech trends, big tech deep dives,
and actionable recommendations for a moderate-risk index-fund-focused investor.

Output: Google Doc + Telegram notification.

Usage:
    # Trigger research manually (fires Manus task)
    python execution/investment_research.py --trigger

    # Register Manus webhook (one-time setup)
    python execution/investment_research.py --register-webhook "https://lewiscity10--claude-orchestrator-directive.modal.run?slug=investment-research-complete"
"""

import os
import sys
import json
import argparse
import requests
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("investment-research")

# ---------------------------------------------------------------------------
# Google Sheets helpers
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


# ---------------------------------------------------------------------------
# Tracker sheet helpers (tab in Automation Config spreadsheet)
# ---------------------------------------------------------------------------

def log_to_tracker(gc, config_sheet_id: str, row_data: dict):
    """Append a row to the Investment Research tracker tab."""
    try:
        spreadsheet = gc.open_by_key(config_sheet_id)
        try:
            ws = spreadsheet.worksheet("Investment Research")
        except Exception:
            ws = spreadsheet.add_worksheet(
                title="Investment Research", rows=100, cols=5
            )
            ws.append_row([
                "report_date", "manus_task_id", "status",
                "doc_url", "completed_at",
            ])

        headers = ws.row_values(1)
        row = [row_data.get(h, "") for h in headers]
        ws.append_row(row)
    except Exception as e:
        logger.error(f"Could not log to tracker: {e}")


def update_tracker_row(gc, config_sheet_id: str, task_id: str, updates: dict):
    """Update a row in the tracker by manus_task_id."""
    try:
        ws = gc.open_by_key(config_sheet_id).worksheet("Investment Research")
        headers = ws.row_values(1)
        records = ws.get_all_records()

        for i, row in enumerate(records):
            if str(row.get("manus_task_id")) == str(task_id):
                row_num = i + 2  # 1-indexed + header
                for key, value in updates.items():
                    if key in headers:
                        col = headers.index(key) + 1
                        ws.update_cell(row_num, col, value)
                return
    except Exception as e:
        logger.error(f"Could not update tracker row: {e}")


# ---------------------------------------------------------------------------
# Manus API
# ---------------------------------------------------------------------------

MANUS_API_BASE = "https://api.manus.ai/v1"


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
    text = unicodedata.normalize("NFKC", text)
    cleaned = []
    for ch in text:
        cp = ord(ch)
        if cp == 0x0A or cp == 0x09:
            cleaned.append(ch)
        elif cp >= 0x20:
            cleaned.append(ch)
    return "".join(cleaned)


def create_google_doc(token_data: dict, title: str,
                      markdown_content: str) -> str:
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
    CHUNK_SIZE = 50_000
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
# Telegram
# ---------------------------------------------------------------------------

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> bool:
    """Send a message via Telegram Bot API."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Telegram message sent to {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Research prompt
# ---------------------------------------------------------------------------

def build_research_prompt(month: str, year: str) -> str:
    """Build the comprehensive investment research prompt for Manus."""
    return f"""You are an investment research analyst producing a monthly report for {month} {year}.

The investor profile:
- UK-based, moderate risk tolerance
- Invests £500/month into a Stocks & Shares ISA on Trading 212
- Strategy: primarily index funds for diversification, open to ETFs, individual stocks, bonds, and commodities when opportunities arise
- Plans to max out Lifetime ISA (£4,000/year) at year-end and keep the rest in the S&S ISA
- Wants actionable, data-driven recommendations — not generic advice

Produce a comprehensive research report covering ALL of the following sections. Use real, current data where available. If exact figures aren't available, state the most recent data point and its date.

---

## Section 1: Macroeconomic Overview

Research and present:
- **Inflation**: UK CPI (latest figure + trend), US CPI, Eurozone HICP
- **Interest rates**: Bank of England base rate, US Federal Reserve funds rate, ECB rate — current levels, recent changes, and market expectations for next moves
- **Unemployment**: UK and US unemployment rates, job openings data (JOLTS for US, ONS for UK)
- **GDP growth**: Latest quarterly figures for UK, US, Eurozone, China
- **Consumer confidence**: UK GfK index, US Consumer Confidence Index

For each metric, explain how the current reading and trend typically affects: equities, bonds, commodities, and the pound sterling.

## Section 2: Asset Class Analysis

For each asset class below, provide: current price/level, YTD performance, 1-year performance, P/E ratio or equivalent valuation metric, and whether it appears overvalued, fairly valued, or undervalued relative to historical averages.

**Index Funds / ETFs:**
- S&P 500 (e.g. VUSA, CSP1)
- FTSE 100 (e.g. ISF, VUKE)
- MSCI World (e.g. VWRL, SWDA)
- Nasdaq 100 (e.g. EQQQ)
- FTSE All-World (e.g. VWRP)
- Any emerging market index worth noting

**Bonds:**
- UK Gilts (short and long duration)
- US Treasuries (10Y yield, 2Y yield, yield curve shape)
- Corporate bonds — investment grade vs high yield
- Is now a good time to be in bonds given the rate cycle?

**Commodities:**
- Gold — current price, YTD, historical context (safe haven demand?)
- Silver — current price, industrial demand outlook
- Oil (Brent) — supply/demand dynamics, OPEC decisions
- Any other commodities worth watching (copper, natural gas, etc.)

Flag any assets that have had a significant pullback (>10% from recent highs) that could represent a buying opportunity.

## Section 3: Market Cycles & Timing

- Where are we in the **business cycle**? (expansion, peak, contraction, trough) — cite supporting indicators
- **Global liquidity / M2 money supply**: What is the current trend? Historically, what has happened to equity markets 6-12 months after M2 expansion/contraction?
- **Federal Reserve balance sheet**: Is the Fed in QT or QE mode? What does this mean for asset prices?
- **Election year effects**: If applicable (US, UK, or major economy elections), what do historical patterns show?
- **Seasonal patterns**: Are there any relevant seasonal effects for this month (e.g. January effect, sell in May, Santa rally)?
- **Liquidity cycles**: Where are we in the typical 4-year liquidity cycle?

## Section 4: Geopolitical Risk Assessment

- Active or escalating **conflicts** that could affect markets (supply chains, energy, commodities)
- **Trade tensions**: tariffs, sanctions, or trade policy changes between major economies
- **Energy security**: any supply risks or major pipeline/shipping disruptions
- Upcoming **elections or political transitions** in major economies
- Any emerging risks that the market may not be pricing in yet

## Section 5: Policy & Regulatory Watch

- Recent or upcoming **UK tax changes** affecting ISA investors (ISA allowance changes, CGT, dividend tax)
- **US policy**: any fiscal stimulus, spending cuts, or regulatory changes affecting markets
- **Industry-specific regulation**: tech (AI regulation), energy (green subsidies/carbon taxes), financials
- **Central bank policy shifts**: any dovish/hawkish pivots signalled
- Any changes to ISA or LISA rules

## Section 6: Technology & Sector Trends

- **AI developments**: major model releases, enterprise adoption, which companies/ETFs benefit most
- **Quantum computing**: any breakthroughs or commercial progress
- **Clean energy / green transition**: policy support, investment flows, key companies
- **Healthcare / biotech**: any significant drug approvals, M&A, or breakthroughs
- **Sector momentum**: which sectors are gaining vs losing relative strength (tech, healthcare, energy, financials, consumer, industrials)

## Section 7: Magnificent 7 Deep Dive

For each of Apple (AAPL), Microsoft (MSFT), Alphabet/Google (GOOGL), Amazon (AMZN), Meta (META), Nvidia (NVDA), and Tesla (TSLA):

- Most recent quarterly earnings: revenue, EPS — did they beat or miss targets?
- Any notable **SEC filings** (10-K, 10-Q, insider trading, buyback announcements)
- Revenue and profit **growth trajectory** (YoY)
- Current **valuation**: P/E ratio, forward P/E, PEG ratio
- **Analyst consensus**: buy/hold/sell distribution, average price target
- **Key risks** specific to this company right now

## Section 8: Recommendations

Based on ALL the above research, provide specific, actionable recommendations for this month:

**Top Picks (3-5):** Index funds or ETFs to buy this month. For each:
- Exact fund name and ticker (available on Trading 212)
- Why now (supported by data from sections above)
- Suggested allocation percentage of the £500

**Opportunistic Buys (1-3):** Individual stocks or sector ETFs that look undervalued or have a compelling near-term catalyst.

**Watch List:** Assets worth monitoring for a potential future entry point — what price level or event would trigger a buy.

**Caution / Reconsider:** Any currently popular investments that may be overvalued, facing headwinds, or have deteriorating fundamentals. Be specific about why.

**Monthly Allocation Suggestion:** How to split the £500 this month, with percentages and reasoning. Example format:
- 50% VWRP (FTSE All-World) — core diversified holding
- 25% VUSA (S&P 500) — US large cap at reasonable valuation
- 15% SGLN (Gold ETC) — hedge against uncertainty
- 10% individual stock pick

---

IMPORTANT FORMATTING:
- Use clear Markdown headers (##, ###) for each section
- Include specific numbers, dates, and data points — not vague statements
- When referencing ETFs, use tickers available on Trading 212 (London Stock Exchange / LSE listed preferred)
- Target length: 4,000-6,000 words
- End with a brief disclaimer that this is AI-generated research, not financial advice
"""


# ---------------------------------------------------------------------------
# Trigger research (called by cron or CLI)
# ---------------------------------------------------------------------------

def trigger_research(token_data: dict = None,
                     slack_notify_fn=None) -> dict:
    """
    Main entry point: trigger monthly investment research via Manus.

    Called by:
    - Modal cron (investment_research_trigger) with token_data
    - Manual CLI run (uses local .env)
    """
    from dotenv import load_dotenv
    load_dotenv()

    manus_api_key = os.getenv("MANUS_API_KEY")
    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")

    if not manus_api_key:
        raise ValueError("MANUS_API_KEY not set in environment")
    if not config_sheet_id:
        raise ValueError("AUTOMATION_CONFIG_SHEET_ID not set in environment")

    notify = slack_notify_fn or (lambda msg: logger.info(msg))

    now = datetime.now(timezone.utc)
    month = now.strftime("%B")
    year = now.strftime("%Y")
    report_date = now.strftime("%Y-%m-%d")

    notify(f"*Investment Research* Starting {month} {year} research")

    # Build prompt and create Manus task
    prompt = build_research_prompt(month, year)

    try:
        result = create_manus_task(manus_api_key, prompt)
        task_id = result.get("id") or result.get("task_id", "unknown")
        logger.info(f"Manus task created: {task_id}")
        notify(f"*Investment Research* Manus task created: `{task_id}`")
    except Exception as e:
        logger.error(f"Failed to create Manus task: {e}")
        notify(f"*Investment Research* ERROR creating Manus task: {e}")
        return {"status": "error", "error": str(e)}

    # Log to tracker tab
    gc = get_gspread_client(token_data)
    log_to_tracker(gc, config_sheet_id, {
        "report_date": report_date,
        "manus_task_id": str(task_id),
        "status": "running",
        "doc_url": "",
        "completed_at": "",
    })

    return {
        "status": "running",
        "task_id": str(task_id),
        "month": f"{month} {year}",
    }


# ---------------------------------------------------------------------------
# Handle Manus completion webhook
# ---------------------------------------------------------------------------

def handle_completion(payload: dict, token_data: dict,
                      slack_notify_fn=None) -> dict:
    """
    Handle Manus task completion webhook.

    Called by the investment-research-complete webhook in modal_webhook.py.
    Downloads the report, creates Google Doc, sends Telegram notification.
    """
    from dotenv import load_dotenv
    load_dotenv()

    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
    telegram_token = os.getenv("INVESTMENT_BOT_TOKEN")
    telegram_chat_id = os.getenv("INVESTMENT_CHAT_ID")

    notify = slack_notify_fn or (lambda msg: logger.info(msg))

    # Extract task details from Manus webhook payload
    event_type = payload.get("event_type", "")
    task_detail = payload.get("task_detail", {})
    task_id = task_detail.get("task_id", payload.get("task_id", "unknown"))
    stop_reason = task_detail.get("stop_reason", "")

    notify(f"*Investment Research* Webhook received: {event_type} "
           f"for task `{task_id}`")

    # Only process completed tasks
    if event_type != "task_stopped" or stop_reason != "finish":
        if event_type == "task_stopped" and stop_reason != "finish":
            gc = get_gspread_client(token_data)
            update_tracker_row(gc, config_sheet_id, str(task_id), {
                "status": "error",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            notify(f"*Investment Research* Task `{task_id}` stopped "
                   f"with reason: {stop_reason}")
        return {
            "status": "skipped",
            "reason": f"event_type={event_type}, stop_reason={stop_reason}",
        }

    # Extract report content from attachments or message
    message = task_detail.get("message", "")
    attachments = task_detail.get("attachments", [])

    report_content = ""
    for att in attachments:
        fname = att.get("file_name", "")
        if fname.endswith((".md", ".txt", ".markdown")):
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
        notify("*Investment Research* ERROR: No report content in Manus output")
        return {"status": "error", "error": "No report content in Manus output"}

    now = datetime.now(timezone.utc)
    month = now.strftime("%B")
    year = now.strftime("%Y")
    doc_title = f"Investment Research - {month} {year}"

    # --- Step 1: Create Google Doc ---
    doc_url = ""
    try:
        doc_url = create_google_doc(token_data, doc_title, report_content)
        notify(f"*Investment Research* Google Doc created: {doc_url}")
    except Exception as e:
        logger.error(f"Google Doc creation failed: {e}")
        notify(f"*Investment Research* Google Doc failed: {e}")

    # --- Step 2: Send Telegram notification ---
    telegram_sent = False
    if telegram_token and telegram_chat_id:
        tg_msg = f"*Monthly Investment Research — {month} {year}*\n\n"
        if doc_url:
            tg_msg += f"[Read the full report]({doc_url})"
        else:
            tg_msg += "Report generation had issues — check logs."
        telegram_sent = send_telegram_message(
            telegram_token, telegram_chat_id, tg_msg
        )

    # --- Step 3: Update tracker ---
    gc = get_gspread_client(token_data)
    update_tracker_row(gc, config_sheet_id, str(task_id), {
        "status": "completed",
        "doc_url": doc_url,
        "completed_at": now.isoformat(),
    })

    return {
        "status": "success",
        "task_id": str(task_id),
        "doc_url": doc_url,
        "telegram_sent": telegram_sent,
    }


# ---------------------------------------------------------------------------
# Webhook entry point (Modal)
# ---------------------------------------------------------------------------

def run(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Entry point for procedural webhook execution from modal_webhook.py.
    Called when investment-research-complete webhook fires with Manus payload.
    """
    return handle_completion(payload, token_data, slack_notify_fn)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Monthly Investment Research via Manus AI"
    )
    parser.add_argument("--trigger", action="store_true",
                        help="Trigger a Manus research task now")
    parser.add_argument("--register-webhook",
                        help="Register Manus webhook with this callback URL")

    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    if args.register_webhook:
        api_key = os.getenv("MANUS_API_KEY")
        if not api_key:
            print("Error: MANUS_API_KEY not set in .env")
            sys.exit(1)
        result = register_manus_webhook(api_key, args.register_webhook)
        print(json.dumps(result, indent=2))
        return

    if args.trigger:
        result = trigger_research()
        print(json.dumps(result, indent=2))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
