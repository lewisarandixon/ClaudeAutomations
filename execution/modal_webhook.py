"""
Modal webhook server for event-driven Claude orchestration.

Deploy: modal deploy execution/modal_webhook.py
Logs:   modal logs claude-orchestrator

Endpoints:
  GET  /test-email              - Test email (hardcoded)
  POST /d/{slug}                - Execute a specific directive by slug
  GET  /list                    - List available webhook slugs

Configure webhooks in execution/webhooks.json
Each slug maps to exactly ONE directive (security isolation).
"""

import modal
import os
import json
import base64
import logging
import urllib.request
import urllib.parse
import re
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claude-orchestrator")

# Define the Modal app
app = modal.App("claude-orchestrator")

# Create image with required packages and local files
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "anthropic",
        "fastapi",
        "google-auth",
        "google-auth-oauthlib",
        "google-api-python-client",
        "requests",
        "apify-client",
        "gspread",
        "pandas",
        "python-dotenv",
        "yt-dlp",  # For YouTube video scraping
        "openai",  # For Manus API OpenAI-compat layer
        "beautifulsoup4",  # For website contact scraping
    )
    .add_local_dir("C:/automation_tools1/directives", remote_path="/app/directives")
    .add_local_dir("C:/automation_tools1/execution", remote_path="/app/execution")
    .add_local_file("C:/automation_tools1/execution/webhooks.json", remote_path="/app/webhooks.json")
)

# All secrets
ALL_SECRETS = [
    modal.Secret.from_name("anthropic-secret"),
    modal.Secret.from_name("google-token"),
    modal.Secret.from_name("env-vars"),
    modal.Secret.from_name("slack-webhook"),
    modal.Secret.from_name("instantly-api"),
    modal.Secret.from_name("apify-secret"),
    modal.Secret.from_name("anymailfinder-secret"),
    modal.Secret.from_name("pandadoc-secret"),
    modal.Secret.from_name("manus-secret"),
    modal.Secret.from_name("telegram-secret"),
    modal.Secret.from_name("airtable-secret"),
    modal.Secret.from_name("microsoft-secret"),
    modal.Secret.from_name("investment-secret"),
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def column_letter(n):
    """Convert column index (0-based) to Excel-style column letter (A, B, ... Z, AA, AB, ...)."""
    result = ""
    while n >= 0:
        result = chr(65 + (n % 26)) + result
        n = n // 26 - 1
    return result


# ============================================================================
# TOOL DEFINITIONS
# ============================================================================

ALL_TOOLS = {
    "send_email": {
        "name": "send_email",
        "description": "Send an email via Gmail.",
        "input_schema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Recipient email address"},
                "subject": {"type": "string", "description": "Email subject line"},
                "body": {"type": "string", "description": "Email body content"}
            },
            "required": ["to", "subject", "body"]
        }
    },
    "read_sheet": {
        "name": "read_sheet",
        "description": "Read data from a Google Sheet. Returns all rows as a 2D array.",
        "input_schema": {
            "type": "object",
            "properties": {
                "spreadsheet_id": {"type": "string", "description": "The Google Sheet ID"},
                "range": {"type": "string", "description": "A1 notation range (e.g., 'Sheet1!A1:D10' or 'Sheet1!A:Z' for all)"}
            },
            "required": ["spreadsheet_id", "range"]
        }
    },
    "update_sheet": {
        "name": "update_sheet",
        "description": "Update cells in a Google Sheet.",
        "input_schema": {
            "type": "object",
            "properties": {
                "spreadsheet_id": {"type": "string", "description": "The Google Sheet ID"},
                "range": {"type": "string", "description": "A1 notation range"},
                "values": {"type": "array", "description": "2D array of values to write"}
            },
            "required": ["spreadsheet_id", "range", "values"]
        }
    },
    "instantly_get_emails": {
        "name": "instantly_get_emails",
        "description": "Get email conversation history from Instantly for a specific lead email address.",
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_email": {"type": "string", "description": "The lead's email address to search for"},
                "limit": {"type": "integer", "description": "Max emails to return (default 10)", "default": 10}
            },
            "required": ["lead_email"]
        }
    },
    "instantly_send_reply": {
        "name": "instantly_send_reply",
        "description": "Send a reply to an email thread in Instantly.",
        "input_schema": {
            "type": "object",
            "properties": {
                "eaccount": {"type": "string", "description": "The email account to send from"},
                "reply_to_uuid": {"type": "string", "description": "The UUID of the email to reply to"},
                "subject": {"type": "string", "description": "Email subject line"},
                "html_body": {"type": "string", "description": "HTML body of the reply"}
            },
            "required": ["eaccount", "reply_to_uuid", "subject", "html_body"]
        }
    },
    "web_search": {
        "name": "web_search",
        "description": "Search the web for information. Use this to research people, companies, products, or any unfamiliar terms.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"}
            },
            "required": ["query"]
        }
    },
    "web_fetch": {
        "name": "web_fetch",
        "description": "Fetch and read content from a specific URL. Returns the text content of the page.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to fetch"}
            },
            "required": ["url"]
        }
    },
    "create_proposal": {
        "name": "create_proposal",
        "description": "Create a PandaDoc proposal document from structured client and project data. Returns document ID and URL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "client": {
                    "type": "object",
                    "description": "Client information",
                    "properties": {
                        "first_name": {"type": "string"},
                        "last_name": {"type": "string"},
                        "email": {"type": "string"},
                        "company": {"type": "string"}
                    },
                    "required": ["email", "company"]
                },
                "project": {
                    "type": "object",
                    "description": "Project details",
                    "properties": {
                        "title": {"type": "string"},
                        "monthOneInvestment": {"type": "string"},
                        "monthTwoInvestment": {"type": "string"},
                        "monthThreeInvestment": {"type": "string"},
                        "problems": {
                            "type": "object",
                            "properties": {
                                "problem01": {"type": "string"},
                                "problem02": {"type": "string"},
                                "problem03": {"type": "string"},
                                "problem04": {"type": "string"}
                            }
                        },
                        "benefits": {
                            "type": "object",
                            "properties": {
                                "benefit01": {"type": "string"},
                                "benefit02": {"type": "string"},
                                "benefit03": {"type": "string"},
                                "benefit04": {"type": "string"}
                            }
                        }
                    },
                    "required": ["title"]
                }
            },
            "required": ["client", "project"]
        }
    }
}

# ============================================================================
# TOOL IMPLEMENTATIONS
# ============================================================================

def send_email_impl(to: str, subject: str, body: str, token_data: dict) -> dict:
    """Send email via Gmail API."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"]
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    service = build("gmail", "v1", credentials=creds)
    message = MIMEText(body)
    message["to"] = to
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    logger.info(f"📧 Email sent to {to} | ID: {result['id']}")
    return {"status": "sent", "message_id": result["id"]}


def read_sheet_impl(spreadsheet_id: str, range: str, token_data: dict) -> dict:
    """Read from Google Sheet."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"]
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range
    ).execute()

    values = result.get("values", [])
    logger.info(f"📊 Read {len(values)} rows from sheet")
    return {"rows": len(values), "values": values}


def update_sheet_impl(spreadsheet_id: str, range: str, values: list, token_data: dict) -> dict:
    """Update Google Sheet."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"]
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    service = build("sheets", "v4", credentials=creds)
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()

    logger.info(f"📊 Updated {result.get('updatedCells', 0)} cells")
    return {"updated_cells": result.get("updatedCells", 0)}


def instantly_get_emails_impl(lead_email: str, limit: int = 10) -> dict:
    """Get email conversation history from Instantly."""
    import requests

    api_key = os.getenv("INSTANTLY_API_KEY")
    if not api_key:
        return {"error": "INSTANTLY_API_KEY not configured"}

    url = "https://api.instantly.ai/api/v2/emails"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"limit": limit, "search": lead_email}

    response = requests.get(url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        logger.error(f"Instantly API error: {response.status_code} - {response.text}")
        return {"error": f"Instantly API error: {response.status_code}"}

    data = response.json()
    items = data.get("items", [])
    logger.info(f"📬 Retrieved {len(items)} emails for {lead_email}")

    # Format emails for easier reading
    formatted = []
    for item in items:
        formatted.append({
            "id": item.get("id"),
            "uuid": item.get("uuid"),
            "from": item.get("from_address_email"),
            "to": item.get("to_address_email_list"),
            "subject": item.get("subject"),
            "body_text": item.get("body", {}).get("text", ""),
            "body_html": item.get("body", {}).get("html", ""),
            "timestamp": item.get("timestamp"),
            "eaccount": item.get("eaccount"),
        })

    return {"count": len(formatted), "emails": formatted}


def instantly_send_reply_impl(eaccount: str, reply_to_uuid: str, subject: str, html_body: str) -> dict:
    """Send a reply via Instantly."""
    import requests

    api_key = os.getenv("INSTANTLY_API_KEY")
    if not api_key:
        return {"error": "INSTANTLY_API_KEY not configured"}

    url = "https://api.instantly.ai/api/v2/emails/reply"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "eaccount": eaccount,
        "reply_to_uuid": reply_to_uuid,
        "subject": subject,
        "body": {"html": html_body}
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code not in [200, 201]:
        logger.error(f"Instantly reply error: {response.status_code} - {response.text}")
        return {"error": f"Instantly API error: {response.status_code}", "details": response.text}

    logger.info(f"📤 Reply sent via Instantly to thread {reply_to_uuid}")
    return {"status": "sent", "reply_to_uuid": reply_to_uuid}


def web_search_impl(query: str) -> dict:
    """Search the web using DuckDuckGo (no API key needed)."""
    import requests

    # Use DuckDuckGo instant answer API
    url = "https://api.duckduckgo.com/"
    params = {
        "q": query,
        "format": "json",
        "no_html": 1,
        "skip_disambig": 1
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        results = []

        # Abstract (main result)
        if data.get("Abstract"):
            results.append({
                "type": "abstract",
                "title": data.get("Heading", ""),
                "text": data.get("Abstract", ""),
                "url": data.get("AbstractURL", "")
            })

        # Related topics
        for topic in data.get("RelatedTopics", [])[:5]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append({
                    "type": "related",
                    "text": topic.get("Text", ""),
                    "url": topic.get("FirstURL", "")
                })

        # If no results from DDG, try a simple scrape approach
        if not results:
            # Fallback: return search suggestion
            results.append({
                "type": "suggestion",
                "text": f"No instant results for '{query}'. Try web_fetch on specific company/person websites.",
                "url": ""
            })

        logger.info(f"🔍 Web search: {query} -> {len(results)} results")
        return {"query": query, "results": results}

    except Exception as e:
        logger.error(f"Web search error: {e}")
        return {"error": str(e), "query": query}


def web_fetch_impl(url: str) -> dict:
    """Fetch and extract text content from a URL."""
    import requests

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Simple HTML to text conversion
        html = response.text

        # Remove script and style elements
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # Truncate if too long
        if len(text) > 15000:
            text = text[:15000] + "... [truncated]"

        logger.info(f"🌐 Fetched {url} ({len(text)} chars)")
        return {"url": url, "content": text, "length": len(text)}

    except Exception as e:
        logger.error(f"Web fetch error: {e}")
        return {"error": str(e), "url": url}


def create_proposal_impl(client: dict, project: dict) -> dict:
    """Create a PandaDoc proposal from structured data."""
    import requests

    API_KEY = os.getenv("PANDADOC_API_KEY")
    if not API_KEY:
        return {"error": "PANDADOC_API_KEY not configured"}

    TEMPLATE_UUID = "G8GhAvKGa9D8dmpwTnEWyV"
    API_URL = "https://api.pandadoc.com/public/v1/documents"

    problems = project.get("problems", {})
    benefits = project.get("benefits", {})

    # Build tokens for PandaDoc template
    tokens = [
        {"name": "Client.Company", "value": client.get("company", "")},
        {"name": "Personalization.Project.Title", "value": project.get("title", "")},
        {"name": "MonthOneInvestment", "value": str(project.get("monthOneInvestment", ""))},
        {"name": "MonthTwoInvestment", "value": str(project.get("monthTwoInvestment", ""))},
        {"name": "MonthThreeInvestment", "value": str(project.get("monthThreeInvestment", ""))},
        {"name": "Personalization.Project.Problem01", "value": problems.get("problem01", "")},
        {"name": "Personalization.Project.Problem02", "value": problems.get("problem02", "")},
        {"name": "Personalization.Project.Problem03", "value": problems.get("problem03", "")},
        {"name": "Personalization.Project.Problem04", "value": problems.get("problem04", "")},
        {"name": "Personalization.Project.Benefit.01", "value": benefits.get("benefit01", "")},
        {"name": "Personalization.Project.Benefit.02", "value": benefits.get("benefit02", "")},
        {"name": "Personalization.Project.Benefit.03", "value": benefits.get("benefit03", "")},
        {"name": "Personalization.Project.Benefit.04", "value": benefits.get("benefit04", "")},
        {"name": "Slide.Footer", "value": f"{client.get('company', 'Client')} x LeftClick"},
        {"name": "Document.CreatedDate", "value": datetime.utcnow().strftime("%B %d, %Y")},
    ]

    # Create document payload
    payload = {
        "name": f"Proposal - {client.get('company', 'Client')} - {project.get('title', 'Project')}",
        "template_uuid": TEMPLATE_UUID,
        "recipients": [
            {
                "email": client.get("email", ""),
                "first_name": client.get("first_name", ""),
                "last_name": client.get("last_name", ""),
                "role": "Client"
            }
        ],
        "tokens": tokens
    }

    headers = {
        "Authorization": f"API-Key {API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        doc_data = response.json()
        doc_id = doc_data.get("id")
        doc_url = f"https://app.pandadoc.com/a/#/documents/{doc_id}"

        logger.info(f"📄 Proposal created: {doc_url}")
        return {
            "success": True,
            "document_id": doc_id,
            "document_url": doc_url,
            "client_company": client.get("company"),
            "project_title": project.get("title")
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"PandaDoc API error: {e}")
        return {"error": f"PandaDoc API error: {str(e)}"}


# Map tool names to implementations
TOOL_IMPLEMENTATIONS = {
    "send_email": lambda **kwargs: send_email_impl(**kwargs),
    "read_sheet": lambda **kwargs: read_sheet_impl(**kwargs),
    "update_sheet": lambda **kwargs: update_sheet_impl(**kwargs),
    "instantly_get_emails": lambda **kwargs: instantly_get_emails_impl(**kwargs),
    "instantly_send_reply": lambda **kwargs: instantly_send_reply_impl(**kwargs),
    "web_search": lambda **kwargs: web_search_impl(**kwargs),
    "web_fetch": lambda **kwargs: web_fetch_impl(**kwargs),
    "create_proposal": lambda **kwargs: create_proposal_impl(**kwargs),
}

# Tools that need token_data
TOOLS_NEEDING_TOKEN = {"send_email", "read_sheet", "update_sheet"}

# ============================================================================
# SLACK NOTIFICATIONS
# ============================================================================

def slack_notify(message: str, blocks: list = None):
    """Log notification (Slack removed — not in use)."""
    logger.info(f"[notify] {message}")


def slack_directive_start(slug: str, directive: str, input_data: dict):
    """Notify Slack of directive execution."""
    input_str = json.dumps(input_data, indent=2)[:800] if input_data else "None"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🎯 Directive: {slug}", "emoji": True}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Directive:* `{directive}`"},
            {"type": "mrkdwn", "text": f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}"}
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Input:*\n```{input_str}```"}},
        {"type": "divider"}
    ]
    slack_notify(f"Directive {slug} started", blocks=blocks)


def slack_thinking(turn, thinking: str):
    truncated = thinking[:2500] + "..." if len(thinking) > 2500 else thinking
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f"🧠 *Turn {turn}:*\n```{truncated}```"}}]
    slack_notify(f"Turn {turn} thinking", blocks=blocks)


def slack_tool_call(turn: int, tool_name: str, tool_input: dict):
    input_str = json.dumps(tool_input, indent=2)[:1500]
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f"🔧 *Turn {turn} - {tool_name}:*\n```{input_str}```"}}]
    slack_notify(f"Tool: {tool_name}", blocks=blocks)


def slack_tool_result(turn: int, tool_name: str, result: str, is_error: bool = False):
    emoji = "❌" if is_error else "✅"
    truncated = result[:1500] + "..." if len(result) > 1500 else result
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f"{emoji} *Result:*\n```{truncated}```"}}]
    slack_notify(f"Result: {tool_name}", blocks=blocks)


def slack_complete(response: str, usage: dict):
    truncated = response[:2000] + "..." if len(response) > 2000 else response
    blocks = [
        {"type": "divider"},
        {"type": "header", "text": {"type": "plain_text", "text": "✨ Complete", "emoji": True}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Tokens:* {usage['input_tokens']}→{usage['output_tokens']}"},
            {"type": "mrkdwn", "text": f"*Turns:* {usage['turns']}"}
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Response:*\n```{truncated}```"}}
    ]
    slack_notify("Complete", blocks=blocks)


def slack_error(error: str):
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f"❌ *Error:*\n```{error[:2000]}```"}}]
    slack_notify(f"Error", blocks=blocks)


# ============================================================================
# PROCEDURAL SCRIPT REGISTRY
# ============================================================================

# Map script names to their module paths
# Scripts must have a run(payload, token_data, slack_notify) -> dict function
PROCEDURAL_SCRIPTS = {
    "instantly_autoreply": "execution.instantly_autoreply",
    "tech_radar_research": "execution.tech_radar_research",
    "lead_funnel_analytics": "execution.lead_funnel_analytics",
}


def run_procedural_script(script_name: str, payload: dict, token_data: dict) -> dict:
    """
    Execute a procedural Python script.
    Scripts are deterministic - Claude is only called for specific creative tasks within.
    """
    import importlib.util
    import sys

    # Add execution dir to path
    sys.path.insert(0, "/app")

    script_path = f"/app/execution/{script_name}.py"

    try:
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Call the run() function
        if hasattr(module, "run"):
            return module.run(payload, token_data, slack_notify)
        else:
            return {"error": f"Script {script_name} has no run() function"}

    except FileNotFoundError:
        return {"error": f"Script not found: {script_path}"}
    except Exception as e:
        logger.error(f"Script execution error: {e}")
        return {"error": str(e)}


# ============================================================================
# CORE ENGINE
# ============================================================================

def load_webhook_config():
    """Load webhook configuration."""
    config_path = Path("/app/webhooks.json")
    if not config_path.exists():
        return {"webhooks": {}}
    return json.loads(config_path.read_text())


def load_directive(directive_name: str) -> str:
    """Load a directive file. Returns content or raises error."""
    directive_path = Path(f"/app/directives/{directive_name}.md")
    if not directive_path.exists():
        raise FileNotFoundError(f"Directive not found: {directive_name}")
    return directive_path.read_text()


def run_directive(
    slug: str,
    directive_content: str,
    input_data: dict,
    allowed_tools: list,
    token_data: dict,
    max_turns: int = 15
) -> dict:
    """Execute a directive with scoped tools."""
    import anthropic

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Build prompt with directive + input
    prompt = f"""You are executing a specific directive. Follow it precisely.

## DIRECTIVE
{directive_content}

## INPUT DATA
{json.dumps(input_data, indent=2) if input_data else "No input data provided."}

## INSTRUCTIONS
1. Read and understand the directive above
2. Use the available tools to accomplish the task
3. Report your results clearly

Execute the directive now."""

    # Filter tools to only allowed ones
    tools = [ALL_TOOLS[t] for t in allowed_tools if t in ALL_TOOLS]

    messages = [{"role": "user", "content": prompt}]
    conversation_log = []
    thinking_log = []
    total_input_tokens = 0
    total_output_tokens = 0
    turn_count = 0

    logger.info(f"🎯 Executing directive: {slug}")
    slack_directive_start(slug, slug, input_data)

    request_kwargs = {
        "model": "claude-opus-4-5-20251101",
        "max_tokens": 40000,
        "tools": tools,
        "messages": messages,
        "thinking": {"type": "enabled", "budget_tokens": 32000}
    }

    response = client.messages.create(**request_kwargs)
    total_input_tokens += response.usage.input_tokens
    total_output_tokens += response.usage.output_tokens

    while response.stop_reason == "tool_use" and turn_count < max_turns:
        turn_count += 1

        # Process thinking
        for block in response.content:
            if block.type == "thinking":
                thinking_log.append({"turn": turn_count, "thinking": block.thinking})
                slack_thinking(turn_count, block.thinking)

        # Find tool call
        tool_use = next((b for b in response.content if b.type == "tool_use"), None)
        if not tool_use:
            break

        # Security check: only execute allowed tools
        if tool_use.name not in allowed_tools:
            tool_result = json.dumps({"error": f"Tool '{tool_use.name}' not permitted for this directive"})
            is_error = True
        else:
            slack_tool_call(turn_count, tool_use.name, tool_use.input)
            conversation_log.append({"turn": turn_count, "tool": tool_use.name, "input": tool_use.input})

            # Execute tool
            is_error = False
            try:
                impl = TOOL_IMPLEMENTATIONS.get(tool_use.name)
                if impl:
                    # Add token_data for tools that need it
                    if tool_use.name in TOOLS_NEEDING_TOKEN:
                        result = impl(**tool_use.input, token_data=token_data)
                    else:
                        result = impl(**tool_use.input)
                    tool_result = json.dumps(result)
                else:
                    tool_result = json.dumps({"error": f"No implementation for {tool_use.name}"})
                    is_error = True
            except Exception as e:
                logger.error(f"Tool error: {e}")
                tool_result = json.dumps({"error": str(e)})
                is_error = True

            conversation_log[-1]["result"] = tool_result
            slack_tool_result(turn_count, tool_use.name, tool_result, is_error)

        # Continue conversation
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": tool_use.id, "content": tool_result}]})

        response = client.messages.create(**{**request_kwargs, "messages": messages})
        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

    # Extract final response
    final_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            final_text += block.text
        if block.type == "thinking":
            thinking_log.append({"turn": "final", "thinking": block.thinking})

    usage = {"input_tokens": total_input_tokens, "output_tokens": total_output_tokens, "turns": turn_count}
    slack_complete(final_text, usage)

    return {
        "response": final_text,
        "thinking": thinking_log,
        "conversation": conversation_log,
        "usage": usage
    }


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.function(image=image, secrets=ALL_SECRETS, timeout=600)
@modal.fastapi_endpoint(method="POST")
def directive(slug: str, payload: dict = None):
    """
    Execute a specific directive by slug.

    Supports two modes:
    - Procedural: "script" in config → runs Python script directly (Claude only for creative tasks)
    - Agentic: "directive" in config → Claude orchestrates using tools

    URL: POST /directive?slug={slug}
    Body: {"data": {...}}  (input data for the directive)
    """
    payload = payload or {}
    input_data = payload.get("data", payload)  # Support both {"data": ...} and flat payload
    max_turns = payload.get("max_turns", 15)

    # Load config
    config = load_webhook_config()
    webhooks = config.get("webhooks", {})

    # Validate slug exists
    if slug not in webhooks:
        return {"status": "error", "error": f"Unknown webhook slug: {slug}", "available": list(webhooks.keys())}

    webhook_config = webhooks[slug]
    token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))

    # Check execution mode: procedural (script) vs agentic (directive)
    script_name = webhook_config.get("script")
    directive_name = webhook_config.get("directive")

    # =========================================================================
    # PROCEDURAL MODE: Run Python script directly
    # =========================================================================
    if script_name:
        logger.info(f"🔧 Running procedural script: {script_name}")
        slack_notify(f"🔧 *Procedural:* `{slug}` → `{script_name}.py`")

        try:
            result = run_procedural_script(script_name, input_data, token_data)

            # Notify completion
            status_emoji = "✅" if result.get("status") == "success" or "error" not in result else "❌"
            slack_notify(f"{status_emoji} *{slug}* complete: {json.dumps(result)[:500]}")

            return {
                "status": result.get("status", "success" if "error" not in result else "error"),
                "slug": slug,
                "mode": "procedural",
                "script": script_name,
                "result": result,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Script error: {e}")
            slack_error(str(e))
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # AGENTIC MODE: Claude orchestrates using directive + tools
    # =========================================================================
    if directive_name:
        allowed_tools = webhook_config.get("tools", ["send_email"])

        try:
            directive_content = load_directive(directive_name)
        except FileNotFoundError as e:
            return {"status": "error", "error": str(e)}

        try:
            result = run_directive(
                slug=slug,
                directive_content=directive_content,
                input_data=input_data,
                allowed_tools=allowed_tools,
                token_data=token_data,
                max_turns=max_turns
            )
            return {
                "status": "success",
                "slug": slug,
                "mode": "agentic",
                "directive": directive_name,
                "response": result["response"],
                "thinking": result["thinking"],
                "conversation": result["conversation"],
                "usage": result["usage"],
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Directive error: {e}")
            slack_error(str(e))
            return {"status": "error", "error": str(e)}

    return {"status": "error", "error": "Webhook config must have either 'script' or 'directive'"}


@app.function(image=image, secrets=ALL_SECRETS, timeout=30)
@modal.fastapi_endpoint(method="GET")
def list_webhooks():
    """List available webhook slugs and their descriptions."""
    config = load_webhook_config()
    webhooks = config.get("webhooks", {})

    return {
        "webhooks": {
            slug: {
                "directive": cfg.get("directive"),
                "script": cfg.get("script"),
                "description": cfg.get("description", ""),
                "tools": cfg.get("tools", [])
            }
            for slug, cfg in webhooks.items()
        }
    }


# ============================================================================
# GENERAL QUERY AGENT - Natural language meta-orchestrator
# ============================================================================

def list_available_directives() -> list[dict]:
    """List all available directives with their descriptions."""
    directives_dir = Path("/app/directives")
    directives = []

    for f in directives_dir.glob("*.md"):
        content = f.read_text()
        # Extract first heading as title
        title = f.stem.replace("_", " ").title()
        # Extract goal/description from content
        desc = ""
        for line in content.split("\n"):
            if line.startswith("## Goal") or line.startswith("## Description"):
                # Get the next non-empty line
                idx = content.find(line)
                remaining = content[idx + len(line):].strip()
                desc = remaining.split("\n")[0].strip()
                break

        directives.append({
            "name": f.stem,
            "title": title,
            "description": desc[:200] if desc else "No description"
        })

    return directives


def list_available_scripts() -> list[dict]:
    """List all available execution scripts."""
    scripts_dir = Path("/app/execution")
    scripts = []

    for f in scripts_dir.glob("*.py"):
        if f.stem.startswith("_"):
            continue
        # Read first docstring
        content = f.read_text()
        desc = ""
        if '"""' in content:
            start = content.find('"""') + 3
            end = content.find('"""', start)
            if end > start:
                desc = content[start:end].strip().split("\n")[0]

        scripts.append({
            "name": f.stem,
            "description": desc[:150] if desc else "No description"
        })

    return scripts


AGENT_TOOLS = {
    **ALL_TOOLS,
    "list_directives": {
        "name": "list_directives",
        "description": "List all available directives (SOPs) in the system.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    "read_directive": {
        "name": "read_directive",
        "description": "Read the full content of a specific directive.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Name of the directive (without .md extension)"}
            },
            "required": ["name"]
        }
    },
    "list_scripts": {
        "name": "list_scripts",
        "description": "List all available execution scripts.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    "run_script": {
        "name": "run_script",
        "description": "Execute a Python script from the execution folder. Returns the script output.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Script name (without .py extension)"},
                "args": {"type": "array", "items": {"type": "string"}, "description": "Command-line arguments"}
            },
            "required": ["name"]
        }
    }
}


def run_agent_tool(tool_name: str, tool_input: dict, token_data: dict) -> dict:
    """Execute an agent tool and return result."""

    if tool_name == "list_directives":
        return {"directives": list_available_directives()}

    elif tool_name == "read_directive":
        name = tool_input.get("name", "")
        try:
            content = load_directive(name)
            return {"name": name, "content": content}
        except FileNotFoundError:
            return {"error": f"Directive '{name}' not found"}

    elif tool_name == "list_scripts":
        return {"scripts": list_available_scripts()}

    elif tool_name == "run_script":
        import subprocess
        name = tool_input.get("name", "")
        args = tool_input.get("args", [])
        script_path = f"/app/execution/{name}.py"

        if not Path(script_path).exists():
            return {"error": f"Script '{name}' not found"}

        try:
            cmd = ["python3", script_path] + args
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd="/app")
            return {
                "stdout": result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout,
                "stderr": result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr,
                "returncode": result.returncode
            }
        except subprocess.TimeoutExpired:
            return {"error": "Script timed out after 5 minutes"}
        except Exception as e:
            return {"error": str(e)}

    # Fall back to standard tool implementations
    elif tool_name in TOOL_IMPLEMENTATIONS:
        impl = TOOL_IMPLEMENTATIONS[tool_name]
        if tool_name in TOOLS_NEEDING_TOKEN:
            return impl(**tool_input, token_data=token_data)
        else:
            return impl(**tool_input)

    return {"error": f"Unknown tool: {tool_name}"}


def call_claude(client, **kwargs) -> tuple:
    """Call Claude API directly (no streaming)."""
    response = client.messages.create(**kwargs)
    return response.content, response.usage.input_tokens, response.usage.output_tokens, response.stop_reason


@app.function(image=image, secrets=ALL_SECRETS, timeout=300)
@modal.fastapi_endpoint(method="GET")
def general_agent(query: str = "", format: str = "json"):
    """
    General-purpose autonomous agent endpoint.
    GET /general-agent?query=Send an email to nick@leftclick.ai
    """
    import anthropic
    from fastapi.responses import JSONResponse

    # No query = return status
    if not query:
        return JSONResponse({
            "status": "ready",
            "message": "Provide a query parameter",
            "example": "/agent?query=Send email to nick@leftclick.ai saying hello"
        })

    slack_notify(f"🤖 *Agent Request*\n```{query[:500]}```")

    # Get API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return JSONResponse({"error": "ANTHROPIC_API_KEY not set"}, status_code=500)

    # Get Google token
    try:
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON", "{}"))
    except:
        token_data = {}

    # Build system prompt
    system = """You are an autonomous agent. Complete tasks directly.

Tools available:
- send_email: Send email. Params: to, subject, body
- read_sheet: Read Google Sheet. Params: spreadsheet_id, range
- update_sheet: Write to sheet. Params: spreadsheet_id, range, values
- list_directives: See available workflows
- read_directive: Read a directive. Params: name

Be concise. Complete tasks fully."""

    tools = list(AGENT_TOOLS.values())
    messages = [{"role": "user", "content": query}]

    client = anthropic.Anthropic(api_key=api_key)
    conversation = []

    try:
        # Initial call
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=system,
            tools=tools,
            messages=messages
        )

        # Agentic loop
        turns = 0
        max_turns = 10

        while response.stop_reason == "tool_use" and turns < max_turns:
            turns += 1
            tool_results = []

            for block in response.content:
                if block.type == "tool_use":
                    slack_notify(f"🔧 *Tool: {block.name}*")

                    try:
                        result = run_agent_tool(block.name, block.input, token_data)
                        result_str = json.dumps(result) if isinstance(result, (dict, list)) else str(result)
                        slack_notify(f"✅ Success: {result_str[:200]}")
                    except Exception as e:
                        result_str = json.dumps({"error": str(e)})
                        slack_notify(f"❌ Error: {str(e)}")

                    conversation.append({"tool": block.name, "result": result_str[:500]})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str[:10000]
                    })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=system,
                tools=tools,
                messages=messages
            )

        # Extract final text
        final = ""
        for block in response.content:
            if hasattr(block, "text"):
                final += block.text

        slack_notify(f"🏁 *Done*\n{final[:500]}")

        return JSONResponse({
            "status": "success",
            "query": query,
            "response": final,
            "turns": turns,
            "conversation": conversation
        })

    except Exception as e:
        slack_notify(f"💥 *Error*: {str(e)}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ============================================================================
# HOURLY CRON SCRAPER
# ============================================================================

def load_cron_config() -> dict:
    """Load cron configuration."""
    config_path = Path("/app/execution/cron_config.json")
    if not config_path.exists():
        return {}
    return json.loads(config_path.read_text())


def append_to_sheet(spreadsheet_id: str, values: list, token_data: dict) -> dict:
    """Append rows to a Google Sheet."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"]
    )
    creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

    service = build("sheets", "v4", credentials=creds)

    result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="A:I",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

    return {"appended_rows": result.get("updates", {}).get("updatedRows", 0)}


@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=60,
    # schedule=modal.Cron("*/5 * * * *")  # Disabled
)
def scheduled_welcome_email():
    """
    Scheduled cron job to send a welcome email every 5 minutes.
    """
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    slack_notify("📧 *Scheduled Welcome Email* - Sending to nick@leftclick.ai")

    try:
        # Get Google token
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON", "{}"))
        if not token_data:
            slack_error("Welcome email: No Google token configured")
            return {"status": "error", "error": "No Google token"}

        creds = Credentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"]
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

        service = build("gmail", "v1", credentials=creds)

        # Build welcome email
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        message = MIMEText(f"""Hi Nick,

This is your scheduled welcome email from the Modal cloud scheduler.

Sent at: {timestamp}

This email is automatically generated every 5 minutes to confirm the scheduled task is running correctly.

Best,
Your Automation System""")

        message["to"] = "nick@leftclick.ai"
        message["subject"] = f"Welcome Email - {timestamp}"
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()

        logger.info(f"📧 Welcome email sent | ID: {result['id']}")
        slack_notify(f"✅ *Welcome email sent* to nick@leftclick.ai | ID: {result['id']}")

        return {"status": "success", "message_id": result["id"], "timestamp": timestamp}

    except Exception as e:
        logger.error(f"Welcome email error: {e}")
        slack_error(f"Welcome email failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=300,
    # schedule=modal.Cron("0 * * * *")  # DISABLED - uncomment to re-enable hourly scraping
)
def hourly_lead_scraper():
    """
    Hourly cron job to scrape leads and append to Google Sheet.
    Runs at the top of every hour.
    """
    from apify_client import ApifyClient

    config = load_cron_config()
    scraper_config = config.get("hourly_scraper", {})

    if not scraper_config:
        logger.error("No hourly_scraper config found")
        slack_error("Cron scraper: No config found")
        return {"status": "error", "error": "No config"}

    sheet_id = scraper_config.get("sheet_id")
    search_query = scraper_config.get("search_query", "marketing agencies")
    location = scraper_config.get("location", "United States")
    max_results = scraper_config.get("max_results_per_run", 25)

    slack_notify(f"⏰ *Hourly Scraper Started*\nQuery: {search_query}\nLocation: {location}")

    # Run Apify Google Maps scraper
    api_token = os.getenv("APIFY_API_TOKEN")
    if not api_token:
        slack_error("APIFY_API_TOKEN not configured")
        return {"status": "error", "error": "No Apify token"}

    client = ApifyClient(api_token)

    full_search = f"{search_query} in {location}"
    run_input = {
        "searchStringsArray": [full_search],
        "maxCrawledPlacesPerSearch": max_results,
        "language": "en",
        "deeperCityScrape": False,
    }

    try:
        run = client.actor("compass/crawler-google-places").call(run_input=run_input)

        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        logger.info(f"Scraped {len(results)} leads")

        if not results:
            slack_notify("⏰ Hourly scraper: No results found")
            return {"status": "success", "leads_found": 0}

        # Format for sheet
        timestamp = datetime.utcnow().isoformat()
        rows = []
        for r in results:
            rows.append([
                timestamp,
                r.get("title", ""),
                "",  # contact_name not available from maps
                "",  # email not available from maps
                r.get("phone", ""),
                r.get("website", ""),
                r.get("address", ""),
                r.get("categoryName", ""),
                "google_maps"
            ])

        # Append to sheet
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        append_result = append_to_sheet(sheet_id, rows, token_data)

        slack_notify(f"✅ *Hourly Scraper Complete*\nLeads: {len(results)}\nAppended: {append_result.get('appended_rows', 0)} rows")

        return {
            "status": "success",
            "leads_found": len(results),
            "appended_rows": append_result.get("appended_rows", 0),
            "sheet_id": sheet_id
        }

    except Exception as e:
        logger.error(f"Cron scraper error: {e}")
        slack_error(f"Hourly scraper failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# EXECUTION-ONLY WEBHOOKS (No Claude orchestration - pure script execution)
# ============================================================================

# Background function for full lead scraping workflow
@app.function(image=image, secrets=ALL_SECRETS, timeout=1800)  # 30 min timeout for full workflow
def scrape_leads_background(queries: list, location: str, limit: int, sheet_id: str, sheet_url: str):
    """
    Background task: Full lead scraping workflow.
    1. Scrape leads via Apify (supports multiple industries in one run)
    2. Upload to Google Sheet
    3. Website scraping (emails, owner info, social media)
    4. Casualize business names and cities
    5. Ingest leads with emails into Airtable (deduped)
    """
    from apify_client import ApifyClient
    import gspread
    import anthropic
    from google.oauth2.credentials import Credentials as UserCredentials
    from google.auth.transport.requests import Request
    import requests as http_requests

    try:
        # ===== STEP 1: Scrape with Apify =====
        search_strings = [f"{q} in {location}" for q in queries]
        slack_notify(f"📥 *Step 1/5: Scraping*\nSearches: {len(search_strings)}\nLocation: {location}\nLimit: {limit} per industry")

        api_token = os.getenv("APIFY_API_TOKEN")
        if not api_token:
            raise ValueError("APIFY_API_TOKEN not configured")

        apify_client = ApifyClient(api_token)

        run_input = {
            "searchStringsArray": search_strings,
            "maxCrawledPlacesPerSearch": limit,
            "language": "en",
            "deeperCityScrape": False,
        }

        run = apify_client.actor("compass/crawler-google-places").call(run_input=run_input)
        scrape_id = run.get("id", "unknown")
        apify_cost_usd = float(run.get("usageTotalUsd") or 0.0)

        results = []
        for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        logger.info(f"Scraped {len(results)} leads")

        if not results:
            slack_notify(f"⚠️ *No leads found for: {', '.join(queries)} in {location}*")
            return {"status": "no_results", "leads_found": 0}

        # ===== STEP 2: Upload to Google Sheet =====
        slack_notify(f"📤 *Step 2/5: Uploading {len(results)} leads to Sheet*")

        import pandas as pd

        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        creds = UserCredentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"]
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.get_worksheet(0)

        # Use pandas to flatten JSON like the real update_sheet.py does
        df = pd.json_normalize(results)

        # Stamp scrape ID on every lead row
        df.insert(0, "scrape_id", scrape_id)

        # Add casual columns: title (business name) and city
        # Google Maps data uses 'title' for business name, no first_name available
        if "title" in df.columns:
            idx = df.columns.get_loc("title")
            df.insert(idx + 1, "casual_title", "")

        if "city" in df.columns:
            idx = df.columns.get_loc("city")
            df.insert(idx + 1, "casual_city_name", "")

        # Convert NaN to empty strings for gspread
        df = df.fillna("")

        # Drop noisy/oversized Apify metadata columns not useful for outreach
        drop_cols = ["hotelAds", "additionalInfo", "imageCategories", "gasPrices",
                     "peopleAlsoSearch", "placesTags", "reviewsTags", "googleFoodUrl",
                     "claimThisBusiness", "isAdvertisement", "fid", "kgmid",
                     "searchPageUrl", "imagesCount"]
        df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

        # Convert any remaining list/dict cells to JSON strings (gspread can't handle complex types)
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # Truncate any cell exceeding Google Sheets 50k char limit
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x[:45000] if isinstance(x, str) and len(x) > 45000 else x)

        # Prepare data: headers + rows
        headers = df.columns.tolist()
        rows = [headers] + df.values.tolist()

        # Resize worksheet if needed
        required_rows = len(rows)
        required_cols = len(headers)
        if required_rows > worksheet.row_count or required_cols > worksheet.col_count:
            worksheet.resize(rows=max(required_rows, worksheet.row_count), cols=max(required_cols, worksheet.col_count))

        # Upload all data
        worksheet.update(values=rows, range_name="A1")

        # ===== STEP 3: Website contact scraping =====
        slack_notify(f"🌐 *Step 3/5: Scraping websites for emails & owner info*")

        try:
            import sys as _sys
            _sys.path.insert(0, "/app/execution")
            from extract_website_contacts import scrape_website_contacts
            from concurrent.futures import ThreadPoolExecutor, as_completed

            # Re-fetch sheet to get current headers/data
            all_data = worksheet.get_all_values()
            header_row = all_data[0]
            data_rows = all_data[1:]

            website_col = header_row.index("website") if "website" in header_row else -1
            title_col_ws = header_row.index("title") if "title" in header_row else -1

            if website_col < 0:
                logger.warning("No website column found, skipping website scraping")
            else:
                # Add output columns if not present
                new_cols = [
                    "email", "email_score",
                    "owner_name", "owner_title", "owner_linkedin",
                    "facebook", "twitter", "linkedin_url", "instagram", "youtube", "tiktok",
                ]
                added = False
                for col in new_cols:
                    if col not in header_row:
                        header_row.append(col)
                        added = True
                if added:
                    worksheet.resize(cols=len(header_row))
                    worksheet.update(values=[header_row], range_name="A1")
                    all_data = worksheet.get_all_values()
                    header_row = all_data[0]
                    data_rows = all_data[1:]

                col_idx = {c: header_row.index(c) for c in new_cols if c in header_row}

                def scrape_one(args):
                    row_idx, row = args
                    website = row[website_col] if len(row) > website_col else ""
                    title = row[title_col_ws] if title_col_ws >= 0 and len(row) > title_col_ws else ""
                    if not website:
                        return row_idx, None
                    try:
                        return row_idx, scrape_website_contacts(website, title, use_claude=False)
                    except Exception:
                        return row_idx, None

                updates = []
                enriched_count = 0

                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = [executor.submit(scrape_one, (i, row)) for i, row in enumerate(data_rows)]
                    for future in as_completed(futures):
                        row_idx, contact = future.result()
                        if not contact or contact.get("error"):
                            continue
                        row_num = row_idx + 2  # +2 for header + 1-indexing

                        best_email = contact.get("best_email", "")
                        email_score = contact.get("best_email_score", 0.0)
                        owner = contact.get("owner_info", {})
                        social = contact.get("social_media", {})

                        if best_email or owner.get("name"):
                            enriched_count += 1

                        def _add(col_name, value):
                            if value and col_name in col_idx:
                                updates.append({"range": f"{column_letter(col_idx[col_name])}{row_num}", "values": [[value]]})

                        _add("email", best_email)
                        _add("email_score", round(email_score, 2) if email_score else "")
                        _add("owner_name", owner.get("name", ""))
                        _add("owner_title", owner.get("title", ""))
                        _add("owner_linkedin", owner.get("linkedin", ""))
                        _add("facebook", social.get("facebook", ""))
                        _add("twitter", social.get("twitter", ""))
                        _add("linkedin_url", social.get("linkedin", ""))
                        _add("instagram", social.get("instagram", ""))
                        _add("youtube", social.get("youtube", ""))
                        _add("tiktok", social.get("tiktok", ""))

                # Batch write all results
                if updates:
                    for i in range(0, len(updates), 500):  # gspread batch limit
                        worksheet.batch_update(updates[i:i+500])

                slack_notify(f"✅ Website scraping done — enriched {enriched_count}/{len(data_rows)} leads")

        except Exception as e:
            logger.warning(f"Website scraping step failed: {e}")

        # ===== STEP 4: Casualize first names, company names, and cities =====
        slack_notify(f"✨ *Step 4/5: Casualizing names (first, company, city)*")

        claude_input_tokens = 0
        claude_output_tokens = 0

        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        if not anthropic_key:
            slack_notify("⚠️ ANTHROPIC_API_KEY not configured, skipping casualization")
        else:
            claude_client = anthropic.Anthropic(api_key=anthropic_key)

            # Re-fetch data
            all_data = worksheet.get_all_values()
            header_row = all_data[0]

            # Find column indices (Google Maps: 'title' = business name, no first_name)
            title_col = header_row.index("title") if "title" in header_row else -1
            city_col = header_row.index("city") if "city" in header_row else -1
            casual_title_col = header_row.index("casual_title") if "casual_title" in header_row else -1
            casual_city_col = header_row.index("casual_city_name") if "casual_city_name" in header_row else -1

            # Batch process 50 at a time
            BATCH_SIZE = 50
            data_rows = all_data[1:]
            total_batches = (len(data_rows) + BATCH_SIZE - 1) // BATCH_SIZE

            for batch_num, batch_start in enumerate(range(0, len(data_rows), BATCH_SIZE), 1):
                batch_end = min(batch_start + BATCH_SIZE, len(data_rows))
                batch_rows = data_rows[batch_start:batch_end]

                # Build records for this batch
                records = []
                for row in batch_rows:
                    title = row[title_col] if title_col >= 0 and len(row) > title_col else ""
                    city = row[city_col] if city_col >= 0 and len(row) > city_col else ""
                    records.append({"title": title, "city": city})

                if not any(r["title"] or r["city"] for r in records):
                    continue

                # Format as compact JSON
                records_json = json.dumps([
                    {"id": i+1, "title": r["title"], "city": r["city"]}
                    for i, r in enumerate(records)
                ])

                prompt = f"""Convert business names and cities to casual forms for cold emails. Return ONLY valid JSON array.

Rules:
- title: Remove "The", legal suffixes (Ltd/LLC/Inc/Corp/LLP), generic words (Group/Services/Solutions/Associates/Partners/Management/Consultants). Use the shortest recognisable name. E.g. "Manchester Dental Associates Ltd" → "Manchester Dental"
- city: Local nicknames (Manchester→Manc, Liverpool→Scouse area is fine but keep Liverpool, Birmingham→Brum). Keep as-is if no common nickname.

Input: {records_json}

Output JSON only (no markdown, no explanations):"""

                try:
                    msg = claude_client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=6000,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    claude_input_tokens += msg.usage.input_tokens
                    claude_output_tokens += msg.usage.output_tokens
                    response_text = msg.content[0].text.strip()

                    # Remove markdown code blocks if present
                    if response_text.startswith("```"):
                        lines = response_text.split('\n')
                        response_text = '\n'.join(lines[1:-1])

                    # Parse JSON response
                    results_json = json.loads(response_text)

                    # Update cells in batch
                    updates = []
                    for i, result in enumerate(results_json):
                        row_num = batch_start + i + 2  # +2 for header and 1-indexing

                        if casual_title_col >= 0:
                            casual_title = result.get("casual_title", result.get("title", ""))
                            cell = f"{column_letter(casual_title_col)}{row_num}"
                            updates.append({"range": cell, "values": [[casual_title]]})

                        if casual_city_col >= 0:
                            casual_city = result.get("casual_city_name", result.get("city", ""))
                            cell = f"{column_letter(casual_city_col)}{row_num}"
                            updates.append({"range": cell, "values": [[casual_city]]})

                    if updates:
                        worksheet.batch_update(updates)

                    logger.info(f"Batch {batch_num}/{total_batches} complete")

                except Exception as e:
                    logger.warning(f"Casualization batch {batch_num} error: {e}")

        # ===== COST LOGGING =====
        try:
            claude_cost_usd = (claude_input_tokens / 1_000_000 * 0.80) + (claude_output_tokens / 1_000_000 * 4.00)
            total_cost_usd = apify_cost_usd + claude_cost_usd
            cost_per_lead = total_cost_usd / len(results) if results else 0

            cost_sheet_id = os.getenv("LEAD_ANALYTICS_SHEET_ID")
            if cost_sheet_id:
                try:
                    cost_ws = gc.open_by_key(cost_sheet_id).worksheet("Cost per scrape")
                except Exception:
                    cost_ws = gc.open_by_key(cost_sheet_id).add_worksheet("Cost per scrape", 1000, 10)
                    cost_ws.append_row(["Scrape ID", "Date", "Location", "Leads", "Apify $", "Claude $", "Total $", "$/Lead", "Sheet URL"])

                cost_ws.append_row([
                    scrape_id,
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                    location,
                    len(results),
                    round(apify_cost_usd, 4),
                    round(claude_cost_usd, 4),
                    round(total_cost_usd, 4),
                    round(cost_per_lead, 4),
                    sheet_url,
                ])
                logger.info(f"Cost logged: ${total_cost_usd:.4f} for {len(results)} leads (scrape {scrape_id})")
        except Exception as e:
            logger.warning(f"Cost logging failed: {e}")

        # ===== STEP 5: Airtable ingest (email-gated, deduped) =====
        slack_notify(f"📋 *Step 5/5: Ingesting leads with emails into Airtable*")

        try:
            import importlib.util as _ilu
            _spec = _ilu.spec_from_file_location(
                "lead_funnel_analytics", "/app/execution/lead_funnel_analytics.py"
            )
            _lfa = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_lfa)

            ingest_result = _lfa.ingest_leads_from_sheet(
                sheet_id=sheet_id,
                token_data=token_data,
                notify_fn=slack_notify,
            )
            slack_notify(
                f"✅ Airtable ingest done — "
                f"added: {ingest_result.get('added', 0)}, "
                f"skipped (dupes/no email): {ingest_result.get('skipped', 0)}"
            )
        except Exception as e:
            logger.warning(f"Airtable ingest step failed: {e}")
            slack_notify(f"⚠️ Airtable ingest failed (non-blocking): {e}")

        # ===== COMPLETE =====
        slack_notify(f"✅ *Lead Scraping Complete!*\nLeads: {len(results)}\nSheet: {sheet_url}")

        return {
            "status": "success",
            "leads_found": len(results),
            "sheet_url": sheet_url
        }

    except Exception as e:
        logger.error(f"Background scrape error: {e}")
        slack_error(f"Lead scraping failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.function(image=image, secrets=ALL_SECRETS, timeout=60)
@modal.fastapi_endpoint(method="GET")
def scrape_leads(queries: str = "", location: str = "United States", limit: int = 50):
    """
    Execution-only: Scrape leads with full workflow. Supports multiple industries in one run.

    URL: GET /scrape-leads?queries=accountants,dentists,solicitors&location=Manchester&limit=50

    'queries' = comma-separated list of industries/search terms. limit = per industry.
    Returns 201 immediately with Google Sheet URL.
    """
    from fastapi.responses import JSONResponse
    import gspread
    from google.oauth2.credentials import Credentials as UserCredentials
    from google.auth.transport.requests import Request

    if not queries:
        return JSONResponse({
            "status": "error",
            "error": "Missing 'queries' parameter",
            "example": "/scrape-leads?queries=accountants,dentists&location=Manchester&limit=50"
        }, status_code=400)

    query_list = [q.strip() for q in queries.split(",") if q.strip()]
    industry_label = f"{len(query_list)} industries" if len(query_list) > 1 else query_list[0]

    try:
        # Create Google Sheet immediately
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        creds = UserCredentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"]
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

        gc = gspread.authorize(creds)

        sheet_name = f"Leads - {location} - {industry_label} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
        sh = gc.create(sheet_name)
        sheet_id = sh.id
        sheet_url = sh.url

        slack_notify(f"🚀 *Lead Scraping Started*\nIndustries: {', '.join(query_list)}\nLocation: {location}\nLimit: {limit}/industry\nSheet: {sheet_url}")

        # Spawn background task
        scrape_leads_background.spawn(query_list, location, limit, sheet_id, sheet_url)

        # Return 201 immediately
        return JSONResponse({
            "status": "accepted",
            "message": f"Scraping {len(query_list)} industries in {location}.",
            "industries": query_list,
            "location": location,
            "limit_per_industry": limit,
            "sheet_url": sheet_url,
            "sheet_name": sheet_name,
        }, status_code=201)

    except Exception as e:
        logger.error(f"Scrape init error: {e}")
        slack_error(f"Scrape init failed: {str(e)}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.function(image=image, secrets=ALL_SECRETS, timeout=300)
@modal.fastapi_endpoint(method="POST")
def generate_proposal(request_body: dict = None):
    """
    Execution-only: Generate a proposal using PandaDoc.

    URL: POST /generate-proposal
    Body: JSON with client info and project details

    For demo, uses local transcript files if no transcripts provided.

    You (the local agent) orchestrate: read transcripts, extract info, format input, call this.
    """
    from fastapi.responses import JSONResponse

    if not request_body:
        # Return example format
        return JSONResponse({
            "status": "info",
            "message": "POST JSON body required",
            "example": {
                "client": {
                    "first_name": "Kelly",
                    "last_name": "Longhouse",
                    "email": "kelly@executivesocial.com",
                    "company": "Executive Social"
                },
                "project": {
                    "title": "LinkedIn Thought Leadership Campaign",
                    "monthOneInvestment": "3500",
                    "monthTwoInvestment": "3500",
                    "monthThreeInvestment": "3500",
                    "problems": {
                        "problem01": "Low LinkedIn engagement despite posting",
                        "problem02": "No time for consistent content creation",
                        "problem03": "Current posts feel too corporate",
                        "problem04": "Missing opportunities to be top-of-mind"
                    },
                    "benefits": {
                        "benefit01": "Increased visibility with target audience",
                        "benefit02": "Consistent professional presence",
                        "benefit03": "More inbound leads from thought leadership",
                        "benefit04": "Time savings on content creation"
                    }
                }
            },
            "demo_transcripts_available": True,
            "demo_kickoff": "/app/demo_kickoff_call_transcript.md",
            "demo_sales": "/app/demo_sales_call_transcript.md"
        })

    slack_notify(f"📄 *Proposal Generation Started*\nClient: {request_body.get('client', {}).get('company', 'Unknown')}")

    try:
        import requests

        API_KEY = os.getenv("PANDADOC_API_KEY")
        if not API_KEY:
            raise ValueError("PANDADOC_API_KEY not configured")

        TEMPLATE_UUID = "G8GhAvKGa9D8dmpwTnEWyV"
        API_URL = "https://api.pandadoc.com/public/v1/documents"

        client = request_body.get("client", {})
        project = request_body.get("project", {})
        problems = project.get("problems", {})
        benefits = project.get("benefits", {})

        # Build tokens
        tokens = [
            {"name": "Client.Company", "value": client.get("company", "")},
            {"name": "Personalization.Project.Title", "value": project.get("title", "")},
            {"name": "MonthOneInvestment", "value": str(project.get("monthOneInvestment", ""))},
            {"name": "MonthTwoInvestment", "value": str(project.get("monthTwoInvestment", ""))},
            {"name": "MonthThreeInvestment", "value": str(project.get("monthThreeInvestment", ""))},
            {"name": "Personalization.Project.Problem01", "value": problems.get("problem01", "")},
            {"name": "Personalization.Project.Problem02", "value": problems.get("problem02", "")},
            {"name": "Personalization.Project.Problem03", "value": problems.get("problem03", "")},
            {"name": "Personalization.Project.Problem04", "value": problems.get("problem04", "")},
            {"name": "Personalization.Project.Benefit.01", "value": benefits.get("benefit01", "")},
            {"name": "Personalization.Project.Benefit.02", "value": benefits.get("benefit02", "")},
            {"name": "Personalization.Project.Benefit.03", "value": benefits.get("benefit03", "")},
            {"name": "Personalization.Project.Benefit.04", "value": benefits.get("benefit04", "")},
            {"name": "Slide.Footer", "value": f"{client.get('company', 'Client')} x LeftClick"},
            {"name": "Document.CreatedDate", "value": datetime.utcnow().strftime("%B %d, %Y")},
        ]

        # Create document
        payload = {
            "name": f"Proposal - {client.get('company', 'Client')} - {project.get('title', 'Project')}",
            "template_uuid": TEMPLATE_UUID,
            "recipients": [
                {
                    "email": client.get("email", ""),
                    "first_name": client.get("first_name", ""),
                    "last_name": client.get("last_name", ""),
                    "role": "Client"
                }
            ],
            "tokens": tokens
        }

        headers = {
            "Authorization": f"API-Key {API_KEY}",
            "Content-Type": "application/json"
        }

        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()

        doc_data = response.json()
        doc_id = doc_data.get("id")
        doc_url = f"https://app.pandadoc.com/a/#/documents/{doc_id}"

        slack_notify(f"✅ *Proposal Created*\nClient: {client.get('company')}\nDoc: {doc_url}")

        return JSONResponse({
            "status": "success",
            "document_id": doc_id,
            "document_url": doc_url,
            "client": client.get("company"),
            "project_title": project.get("title")
        })

    except Exception as e:
        logger.error(f"Proposal error: {e}")
        slack_error(f"Proposal failed: {str(e)}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.function(image=image, secrets=ALL_SECRETS, timeout=60)
@modal.fastapi_endpoint(method="GET")
def read_demo_transcript(name: str = "kickoff"):
    """
    Read demo transcripts stored on the server.

    URL: GET /read-demo-transcript?name=kickoff
    URL: GET /read-demo-transcript?name=sales
    """
    from fastapi.responses import JSONResponse

    transcript_map = {
        "kickoff": "/app/demo_kickoff_call_transcript.md",
        "sales": "/app/demo_sales_call_transcript.md"
    }

    if name not in transcript_map:
        return JSONResponse({
            "status": "error",
            "error": f"Unknown transcript: {name}",
            "available": list(transcript_map.keys())
        }, status_code=400)

    try:
        with open(transcript_map[name], "r") as f:
            content = f.read()

        return JSONResponse({
            "status": "success",
            "name": name,
            "content": content
        })
    except Exception as e:
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.function(image=image, secrets=ALL_SECRETS, timeout=300)
@modal.fastapi_endpoint(method="GET")
def create_proposal_from_transcript(transcript: str = "sales", demo: bool = True):
    """
    End-to-end proposal generation from transcript.

    URL: GET /create-proposal-from-transcript?transcript=sales&demo=true

    This endpoint:
    1. Reads the demo transcript (stored locally on Modal)
    2. Uses Claude to extract client info and generate expanded problems/benefits
    3. Creates a PandaDoc proposal with all the details

    Parameters:
    - transcript: "sales" or "kickoff" (default: sales)
    - demo: If true, uses stored demo transcripts (default: true)
    """
    from fastapi.responses import JSONResponse
    import anthropic
    import requests

    transcript_map = {
        "kickoff": "/app/demo_kickoff_call_transcript.md",
        "sales": "/app/demo_sales_call_transcript.md"
    }

    if transcript not in transcript_map:
        return JSONResponse({
            "status": "error",
            "error": f"Unknown transcript: {transcript}",
            "available": list(transcript_map.keys())
        }, status_code=400)

    slack_notify(f"📄 *Create Proposal from Transcript*\nTranscript: {transcript}\nDemo mode: {demo}")

    try:
        # Step 1: Read the transcript
        with open(transcript_map[transcript], "r") as f:
            transcript_content = f.read()

        slack_notify(f"📝 *Step 1/3: Transcript loaded*\n{len(transcript_content)} characters")

        # Step 2: Use Claude to extract info and generate expanded content
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        if not anthropic_key:
            raise ValueError("ANTHROPIC_API_KEY not configured")

        client = anthropic.Anthropic(api_key=anthropic_key)

        extraction_prompt = f"""Analyze this sales call transcript and extract the following information. Return ONLY valid JSON.

TRANSCRIPT:
{transcript_content}

Extract and return this exact JSON structure:
{{
  "client": {{
    "firstName": "first name of the prospect",
    "lastName": "last name of the prospect",
    "email": "their email (use placeholder if not mentioned)",
    "company": "their company name"
  }},
  "project": {{
    "title": "short 3-4 word project title (e.g. 'Outbound Lead System', 'LinkedIn Growth Engine')",
    "monthOneInvestment": "investment amount for month 1 (use 1980 if revenue share mentioned)",
    "monthTwoInvestment": "monthly amount (use 0 for revenue share)",
    "monthThreeInvestment": "monthly amount (use 0 for revenue share)",
    "problems": {{
      "problem01": "Expanded 1-2 paragraph (max 50 words) about their first pain point. Use 'you' language, focus on revenue impact.",
      "problem02": "Expanded problem about their second pain point.",
      "problem03": "Expanded problem about their third pain point.",
      "problem04": "Expanded problem about their fourth pain point."
    }},
    "benefits": {{
      "benefit01": "Expanded 1-2 paragraph (max 50 words) about benefit 1. Focus on ROI and concrete deliverables.",
      "benefit02": "Expanded benefit 2.",
      "benefit03": "Expanded benefit 3.",
      "benefit04": "Expanded benefit 4."
    }}
  }}
}}

RULES for problems:
- Use direct "you" language (not third-person)
- Focus on revenue impact and dollar amounts
- Be specific and actionable
- Example: "Right now, your top-of-funnel is converting very poorly to booked meetings. You have no problem generating opportunities; your problem is capitalizing on them."

RULES for benefits:
- Use direct "you" language
- Emphasize ROI and payback period
- Focus on concrete deliverables and measurable results

Return ONLY the JSON, no markdown code blocks or explanations."""

        msg = client.messages.create(
            model="claude-opus-4-5-20251101",
            max_tokens=4000,
            messages=[{"role": "user", "content": extraction_prompt}]
        )

        response_text = msg.content[0].text.strip()

        # Remove markdown code blocks if present
        if response_text.startswith("```"):
            lines = response_text.split('\n')
            response_text = '\n'.join(lines[1:-1])

        extracted_data = json.loads(response_text)

        slack_notify(f"🧠 *Step 2/3: Info extracted*\nClient: {extracted_data['client']['company']}")

        # Step 3: Create PandaDoc proposal
        API_KEY = os.getenv("PANDADOC_API_KEY")
        if not API_KEY:
            raise ValueError("PANDADOC_API_KEY not configured")

        TEMPLATE_UUID = "G8GhAvKGa9D8dmpwTnEWyV"
        API_URL = "https://api.pandadoc.com/public/v1/documents"

        client_info = extracted_data.get("client", {})
        project = extracted_data.get("project", {})
        problems = project.get("problems", {})
        benefits = project.get("benefits", {})

        # Build tokens
        tokens = [
            {"name": "Client.Company", "value": client_info.get("company", "")},
            {"name": "Personalization.Project.Title", "value": project.get("title", "")},
            {"name": "MonthOneInvestment", "value": str(project.get("monthOneInvestment", ""))},
            {"name": "MonthTwoInvestment", "value": str(project.get("monthTwoInvestment", ""))},
            {"name": "MonthThreeInvestment", "value": str(project.get("monthThreeInvestment", ""))},
            {"name": "Personalization.Project.Problem01", "value": problems.get("problem01", "")},
            {"name": "Personalization.Project.Problem02", "value": problems.get("problem02", "")},
            {"name": "Personalization.Project.Problem03", "value": problems.get("problem03", "")},
            {"name": "Personalization.Project.Problem04", "value": problems.get("problem04", "")},
            {"name": "Personalization.Project.Benefit.01", "value": benefits.get("benefit01", "")},
            {"name": "Personalization.Project.Benefit.02", "value": benefits.get("benefit02", "")},
            {"name": "Personalization.Project.Benefit.03", "value": benefits.get("benefit03", "")},
            {"name": "Personalization.Project.Benefit.04", "value": benefits.get("benefit04", "")},
            {"name": "Slide.Footer", "value": f"{client_info.get('company', 'Client')} x LeftClick"},
            {"name": "Document.CreatedDate", "value": datetime.utcnow().strftime("%B %d, %Y")},
        ]

        # Create document
        payload = {
            "name": f"Proposal - {client_info.get('company', 'Client')} - {project.get('title', 'Project')}",
            "template_uuid": TEMPLATE_UUID,
            "recipients": [
                {
                    "email": client_info.get("email", "demo@example.com"),
                    "first_name": client_info.get("firstName", ""),
                    "last_name": client_info.get("lastName", ""),
                    "role": "Client"
                }
            ],
            "tokens": tokens
        }

        headers = {
            "Authorization": f"API-Key {API_KEY}",
            "Content-Type": "application/json"
        }

        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        doc_data = response.json()
        doc_id = doc_data.get("id")
        doc_url = f"https://app.pandadoc.com/a/#/documents/{doc_id}"

        slack_notify(f"✅ *Step 3/3: Proposal Created*\nClient: {client_info.get('company')}\nDoc: {doc_url}")

        return JSONResponse({
            "status": "success",
            "transcript_used": transcript,
            "document_id": doc_id,
            "document_url": doc_url,
            "client": client_info,
            "project_title": project.get("title"),
            "extracted_data": extracted_data
        })

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        slack_error(f"Failed to parse Claude response: {str(e)}")
        return JSONResponse({
            "status": "error",
            "error": f"Failed to parse extracted data: {str(e)}",
            "raw_response": response_text[:1000] if 'response_text' in dir() else "N/A"
        }, status_code=500)

    except Exception as e:
        logger.error(f"Proposal creation error: {e}")
        slack_error(f"Proposal creation failed: {str(e)}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


# ============================================================================
# YOUTUBE OUTLIER DETECTION (Using Apify - more reliable in cloud)
# ============================================================================

def scrape_youtube_with_apify(keywords: list, max_per_keyword: int, days_back: int) -> list:
    """
    FAST YouTube search using streamers/youtube-scraper.
    ~15 seconds for 3 results. Pay-per-result pricing.
    """
    from apify_client import ApifyClient

    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        slack_notify("Error: APIFY_API_TOKEN not set")
        return []

    client = ApifyClient(apify_token)
    all_videos = []

    for keyword in keywords:
        try:
            slack_notify(f"Searching: {keyword}")

            # streamers/youtube-scraper - exact input schema
            run_input = {
                "searchQueries": [keyword],
                "maxResults": max_per_keyword,
                "maxResultsShorts": 0,
                "maxResultStreams": 0,
            }

            run = client.actor("streamers/youtube-scraper").call(run_input=run_input, timeout_secs=60)

            count = 0
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                video_id = item.get("id") or item.get("videoId")
                if not video_id:
                    url = item.get("url") or ""
                    if "v=" in url:
                        video_id = url.split("v=")[-1].split("&")[0]

                view_count = item.get("viewCount") or 0

                video_data = {
                    "title": item.get("title"),
                    "url": item.get("url") or f"https://www.youtube.com/watch?v={video_id}",
                    "view_count": view_count,
                    "channel_name": item.get("channelName"),
                    "channel_url": item.get("channelUrl"),
                    "thumbnail_url": item.get("thumbnailUrl"),
                    "date": item.get("date"),
                    "video_id": video_id,
                }

                if video_data["title"] and video_data["video_id"]:
                    all_videos.append(video_data)
                    count += 1

            slack_notify(f"Found {count} videos for '{keyword}'")

        except Exception as e:
            error_msg = str(e)[:150]
            logger.error(f"Apify error for '{keyword}': {error_msg}")
            slack_notify(f"Apify error: {error_msg}")

    return all_videos


def get_channel_average_apify(channel_url: str, apify_client) -> int:
    """
    Skip channel average calculation - not needed with absolute view thresholds.
    We'll score videos by absolute view count instead of relative to channel average.
    """
    return 0


def fetch_youtube_transcript(video_id, apify_client):
    """Fetch transcript using Apify (karamelo/youtube-transcripts)."""
    if not video_id:
        return None

    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        run_input = {"urls": [video_url]}
        run = apify_client.actor("karamelo/youtube-transcripts").call(run_input=run_input, timeout_secs=120)

        dataset_items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())

        if dataset_items and len(dataset_items) > 0:
            transcript_data = dataset_items[0]
            captions = transcript_data.get("captions", [])
            if captions and isinstance(captions, list):
                return " ".join(captions)

        return None
    except Exception as e:
        logger.warning(f"Transcript error for {video_id}: {str(e)[:100]}")
        return None


def summarize_youtube_transcript(text, anthropic_client):
    """Summarize transcript using Claude Sonnet 4.5."""
    prompt = f"""Analyze this YouTube video transcript and provide a summary for a content creator.

Transcript: {text[:100000]}

Output Format (plain text, no markdown):

1. High-Level Overview: Write 2-3 sentences summarizing what the video is about and why it's resonating with viewers.

2. Section-by-Section Summary: Break down the video's content into distinct sections with clear transitions. For each section, describe what was covered.

Do not use any markdown formatting (no asterisks, no bullet points, no headers with #). Just plain text with numbered sections."""

    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1000,
            temperature=0.7,
            system="You are an expert YouTube strategist.",
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        return f"Error summarizing: {e}"


@app.function(image=image, secrets=ALL_SECRETS, timeout=1800)
def youtube_outliers_background(
    keywords: list,
    days_back: int,
    max_videos_per_keyword: int,
    top_n: int,
    min_score: float,
    sheet_id: str,
    sheet_url: str
):
    """
    Background task: Full YouTube outlier detection workflow.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from apify_client import ApifyClient
    import anthropic
    import gspread
    from google.oauth2.credentials import Credentials as UserCredentials
    from google.auth.transport.requests import Request

    try:
        # Step 1: Scrape Videos using Apify (yt-dlp blocked on cloud IPs)
        slack_notify(f"Step 1/5: Scraping YouTube via Apify\nKeywords: {len(keywords)}, Days: {days_back}")

        all_videos = scrape_youtube_with_apify(keywords, max_videos_per_keyword, days_back)

        unique_videos = {v['video_id']: v for v in all_videos if v.get('video_id')}.values()
        videos = list(unique_videos)

        slack_notify(f"Found {len(videos)} unique videos")

        if not videos:
            slack_notify("No videos found - check Apify actor availability")
            return {"status": "no_results", "videos_found": 0}

        # Step 2: Skip channel stats (too slow with Apify) - use absolute view ranking instead
        slack_notify("Step 2/5: Ranking by view count (skipping channel stats for speed)")

        # Step 3: Rank videos by view count (simple but effective)
        slack_notify("Step 3/5: Selecting top videos by views")

        # Filter videos with view counts and sort by views
        videos_with_views = [v for v in videos if v.get("view_count") and v.get("view_count") > 0]
        videos_with_views.sort(key=lambda x: x.get("view_count", 0), reverse=True)

        # Take top N videos as "outliers"
        top_outliers = videos_with_views[:top_n]

        # Add placeholder scores based on view count
        for i, video in enumerate(top_outliers):
            video["outlier_score"] = round(video.get("view_count", 0) / 1000, 2)  # Score = views/1000
            video["channel_avg"] = 0  # Not calculated

        slack_notify(f"Selected top {len(top_outliers)} videos by view count")

        if not top_outliers:
            slack_notify("No outliers found above threshold")
            return {"status": "no_outliers", "videos_found": len(videos)}

        # Step 4: Fetch Transcripts & Summarize
        slack_notify("Step 4/5: Fetching transcripts & summarizing")

        apify_token = os.getenv("APIFY_API_TOKEN")
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")

        if apify_token and anthropic_key:
            apify_client = ApifyClient(apify_token)
            claude_client = anthropic.Anthropic(api_key=anthropic_key)

            def process_outlier(video):
                video_id = video.get("video_id")
                transcript = fetch_youtube_transcript(video_id, apify_client)
                video["summary"] = summarize_youtube_transcript(transcript, claude_client) if transcript else "No transcript available."
                return video

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_outlier, v) for v in top_outliers]
                processed = [future.result() for future in as_completed(futures)]
            top_outliers = processed
        else:
            for video in top_outliers:
                video["summary"] = "API keys not configured"

        top_outliers.sort(key=lambda x: x["outlier_score"], reverse=True)

        # Step 5: Upload to Google Sheet
        slack_notify(f"Step 5/5: Uploading {len(top_outliers)} outliers to Sheet")

        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        creds = UserCredentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"]
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        ws = sh.get_worksheet(0)

        headers = ["Outlier Score", "Title", "Video Link", "View Count", "Channel Name", "Channel Avg", "Thumbnail", "Summary", "Publish Date"]
        ws.append_row(headers)

        rows = []
        for v in top_outliers:
            rows.append([
                v.get("outlier_score"),
                v.get("title"),
                v.get("url"),
                v.get("view_count"),
                v.get("channel_name"),
                v.get("channel_avg"),
                f'=IMAGE("{v.get("thumbnail_url")}")',
                v.get("summary"),
                v.get("date")
            ])

        ws.append_rows(rows, value_input_option='USER_ENTERED')

        slack_notify(f"YouTube Outliers Complete!\nOutliers: {len(top_outliers)}\nSheet: {sheet_url}")

        return {"status": "success", "videos_scraped": len(videos), "outliers_found": len(top_outliers), "sheet_url": sheet_url}

    except Exception as e:
        logger.error(f"YouTube outliers error: {e}")
        slack_error(f"YouTube outliers failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.function(image=image, secrets=ALL_SECRETS, timeout=60)
@modal.fastapi_endpoint(method="GET")
def youtube_outliers(
    keywords: str = "",
    days: int = 7,
    max_per_keyword: int = 30,
    top_n: int = 10,
    min_score: float = 0.9
):
    """
    Find YouTube outlier videos.

    URL: GET /youtube-outliers?keywords=AI+agents,ChatGPT&days=7&top_n=10

    Returns 201 immediately with Google Sheet URL. Background task scrapes,
    calculates scores, fetches transcripts, summarizes, and uploads to Sheet.
    Monitor progress via Slack.
    """
    from fastapi.responses import JSONResponse
    import gspread
    from google.oauth2.credentials import Credentials as UserCredentials
    from google.auth.transport.requests import Request

    default_keywords = [
        "agentic workflows",
        "AI agents",
        "agent framework",
        "multi-agent systems",
        "AI automation agents",
        "LangGraph",
        "CrewAI",
        "AutoGPT"
    ]

    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()] if keywords else default_keywords

    try:
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        creds = UserCredentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"]
        )
        creds.refresh(Request())  # Always refresh — expiry not tracked in stored token

        gc = gspread.authorize(creds)
        sheet_name = f"YouTube Outliers {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
        sh = gc.create(sheet_name)
        sheet_id = sh.id
        sheet_url = sh.url

        slack_notify(f"YouTube Outliers Started\nKeywords: {', '.join(keyword_list[:3])}{'...' if len(keyword_list) > 3 else ''}\nDays: {days}\nSheet: {sheet_url}")

        youtube_outliers_background.spawn(keyword_list, days, max_per_keyword, top_n, min_score, sheet_id, sheet_url)

        return JSONResponse({
            "status": "accepted",
            "message": "YouTube outlier detection started. Monitor Slack for progress.",
            "sheet_url": sheet_url,
            "sheet_name": sheet_name,
            "keywords": keyword_list,
            "workflow": [
                "1. Scraping YouTube videos via yt-dlp",
                "2. Fetching channel statistics",
                "3. Calculating outlier scores",
                "4. Fetching transcripts via Apify",
                "5. Summarizing with Claude",
                "6. Uploading to Google Sheet"
            ]
        }, status_code=201)

    except Exception as e:
        logger.error(f"YouTube outliers init error: {e}")
        slack_error(f"YouTube outliers init failed: {str(e)}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# ============================================================================
# TECH RADAR - Weekly Research Cron (Friday 9am GMT)
# ============================================================================

@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=120,
    schedule=modal.Cron("0 9 * * 5"),  # Friday 9am UTC (= GMT)
)
def tech_radar_trigger():
    """
    Weekly cron: Trigger Tech Radar research scan via Manus.
    Fires every Friday at 9am GMT.
    Creates a Manus task and logs it to the tracker sheet.
    Manus webhook calls back to /d/tech-radar-complete when done.
    Completion handler creates Google Doc + podcast + notifications.
    """
    import importlib.util
    import sys

    slack_notify("🔬 *Tech Radar* Weekly scan triggered (Friday 9am GMT)")

    token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))

    # Import the tech_radar_research module
    sys.path.insert(0, "/app")
    spec = importlib.util.spec_from_file_location(
        "tech_radar_research", "/app/execution/tech_radar_research.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        result = module.trigger_scan(
            token_data=token_data,
            slack_notify_fn=slack_notify,
        )

        status_emoji = "✅" if result.get("status") == "running" else "❌"
        slack_notify(
            f"{status_emoji} *Tech Radar* Trigger result: "
            f"task_id=`{result.get('task_id', 'N/A')}`, "
            f"period={result.get('scan_period', 'N/A')}"
        )

        return result

    except Exception as e:
        logger.error(f"Tech Radar trigger error: {e}")
        slack_error(f"Tech Radar trigger failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# LEAD FUNNEL - Weekly Summary Cron (Monday 8am UTC)
# ============================================================================

@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=120,
    schedule=modal.Cron("0 8 * * 1"),  # Monday 8am UTC
)
def lead_funnel_weekly():
    """
    Weekly cron: Send lead funnel summary via Telegram.
    Fires every Monday at 8am UTC.
    """
    import importlib.util
    import sys

    slack_notify("📊 *Lead Funnel* Weekly summary triggered (Monday 8am UTC)")

    token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))

    sys.path.insert(0, "/app")
    spec = importlib.util.spec_from_file_location(
        "lead_funnel_analytics", "/app/execution/lead_funnel_analytics.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        result = module.run_weekly(
            token_data=token_data,
            notify_fn=slack_notify,
        )

        status_emoji = "✅" if result.get("status") == "success" else "❌"
        slack_notify(
            f"{status_emoji} *Lead Funnel* Weekly result: "
            f"leads={result.get('total_leads', 'N/A')}, "
            f"conversion={result.get('overall_conversion', 'N/A')}%"
        )
        return result

    except Exception as e:
        logger.error(f"Lead Funnel weekly error: {e}")
        slack_error(f"Lead Funnel weekly failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# LEAD FUNNEL - Monthly Report Cron (1st of month, 8am UTC)
# ============================================================================

@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=180,
    schedule=modal.Cron("0 8 1 * *"),  # 1st of month, 8am UTC
)
def lead_funnel_monthly():
    """
    Monthly cron: Generate full lead funnel report as Google Doc + Telegram.
    Fires on the 1st of every month at 8am UTC.
    """
    import importlib.util
    import sys

    slack_notify("📊 *Lead Funnel* Monthly report triggered (1st of month)")

    token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))

    sys.path.insert(0, "/app")
    spec = importlib.util.spec_from_file_location(
        "lead_funnel_analytics", "/app/execution/lead_funnel_analytics.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        result = module.run_monthly(
            token_data=token_data,
            notify_fn=slack_notify,
        )

        status_emoji = "✅" if result.get("status") == "success" else "❌"
        slack_notify(
            f"{status_emoji} *Lead Funnel* Monthly result: "
            f"leads={result.get('total_leads', 'N/A')}, "
            f"doc={result.get('doc_url', 'N/A')}"
        )
        return result

    except Exception as e:
        logger.error(f"Lead Funnel monthly error: {e}")
        slack_error(f"Lead Funnel monthly failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# INVESTMENT RESEARCH - Monthly Cron (1st of month, 7am UTC)
# ============================================================================

@app.function(
    image=image,
    secrets=ALL_SECRETS,
    timeout=120,
    schedule=modal.Cron("0 7 1 * *"),  # 1st of month, 7am UTC
)
def investment_research_trigger():
    """
    Monthly cron: Trigger investment research via Manus.
    Fires on the 1st of every month at 7am UTC.
    Creates a Manus task and logs it to the tracker tab.
    Manus webhook calls back to /d/investment-research-complete when done.
    Completion handler creates Google Doc + Telegram notification.
    """
    import importlib.util
    import sys

    slack_notify("📈 *Investment Research* Monthly scan triggered (1st of month)")

    token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))

    sys.path.insert(0, "/app")
    spec = importlib.util.spec_from_file_location(
        "investment_research", "/app/execution/investment_research.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        result = module.trigger_research(
            token_data=token_data,
            slack_notify_fn=slack_notify,
        )

        status_emoji = "✅" if result.get("status") == "running" else "❌"
        slack_notify(
            f"{status_emoji} *Investment Research* Trigger result: "
            f"task_id=`{result.get('task_id', 'N/A')}`, "
            f"month={result.get('report_month', 'N/A')}"
        )

        return result

    except Exception as e:
        logger.error(f"Investment Research trigger error: {e}")
        slack_error(f"Investment Research trigger failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.local_entrypoint()
def main():
    print("Modal Claude Orchestrator - Directive Edition")
    print("=" * 50)
    print("Deploy:  modal deploy execution/modal_webhook.py")
    print("Logs:    modal logs claude-orchestrator")
    print("")
    print("Endpoints:")
    print("  POST /directive?slug={slug}  - Execute a directive")
    print("  GET  /agent?query=...        - General-purpose agent (proof of concept)")
    print("  GET  /list-webhooks          - List available slugs")
    print("  GET  /test-email             - Test email")
    print("")
    print("Execution-Only Endpoints (for local agent orchestration):")
    print("  GET  /scrape-leads?query=dentists&location=US&limit=100")
    print("  POST /generate-proposal      - Body: {client, project}")
    print("  GET  /read-demo-transcript?name=kickoff|sales")
    print("  GET  /create-proposal-from-transcript?transcript=sales")
    print("  GET  /youtube-outliers?keywords=AI+agents,ChatGPT&days=7&top_n=10")
    print("")
    print("Cron Jobs:")
    print("  hourly_lead_scraper          - Runs every hour")
    print("")
    print("Configure webhooks in: execution/webhooks.json")
