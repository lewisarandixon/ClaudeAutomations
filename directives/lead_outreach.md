# Lead Outreach

## Purpose
Create personalised draft emails in Outlook (Microsoft 365) for leads scraped via the GMaps pipeline. Drafts are created — not sent — so they can be reviewed before sending.

## How It Works

1. Fetch un-emailed leads from Airtable (Contact Email not empty, Messaged At blank)
2. Load industry-based templates from Google Sheets ("Outreach Templates" tab)
3. Personalise each template with lead data (name, company, city, industry)
4. Create draft emails in Outlook via Microsoft Graph API
5. Update Airtable records with `Messaged At` date
6. Send Telegram summary notification

## Templates

Stored in the **Outreach Templates** tab of the Automation Config spreadsheet (`1W-DJCE2XDEj5IRcmHQjuNJ6aaKouPbZOyD9Cbt3KYFM`).

### Columns
| Column | Description |
|--------|-------------|
| Industry | Must match Airtable Industry field exactly. "Default" is the fallback. |
| Subject | Email subject line for this industry. |
| Bullets | Newline-separated list of automation examples (industry-specific). |
| Deck File ID | Google Drive file ID for the industry presentation (optional). |
| Deck Filename | Display name for the presentation file (optional). |

### Email Structure (shared across all industries, hardcoded in Python)

The email body is the same for every industry — only the bullets and deck link change:

1. Time-of-day greeting + contact name (or no name)
2. "I hope you're well."
3. Lewis intro (22-year-old automation enthusiast, keen to help local businesses)
4. Local service positioning line + company mention (if available)
5. **"Common automations I build:"** + industry-specific bullet points (from Sheets)
6. **Deck link** with styled button (from Sheets, optional — omitted if no Deck File ID)
7. Call to action (10-15 min chat)
8. "Best, Lewis"

### Template Strategy
- **Default** row is required — used when no industry-specific template exists
- Only bullets, subject, and deck change per industry — the rest is shared
- Edit industry-specific parts in Google Sheets, no code changes needed
- To add a new industry: add a row with matching Industry name, subject, bullets, and optionally a deck
- Default template has no deck (empty Deck File ID) — just bullets

## Sending
- Emails are created as **drafts** in the Outlook mailbox (not auto-sent)
- Review drafts in Outlook, then send manually or select all and send
- This avoids accidental sends and allows quality checks

## Tracking

- **Messaged At** field in Airtable (date) — set when draft is created
- Does NOT change Lead Status (which is managed by the funnel analytics system)
- Query for un-emailed leads: `{Contact Email} != '' AND {Messaged At} = BLANK()`

## Communications Table Logging

After each draft is created, log the outbound email to the Airtable Communications table:

| Field | Value |
|-------|-------|
| Direction | `Outbound` |
| Contact Email | lead's email address |
| Lead Name | company name |
| Message | email subject line |
| Status | `Messaged` |
| Date/time | timestamp of draft creation |

**Env var:** `AIRTABLE_COMMUNICATIONS_ID` — set in `.env` and Modal secret.
Logs immediately after each successful `create_draft_email()` call, alongside the `Messaged At` update. Failures are warned but do not block the outreach run.

## CLI Usage
```bash
# Test Microsoft Graph authentication
python execution/lead_outreach.py --test-auth

# Preview emails without creating drafts
python execution/lead_outreach.py --industry "Dentist" --limit 5 --dry-run

# Create drafts for one industry
python execution/lead_outreach.py --industry "Dentist" --limit 10

# Create drafts for all industries
python execution/lead_outreach.py --all --limit 50
```

## Webhook
Slug: `lead-outreach`
```json
{"action": "send", "industry": "Dentist", "limit": 10}
{"action": "send", "all": true, "limit": 50}
{"action": "send", "industry": "Dentist", "dry_run": true}
{"action": "test_auth"}
```

## Rate Limiting
- 2-second delay between draft creations
- Airtable batch updates: 10 records per PATCH, 0.2s delay

## Pre-requisites

### Azure AD App Registration (Lewis does once)
1. Go to https://entra.microsoft.com -> Identity -> Applications -> App registrations -> New
2. Name: `All In One Email Outreach`, Single tenant, no redirect URI
3. Copy **Application (client) ID** and **Directory (tenant) ID**
4. Certificates & secrets -> New client secret (24 months) -> copy **Value**
5. API permissions -> Add -> Microsoft Graph -> **Application permissions** -> `Mail.ReadWrite`
6. Click **Grant admin consent**

### Environment Variables
```
MICROSOFT_TENANT_ID=<from step 3>
MICROSOFT_CLIENT_ID=<from step 3>
MICROSOFT_CLIENT_SECRET=<from step 4>
MICROSOFT_SENDER_EMAIL=support@allinonesolutions.co.uk
```

### Modal Secret
```bash
modal secret create microsoft-secret \
  MICROSOFT_TENANT_ID=... \
  MICROSOFT_CLIENT_ID=... \
  MICROSOFT_CLIENT_SECRET=... \
  MICROSOFT_SENDER_EMAIL=support@allinonesolutions.co.uk
```

### Airtable

- Add **Messaged At** field (type: Date) to Leads table
- Can be done via Airtable UI

## Dependencies

### Environment Variables
- MICROSOFT_TENANT_ID
- MICROSOFT_CLIENT_ID
- MICROSOFT_CLIENT_SECRET
- MICROSOFT_SENDER_EMAIL
- AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_LEADS_ID
- AUTOMATION_CONFIG_SHEET_ID
- LEAD_ANALYTICS_BOT_TOKEN / TELEGRAM_BOT_TOKEN (for notifications)
- LEAD_ANALYTICS_CHAT_ID / TELEGRAM_CHAT_ID
- Google OAuth token (token.json or GOOGLE_TOKEN_JSON)

### Python Packages
- requests (Microsoft Graph API, Airtable, Telegram)
- gspread, google-auth (Google Sheets for templates)
- python-dotenv

## Files
- `directives/lead_outreach.md` — This file
- `execution/lead_outreach.py` — Main script
- `execution/modal_webhook.py` — Webhook routing (lead-outreach slug)
- `execution/webhooks.json` — Webhook slug configuration

## Error Handling
- Missing template for industry -> uses "Default" template
- Missing client name -> "Hi there," (not "Hi ,")
- Microsoft auth failure -> returns error, no partial sends
- Individual draft failure -> logged, continues to next lead
- Airtable update failure -> logged, drafts still created
- Telegram failure -> logged, does not block

## Verification
1. `--test-auth` confirms Microsoft Graph token works and can access drafts folder
2. `--dry-run` shows personalised subjects without creating anything
3. Single draft: `--industry "Dentist" --limit 1` then check Outlook drafts
4. Check Airtable record has `Messaged At` populated

## Learnings
- Microsoft Graph API: `POST /users/{sender}/messages` creates a draft; `POST /users/{sender}/sendMail` sends immediately. We use the draft endpoint.
- Application permissions (`Mail.ReadWrite`) require admin consent in Azure AD
- M365 Business Basic supports Graph API access
- Draft approach is safer for cold outreach — allows review before sending
