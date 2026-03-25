# Monthly Investment Research

## Purpose

Automated monthly investment research for Lewis's Stocks & Shares ISA (Trading 212, £500/month). Manus AI does deep research on the 1st of every month covering macro data, asset classes, market cycles, geopolitics, sector trends, and individual stocks — then produces a comprehensive Google Doc with actionable recommendations.

## Schedule

1st of every month at 7am UTC via Modal cron.

## Process

### Step 1: Trigger (Cron)

The `investment_research_trigger` Modal cron function fires on the 1st of every month at 7am UTC. It:

1. Builds a comprehensive research prompt covering 8 sections
2. Calls Manus API to create a research task (agentProfile: "manus-1.6")
3. Logs `task_id` to the "Investment Research" tab in the Automation Config spreadsheet with status "running"
4. Returns immediately (async — Manus does the work)

### Step 2: Manus Executes Research

Manus autonomously researches all 8 sections (~10-30 minutes), covering:

1. Macroeconomic Overview (inflation, rates, GDP, unemployment)
2. Asset Class Analysis (index funds, bonds, commodities, emerging markets)
3. Market Cycles & Timing (business cycle, liquidity, seasonal patterns)
4. Geopolitical Risk Assessment (conflicts, trade, energy)
5. Policy & Regulatory Watch (tax, regulation, central bank)
6. Technology & Sector Trends (AI, clean energy, healthcare)
7. Magnificent 7 Deep Dive (earnings, valuations, analyst consensus)
8. Recommendations (top picks, allocation for £500/month)

### Step 3: Completion Webhook

When Manus finishes (task_stopped, stop_reason: "finish"):

1. The `investment-research-complete` webhook receives the payload
2. Extracts the Markdown report from Manus output/attachments
3. Creates a Google Doc with the report content
4. Sends Telegram notification with Google Doc link
5. Updates "Investment Research" tracker tab: status -> "completed", doc_url, completed_at

## Investor Profile

- Moderate risk tolerance
- Primarily index funds for diversification
- Saves £500/month into ISA
- Maxes LISA at year-end, keeps rest in ISA
- Platform: Trading 212

## Google Sheets

### Investment Research Tracker

Tab: "Investment Research" in Automation Config spreadsheet (`1W-DJCE2XDEj5IRcmHQjuNJ6aaKouPbZOyD9Cbt3KYFM`)

Columns: report_date | manus_task_id | status | doc_url | completed_at

## CLI Usage

```bash
# Trigger research manually
python execution/investment_research.py --trigger

# Register Manus webhook (one-time setup)
python execution/investment_research.py --register-webhook "https://lewiscity10--claude-orchestrator-directive.modal.run?slug=investment-research-complete"
```

## Webhook

Slug: `investment-research-complete`

Receives Manus completion webhook payload with `event_type: "task_stopped"`, `stop_reason: "finish"`.

## Dependencies

### Environment Variables

- MANUS_API_KEY
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- AUTOMATION_CONFIG_SHEET_ID
- Google OAuth token (token.json or GOOGLE_TOKEN_JSON)

### Python Packages

- requests (Manus API, Telegram)
- gspread, google-auth (Google Sheets tracker)
- google-api-python-client (Google Docs creation)

## Files

- `directives/investment_research.md` — This file
- `execution/investment_research.py` — Main script
- `execution/modal_webhook.py` — Contains cron trigger + webhook routing
- `execution/webhooks.json` — Webhook slug configuration

## Error Handling

- Manus task failure -> status set to "error" in tracker, Telegram notification sent
- Google Doc creation failure -> report logged, error reported
- Telegram failure -> logged, does not block
- Missing tracker tab -> auto-created on first run

## Verification

1. `--trigger` manually fires a Manus task, returns task ID
2. Check tracker tab shows status="running"
3. When Manus completes (~10-30 min): Google Doc created, Telegram sent
4. Tracker tab updated to status="completed" with doc_url

## Learnings

(Updated as issues are discovered)
