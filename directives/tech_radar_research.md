# Weekly Tech Radar Research

## Purpose
Automated weekly scan of AI/automation sources for new developments, compiled into a structured research report with an AI-generated podcast. Research only — no content generation. You review the report and podcast, then decide what to act on.

## Schedule
Every Friday at 9am GMT via Modal cron.

## Process

### Step 1: Trigger (Cron)
The `tech_radar_trigger` Modal cron function fires every Friday at 9am GMT. It:
1. Reads `last_tech_scan_date` from the "Automation Config" sheet
2. Calculates scan period (last_scan_date to now)
3. Builds a Manus prompt with all sources and the output format
4. Calls Manus API to create a research task (agentProfile: "manus-1.6")
5. Logs `task_id` to the "Tech Radar Tracker" sheet with status "running"
6. Returns immediately (async — Manus does the work)

### Step 2: Manus Executes Research
Manus autonomously scans all sources (see below), extracts relevant developments from the scan period, and produces a structured Markdown report.

### Step 3: Completion Webhook
When Manus finishes (task_stopped, stop_reason: "finish"):
1. The `tech-radar-complete` webhook receives the payload
2. Extracts the Markdown report from Manus output/attachments
3. Creates a Google Doc with the report content
4. Generates a NotebookLM-style podcast via Google Cloud Podcast API:
   - Sends report text as context to Podcast API
   - Polls for completion (~2-5 min)
   - Downloads MP3
   - Uploads MP3 to Google Drive (shareable link)
5. Sends Telegram notification with Google Doc link + podcast link
6. Sends email backup with both links
7. Updates "Tech Radar Tracker" sheet: status -> "completed", doc_url, podcast_url, completed_at
8. Updates "Automation Config": last_tech_scan_date -> today

## Sources to Scan

### AI Platforms (Priority)
- OpenAI blog & API changelog
- Anthropic blog & Claude release notes
- Google AI blog (Gemini updates)
- Manus AI updates
- Mistral, Cohere, xAI announcements

### Automation Platforms
- Make.com blog
- n8n blog
- Zapier blog
- Pipedream blog

### News & Aggregators
- Product Hunt (AI, Automation, Dev Tools categories)
- Hacker News (search: automation, AI tool, LLM, agent)
- Ben's Bites newsletter
- The Rundown AI newsletter
- TLDR AI newsletter

### YouTube (Recent Uploads)
- Nate Herk, Matt Wolfe, AI Advantage
- Liam Ottley, AI Jason, Skill Leap AI, WorldofAI

### Reddit (Last 7 Days)
- r/automation, r/nocode, r/artificial, r/ChatGPT
- r/LocalLLaMA, r/ClaudeAI, r/OpenAI

### Twitter/X
- Search: "new AI model", "automation update", "API release"
- Accounts: @OpenAI, @AnthropicAI, @GoogleAI

## What to Extract
- AI model updates (releases, API changes, new features, pricing)
- Automation platform updates (features, integrations)
- New tools launched (name, category, what it does, integration potential, URL, date)
- Integration announcements (Platform A + B, what it enables)
- Trending topics (what's being discussed, where, why)
- Industry news (funding, acquisitions, partnerships)
- Notable YouTube videos (title, channel, key takeaway)

## Output Format (Markdown Report)

The report uses this structure:
- Weekly AI/Automation Tech Radar header with date
- Executive Summary (2-3 sentences)
- AI Model Updates (by provider: OpenAI, Anthropic, Google AI, Manus AI, Other)
- Automation Platform Updates (Make.com, n8n, Zapier)
- New Tools Launched (each with Category, What it does, Why it matters, Integration potential, Link, Released)
- Integration Announcements
- Trending Discussions
- Industry News
- Notable YouTube Videos
- Opportunities for You (High/Medium/Low Priority)
- Research Methodology

## Podcast Generation

Uses the Google Cloud Podcast API (standalone — no NotebookLM Enterprise notebook required).

**Endpoint:** `POST https://discoveryengine.googleapis.com/v1/projects/{PROJECT_ID}/locations/global/podcasts`

**How it works:**
1. The Manus research report text is sent as context to the Podcast API
2. `podcastConfig.focus` is set to summarize the most important AI/automation developments
3. `podcastConfig.length` is set to "STANDARD" (~10 min podcast)
4. API returns an operation ID — poll until complete
5. Download the MP3 via the operation download endpoint
6. Upload MP3 to Google Drive, get shareable link

**Requirements:**
- Google Cloud project with Discovery Engine API enabled
- IAM role: `roles/discoveryengine.podcastApiUser`
- GCP_PROJECT_ID in environment variables

**Fallback:** If podcast generation fails (API unavailable, quota exceeded, etc.), the automation continues without it — Google Doc + email are still delivered. Podcast failure is logged but does not block the rest of the pipeline.

## Google Sheets

### Tech Radar Tracker
Spreadsheet: (set TECH_RADAR_TRACKER_SHEET_ID in .env)
Columns: scan_date, scan_period_start, scan_period_end, manus_task_id, status, doc_url, podcast_url, completed_at

### Automation Config
Spreadsheet: (set AUTOMATION_CONFIG_SHEET_ID in .env)
Rows: setting_name | setting_value (e.g. last_tech_scan_date | 2026-02-01)

## Dependencies

### Environment Variables
- MANUS_API_KEY
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- YOUR_EMAIL
- TECH_RADAR_TRACKER_SHEET_ID
- AUTOMATION_CONFIG_SHEET_ID
- GCP_PROJECT_ID (for Podcast API)

### Python Packages
- requests (for Manus REST API + Telegram + Podcast API)
- google-auth, google-api-python-client, gspread
- google-auth-oauthlib

## Files
- directives/tech_radar_research.md — This file
- execution/tech_radar_research.py — Standalone script (can also be run manually)
- execution/modal_webhook.py — Contains tech_radar_trigger cron + tech-radar-complete webhook

## Error Handling
- If Manus task fails: status set to "error" in tracker, Slack notification sent
- If Google Doc creation fails: report sent as email attachment instead
- If podcast generation fails: continues without podcast, logs warning
- If Telegram fails: email backup ensures delivery
- If last_scan_date missing: defaults to 7 days ago

## Cost Estimate
- Manus task: ~150 credits (~$1.50 per scan)
- Google Cloud Podcast API: Free tier or minimal cost
- Google Sheets/Docs/Telegram/Email: Free
- Total: ~$1.50-2.00/week, ~$6-8/month

## Learnings
(Updated as issues are discovered)
