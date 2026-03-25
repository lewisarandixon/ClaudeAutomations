# Lead Funnel Analytics

## Purpose
Automated lead ingestion from Google Sheets into Airtable, plus funnel performance analytics delivered weekly (Telegram) and monthly (Google Doc + Manus AI insights + Telegram). Tracks conversion rates across 7 pipeline stages, exit statuses, lead score effectiveness, industry performance, and cross-field analytics.

## Schedule
- **Weekly summary:** Monday 8am UTC — Telegram message with key metrics + cross-field highlights
- **Monthly report:** 1st of month, 8am UTC — full Google Doc report + Manus AI deep-dive + Telegram notification
- **Lead ingestion:** On-demand via webhook or CLI (after scrape completes)

## Telegram Bot
Dedicated bot for lead analytics (separate from tech radar bot):
- Bot: @Lead_Analytics_Manus_Bot
- Env vars: `LEAD_ANALYTICS_BOT_TOKEN`, `LEAD_ANALYTICS_CHAT_ID`
- Falls back to `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` if not set

## Funnel Stages (in order)
1. Messaged
2. Responded
3. Meeting Booked
4. Second Meeting Booked
5. Proposal Sent
6. Negotiating
7. Won (active project)

### Exit Statuses (non-linear)
- Lost
- Not fit
- AWOL
- Closing

## Process

### Lead Ingestion (Sheets → Airtable)
1. Read leads from Google Sheet (gmaps_lead_pipeline output format)
2. Fetch existing Airtable leads for deduplication
3. Dedupe by: Contact Email (primary), Company Name + City/County (fallback)
4. Normalise Industry field via `INDUSTRY_NORMALISATION` mapping (Google Maps category → Airtable single-select)
5. Map Sheet columns to Airtable fields:
   - business_name → Company / Business Name
   - owner_name → Client Name
   - emails (first) → Contact Email
   - phone → Phone / WhatsApp Number
   - category → Industry (normalised)
   - website → Company Website URL
   - address → Address
   - city → City/County
   - state → Country/State
   - rating → Rating
   - google_maps_url → Google Maps URL
   - facebook/linkedin/etc → Social Media Link (first available)
6. Auto-set on ingestion:
   - Lead Status = "Messaged"
   - Platform = "Scraped Lead"
   - Messaged At = today
   - Lead Created At = today
7. Create records in Airtable (batches of 10)

### Industry Normalisation
The `normalise_industry()` function maps Google Maps categories to Airtable options:
1. Exact match (lowercase) → mapped value
2. Substring match (either direction) → mapped value
3. No match → "Other"

**29 Airtable Industry options:** Accountant, Financial Advisor, Solicitor, Dentist, Veterinarian, Mortgage Broker, Real Estate Agency, Hotel, Kennel, Insurance Agency, Recruitment Agency, Consultant, Architect, Optician, Physiotherapist, Chiropractor, Electrician, Plumber, Construction Company, Restaurant, Spa, Travel Agency, Event Venue, Car Dealership, Driving School, Mechanic, Dog Groomer, Marketing Agency, Other

### Weekly Summary (Telegram)
1. Fetch all leads from Airtable
2. Compute funnel conversion rates at each stage
3. Compute lead score band performance
4. Track exit statuses (Lost/Not fit/AWOL/Closing)
5. Compute cross-field analytics (score x industry, rating x conversion, geography, time-to-progress)
6. Identify biggest drop-off point
7. Send concise summary via Telegram (dedicated lead analytics bot)

### Monthly Report (Google Doc + Manus)
1. Fetch all leads from Airtable
2. Load previous month's snapshot for comparison
3. Compute full analytics:
   - Funnel conversion rates (stage-by-stage)
   - Exit status breakdown
   - Lead score banding (Low/Med-Low/Med/Med-High/High/Unscored)
   - Score threshold analysis (find optimal cutoff)
   - Score-stage correlation (Pearson)
   - Industry breakdown (conversion rates per industry)
   - Cross-field analytics (score x industry, rating x conversion, geography, time-to-progress)
   - Month-over-month deltas (vs previous snapshot)
4. Generate Markdown report (Python quantitative tables)
5. Create Google Doc with report
6. Save monthly snapshot to Automation Config spreadsheet ("Analytics Snapshots" tab)
7. Send Telegram notification with Doc link
8. Trigger Manus AI deep-dive task (async):
   - Sends all metrics to Manus with structured prompt
   - Manus analyses data and returns qualitative insights
   - On completion webhook, combines Python tables + Manus insights into one Google Doc
   - Sends Telegram notification with combined report link

## Metrics

### Funnel Conversion
- Stage-by-stage conversion rate = leads at stage N+1 / leads at stage N
- A lead at stage N counts toward all stages 1 through N
- Overall conversion = Won / Messaged

### Exit Tracking
- Counts leads in Lost, Not fit, AWOL, Closing statuses
- Reported in both weekly Telegram and monthly Google Doc

### Lead Score Bands
| Band | Range |
|------|-------|
| Low | 0–25 |
| Medium-Low | 26–50 |
| Medium | 51–75 |
| Medium-High | 76–100 |
| High | 101+ |
| Unscored | null or 0 |

### Score Threshold Analysis
Tests cutoffs at 20, 30, 40, 50, 60, 70, 80, 90, 100 and finds the threshold that gives the highest conversion lift (above vs below). Minimum 10 leads per group.

### Industry Breakdown
Groups leads by Industry field, computes response rate and win rate for each industry with 10+ leads.

### Cross-Field Analytics
- **Score x Industry Matrix:** avg lead score and conversion rate per industry, flags anomalies (high score + low conversion, or vice versa)
- **Rating x Conversion:** Google Maps rating bands (1-2, 2-3, 3-4, 4-5, unrated) vs response/win rates
- **Geography Performance:** response and win rate by Country/State
- **Time-to-Progress:** average days between each stage transition (requires timestamp fields)

### Period Comparison (Month-over-Month)
- Snapshots stored in Automation Config spreadsheet (`Analytics Snapshots` tab)
- Each monthly run saves: total leads, overall conversion, stage counts, exit counts, top industry
- Monthly report includes delta table: change vs previous month

## Manus AI Integration
- **Pattern:** Same as Tech Radar (async task → webhook callback)
- **Flow:** Monthly cron runs Python analytics → sends metrics to Manus → Manus returns insights → completion webhook combines into single Doc
- **Prompt:** B2B sales analytics consultant perspective — executive insights, funnel bottleneck analysis, industry recommendations, scoring assessment, actionable next steps
- **Fallback:** If Manus fails, Python-only report is still created and sent

## Webhooks
- `lead-ingest` — Trigger lead ingestion: `{"action": "ingest", "sheet_id": "..."}`
- `lead-funnel-report` — Trigger report: `{"action": "weekly"}` or `{"action": "monthly"}`
- `lead-funnel-manus-complete` — Handle Manus completion: auto-detected from Manus payload (event_type + task_detail)

## Dependencies

### Environment Variables
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID
- AIRTABLE_LEADS_ID
- LEAD_ANALYTICS_BOT_TOKEN (dedicated bot, fallback: TELEGRAM_BOT_TOKEN)
- LEAD_ANALYTICS_CHAT_ID (fallback: TELEGRAM_CHAT_ID)
- MANUS_API_KEY (for monthly deep-dive, optional)
- AUTOMATION_CONFIG_SHEET_ID (for snapshot storage)
- Google OAuth token (token.json or GOOGLE_TOKEN_JSON)

### Airtable Fields Used
- Lead Status (single select: Messaged / Responded / Meeting Booked / Second Meeting Booked / Proposal Sent / Negotiating / Won (active project) / Lost / Not fit / AWOL / Closing)
- Lead Score (number)
- Industry (single select, 29 options)
- Contact Email
- Company / Business Name
- City/County, Country/State
- Address, Rating, Google Maps URL, Social Media Link
- Platform (single select)
- Messaged At, Responded At, Meeting 1 Date, Meeting 2 Date, Proposal Sent At, Accepted At, Client At, Lead Created At (date fields)

### Python Packages
- requests (Airtable API, Telegram, Manus API)
- gspread, google-auth, google-api-python-client (Google Sheets/Docs)
- python-dotenv

## Files
- `directives/lead_funnel_analytics.md` — This file
- `execution/lead_funnel_analytics.py` — Main script
- `execution/modal_webhook.py` — Contains cron functions + webhook routing
- `execution/webhooks.json` — Webhook slug configuration

## Error Handling
- Airtable pagination: follows offset cursor, max 100 records per page
- Airtable rate limit: 0.2s delay between requests (5 req/s)
- Airtable batch limit: 10 records per POST
- If < 5 total leads: skips analytics, returns "not enough data"
- If industry has < 10 leads: excluded from breakdown
- If score threshold groups have < 10 leads: excluded from analysis
- Google Doc failure: logged, Telegram still sent
- Telegram failure: logged, does not block
- Manus failure: logged, Python-only report still created
- Snapshot save failure: logged, does not block report
- Industry normalisation: unrecognised categories mapped to "Other"

## Future Enhancements
- n8n Airtable trigger to auto-fill timestamp fields when Lead Status changes
- Threshold alerts (conversion drop → immediate Telegram notification)
- Cohort analysis (leads by month, trends)
- Write metrics back to Airtable for dashboards

## Learnings
- Airtable API doesn't support field deletion or single-select option creation via API (needs UI or PAT with schema.bases:write scope)
- Airtable auto-creates single-select options when records are written with new values (if PAT has permission)
- Google Maps `categoryName` field is usually clean but varies (e.g. "Dental clinic" not "Dentist") — normalisation map handles this
- Manus webhook is registered globally — all tasks fire to the same webhook URL, routing by slug
- Telegram arrows (→) cause UnicodeEncodeError on Windows — use -> instead in Telegram messages
