#!/usr/bin/env python3
"""
Re-scrape emails for leads in an existing Google Sheet.

Reads rows where `website` is set but `email` is empty, runs the website
scraper on each, and writes results back to the same sheet in-place.
Optionally pushes found leads to Airtable afterwards.

Usage:
    python execution/rescrape_sheet_emails.py --sheet-id SHEET_ID
    python execution/rescrape_sheet_emails.py --sheet-id SHEET_ID --workers 15 --no-airtable
"""

import os
import sys
import json
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]


def get_gspread_client():
    token_data = json.load(open("token.json"))
    creds = Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri"),
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes", SCOPES),
    )
    creds.refresh(Request())
    return gspread.authorize(creds)


def scrape_one(row_num, website, title):
    """Scrape a single website and return (row_num, email, score)."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from execution.extract_website_contacts import scrape_website_contacts

    try:
        result = scrape_website_contacts(website, business_name=title, use_claude=False)
        email = result.get("best_email", "")
        score = result.get("best_email_score", 0.0)
        if email:
            logger.info(f"  Row {row_num}: Found {email} (score={score:.2f}) — {title}")
        else:
            logger.info(f"  Row {row_num}: No email — {title} ({result.get('error', 'not found')})")
        return row_num, email, score
    except Exception as e:
        logger.warning(f"  Row {row_num}: Error scraping {website}: {e}")
        return row_num, "", 0.0


def main():
    parser = argparse.ArgumentParser(description="Re-scrape emails for leads in a Google Sheet")
    parser.add_argument("--sheet-id", required=True, help="Google Sheet ID")
    parser.add_argument("--workers", type=int, default=15, help="Parallel workers (default: 15)")
    parser.add_argument("--no-airtable", action="store_true", help="Skip Airtable push")
    args = parser.parse_args()

    gc = get_gspread_client()
    sheet = gc.open_by_key(args.sheet_id).sheet1
    headers = sheet.row_values(1)

    if "website" not in headers:
        print("ERROR: No 'website' column found in sheet")
        sys.exit(1)

    website_col = headers.index("website") + 1   # 1-indexed
    email_col   = headers.index("email") + 1 if "email" in headers else None
    score_col   = headers.index("email_score") + 1 if "email_score" in headers else None
    title_col   = headers.index("title") + 1 if "title" in headers else None

    if not email_col:
        print("ERROR: No 'email' column found in sheet")
        sys.exit(1)

    all_values = sheet.get_all_values()
    rows_to_scrape = []
    for i, row in enumerate(all_values[1:], start=2):  # row 2 onwards (1-indexed)
        website = row[website_col - 1].strip() if len(row) >= website_col else ""
        email   = row[email_col - 1].strip() if len(row) >= email_col else ""
        title   = row[title_col - 1].strip() if title_col and len(row) >= title_col else ""
        if website and not email:
            rows_to_scrape.append((i, website, title))

    total = len(rows_to_scrape)
    print(f"\n{total} rows to scrape (have website, missing email)")
    if total == 0:
        print("Nothing to do.")
        return

    # --- Scrape in parallel ---
    results = {}  # row_num -> (email, score)
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(scrape_one, row_num, website, title): row_num
            for row_num, website, title in rows_to_scrape
        }
        for future in as_completed(futures):
            row_num, email, score = future.result()
            if email:
                results[row_num] = (email, score)
            done += 1
            if done % 50 == 0 or done == total:
                found = len(results)
                print(f"  Progress: {done}/{total} scraped, {found} emails found so far")

    found_count = len(results)
    print(f"\nScraping complete: {found_count}/{total} emails found ({found_count/total*100:.0f}%)")

    if not results:
        print("No emails found — nothing to write back.")
        return

    def col_letter(n):
        """Convert 1-based column index to spreadsheet letter (A, B, ... Z, AA, AB, ...)."""
        result = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    # --- Batch write back to sheet ---
    print(f"Writing {found_count} emails back to sheet...")
    batch = []
    for row_num, (email, score) in results.items():
        batch.append({"range": f"{col_letter(email_col)}{row_num}", "values": [[email]]})
        if score_col:
            batch.append({"range": f"{col_letter(score_col)}{row_num}", "values": [[round(score, 2)]]})

    sheet.spreadsheet.values_batch_update(body={
        "valueInputOption": "RAW",
        "data": batch,
    })
    print("Sheet updated.")

    # --- Airtable push ---
    if not args.no_airtable:
        print("\nPushing to Airtable...")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "lead_funnel_analytics",
                os.path.join(os.path.dirname(__file__), "lead_funnel_analytics.py"),
            )
            lfa = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(lfa)

            token_data = json.load(open("token.json"))
            result = lfa.ingest_leads_from_sheet(
                sheet_id=args.sheet_id,
                token_data=token_data,
                notify_fn=print,
            )
            print(f"Airtable: added={result.get('added', 0)}, skipped={result.get('skipped', 0)}")
        except Exception as e:
            print(f"Airtable push failed: {e}")


if __name__ == "__main__":
    main()
