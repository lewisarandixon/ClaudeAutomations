#!/usr/bin/env python3
"""
Extract contact information from business websites.

Scrapes a business website for emails, phone numbers, social media links,
owner/team info, and business hours. Uses regex + HTML parsing for
deterministic extraction, then Claude Haiku for structured owner/team data.

Used by gmaps_lead_pipeline.py as Step 2 of the lead generation pipeline.

Usage:
    python execution/extract_website_contacts.py --url "https://example.com"
    python execution/extract_website_contacts.py --url "https://example.com" --name "Example Dentist"
    python execution/extract_website_contacts.py --url "https://example.com" --no-claude
"""

import os
import re
import sys
import json
import argparse
import logging
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10
MAX_CONTACT_PAGES = 5
MAX_TEXT_FOR_CLAUDE = 3000  # chars — keeps Haiku costs ~$0.004/lead

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Pages likely to contain contact info (checked in order)
CONTACT_PAGE_PATHS = [
    "/contact", "/contact-us", "/contactus",
    "/about", "/about-us", "/aboutus",
    "/team", "/our-team", "/the-team", "/meet-the-team",
    "/staff", "/people", "/leadership",
    "/get-in-touch",
]

# Email regex — matches standard email addresses
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# False positive email patterns to filter out
EMAIL_BLACKLIST_PATTERNS = [
    r".*\.(png|jpg|jpeg|gif|svg|webp|ico|css|js|woff|ttf|eot)$",
    r".*@example\.(com|org|net)$",
    r".*@sentry\.io$",
    r".*@.*\.local$",
    r"^noreply@",
    r"^no-reply@",
    r"^mailer-daemon@",
    r"^postmaster@",
    r".*wixpress\.com$",
    r".*squarespace\.com$",
    r".*wordpress\.com$",
]
EMAIL_BLACKLIST = [re.compile(p, re.IGNORECASE) for p in EMAIL_BLACKLIST_PATTERNS]

# Obfuscated email patterns — e.g. "name [at] domain [dot] com"
OBFUSCATED_EMAIL_PATTERNS = [
    # name [at] domain [dot] com  /  name (at) domain (dot) com
    # TLD group allows compound extensions like co.uk via (?:\.[a-zA-Z]{2,})*
    re.compile(
        r"([a-zA-Z0-9._%+\-]+)\s*[\[\(]at[\]\)]\s*([a-zA-Z0-9.\-]+)\s*[\[\(]dot[\]\)]\s*([a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})*)",
        re.IGNORECASE,
    ),
    # name AT domain DOT com (no brackets, space-separated)
    re.compile(
        r"([a-zA-Z0-9._%+\-]+)\s+at\s+([a-zA-Z0-9.\-]+)\s+dot\s+([a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})*)\b",
        re.IGNORECASE,
    ),
]


def _extract_obfuscated_emails(text: str) -> list[str]:
    """Extract emails written in common obfuscated forms and return as proper addresses."""
    found = []
    seen = set()
    for pattern in OBFUSCATED_EMAIL_PATTERNS:
        for match in pattern.findall(text):
            email = f"{match[0]}@{match[1]}.{match[2]}".lower()
            if email not in seen and not any(bl.match(email) for bl in EMAIL_BLACKLIST):
                seen.add(email)
                found.append(email)
    return found


# Phone regex patterns (UK + US)
PHONE_PATTERNS = [
    # UK: +44, 0044
    re.compile(r"(?:\+44|0044)\s*\d[\d\s\-]{8,12}\d"),
    # UK: 01xxx, 02xxx, 03xxx, 07xxx
    re.compile(r"\b0[1-37]\d{2,3}[\s\-]?\d{3,4}[\s\-]?\d{3,4}\b"),
    # US/CA: +1 (xxx) xxx-xxxx
    re.compile(r"\+?1?\s*\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}\b"),
    # International: +XX XXX XXX XXXX (generic)
    re.compile(r"\+\d{1,3}\s\d[\d\s\-]{7,14}\d"),
]

# Social media domain patterns
SOCIAL_DOMAINS = {
    "facebook": ["facebook.com", "fb.com"],
    "twitter": ["twitter.com", "x.com"],
    "linkedin": ["linkedin.com"],
    "instagram": ["instagram.com"],
    "youtube": ["youtube.com"],
    "tiktok": ["tiktok.com"],
}


# ---------------------------------------------------------------------------
# HTML fetching
# ---------------------------------------------------------------------------

def _fetch_page(url: str) -> tuple[str, int]:
    """
    Fetch a page and return (html_content, status_code).
    Returns ("", status_code) on error.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True, verify=True)
        if resp.status_code == 200:
            return resp.text, 200
        return "", resp.status_code
    except requests.exceptions.SSLError:
        # Retry without SSL verification
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                                allow_redirects=True, verify=False)
            if resp.status_code == 200:
                return resp.text, 200
            return "", resp.status_code
        except Exception:
            return "", 0
    except requests.exceptions.ConnectionError:
        return "", 0
    except requests.exceptions.Timeout:
        return "", 408
    except Exception:
        return "", 0


def _find_contact_pages(base_url: str, html: str) -> list[str]:
    """
    Find URLs to contact/about/team pages by:
    1. Checking known paths on the domain
    2. Scanning page links for contact-related URLs
    """
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    found_urls = set()

    # Strategy 1: Check known paths directly
    for path in CONTACT_PAGE_PATHS:
        found_urls.add(base + path)

    # Strategy 2: Scan links in the page
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].lower().strip()
            # Check if the link text or href suggests a contact page
            link_text = (a_tag.get_text() or "").lower().strip()
            contact_keywords = ["contact", "about", "team", "staff", "people",
                                "get in touch", "reach us", "meet"]

            if any(kw in href or kw in link_text for kw in contact_keywords):
                full_url = urljoin(base_url, a_tag["href"])
                # Only follow links on the same domain
                if urlparse(full_url).netloc == parsed.netloc:
                    found_urls.add(full_url)
    except Exception:
        pass

    # Remove the base URL itself (we already fetched it)
    found_urls.discard(base_url)
    found_urls.discard(base_url.rstrip("/"))
    found_urls.discard(base + "/")

    return list(found_urls)[:MAX_CONTACT_PAGES]


def _find_sitemap_pages(base_url: str) -> list[str]:
    """
    Check /sitemap.xml (and /sitemap_index.xml) for contact/about/team URLs.
    Returns up to 5 matching URLs from the same domain.
    """
    from xml.etree import ElementTree as ET

    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    contact_keywords = ["contact", "about", "team", "staff", "people", "get-in-touch", "reach"]

    for sitemap_path in ["/sitemap.xml", "/sitemap_index.xml"]:
        html, status = _fetch_page(base + sitemap_path)
        if not html:
            continue
        try:
            root = ET.fromstring(html)
            ns = (root.tag.split("}")[0] + "}") if root.tag.startswith("{") else ""
            urls = []
            for loc in root.iter(f"{ns}loc"):
                url = (loc.text or "").strip()
                if not url:
                    continue
                url_path = urlparse(url).path.lower()
                if (urlparse(url).netloc == parsed.netloc
                        and any(kw in url_path for kw in contact_keywords)):
                    urls.append(url)
            if urls:
                return urls[:5]
        except Exception:
            continue

    return []


# ---------------------------------------------------------------------------
# Regex extraction
# ---------------------------------------------------------------------------

def _extract_emails(text: str) -> list[str]:
    """Extract and deduplicate email addresses, filtering false positives."""
    raw_emails = EMAIL_REGEX.findall(text)

    clean = []
    seen = set()
    for email in raw_emails:
        email_lower = email.lower()
        if email_lower in seen:
            continue
        # Filter blacklisted patterns
        if any(bl.match(email_lower) for bl in EMAIL_BLACKLIST):
            continue
        seen.add(email_lower)
        clean.append(email)

    return clean


# Generic catch-all email prefixes — lower value for outreach
_GENERIC_PREFIXES = {
    "info", "hello", "contact", "enquiries", "enquiry", "enquire",
    "admin", "support", "sales", "office", "mail", "help", "team",
    "reception", "accounts", "bookings", "booking", "general", "hi",
    "hey", "service", "services", "noreply", "no-reply",
}


def _score_email(email: str, website_domain: str, source_path: str = "") -> float:
    """
    Score an email 0.0–1.0 based on domain match and source page.

    Scoring tiers:
      1.0  domain-match + contact page + specific prefix
      0.9  domain-match + contact page + generic prefix
      0.85 domain-match + about/team page + specific prefix
      0.75 domain-match + about/team page + generic prefix
      0.7  domain-match + homepage + specific prefix
      0.5  domain-match + homepage + generic prefix (info@, hello@ etc.)
      0.3  no domain match (external / third-party email)
    """
    try:
        email_domain = email.split("@")[-1].lower().lstrip("www.")
        site_domain = website_domain.lower().replace("http://", "").replace("https://", "").split("/")[0].lstrip("www.")

        domain_match = (
            email_domain == site_domain
            or site_domain.endswith("." + email_domain)
            or email_domain.endswith("." + site_domain)
        )
    except Exception:
        domain_match = False

    prefix = email.split("@")[0].lower()
    is_generic = prefix in _GENERIC_PREFIXES

    contact_page = any(kw in source_path.lower() for kw in ["/contact", "/get-in-touch"])
    about_page = any(kw in source_path.lower() for kw in ["/about", "/team", "/staff", "/people", "/leadership", "/meet"])

    if not domain_match:
        return 0.3

    if contact_page:
        return 0.9 if is_generic else 1.0
    elif about_page:
        return 0.75 if is_generic else 0.85
    else:
        return 0.5 if is_generic else 0.7


def _best_email(scored_emails: list[tuple[str, float]]) -> tuple[str, float]:
    """Return the (email, score) with the highest score. Empty string if none."""
    if not scored_emails:
        return "", 0.0
    return max(scored_emails, key=lambda x: x[1])


def _extract_phones(text: str) -> list[str]:
    """Extract phone numbers using UK + US patterns."""
    found = []
    seen = set()

    for pattern in PHONE_PATTERNS:
        for match in pattern.findall(text):
            # Normalise: strip whitespace
            normalised = re.sub(r"\s+", " ", match.strip())
            if normalised not in seen and len(normalised) >= 7:
                seen.add(normalised)
                found.append(normalised)

    return found


def _extract_social_links(html: str) -> dict:
    """Extract social media URLs from <a> tags."""
    social = {platform: "" for platform in SOCIAL_DOMAINS}

    try:
        soup = BeautifulSoup(html, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].lower().strip()
            for platform, domains in SOCIAL_DOMAINS.items():
                if social[platform]:  # Already found
                    continue
                for domain in domains:
                    if domain in href:
                        # Store the original (non-lowered) URL
                        social[platform] = a_tag["href"].strip()
                        break
    except Exception:
        pass

    return social


# ---------------------------------------------------------------------------
# Claude Haiku extraction (owner/team info from unstructured text)
# ---------------------------------------------------------------------------

def _extract_with_claude(page_text: str, business_name: str = None) -> dict:
    """
    Use Claude Haiku to extract structured owner/team data from page text.
    Returns dict with owner_info, team_members, business_hours.
    Falls back to empty dict if API unavailable or fails.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {}

    # Truncate text to keep costs down
    text = page_text[:MAX_TEXT_FOR_CLAUDE]
    if len(page_text) > MAX_TEXT_FOR_CLAUDE:
        text += "\n...[truncated]"

    business_context = f' for "{business_name}"' if business_name else ""

    prompt = f"""Extract contact information{business_context} from this website text.
Return ONLY a JSON object (no markdown, no explanation) with this exact structure:

{{
  "owner_name": "Full name of owner/founder/director or empty string",
  "owner_title": "Their job title or empty string",
  "owner_email": "Their personal email if found or empty string",
  "owner_phone": "Their direct phone if found or empty string",
  "owner_linkedin": "Their LinkedIn URL if found or empty string",
  "team_members": [
    {{"name": "Full name", "title": "Job title", "email": "", "phone": "", "linkedin": ""}}
  ],
  "business_hours": "Opening hours if found or empty string"
}}

Rules:
- Only include people who are clearly named on the page (directors, founders, owners, managers)
- Do not guess or fabricate — leave empty if not found
- team_members should be empty array if no team is listed
- For business_hours, use the format shown on the page

WEBSITE TEXT:
{text}"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
        )

        result_text = ""
        for block in response.content:
            if hasattr(block, "text"):
                result_text = block.text
                break

        # Handle markdown code blocks
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]

        return json.loads(result_text.strip())

    except Exception as e:
        logger.warning(f"Claude extraction failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_website_contacts(website_url: str, business_name: str = None,
                             use_claude: bool = True) -> dict:
    """
    Scrape a business website for contact information.

    Args:
        website_url: Full URL of the business website
        business_name: Optional business name for context
        use_claude: Whether to use Claude Haiku for owner/team extraction

    Returns:
        Dict with emails, phones, social_media, owner_info, team_members, etc.
    """
    result = {
        "emails": [],
        "scored_emails": [],      # list of (email, score) tuples, sorted best first
        "best_email": "",         # highest scoring email
        "best_email_score": 0.0,  # confidence score 0.0-1.0
        "phone_numbers": [],
        "business_hours": "",
        "social_media": {p: "" for p in SOCIAL_DOMAINS},
        "owner_info": {"name": "", "title": "", "email": "", "phone": "", "linkedin": ""},
        "team_members": [],
        "additional_contacts": [],
        "_pages_scraped": 0,
        "_search_enriched": False,
    }

    if not website_url:
        result["error"] = "No website URL provided"
        return result

    # Ensure URL has scheme
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    # Derive site domain for scoring
    site_domain = urlparse(website_url).netloc.lstrip("www.")

    # --- Fetch main page ---
    main_html, status = _fetch_page(website_url)
    if not main_html:
        if status == 403:
            result["error"] = "Site blocked scraping (403)"
        elif status == 0:
            result["error"] = "Connection failed (DNS or network error)"
        elif status == 408:
            result["error"] = "Request timed out"
        else:
            result["error"] = f"HTTP {status}"
        return result

    result["_pages_scraped"] = 1
    all_html = main_html

    # Track emails per-page for accurate scoring
    scored_email_map = {}  # email -> best score seen

    def _process_page_html(html: str, page_url: str) -> str:
        """Extract text and score emails found on a specific page."""
        soup = None
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
        except Exception:
            text = html

        page_path = urlparse(page_url).path

        # 1. Emails visible in page text (existing behaviour)
        for email in _extract_emails(text):
            score = _score_email(email, site_domain, page_path)
            existing = scored_email_map.get(email.lower(), 0.0)
            if score > existing:
                scored_email_map[email.lower()] = score

        # 2. Emails in mailto: href attributes (often missed by text extraction)
        if soup:
            try:
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if href.lower().startswith("mailto:"):
                        email = href[7:].split("?")[0].strip().lower()
                        if "@" in email and not any(bl.match(email) for bl in EMAIL_BLACKLIST):
                            score = _score_email(email, site_domain, page_path)
                            existing = scored_email_map.get(email, 0.0)
                            if score > existing:
                                scored_email_map[email] = score
            except Exception:
                pass

        # 3. Obfuscated emails e.g. "name [at] domain [dot] com"
        for email in _extract_obfuscated_emails(text):
            score = _score_email(email, site_domain, page_path)
            existing = scored_email_map.get(email, 0.0)
            if score > existing:
                scored_email_map[email] = score

        return text

    all_text = _process_page_html(main_html, website_url)

    # --- Fetch contact/about/team pages (known paths + page links + sitemap) ---
    contact_urls = _find_contact_pages(website_url, main_html)

    sitemap_urls = _find_sitemap_pages(website_url)
    for url in sitemap_urls:
        if url not in contact_urls:
            contact_urls.append(url)

    for url in contact_urls:
        html, status = _fetch_page(url)
        if html:
            result["_pages_scraped"] += 1
            all_html += "\n" + html
            all_text += "\n" + _process_page_html(html, url)

    # --- Layer 2: Regex extraction + scoring ---
    scored_list = sorted(scored_email_map.items(), key=lambda x: x[1], reverse=True)
    result["scored_emails"] = scored_list
    result["emails"] = [e for e, _ in scored_list]
    if scored_list:
        result["best_email"], result["best_email_score"] = scored_list[0]

    result["phone_numbers"] = _extract_phones(all_text)
    result["social_media"] = _extract_social_links(all_html)

    # --- Layer 3: Claude extraction ---
    if use_claude and all_text:
        claude_data = _extract_with_claude(all_text, business_name)

        if claude_data:
            # Owner info
            result["owner_info"] = {
                "name": claude_data.get("owner_name", ""),
                "title": claude_data.get("owner_title", ""),
                "email": claude_data.get("owner_email", ""),
                "phone": claude_data.get("owner_phone", ""),
                "linkedin": claude_data.get("owner_linkedin", ""),
            }

            # Team members
            raw_team = claude_data.get("team_members", [])
            if isinstance(raw_team, list):
                result["team_members"] = [
                    {
                        "name": m.get("name", ""),
                        "title": m.get("title", ""),
                        "email": m.get("email", ""),
                        "phone": m.get("phone", ""),
                        "linkedin": m.get("linkedin", ""),
                    }
                    for m in raw_team
                    if isinstance(m, dict) and m.get("name")
                ]

            # Business hours
            if claude_data.get("business_hours"):
                result["business_hours"] = claude_data["business_hours"]

            # Add any owner email Claude found to the emails list
            if result["owner_info"]["email"]:
                owner_email = result["owner_info"]["email"].lower()
                if owner_email not in [e.lower() for e in result["emails"]]:
                    result["emails"].append(result["owner_info"]["email"])

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract contact information from a business website"
    )
    parser.add_argument("--url", required=True, help="Website URL to scrape")
    parser.add_argument("--name", help="Business name (optional, helps Claude)")
    parser.add_argument("--no-claude", action="store_true",
                        help="Skip Claude extraction (regex only)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    result = scrape_website_contacts(
        website_url=args.url,
        business_name=args.name,
        use_claude=not args.no_claude,
    )

    print(json.dumps(result, indent=2))

    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
