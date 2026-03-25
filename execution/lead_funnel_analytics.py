#!/usr/bin/env python3
"""
Lead Funnel Analytics — Airtable lead ingestion + funnel performance analytics.

Ingests leads from Google Sheets into Airtable (with dedupe), computes funnel
conversion rates, lead score analytics, and industry breakdowns, then delivers
weekly Telegram summaries and monthly Google Doc reports.

Usage:
    # Weekly summary (Telegram)
    python execution/lead_funnel_analytics.py --weekly

    # Monthly report (Google Doc + Telegram)
    python execution/lead_funnel_analytics.py --monthly

    # Ingest leads from a Google Sheet into Airtable
    python execution/lead_funnel_analytics.py --ingest --sheet-id "1abc..."

    # Run analytics only (print to stdout)
    python execution/lead_funnel_analytics.py --analytics
"""

import os
import sys
import json
import time
import argparse
import requests
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lead-funnel")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FUNNEL_STAGES = [
    "Messaged",
    "Responded",
    "Meeting Booked",
    "Second Meeting Booked",
    "Proposal Sent",
    "Negotiating",
    "Won (active project)",
]

# Non-funnel exit statuses
EXIT_STATUSES = ["Lost", "Not fit", "AWOL", "Closing"]

# Map each stage to its numeric position (1-indexed)
STAGE_ORDER = {stage: i + 1 for i, stage in enumerate(FUNNEL_STAGES)}

SCORE_BANDS = [
    ("Low", 0, 25),
    ("Medium-Low", 26, 50),
    ("Medium", 51, 75),
    ("Medium-High", 76, 100),
    ("High", 101, 999999),
]

# Thresholds to test for score cutoff analysis
SCORE_THRESHOLDS = [20, 30, 40, 50, 60, 70, 80, 90, 100]

MIN_SEGMENT_SIZE = 10

# Google Sheets scopes (same as tech_radar_research.py)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# ---------------------------------------------------------------------------
# Industry normalisation: Google Maps category → Airtable single-select
# ---------------------------------------------------------------------------

INDUSTRY_NORMALISATION = {
    # ---- Accountant ----
    "accountant": "Accountant",
    "accounting firm": "Accountant",
    "accounting service": "Accountant",
    "accounting office": "Accountant",
    "tax preparation service": "Accountant",
    "tax preparation": "Accountant",
    "tax consultant": "Accountant",
    "tax advisor": "Accountant",
    "tax adviser": "Accountant",
    "tax accountant": "Accountant",
    "tax service": "Accountant",
    "tax return service": "Accountant",
    "chartered accountant": "Accountant",
    "certified public accountant": "Accountant",
    "cpa firm": "Accountant",
    "bookkeeper": "Accountant",
    "bookkeeping service": "Accountant",
    "payroll service": "Accountant",
    "payroll company": "Accountant",
    "audit firm": "Accountant",
    "auditor": "Accountant",
    "forensic accountant": "Accountant",
    "management accountant": "Accountant",

    # ---- Financial Advisor ----
    "financial advisor": "Financial Advisor",
    "financial adviser": "Financial Advisor",
    "financial planner": "Financial Advisor",
    "financial consultant": "Financial Advisor",
    "financial services": "Financial Advisor",
    "financial planning service": "Financial Advisor",
    "wealth management service": "Financial Advisor",
    "wealth management": "Financial Advisor",
    "wealth advisor": "Financial Advisor",
    "investment advisor": "Financial Advisor",
    "investment adviser": "Financial Advisor",
    "investment company": "Financial Advisor",
    "investment firm": "Financial Advisor",
    "investment service": "Financial Advisor",
    "pension advisor": "Financial Advisor",
    "pension adviser": "Financial Advisor",
    "retirement planning service": "Financial Advisor",
    "portfolio manager": "Financial Advisor",
    "stockbroker": "Financial Advisor",
    "fund manager": "Financial Advisor",

    # ---- Solicitor ----
    "solicitor": "Solicitor",
    "solicitors": "Solicitor",
    "law firm": "Solicitor",
    "lawyer": "Solicitor",
    "legal firm": "Solicitor",
    "legal services": "Solicitor",
    "legal service": "Solicitor",
    "legal office": "Solicitor",
    "legal practice": "Solicitor",
    "legal aid service": "Solicitor",
    "legal aid": "Solicitor",
    "attorney": "Solicitor",
    "barrister": "Solicitor",
    "conveyancer": "Solicitor",
    "conveyancing": "Solicitor",
    "notary": "Solicitor",
    "notary public": "Solicitor",
    "law office": "Solicitor",
    "litigation": "Solicitor",
    "family lawyer": "Solicitor",
    "family law attorney": "Solicitor",
    "immigration lawyer": "Solicitor",
    "immigration attorney": "Solicitor",
    "criminal lawyer": "Solicitor",
    "criminal defense attorney": "Solicitor",
    "personal injury lawyer": "Solicitor",
    "personal injury attorney": "Solicitor",
    "employment lawyer": "Solicitor",
    "employment attorney": "Solicitor",
    "divorce lawyer": "Solicitor",
    "corporate lawyer": "Solicitor",
    "property lawyer": "Solicitor",
    "wills and probate": "Solicitor",
    "probate lawyer": "Solicitor",
    "estate planning attorney": "Solicitor",
    "commercial lawyer": "Solicitor",
    "intellectual property attorney": "Solicitor",
    "patent attorney": "Solicitor",

    # ---- Dentist ----
    "dentist": "Dentist",
    "dental clinic": "Dentist",
    "dental surgery": "Dentist",
    "dental practice": "Dentist",
    "dental office": "Dentist",
    "dental center": "Dentist",
    "dental centre": "Dentist",
    "dental care": "Dentist",
    "dental laboratory": "Dentist",
    "dental lab": "Dentist",
    "cosmetic dentist": "Dentist",
    "orthodontist": "Dentist",
    "orthodontic clinic": "Dentist",
    "dental hygienist": "Dentist",
    "endodontist": "Dentist",
    "periodontist": "Dentist",
    "prosthodontist": "Dentist",
    "pediatric dentist": "Dentist",
    "paediatric dentist": "Dentist",
    "oral surgeon": "Dentist",
    "oral surgery": "Dentist",
    "teeth whitening service": "Dentist",
    "teeth whitening": "Dentist",
    "dental implant": "Dentist",
    "dental implants provider": "Dentist",
    "family dentist": "Dentist",
    "emergency dentist": "Dentist",
    "denture care center": "Dentist",
    "denturist": "Dentist",

    # ---- Veterinarian ----
    "veterinarian": "Veterinarian",
    "veterinary care": "Veterinarian",
    "veterinary clinic": "Veterinarian",
    "veterinary hospital": "Veterinarian",
    "veterinary surgery": "Veterinarian",
    "veterinary practice": "Veterinarian",
    "veterinary service": "Veterinarian",
    "veterinary surgeon": "Veterinarian",
    "animal hospital": "Veterinarian",
    "animal clinic": "Veterinarian",
    "animal care": "Veterinarian",
    "animal doctor": "Veterinarian",
    "pet clinic": "Veterinarian",
    "pet hospital": "Veterinarian",
    "pet doctor": "Veterinarian",
    "emergency vet": "Veterinarian",
    "emergency veterinarian": "Veterinarian",
    "equine vet": "Veterinarian",
    "equine veterinarian": "Veterinarian",
    "small animal vet": "Veterinarian",
    "vet clinic": "Veterinarian",
    "vet surgery": "Veterinarian",
    "vet practice": "Veterinarian",

    # ---- Mortgage Broker ----
    "mortgage broker": "Mortgage Broker",
    "mortgage lender": "Mortgage Broker",
    "mortgage company": "Mortgage Broker",
    "mortgage advisor": "Mortgage Broker",
    "mortgage adviser": "Mortgage Broker",
    "mortgage consultant": "Mortgage Broker",
    "mortgage service": "Mortgage Broker",
    "mortgage specialist": "Mortgage Broker",
    "remortgage": "Mortgage Broker",
    "home loan company": "Mortgage Broker",
    "home loan broker": "Mortgage Broker",

    # ---- Real Estate Agency ----
    "real estate agency": "Real Estate Agency",
    "real estate agent": "Real Estate Agency",
    "real estate broker": "Real Estate Agency",
    "real estate company": "Real Estate Agency",
    "real estate office": "Real Estate Agency",
    "real estate consultant": "Real Estate Agency",
    "estate agent": "Real Estate Agency",
    "estate agency": "Real Estate Agency",
    "letting agency": "Real Estate Agency",
    "letting agent": "Real Estate Agency",
    "lettings agency": "Real Estate Agency",
    "property management company": "Real Estate Agency",
    "property management": "Real Estate Agency",
    "property agent": "Real Estate Agency",
    "property consultant": "Real Estate Agency",
    "property developer": "Real Estate Agency",
    "property sales": "Real Estate Agency",
    "property services": "Real Estate Agency",
    "property valuer": "Real Estate Agency",
    "property valuation": "Real Estate Agency",
    "commercial real estate": "Real Estate Agency",
    "commercial real estate agency": "Real Estate Agency",
    "commercial property agent": "Real Estate Agency",
    "housing association": "Real Estate Agency",
    "land agent": "Real Estate Agency",
    "surveyor": "Real Estate Agency",
    "chartered surveyor": "Real Estate Agency",
    "building surveyor": "Real Estate Agency",
    "house agent": "Real Estate Agency",
    "realtor": "Real Estate Agency",

    # ---- Hotel ----
    "hotel": "Hotel",
    "bed and breakfast": "Hotel",
    "bed & breakfast": "Hotel",
    "b&b": "Hotel",
    "guest house": "Hotel",
    "guesthouse": "Hotel",
    "hostel": "Hotel",
    "motel": "Hotel",
    "resort": "Hotel",
    "resort hotel": "Hotel",
    "boutique hotel": "Hotel",
    "budget hotel": "Hotel",
    "luxury hotel": "Hotel",
    "country house hotel": "Hotel",
    "apart hotel": "Hotel",
    "aparthotel": "Hotel",
    "apartment hotel": "Hotel",
    "serviced apartment": "Hotel",
    "serviced apartments": "Hotel",
    "holiday cottage": "Hotel",
    "holiday home": "Hotel",
    "holiday let": "Hotel",
    "holiday park": "Hotel",
    "holiday accommodation": "Hotel",
    "self catering": "Hotel",
    "self-catering accommodation": "Hotel",
    "lodge": "Hotel",
    "country inn": "Hotel",
    "coaching inn": "Hotel",
    "camping": "Hotel",
    "campsite": "Hotel",
    "camp site": "Hotel",
    "glamping": "Hotel",
    "caravan park": "Hotel",
    "caravan site": "Hotel",
    "extended stay hotel": "Hotel",

    # ---- Kennel ----
    "kennel": "Kennel",
    "kennels": "Kennel",
    "dog boarding kennel": "Kennel",
    "dog boarding": "Kennel",
    "dog kennels": "Kennel",
    "pet boarding service": "Kennel",
    "pet boarding": "Kennel",
    "pet hotel": "Kennel",
    "dog daycare": "Kennel",
    "doggy daycare": "Kennel",
    "dog day care": "Kennel",
    "cattery": "Kennel",
    "cat boarding": "Kennel",
    "pet sitting": "Kennel",
    "pet sitter": "Kennel",
    "pet sitting service": "Kennel",
    "animal boarding": "Kennel",
    "animal boarding facility": "Kennel",
    "pet minding": "Kennel",
    "dog walker": "Kennel",
    "dog walking service": "Kennel",
    "pet care service": "Kennel",

    # ---- Insurance Agency ----
    "insurance agency": "Insurance Agency",
    "insurance broker": "Insurance Agency",
    "insurance company": "Insurance Agency",
    "insurance agent": "Insurance Agency",
    "insurance consultant": "Insurance Agency",
    "insurance adviser": "Insurance Agency",
    "insurance advisor": "Insurance Agency",
    "insurance services": "Insurance Agency",
    "life insurance agency": "Insurance Agency",
    "health insurance agency": "Insurance Agency",
    "car insurance agency": "Insurance Agency",
    "auto insurance agency": "Insurance Agency",
    "general insurance agency": "Insurance Agency",
    "commercial insurance": "Insurance Agency",
    "business insurance": "Insurance Agency",
    "home insurance": "Insurance Agency",
    "pet insurance": "Insurance Agency",
    "insurance underwriter": "Insurance Agency",

    # ---- Recruitment Agency ----
    "recruitment agency": "Recruitment Agency",
    "recruitment company": "Recruitment Agency",
    "recruitment firm": "Recruitment Agency",
    "recruitment consultant": "Recruitment Agency",
    "recruitment service": "Recruitment Agency",
    "employment agency": "Recruitment Agency",
    "staffing agency": "Recruitment Agency",
    "staffing company": "Recruitment Agency",
    "temp agency": "Recruitment Agency",
    "temporary employment agency": "Recruitment Agency",
    "headhunter": "Recruitment Agency",
    "executive search": "Recruitment Agency",
    "executive search firm": "Recruitment Agency",
    "talent agency": "Recruitment Agency",
    "personnel agency": "Recruitment Agency",
    "job agency": "Recruitment Agency",
    "job placement agency": "Recruitment Agency",
    "career agency": "Recruitment Agency",
    "hiring agency": "Recruitment Agency",

    # ---- Consultant ----
    "consultant": "Consultant",
    "consulting firm": "Consultant",
    "consulting company": "Consultant",
    "consultancy": "Consultant",
    "management consultant": "Consultant",
    "management consultancy": "Consultant",
    "business consultant": "Consultant",
    "business consultancy": "Consultant",
    "business advisory service": "Consultant",
    "strategy consultant": "Consultant",
    "strategy consulting": "Consultant",
    "it consultant": "Consultant",
    "it consulting": "Consultant",
    "technology consultant": "Consultant",
    "professional services": "Consultant",
    "advisory firm": "Consultant",
    "advisory service": "Consultant",
    "environmental consultant": "Consultant",
    "health and safety consultant": "Consultant",
    "hr consultant": "Consultant",
    "human resources consultant": "Consultant",

    # ---- Architect ----
    "architect": "Architect",
    "architectural firm": "Architect",
    "architecture firm": "Architect",
    "architectural practice": "Architect",
    "architectural studio": "Architect",
    "architectural services": "Architect",
    "architectural designer": "Architect",
    "architecture studio": "Architect",
    "building designer": "Architect",
    "design studio": "Architect",
    "landscape architect": "Architect",
    "interior architect": "Architect",

    # ---- Optician ----
    "optician": "Optician",
    "opticians": "Optician",
    "optometrist": "Optician",
    "eye care center": "Optician",
    "eye care centre": "Optician",
    "eye care clinic": "Optician",
    "optical store": "Optician",
    "optical shop": "Optician",
    "eyewear store": "Optician",
    "glasses shop": "Optician",
    "spectacle shop": "Optician",
    "contact lens supplier": "Optician",
    "eye clinic": "Optician",
    "eye test": "Optician",
    "vision care": "Optician",
    "ophthalmologist": "Optician",
    "laser eye surgery": "Optician",
    "laser eye clinic": "Optician",
    "eye surgery center": "Optician",

    # ---- Physiotherapist ----
    "physiotherapist": "Physiotherapist",
    "physiotherapy": "Physiotherapist",
    "physiotherapy clinic": "Physiotherapist",
    "physiotherapy practice": "Physiotherapist",
    "physical therapist": "Physiotherapist",
    "physical therapy clinic": "Physiotherapist",
    "physical therapy": "Physiotherapist",
    "sports therapist": "Physiotherapist",
    "sports therapy": "Physiotherapist",
    "sports physiotherapist": "Physiotherapist",
    "sports physio": "Physiotherapist",
    "osteopath": "Physiotherapist",
    "osteopathy": "Physiotherapist",
    "osteopathic clinic": "Physiotherapist",
    "rehabilitation centre": "Physiotherapist",
    "rehabilitation center": "Physiotherapist",
    "occupational therapist": "Physiotherapist",
    "occupational therapy": "Physiotherapist",
    "musculoskeletal therapist": "Physiotherapist",
    "myotherapist": "Physiotherapist",

    # ---- Chiropractor ----
    "chiropractor": "Chiropractor",
    "chiropractic clinic": "Chiropractor",
    "chiropractic centre": "Chiropractor",
    "chiropractic center": "Chiropractor",
    "chiropractic office": "Chiropractor",
    "chiropractic practice": "Chiropractor",
    "chiropractic": "Chiropractor",
    "spinal care": "Chiropractor",
    "spine clinic": "Chiropractor",

    # ---- Electrician ----
    "electrician": "Electrician",
    "electrical contractor": "Electrician",
    "electrical installation service": "Electrician",
    "electrical company": "Electrician",
    "electrical services": "Electrician",
    "electrical engineer": "Electrician",
    "electrical repair service": "Electrician",
    "electrical maintenance": "Electrician",
    "electrical supply store": "Electrician",
    "electrical wholesaler": "Electrician",
    "rewiring service": "Electrician",
    "emergency electrician": "Electrician",

    # ---- Plumber ----
    "plumber": "Plumber",
    "plumbing service": "Plumber",
    "plumbing contractor": "Plumber",
    "plumbing company": "Plumber",
    "plumbing supply store": "Plumber",
    "plumbing repair service": "Plumber",
    "heating engineer": "Plumber",
    "heating contractor": "Plumber",
    "heating company": "Plumber",
    "gas engineer": "Plumber",
    "gas fitter": "Plumber",
    "gas installer": "Plumber",
    "boiler repair service": "Plumber",
    "boiler installation": "Plumber",
    "boiler service": "Plumber",
    "central heating installer": "Plumber",
    "central heating service": "Plumber",
    "drain cleaning service": "Plumber",
    "drainage contractor": "Plumber",
    "drainage service": "Plumber",
    "bathroom fitter": "Plumber",
    "bathroom installation": "Plumber",
    "bathroom installer": "Plumber",
    "emergency plumber": "Plumber",

    # ---- Construction Company ----
    "construction company": "Construction Company",
    "construction contractor": "Construction Company",
    "construction firm": "Construction Company",
    "general contractor": "Construction Company",
    "building firm": "Construction Company",
    "building company": "Construction Company",
    "building contractor": "Construction Company",
    "builder": "Construction Company",
    "builders": "Construction Company",
    "roofing contractor": "Construction Company",
    "roofing company": "Construction Company",
    "roofer": "Construction Company",
    "house builder": "Construction Company",
    "home builder": "Construction Company",
    "home improvement": "Construction Company",
    "home improvement store": "Construction Company",
    "renovation contractor": "Construction Company",
    "renovation company": "Construction Company",
    "refurbishment company": "Construction Company",
    "demolition contractor": "Construction Company",
    "scaffolding company": "Construction Company",
    "scaffolding contractor": "Construction Company",
    "civil engineering company": "Construction Company",
    "civil engineer": "Construction Company",
    "groundwork contractor": "Construction Company",
    "paving contractor": "Construction Company",
    "paving company": "Construction Company",
    "fencing contractor": "Construction Company",
    "fencing company": "Construction Company",
    "bricklayer": "Construction Company",
    "bricklaying": "Construction Company",
    "carpenter": "Construction Company",
    "carpentry service": "Construction Company",
    "joiner": "Construction Company",
    "joinery": "Construction Company",
    "plasterer": "Construction Company",
    "plastering contractor": "Construction Company",
    "tiler": "Construction Company",
    "tiling service": "Construction Company",
    "painter": "Construction Company",
    "painter and decorator": "Construction Company",
    "painting contractor": "Construction Company",
    "decorator": "Construction Company",
    "decorating service": "Construction Company",
    "window installer": "Construction Company",
    "window fitter": "Construction Company",
    "window company": "Construction Company",
    "door installer": "Construction Company",
    "glazier": "Construction Company",
    "glazing company": "Construction Company",
    "conservatory installer": "Construction Company",
    "extension builder": "Construction Company",
    "loft conversion specialist": "Construction Company",
    "loft conversion company": "Construction Company",
    "kitchen fitter": "Construction Company",
    "kitchen installer": "Construction Company",
    "flooring contractor": "Construction Company",
    "flooring company": "Construction Company",
    "flooring store": "Construction Company",
    "insulation contractor": "Construction Company",
    "damp proofing company": "Construction Company",
    "structural engineer": "Construction Company",
    "building materials supplier": "Construction Company",
    "timber merchant": "Construction Company",
    "handyman": "Construction Company",
    "handyman service": "Construction Company",
    "locksmith": "Construction Company",

    # ---- Restaurant ----
    "restaurant": "Restaurant",
    "cafe": "Restaurant",
    "coffee shop": "Restaurant",
    "coffee house": "Restaurant",
    "tea room": "Restaurant",
    "tea house": "Restaurant",
    "bistro": "Restaurant",
    "brasserie": "Restaurant",
    "diner": "Restaurant",
    "fast food restaurant": "Restaurant",
    "takeaway": "Restaurant",
    "takeaway restaurant": "Restaurant",
    "take away": "Restaurant",
    "fish and chips": "Restaurant",
    "fish and chip shop": "Restaurant",
    "chip shop": "Restaurant",
    "pizzeria": "Restaurant",
    "pizza restaurant": "Restaurant",
    "pizza delivery": "Restaurant",
    "sushi restaurant": "Restaurant",
    "sushi bar": "Restaurant",
    "chinese restaurant": "Restaurant",
    "indian restaurant": "Restaurant",
    "italian restaurant": "Restaurant",
    "thai restaurant": "Restaurant",
    "mexican restaurant": "Restaurant",
    "japanese restaurant": "Restaurant",
    "korean restaurant": "Restaurant",
    "turkish restaurant": "Restaurant",
    "lebanese restaurant": "Restaurant",
    "greek restaurant": "Restaurant",
    "vietnamese restaurant": "Restaurant",
    "french restaurant": "Restaurant",
    "american restaurant": "Restaurant",
    "african restaurant": "Restaurant",
    "caribbean restaurant": "Restaurant",
    "middle eastern restaurant": "Restaurant",
    "mediterranean restaurant": "Restaurant",
    "asian restaurant": "Restaurant",
    "seafood restaurant": "Restaurant",
    "steakhouse": "Restaurant",
    "grill restaurant": "Restaurant",
    "barbecue restaurant": "Restaurant",
    "burger restaurant": "Restaurant",
    "noodle restaurant": "Restaurant",
    "noodle bar": "Restaurant",
    "curry house": "Restaurant",
    "kebab shop": "Restaurant",
    "sandwich shop": "Restaurant",
    "delicatessen": "Restaurant",
    "deli": "Restaurant",
    "patisserie": "Restaurant",
    "bakery": "Restaurant",
    "ice cream shop": "Restaurant",
    "dessert shop": "Restaurant",
    "dessert restaurant": "Restaurant",
    "juice bar": "Restaurant",
    "smoothie bar": "Restaurant",
    "vegan restaurant": "Restaurant",
    "vegetarian restaurant": "Restaurant",
    "brunch restaurant": "Restaurant",
    "breakfast restaurant": "Restaurant",
    "food truck": "Restaurant",
    "catering service": "Restaurant",
    "caterer": "Restaurant",
    "buffet restaurant": "Restaurant",
    "canteen": "Restaurant",
    "gastropub": "Restaurant",
    "public house": "Restaurant",
    "wine bar": "Restaurant",
    "cocktail bar": "Restaurant",
    "sports bar": "Restaurant",
    "tapas bar": "Restaurant",
    "lounge bar": "Restaurant",
    "karaoke bar": "Restaurant",
    "shisha bar": "Restaurant",

    # ---- Spa / Beauty ----
    # NOTE: bare "spa" removed to avoid false positives (e.g. "space").
    # "spa" as a raw input is caught by substring matching against "day spa" etc.
    "spa resort": "Spa",
    "spa hotel": "Spa",
    "barber": "Spa",
    "barber shop": "Spa",
    "barbershop": "Spa",
    "beauty salon": "Spa",
    "beauty parlour": "Spa",
    "beauty parlor": "Spa",
    "beauty therapist": "Spa",
    "beauty treatment": "Spa",
    "beauty clinic": "Spa",
    "beauty center": "Spa",
    "beauty centre": "Spa",
    "hair salon": "Spa",
    "hairdresser": "Spa",
    "hairdressers": "Spa",
    "hair stylist": "Spa",
    "hair dresser": "Spa",
    "unisex hairdresser": "Spa",
    "nail salon": "Spa",
    "nail bar": "Spa",
    "nail technician": "Spa",
    "day spa": "Spa",
    "health spa": "Spa",
    "medical spa": "Spa",
    "massage therapist": "Spa",
    "massage therapy": "Spa",
    "massage parlour": "Spa",
    "massage center": "Spa",
    "massage centre": "Spa",
    "tanning salon": "Spa",
    "tanning studio": "Spa",
    "waxing salon": "Spa",
    "waxing service": "Spa",
    "eyelash salon": "Spa",
    "lash bar": "Spa",
    "lash technician": "Spa",
    "eyelash extension": "Spa",
    "makeup artist": "Spa",
    "make up artist": "Spa",
    "aesthetic clinic": "Spa",
    "aesthetics clinic": "Spa",
    "cosmetic clinic": "Spa",
    "skin care clinic": "Spa",
    "skincare clinic": "Spa",
    "facial spa": "Spa",
    "laser hair removal": "Spa",
    "laser clinic": "Spa",
    "dermatologist": "Spa",
    "wellness centre": "Spa",
    "wellness center": "Spa",
    "wellness spa": "Spa",
    "tattoo shop": "Spa",
    "tattoo parlour": "Spa",
    "tattoo studio": "Spa",
    "piercing studio": "Spa",
    "body piercing": "Spa",
    "threading salon": "Spa",
    "brow bar": "Spa",
    "eyebrow bar": "Spa",
    "botox clinic": "Spa",
    "medspa": "Spa",

    # ---- Travel Agency ----
    "travel agency": "Travel Agency",
    "travel agent": "Travel Agency",
    "travel agents": "Travel Agency",
    "tour operator": "Travel Agency",
    "travel company": "Travel Agency",
    "travel service": "Travel Agency",
    "travel consultant": "Travel Agency",
    "travel shop": "Travel Agency",
    "holiday company": "Travel Agency",
    "holiday agent": "Travel Agency",
    "cruise agent": "Travel Agency",
    "cruise line": "Travel Agency",
    "tour guide": "Travel Agency",
    "tour company": "Travel Agency",
    "tourist agency": "Travel Agency",
    "visa service": "Travel Agency",
    "visa agency": "Travel Agency",

    # ---- Event Venue ----
    "event venue": "Event Venue",
    "events venue": "Event Venue",
    "event space": "Event Venue",
    "event hall": "Event Venue",
    "wedding venue": "Event Venue",
    "wedding planner": "Event Venue",
    "conference centre": "Event Venue",
    "conference center": "Event Venue",
    "conference venue": "Event Venue",
    "banquet hall": "Event Venue",
    "function room": "Event Venue",
    "function venue": "Event Venue",
    "reception venue": "Event Venue",
    "party venue": "Event Venue",
    "entertainment venue": "Event Venue",
    "concert hall": "Event Venue",
    "concert venue": "Event Venue",
    "music venue": "Event Venue",
    "exhibition centre": "Event Venue",
    "exhibition center": "Event Venue",
    "community hall": "Event Venue",
    "community centre": "Event Venue",
    "community center": "Event Venue",
    "village hall": "Event Venue",
    "town hall": "Event Venue",
    "ballroom": "Event Venue",
    "nightclub": "Event Venue",
    "night club": "Event Venue",
    "theatre": "Event Venue",
    "theater": "Event Venue",
    "performing arts theater": "Event Venue",
    "cinema": "Event Venue",
    "stadium": "Event Venue",
    "arena": "Event Venue",
    "bowling alley": "Event Venue",
    "amusement park": "Event Venue",
    "theme park": "Event Venue",
    "event planner": "Event Venue",
    "event management company": "Event Venue",

    # ---- Car Dealership ----
    "car dealership": "Car Dealership",
    "car dealer": "Car Dealership",
    "car dealers": "Car Dealership",
    "car sales": "Car Dealership",
    "car showroom": "Car Dealership",
    "car supermarket": "Car Dealership",
    "used car dealer": "Car Dealership",
    "used car dealership": "Car Dealership",
    "new car dealer": "Car Dealership",
    "auto dealer": "Car Dealership",
    "auto dealership": "Car Dealership",
    "automobile dealer": "Car Dealership",
    "vehicle dealer": "Car Dealership",
    "vehicle sales": "Car Dealership",
    "motor dealer": "Car Dealership",
    "motor trade": "Car Dealership",
    "motorcycle dealer": "Car Dealership",
    "motorcycle dealership": "Car Dealership",
    "van dealer": "Car Dealership",
    "commercial vehicle dealer": "Car Dealership",
    "truck dealer": "Car Dealership",
    "caravan dealer": "Car Dealership",

    # ---- Driving School ----
    "driving school": "Driving School",
    "driving instructor": "Driving School",
    "driving lesson": "Driving School",
    "driving lessons": "Driving School",
    "driver training": "Driving School",
    "learner driver": "Driving School",
    "driving test centre": "Driving School",
    "driving test center": "Driving School",
    "motorcycle training": "Driving School",
    "advanced driving": "Driving School",
    "pass plus": "Driving School",
    "driving centre": "Driving School",
    "driving academy": "Driving School",

    # ---- Mechanic ----
    "mechanic": "Mechanic",
    "auto repair shop": "Mechanic",
    "auto repair": "Mechanic",
    "auto body shop": "Mechanic",
    "auto mechanic": "Mechanic",
    "auto electrician": "Mechanic",
    "auto service": "Mechanic",
    "car repair": "Mechanic",
    "car repair service": "Mechanic",
    "car mechanic": "Mechanic",
    "car servicing": "Mechanic",
    "car service center": "Mechanic",
    "car service centre": "Mechanic",
    "car maintenance": "Mechanic",
    "car wash": "Mechanic",
    "vehicle repair": "Mechanic",
    "vehicle servicing": "Mechanic",
    "vehicle inspection": "Mechanic",
    "motor mechanic": "Mechanic",
    "garage": "Mechanic",
    "mot testing station": "Mechanic",
    "mot test centre": "Mechanic",
    "mot centre": "Mechanic",
    "tyre shop": "Mechanic",
    "tyre fitting": "Mechanic",
    "tyre centre": "Mechanic",
    "tire shop": "Mechanic",
    "tire dealer": "Mechanic",
    "exhaust centre": "Mechanic",
    "exhaust service": "Mechanic",
    "brake specialist": "Mechanic",
    "brake service": "Mechanic",
    "clutch specialist": "Mechanic",
    "gearbox specialist": "Mechanic",
    "transmission repair": "Mechanic",
    "engine repair": "Mechanic",
    "windscreen repair": "Mechanic",
    "windshield repair": "Mechanic",
    "wheel alignment service": "Mechanic",
    "body shop": "Mechanic",
    "panel beater": "Mechanic",
    "auto glass shop": "Mechanic",
    "oil change service": "Mechanic",
    "smog inspection station": "Mechanic",
    "car detailing service": "Mechanic",
    "auto parts store": "Mechanic",

    # ---- Dog Groomer ----
    "dog groomer": "Dog Groomer",
    "dog grooming": "Dog Groomer",
    "dog grooming service": "Dog Groomer",
    "pet groomer": "Dog Groomer",
    "pet grooming service": "Dog Groomer",
    "pet grooming": "Dog Groomer",
    "mobile dog groomer": "Dog Groomer",
    "mobile groomer": "Dog Groomer",
    "mobile pet groomer": "Dog Groomer",
    "grooming salon": "Dog Groomer",
    "grooming parlour": "Dog Groomer",
    "grooming parlor": "Dog Groomer",
    "dog wash": "Dog Groomer",
    "pet spa": "Dog Groomer",
    "pet styling": "Dog Groomer",
    "canine groomer": "Dog Groomer",
    "cat groomer": "Dog Groomer",
    "cat grooming": "Dog Groomer",

    # ---- Marketing Agency ----
    "marketing agency": "Marketing Agency",
    "marketing company": "Marketing Agency",
    "marketing firm": "Marketing Agency",
    "marketing consultant": "Marketing Agency",
    "marketing service": "Marketing Agency",
    "advertising agency": "Marketing Agency",
    "advertising company": "Marketing Agency",
    "digital marketing agency": "Marketing Agency",
    "digital marketing company": "Marketing Agency",
    "digital marketing": "Marketing Agency",
    "digital agency": "Marketing Agency",
    "seo company": "Marketing Agency",
    "seo agency": "Marketing Agency",
    "seo consultant": "Marketing Agency",
    "seo service": "Marketing Agency",
    "web design agency": "Marketing Agency",
    "web design company": "Marketing Agency",
    "web designer": "Marketing Agency",
    "web developer": "Marketing Agency",
    "web development company": "Marketing Agency",
    "website designer": "Marketing Agency",
    "it services": "Marketing Agency",
    "it service provider": "Marketing Agency",
    "it company": "Marketing Agency",
    "it support": "Marketing Agency",
    "software company": "Marketing Agency",
    "software development company": "Marketing Agency",
    "app developer": "Marketing Agency",
    "app development company": "Marketing Agency",
    "pr agency": "Marketing Agency",
    "public relations firm": "Marketing Agency",
    "public relations agency": "Marketing Agency",
    "branding agency": "Marketing Agency",
    "brand consultant": "Marketing Agency",
    "social media agency": "Marketing Agency",
    "social media marketing": "Marketing Agency",
    "social media consultant": "Marketing Agency",
    "content agency": "Marketing Agency",
    "content marketing agency": "Marketing Agency",
    "creative agency": "Marketing Agency",
    "design agency": "Marketing Agency",
    "graphic design": "Marketing Agency",
    "graphic designer": "Marketing Agency",
    "media agency": "Marketing Agency",
    "communications agency": "Marketing Agency",
    "video production company": "Marketing Agency",
    "video production": "Marketing Agency",
    "photography studio": "Marketing Agency",
    "photographer": "Marketing Agency",
    "printing service": "Marketing Agency",
    "print shop": "Marketing Agency",
    "sign maker": "Marketing Agency",
    "signage company": "Marketing Agency",
    "copywriter": "Marketing Agency",
    "telemarketing service": "Marketing Agency",
}

# All valid Airtable Industry options (for substring matching fallback)
VALID_INDUSTRIES = set(INDUSTRY_NORMALISATION.values()) | {"Other"}


def normalise_industry(raw_category: str) -> str:
    """
    Normalise a Google Maps category to an Airtable Industry option.
    1. Exact match in normalisation map
    2. Substring match (map key in raw or raw in map key)
    3. Fallback to "Other"
    """
    if not raw_category:
        return "Other"

    raw_lower = raw_category.strip().lower()

    # Exact match
    if raw_lower in INDUSTRY_NORMALISATION:
        return INDUSTRY_NORMALISATION[raw_lower]

    # Substring match: check if any key is contained in raw or vice versa
    for key, value in INDUSTRY_NORMALISATION.items():
        if key in raw_lower or raw_lower in key:
            return value

    return "Other"


# ---------------------------------------------------------------------------
# Email ranking (pick best contact email from scraped list)
# ---------------------------------------------------------------------------

# Prefix tiers for email ranking (higher = better for outreach)
_EMAIL_TIER_1 = {
    "hello", "info", "contact", "enquiries", "enquiry", "office", "admin",
    "help", "support", "enquire",
}
_EMAIL_TIER_2 = {
    "reception", "team", "sales", "general", "mail", "bookings",
    "appointments", "accounts", "billing",
    # Industry-specific (common UK SMB patterns)
    "practice", "surgery", "clinic", "studio", "reservations",
    "service", "services", "director", "manager",
    # Other functional
    "welcome", "frontdesk", "front-desk", "pa",
    "lettings", "orders", "hire",
}
_EMAIL_SKIP = {
    "noreply", "no-reply", "no.reply", "donotreply", "jobs", "careers",
    "press", "media", "marketing", "feedback", "abuse", "postmaster",
    "mailer-daemon", "webmaster", "root",
    # Automated / system
    "newsletter", "alerts", "notifications", "unsubscribe", "bounces",
    # Internal / compliance
    "hr", "recruitment", "hiring", "complaints",
    "gdpr", "privacy", "dpo",
    # Other junk
    "test", "dev", "staging", "demo", "spam", "security",
}


def pick_best_email(emails_str: str, owner_email: str = "",
                    website_url: str = "") -> str:
    """
    Pick the best outreach email from a comma-separated list.

    Ranking (domain-matching business email is king):
      1. Domain-matching + tier-1 prefix (info@company.com, hello@)
      2. Domain-matching + tier-2 prefix (sales@company.com, reception@)
      3. Domain-matching + other (any @company.com email)
      4. Non-matching + tier-1 prefix
      5. Non-matching + other
      Skip: noreply@, jobs@, careers@, press@ etc.

    Owner email gets a small tiebreaker bonus, not dominant.
    Domain matching = email domain matches website domain.
    """
    # Parse all candidate emails
    candidates = []
    if emails_str:
        candidates = [e.strip().lower() for e in emails_str.split(",") if e.strip()]
    if owner_email and owner_email.strip():
        oe = owner_email.strip().lower()
        if oe not in candidates:
            candidates.insert(0, oe)

    if not candidates:
        return ""

    # Extract website domain for matching
    site_domain = ""
    if website_url:
        url_lower = website_url.lower().replace("https://", "").replace("http://", "")
        url_lower = url_lower.split("/")[0]  # strip path
        url_lower = url_lower.lstrip("www.")  # strip www.
        site_domain = url_lower

    def score_email(email: str) -> int:
        """Score an email — higher is better."""
        if "@" not in email:
            return -100

        prefix, domain = email.rsplit("@", 1)

        # Skip junk prefixes
        if prefix in _EMAIL_SKIP:
            return -50

        score = 0

        # Owner email tiebreaker (small bonus, doesn't override domain match)
        if owner_email and email == owner_email.strip().lower():
            score += 3

        # Domain match bonus (email domain matches website)
        email_domain = domain.lstrip("www.")
        if site_domain and email_domain == site_domain:
            score += 20

        # Prefix tier bonus
        if prefix in _EMAIL_TIER_1:
            score += 10
        elif prefix in _EMAIL_TIER_2:
            score += 5
        else:
            score += 1  # any other real email

        return score

    # Score and sort
    scored = [(score_email(e), e) for e in candidates]
    scored.sort(key=lambda x: x[0], reverse=True)

    # Return highest-scoring email (if it's not junk)
    best_score, best_email = scored[0]
    return best_email if best_score > -50 else ""


# ---------------------------------------------------------------------------
# Google auth helpers (mirrors tech_radar_research.py)
# ---------------------------------------------------------------------------

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


def create_google_doc(token_data: dict, title: str, markdown_content: str) -> str:
    """Create a Google Doc with report content. Returns the doc URL."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
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
        elif os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        else:
            raise RuntimeError("No Google credentials found")

        if creds.expired:
            creds.refresh(Request())

    docs_service = build("docs", "v1", credentials=creds)
    doc = docs_service.documents().create(body={"title": title}).execute()
    doc_id = doc["documentId"]
    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"

    requests_body = [
        {
            "insertText": {
                "location": {"index": 1},
                "text": markdown_content,
            }
        }
    ]
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests_body},
    ).execute()

    logger.info(f"Created Google Doc: {doc_url}")
    return doc_url


# ---------------------------------------------------------------------------
# Telegram (same pattern as tech_radar_research.py)
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
# Airtable API helpers
# ---------------------------------------------------------------------------

def get_airtable_records(api_key: str, base_id: str, table_id: str,
                         view: str = None, fields: list = None) -> list:
    """
    Fetch all records from an Airtable table (paginated).
    Returns a list of dicts with 'id' and 'fields' keys.
    """
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {}
    if view:
        params["view"] = view
    if fields:
        for f in fields:
            params.setdefault("fields[]", [])
        # Use list format for fields
        params = {}
        if view:
            params["view"] = view

    all_records = []
    offset = None

    while True:
        req_params = dict(params)
        if offset:
            req_params["offset"] = offset
        if fields:
            req_params["fields[]"] = fields

        resp = requests.get(url, headers=headers, params=req_params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("records", [])
        all_records.extend(records)
        logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")

        offset = data.get("offset")
        if not offset:
            break

        time.sleep(0.2)  # Rate limit: 5 req/s

    return all_records


def create_airtable_records(api_key: str, base_id: str, table_id: str,
                            records: list) -> list:
    """
    Create records in Airtable in batches of 10.
    Each record should be a dict of field name → value.
    Returns list of created record IDs.
    """
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    created_ids = []
    # Batch in groups of 10 (Airtable limit)
    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        payload = {
            "records": [{"fields": rec} for rec in batch],
        }

        # Auto-strip unknown fields and retry (handles multiple bad fields)
        bad_fields = set()
        for _ in range(10):  # Max 10 unknown fields before giving up
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.ok:
                break
            err_body = resp.json() if resp.content else {}
            err_type = err_body.get("error", {}).get("type", "")
            err_msg = err_body.get("error", {}).get("message", "")
            if err_type == "UNKNOWN_FIELD_NAME" and '"' in err_msg:
                bad_field = err_msg.split('"')[1]
                bad_fields.add(bad_field)
                logger.warning(f"Airtable unknown field '{bad_field}' — stripping and retrying")
                payload["records"] = [
                    {"fields": {k: v for k, v in rec["fields"].items() if k not in bad_fields}}
                    for rec in payload["records"]
                ]
            else:
                raise Exception(f"Airtable {resp.status_code}: {resp.text}")
        else:
            raise Exception(f"Airtable still failing after stripping {bad_fields}")
        if bad_fields:
            logger.warning(f"Stripped unknown Airtable fields: {bad_fields}")
        data = resp.json()

        for rec in data.get("records", []):
            created_ids.append(rec["id"])

        logger.info(f"Created batch {i // 10 + 1}: {len(batch)} records")
        time.sleep(0.2)  # Rate limit

    return created_ids


# ---------------------------------------------------------------------------
# Lead ingestion: Google Sheets → Airtable
# ---------------------------------------------------------------------------

def ingest_leads_from_sheet(sheet_id: str, token_data: dict = None,
                            notify_fn=None) -> dict:
    """
    Read leads from a Google Sheet (gmaps_lead_pipeline output format)
    and sync new ones into Airtable with deduplication.

    Returns dict with counts of processed/added/skipped leads.
    """
    from dotenv import load_dotenv
    load_dotenv()

    notify = notify_fn or (lambda msg: logger.info(msg))

    airtable_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    leads_table_id = os.getenv("AIRTABLE_LEADS_ID")

    if not all([airtable_key, base_id, leads_table_id]):
        raise ValueError("AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_LEADS_ID must be set")

    # Step 1: Read leads from Google Sheet
    gc = get_gspread_client(token_data)
    sheet = gc.open_by_key(sheet_id).sheet1
    sheet_data = sheet.get_all_records()
    notify(f"*Lead Ingestion* Read {len(sheet_data)} rows from Google Sheet")

    if not sheet_data:
        return {"processed": 0, "added": 0, "skipped": 0, "errors": 0}

    # Step 2: Fetch existing Airtable leads for dedupe
    existing_records = get_airtable_records(airtable_key, base_id, leads_table_id)
    notify(f"*Lead Ingestion* Found {len(existing_records)} existing Airtable records")

    # Build dedupe sets
    existing_emails = set()
    existing_companies = set()
    for rec in existing_records:
        fields = rec.get("fields", {})
        email = (fields.get("Contact Email") or "").strip().lower()
        if email:
            existing_emails.add(email)
        company = (fields.get("Company / Business Name") or "").strip().lower()
        city = (fields.get("City/County") or "").strip().lower()
        state = (fields.get("Country/State") or "").strip().lower()
        if company:
            existing_companies.add((company, f"{city}, {state}".strip(", ")))

    # Step 3: Map and dedupe
    new_records = []
    skipped = 0
    errors = 0
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for row in sheet_data:
        try:
            # Pick best email — 'emails' (legacy) or 'email' (Google Maps pipeline)
            raw_emails = str(row.get("emails", "") or row.get("email", "") or "")
            owner_email_col = str(row.get("owner_email", "") or "")
            website_col = str(row.get("website", "") or "")
            email = pick_best_email(raw_emails, owner_email_col, website_col)

            # Only ingest leads that have an email
            if not email:
                skipped += 1
                continue

            # 'business_name' (legacy) or 'title' (Google Maps pipeline)
            company = str(row.get("business_name", "") or row.get("title", "") or "").strip()
            city = str(row.get("city", "") or "").strip()
            state = str(row.get("state", "") or "").strip()
            location = f"{city}, {state}".strip(", ").lower()

            # Dedupe check
            if email in existing_emails:
                skipped += 1
                continue
            if company and (company.lower(), location) in existing_companies:
                skipped += 1
                continue

            # 'category' (legacy) or 'categoryName' (Google Maps pipeline)
            # raw_category goes into Original Category, normalised value goes into Industry
            raw_category = str(row.get("category", "") or row.get("categoryName", "") or "")
            industry = normalise_industry(raw_category)

            # Format all social media links into one field
            # 'linkedin_url' is the Google Maps pipeline name; 'linkedin' is legacy
            social_fields = [
                ("linkedin_url", "LinkedIn"),
                ("linkedin", "LinkedIn"),
                ("facebook", "Facebook"),
                ("instagram", "Instagram"),
                ("twitter", "Twitter/X"),
                ("youtube", "YouTube"),
                ("tiktok", "TikTok"),
            ]
            social_lines = []
            seen_labels = set()
            for social_field, label in social_fields:
                if label in seen_labels:
                    continue
                val = str(row.get(social_field, "") or "").strip()
                if val and val.startswith("http"):
                    social_lines.append(f"{label} - {val}")
                    seen_labels.add(label)
            social_url = "\n".join(social_lines)

            # Parse rating
            raw_rating = row.get("rating", "")
            rating = None
            if raw_rating:
                try:
                    rating = float(raw_rating)
                except (ValueError, TypeError):
                    pass

            # 'google_maps_url' (legacy) or 'url' (Google Maps pipeline)
            maps_url = str(row.get("google_maps_url", "") or row.get("url", "") or "")

            # Map fields
            record = {
                "Company / Business Name": company,
                "Client Name": str(row.get("owner_name", "") or ""),
                "Contact Email": email,
                "Phone / WhatsApp Number": str(row.get("phone", "") or "").strip()
                    or str(row.get("additional_phones", "") or "").split(",")[0].strip(),
                "Industry": industry,
                "Original Category": raw_category,
                "Company Website URL": str(row.get("website", "") or ""),
                "Address": str(row.get("address", "") or ""),
                "City/County": city,
                "Country/State": state,
                "Google Maps URL": maps_url,
                "Lead Status": "Messaged",
                "Platform": "Scraped Lead",
                "Messaged At": today_iso,
                "Lead Created At": today_iso,
                "Apify ID": str(row.get("scrape_id", "") or ""),
            }

            # Optional fields (only set if non-empty)
            if social_url:
                record["Social Media Link"] = social_url
            if rating is not None:
                record["Rating"] = rating
            raw_score = row.get("email_score", "")
            if raw_score != "" and raw_score is not None:
                try:
                    record["Email Score"] = float(raw_score)
                except (ValueError, TypeError):
                    pass

            # Only add if we have at least a company name
            if not record["Company / Business Name"]:
                errors += 1
                continue

            new_records.append(record)
            # Add to dedupe sets to prevent intra-batch dupes
            if email:
                existing_emails.add(email)
            if company:
                existing_companies.add((company.lower(), location))

        except Exception as e:
            logger.warning(f"Error processing row: {e}")
            errors += 1

    # Step 4: Create records in Airtable
    if new_records:
        create_airtable_records(airtable_key, base_id, leads_table_id, new_records)
        notify(f"*Lead Ingestion* Added {len(new_records)} new leads to Airtable")
    else:
        notify("*Lead Ingestion* No new leads to add (all duplicates or invalid)")

    result = {
        "processed": len(sheet_data),
        "added": len(new_records),
        "skipped": skipped,
        "errors": errors,
    }
    logger.info(f"Ingestion result: {result}")
    return result


# ---------------------------------------------------------------------------
# Analytics: funnel metrics
# ---------------------------------------------------------------------------

def compute_funnel_metrics(records: list) -> dict:
    """
    Compute funnel conversion rates from Airtable lead records.
    Returns dict with stage counts, conversion rates, and overall rate.
    """
    # Count leads at each stage or beyond
    stage_counts = {stage: 0 for stage in FUNNEL_STAGES}
    exit_counts = {status: 0 for status in EXIT_STATUSES}
    total = len(records)

    for rec in records:
        fields = rec.get("fields", {})
        status = fields.get("Lead Status", "")

        # Track exit statuses
        if status in EXIT_STATUSES:
            exit_counts[status] += 1
            continue

        if status not in STAGE_ORDER:
            continue
        lead_stage_num = STAGE_ORDER[status]
        # A lead at stage N has reached all stages 1..N
        for stage, num in STAGE_ORDER.items():
            if num <= lead_stage_num:
                stage_counts[stage] += 1

    # Conversion rates between consecutive stages
    conversions = []
    for i in range(len(FUNNEL_STAGES) - 1):
        current = FUNNEL_STAGES[i]
        next_stage = FUNNEL_STAGES[i + 1]
        current_count = stage_counts[current]
        next_count = stage_counts[next_stage]
        rate = (next_count / current_count * 100) if current_count > 0 else 0.0
        conversions.append({
            "from": current,
            "to": next_stage,
            "from_count": current_count,
            "to_count": next_count,
            "rate": round(rate, 1),
        })

    # Overall conversion
    messaged = stage_counts.get("Messaged", 0)
    won = stage_counts.get("Won (active project)", 0)
    overall_rate = (won / messaged * 100) if messaged > 0 else 0.0

    return {
        "total_leads": total,
        "stage_counts": stage_counts,
        "exit_counts": exit_counts,
        "conversions": conversions,
        "overall_rate": round(overall_rate, 1),
    }


def compute_score_analytics(records: list) -> dict:
    """
    Compute lead score analytics: banding, conversion by band,
    and threshold analysis.
    """
    # Categorize leads into score bands
    bands = {label: [] for label, _, _ in SCORE_BANDS}
    bands["Unscored"] = []

    for rec in records:
        fields = rec.get("fields", {})
        score = fields.get("Lead Score")
        status = fields.get("Lead Status", "")

        if score is None or score == 0 or score == "":
            bands["Unscored"].append({"status": status, "score": 0})
            continue

        try:
            score = float(score)
        except (ValueError, TypeError):
            bands["Unscored"].append({"status": status, "score": 0})
            continue

        placed = False
        for label, low, high in SCORE_BANDS:
            if low <= score <= high:
                bands[label].append({"status": status, "score": score})
                placed = True
                break
        if not placed:
            bands["Unscored"].append({"status": status, "score": score})

    # Conversion by band
    band_stats = []
    for label in [b[0] for b in SCORE_BANDS] + ["Unscored"]:
        leads = bands[label]
        total = len(leads)
        if total == 0:
            continue

        responded_plus = sum(
            1 for l in leads
            if l["status"] in STAGE_ORDER and STAGE_ORDER[l["status"]] >= 2
        )
        clients = sum(
            1 for l in leads
            if l["status"] in STAGE_ORDER and STAGE_ORDER[l["status"]] >= 7
        )

        band_stats.append({
            "band": label,
            "count": total,
            "responded_rate": round(responded_plus / total * 100, 1) if total else 0,
            "win_rate": round(clients / total * 100, 1) if total else 0,
        })

    # Threshold analysis
    scored_leads = []
    for rec in records:
        fields = rec.get("fields", {})
        score = fields.get("Lead Score")
        status = fields.get("Lead Status", "")
        if score and score != 0 and status in STAGE_ORDER:
            try:
                scored_leads.append({
                    "score": float(score),
                    "is_client": STAGE_ORDER[status] >= 7,
                    "stage_num": STAGE_ORDER[status],
                })
            except (ValueError, TypeError):
                pass

    threshold_results = []
    best_threshold = None
    best_lift = 0

    for threshold in SCORE_THRESHOLDS:
        above = [l for l in scored_leads if l["score"] >= threshold]
        below = [l for l in scored_leads if l["score"] < threshold]

        if len(above) < MIN_SEGMENT_SIZE or len(below) < MIN_SEGMENT_SIZE:
            continue

        conv_above = sum(1 for l in above if l["is_client"]) / len(above) if above else 0
        conv_below = sum(1 for l in below if l["is_client"]) / len(below) if below else 0
        lift = (conv_above / conv_below) if conv_below > 0 else 0

        threshold_results.append({
            "threshold": threshold,
            "above_count": len(above),
            "below_count": len(below),
            "conv_above": round(conv_above * 100, 1),
            "conv_below": round(conv_below * 100, 1),
            "lift": round(lift, 2),
        })

        if lift > best_lift:
            best_lift = lift
            best_threshold = threshold

    # Correlation (Pearson) between score and stage reached
    correlation = None
    if len(scored_leads) >= MIN_SEGMENT_SIZE:
        scores = [l["score"] for l in scored_leads]
        stages = [l["stage_num"] for l in scored_leads]
        n = len(scores)
        mean_s = sum(scores) / n
        mean_st = sum(stages) / n
        cov = sum((scores[i] - mean_s) * (stages[i] - mean_st) for i in range(n)) / n
        std_s = (sum((s - mean_s) ** 2 for s in scores) / n) ** 0.5
        std_st = (sum((s - mean_st) ** 2 for s in stages) / n) ** 0.5
        if std_s > 0 and std_st > 0:
            correlation = round(cov / (std_s * std_st), 3)

    return {
        "band_stats": band_stats,
        "threshold_results": threshold_results,
        "best_threshold": best_threshold,
        "best_lift": round(best_lift, 2) if best_lift else None,
        "correlation": correlation,
    }


def compute_industry_metrics(records: list) -> list:
    """
    Compute funnel metrics broken down by industry.
    Only includes industries with >= MIN_SEGMENT_SIZE leads.
    """
    # Group by industry
    by_industry = {}
    for rec in records:
        fields = rec.get("fields", {})
        industry = (fields.get("Industry") or "Unknown").strip().title()
        by_industry.setdefault(industry, []).append(rec)

    results = []
    for industry, leads in sorted(by_industry.items()):
        if len(leads) < MIN_SEGMENT_SIZE:
            continue

        # Count stages
        messaged = sum(
            1 for l in leads
            if l["fields"].get("Lead Status") in STAGE_ORDER
            and STAGE_ORDER[l["fields"]["Lead Status"]] >= 1
        )
        responded = sum(
            1 for l in leads
            if l["fields"].get("Lead Status") in STAGE_ORDER
            and STAGE_ORDER[l["fields"]["Lead Status"]] >= 2
        )
        clients = sum(
            1 for l in leads
            if l["fields"].get("Lead Status") in STAGE_ORDER
            and STAGE_ORDER[l["fields"]["Lead Status"]] >= 7
        )

        # Average lead score
        scores = []
        for l in leads:
            s = l["fields"].get("Lead Score")
            if s and s != 0:
                try:
                    scores.append(float(s))
                except (ValueError, TypeError):
                    pass

        avg_score = round(sum(scores) / len(scores), 1) if scores else None

        response_rate = round(responded / messaged * 100, 1) if messaged > 0 else 0
        close_rate = round(clients / messaged * 100, 1) if messaged > 0 else 0

        results.append({
            "industry": industry,
            "total": len(leads),
            "messaged": messaged,
            "responded": responded,
            "clients": clients,
            "response_rate": response_rate,
            "close_rate": close_rate,
            "avg_score": avg_score,
        })

    # Sort by close rate descending
    results.sort(key=lambda x: x["close_rate"], reverse=True)
    return results


def compute_cross_field_analytics(records: list) -> dict:
    """
    Cross-field analytics: score × industry, rating × conversion,
    geography performance, and time-to-progress between stages.
    """
    # ---- 2a: Score × Industry matrix ----
    score_by_industry = {}
    for rec in records:
        fields = rec.get("fields", {})
        industry = (fields.get("Industry") or "").strip().title()
        status = fields.get("Lead Status", "")
        score = fields.get("Lead Score")

        if not industry or industry == "Unknown":
            continue

        score_by_industry.setdefault(industry, []).append({
            "score": float(score) if score and score != 0 else None,
            "status": status,
        })

    score_industry_matrix = []
    for industry, leads in sorted(score_by_industry.items()):
        if len(leads) < MIN_SEGMENT_SIZE:
            continue

        scored = [l["score"] for l in leads if l["score"] is not None]
        avg_score = round(sum(scored) / len(scored), 1) if scored else None

        in_funnel = [l for l in leads if l["status"] in STAGE_ORDER]
        responded = sum(1 for l in in_funnel if STAGE_ORDER.get(l["status"], 0) >= 2)
        won = sum(1 for l in in_funnel if STAGE_ORDER.get(l["status"], 0) >= 7)

        total_funnel = len(in_funnel)
        response_rate = round(responded / total_funnel * 100, 1) if total_funnel else 0
        win_rate = round(won / total_funnel * 100, 1) if total_funnel else 0

        score_industry_matrix.append({
            "industry": industry,
            "total": len(leads),
            "avg_score": avg_score,
            "response_rate": response_rate,
            "win_rate": win_rate,
            "high_score_low_conv": avg_score is not None and avg_score > 60 and win_rate < 5,
            "low_score_high_conv": avg_score is not None and avg_score < 40 and win_rate > 10,
        })

    score_industry_matrix.sort(key=lambda x: x["win_rate"], reverse=True)

    # ---- 2b: Rating × Conversion ----
    RATING_BANDS = [
        ("1.0 - 2.0", 1.0, 2.0),
        ("2.0 - 3.0", 2.0, 3.0),
        ("3.0 - 4.0", 3.0, 4.0),
        ("4.0 - 5.0", 4.0, 5.0),
    ]

    rating_groups = {label: [] for label, _, _ in RATING_BANDS}
    rating_groups["Unrated"] = []

    for rec in records:
        fields = rec.get("fields", {})
        status = fields.get("Lead Status", "")
        rating = fields.get("Rating")

        if rating is None or rating == "" or rating == 0:
            rating_groups["Unrated"].append(status)
            continue

        try:
            rating = float(rating)
        except (ValueError, TypeError):
            rating_groups["Unrated"].append(status)
            continue

        placed = False
        for label, low, high in RATING_BANDS:
            if low <= rating <= high:
                rating_groups[label].append(status)
                placed = True
                break
        if not placed:
            rating_groups["Unrated"].append(status)

    rating_conversion = []
    for label in [b[0] for b in RATING_BANDS] + ["Unrated"]:
        statuses = rating_groups[label]
        total = len(statuses)
        if total < MIN_SEGMENT_SIZE:
            continue

        in_funnel = [s for s in statuses if s in STAGE_ORDER]
        responded = sum(1 for s in in_funnel if STAGE_ORDER[s] >= 2)
        won = sum(1 for s in in_funnel if STAGE_ORDER[s] >= 7)
        total_funnel = len(in_funnel)

        rating_conversion.append({
            "band": label,
            "total": total,
            "response_rate": round(responded / total_funnel * 100, 1) if total_funnel else 0,
            "win_rate": round(won / total_funnel * 100, 1) if total_funnel else 0,
        })

    # ---- 2c: Geography performance ----
    by_region = {}
    for rec in records:
        fields = rec.get("fields", {})
        region = (fields.get("Country/State") or "").strip().title()
        status = fields.get("Lead Status", "")

        if not region:
            continue
        by_region.setdefault(region, []).append(status)

    geography = []
    for region, statuses in sorted(by_region.items()):
        if len(statuses) < MIN_SEGMENT_SIZE:
            continue

        in_funnel = [s for s in statuses if s in STAGE_ORDER]
        responded = sum(1 for s in in_funnel if STAGE_ORDER[s] >= 2)
        won = sum(1 for s in in_funnel if STAGE_ORDER[s] >= 7)
        total_funnel = len(in_funnel)

        geography.append({
            "region": region,
            "total": len(statuses),
            "response_rate": round(responded / total_funnel * 100, 1) if total_funnel else 0,
            "win_rate": round(won / total_funnel * 100, 1) if total_funnel else 0,
        })

    geography.sort(key=lambda x: x["win_rate"], reverse=True)

    # ---- 2d: Time-to-progress between stages ----
    STAGE_DATE_FIELDS = [
        ("Messaged", "Messaged At"),
        ("Responded", "Responded At"),
        ("Meeting Booked", "Meeting 1 Date"),
        ("Second Meeting Booked", "Meeting 2 Date"),
        ("Proposal Sent", "Proposal Sent At"),
        ("Won (active project)", "Accepted At"),
    ]

    stage_transitions = []
    for i in range(len(STAGE_DATE_FIELDS) - 1):
        from_stage, from_field = STAGE_DATE_FIELDS[i]
        to_stage, to_field = STAGE_DATE_FIELDS[i + 1]

        days_list = []
        for rec in records:
            fields = rec.get("fields", {})
            from_date_str = fields.get(from_field, "")
            to_date_str = fields.get(to_field, "")

            if not from_date_str or not to_date_str:
                continue

            try:
                from_date = datetime.strptime(str(from_date_str)[:10], "%Y-%m-%d")
                to_date = datetime.strptime(str(to_date_str)[:10], "%Y-%m-%d")
                delta = (to_date - from_date).days
                if delta >= 0:
                    days_list.append(delta)
            except (ValueError, TypeError):
                continue

        if len(days_list) >= 3:
            avg_days = round(sum(days_list) / len(days_list), 1)
            median_days = sorted(days_list)[len(days_list) // 2]
            stage_transitions.append({
                "from": from_stage,
                "to": to_stage,
                "sample_size": len(days_list),
                "avg_days": avg_days,
                "median_days": median_days,
                "min_days": min(days_list),
                "max_days": max(days_list),
            })

    return {
        "score_industry_matrix": score_industry_matrix,
        "rating_conversion": rating_conversion,
        "geography": geography,
        "stage_transitions": stage_transitions,
    }


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_weekly_telegram(funnel: dict, score: dict, industries: list,
                           period_label: str, cross_field: dict = None) -> str:
    """Format a concise weekly Telegram summary."""
    lines = [f"*Lead Funnel — {period_label}*\n"]

    # Funnel overview
    sc = funnel["stage_counts"]
    ec = funnel.get("exit_counts", {})
    lines.append(f"Total leads: {funnel['total_leads']}")
    lines.append(f"Messaged: {sc.get('Messaged', 0)}")
    lines.append(f"Responded: {sc.get('Responded', 0)}")
    lines.append(f"Meetings: {sc.get('Meeting Booked', 0)}")
    lines.append(f"Proposals: {sc.get('Proposal Sent', 0)}")
    lines.append(f"Won: {sc.get('Won (active project)', 0)}")
    lines.append(f"Overall conversion: {funnel['overall_rate']}%")

    # Exit statuses
    total_exits = sum(ec.values())
    if total_exits > 0:
        exit_parts = [f"{k}: {v}" for k, v in ec.items() if v > 0]
        lines.append(f"Exits: {total_exits} ({', '.join(exit_parts)})")
    lines.append("")

    # Key conversion rates
    lines.append("*Stage Conversions:*")
    for c in funnel["conversions"]:
        lines.append(f"  {c['from']} -> {c['to']}: {c['rate']}%")
    lines.append("")

    # Biggest drop-off
    if funnel["conversions"]:
        worst = min(funnel["conversions"], key=lambda x: x["rate"])
        lines.append(f"*Biggest drop-off:* {worst['from']} -> {worst['to']} ({worst['rate']}%)\n")

    # Score insight
    if score.get("best_threshold"):
        lines.append(
            f"*Score cutoff:* >= {score['best_threshold']} "
            f"gives {score['best_lift']}x conversion lift"
        )
    if score.get("correlation") is not None:
        strength = "strong" if abs(score["correlation"]) > 0.5 else \
                   "moderate" if abs(score["correlation"]) > 0.3 else "weak"
        lines.append(f"Score-stage correlation: {score['correlation']} ({strength})")

    # Top industry
    if industries:
        top = industries[0]
        lines.append(
            f"\n*Top industry:* {top['industry']} "
            f"({top['close_rate']}% close rate, {top['total']} leads)"
        )

    # Cross-field highlights
    if cross_field:
        cf_lines = []

        # Rating insight
        if cross_field.get("rating_conversion"):
            best_rating = max(cross_field["rating_conversion"], key=lambda x: x["win_rate"])
            if best_rating["win_rate"] > 0:
                cf_lines.append(f"Best rating band: {best_rating['band']} ({best_rating['win_rate']}% win rate)")

        # Geography insight
        if cross_field.get("geography"):
            top_region = cross_field["geography"][0]
            cf_lines.append(f"Top region: {top_region['region']} ({top_region['win_rate']}% win rate)")

        # Time-to-progress highlight
        if cross_field.get("stage_transitions"):
            slowest = max(cross_field["stage_transitions"], key=lambda x: x["avg_days"])
            cf_lines.append(f"Slowest transition: {slowest['from']} -> {slowest['to']} (avg {slowest['avg_days']}d)")

        # Score x Industry anomalies
        anomalies = [x for x in cross_field.get("score_industry_matrix", [])
                     if x.get("high_score_low_conv") or x.get("low_score_high_conv")]
        for a in anomalies[:2]:
            if a["high_score_low_conv"]:
                cf_lines.append(f"Anomaly: {a['industry']} high score ({a['avg_score']}) but low win rate ({a['win_rate']}%)")
            elif a["low_score_high_conv"]:
                cf_lines.append(f"Opportunity: {a['industry']} low score ({a['avg_score']}) but high win rate ({a['win_rate']}%)")

        if cf_lines:
            lines.append("\n*Cross-Field Insights:*")
            for cl in cf_lines:
                lines.append(f"  {cl}")

    return "\n".join(lines)


def format_monthly_report(funnel: dict, score: dict, industries: list,
                          period_label: str, cross_field: dict = None,
                          deltas: dict = None) -> str:
    """Format a full monthly Markdown report for Google Docs."""
    lines = [
        f"# Lead Funnel Analytics Report",
        f"## {period_label}\n",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n",
        "---\n",
    ]

    # Executive Summary
    sc = funnel["stage_counts"]
    ec = funnel.get("exit_counts", {})
    lines.append("## Executive Summary\n")
    lines.append(
        f"Total leads in pipeline: {funnel['total_leads']}. "
        f"Overall conversion (Messaged -> Won): {funnel['overall_rate']}%. "
        f"{sc.get('Won (active project)', 0)} active projects from {sc.get('Messaged', 0)} messaged leads."
    )
    if funnel["conversions"]:
        worst = min(funnel["conversions"], key=lambda x: x["rate"])
        lines.append(
            f"\nBiggest bottleneck: {worst['from']} -> {worst['to']} "
            f"at {worst['rate']}% conversion.\n"
        )

    # Month-over-month comparison
    if deltas:
        lines.append(f"### vs Previous Period ({deltas['prev_period']})\n")
        lines.append("| Metric | Change |")
        lines.append("|--------|-------:|")
        lines.append(f"| Total Leads | {deltas['total_leads']} |")
        lines.append(f"| Overall Conversion | {deltas['overall_conversion']} |")
        lines.append(f"| Responded | {deltas['responded']} |")
        lines.append(f"| Meetings | {deltas['meetings']} |")
        lines.append(f"| Proposals | {deltas['proposals']} |")
        lines.append(f"| Won | {deltas['won']} |")
        lines.append(f"| Exits | {deltas['exits']} |")
        lines.append("")

    # Funnel Breakdown
    lines.append("---\n")
    lines.append("## Funnel Breakdown\n")
    lines.append("| Stage | Count | Conversion to Next |")
    lines.append("|-------|------:|-------------------:|")

    for i, stage in enumerate(FUNNEL_STAGES):
        count = sc.get(stage, 0)
        conv = ""
        for c in funnel["conversions"]:
            if c["from"] == stage:
                conv = f"{c['rate']}%"
                break
        if i == len(FUNNEL_STAGES) - 1:
            conv = "—"
        lines.append(f"| {stage} | {count} | {conv} |")
    lines.append("")

    # Exit statuses
    total_exits = sum(ec.values())
    if total_exits > 0:
        lines.append("### Funnel Exits\n")
        lines.append("| Status | Count |")
        lines.append("|--------|------:|")
        for status in EXIT_STATUSES:
            count = ec.get(status, 0)
            if count > 0:
                lines.append(f"| {status} | {count} |")
        lines.append(f"| **Total exits** | **{total_exits}** |")
        lines.append("")

    # Lead Score Analysis
    lines.append("---\n")
    lines.append("## Lead Score Analysis\n")

    if score["band_stats"]:
        lines.append("### Conversion by Score Band\n")
        lines.append("| Band | Leads | Response Rate | Win Rate |")
        lines.append("|------|------:|--------------:|------------:|")
        for b in score["band_stats"]:
            lines.append(
                f"| {b['band']} | {b['count']} | {b['responded_rate']}% | {b['win_rate']}% |"
            )
        lines.append("")

    if score["threshold_results"]:
        lines.append("### Score Threshold Analysis\n")
        lines.append("| Threshold | Above | Below | Conv Above | Conv Below | Lift |")
        lines.append("|----------:|------:|------:|-----------:|-----------:|-----:|")
        for t in score["threshold_results"]:
            lines.append(
                f"| >= {t['threshold']} | {t['above_count']} | {t['below_count']} "
                f"| {t['conv_above']}% | {t['conv_below']}% | {t['lift']}x |"
            )
        lines.append("")
        if score["best_threshold"]:
            lines.append(
                f"**Recommended cutoff:** Score >= {score['best_threshold']} "
                f"(provides {score['best_lift']}x lift in conversion to Client)\n"
            )

    if score["correlation"] is not None:
        strength = "strong" if abs(score["correlation"]) > 0.5 else \
                   "moderate" if abs(score["correlation"]) > 0.3 else "weak"
        lines.append(
            f"**Score-Stage Correlation:** {score['correlation']} ({strength}). "
        )
        if abs(score["correlation"]) < 0.3:
            lines.append(
                "Lead scoring has weak predictive power — "
                "consider recalibrating the scoring model.\n"
            )
        else:
            lines.append(
                "Lead scoring is working — higher scores predict further funnel progress.\n"
            )

    # Industry Breakdown
    lines.append("---\n")
    lines.append("## Industry Breakdown\n")

    if industries:
        lines.append("| Industry | Leads | Response Rate | Close Rate | Avg Score |")
        lines.append("|----------|------:|--------------:|-----------:|----------:|")
        for ind in industries:
            avg_s = f"{ind['avg_score']}" if ind["avg_score"] else "—"
            lines.append(
                f"| {ind['industry']} | {ind['total']} "
                f"| {ind['response_rate']}% | {ind['close_rate']}% | {avg_s} |"
            )
        lines.append("")

        # Recommendations
        if len(industries) >= 2:
            best = industries[0]
            worst = industries[-1]
            lines.append(
                f"**Best performing:** {best['industry']} "
                f"({best['close_rate']}% close rate)\n"
            )
            lines.append(
                f"**Worst performing:** {worst['industry']} "
                f"({worst['close_rate']}% close rate)\n"
            )
    else:
        lines.append("Not enough data per industry (minimum 10 leads required).\n")

    # Cross-Field Analytics
    if cross_field:
        lines.append("---\n")
        lines.append("## Cross-Field Analytics\n")

        # Score × Industry
        if cross_field.get("score_industry_matrix"):
            lines.append("### Score x Industry Matrix\n")
            lines.append("| Industry | Leads | Avg Score | Response Rate | Win Rate | Flag |")
            lines.append("|----------|------:|----------:|--------------:|---------:|------|")
            for row in cross_field["score_industry_matrix"]:
                avg_s = f"{row['avg_score']}" if row["avg_score"] else "—"
                flag = ""
                if row.get("high_score_low_conv"):
                    flag = "High score, low conversion"
                elif row.get("low_score_high_conv"):
                    flag = "Low score, high conversion"
                lines.append(
                    f"| {row['industry']} | {row['total']} | {avg_s} "
                    f"| {row['response_rate']}% | {row['win_rate']}% | {flag} |"
                )
            lines.append("")

        # Rating × Conversion
        if cross_field.get("rating_conversion"):
            lines.append("### Google Maps Rating x Conversion\n")
            lines.append("| Rating Band | Leads | Response Rate | Win Rate |")
            lines.append("|-------------|------:|--------------:|---------:|")
            for row in cross_field["rating_conversion"]:
                lines.append(
                    f"| {row['band']} | {row['total']} "
                    f"| {row['response_rate']}% | {row['win_rate']}% |"
                )
            lines.append("")

        # Geography
        if cross_field.get("geography"):
            lines.append("### Geography Performance\n")
            lines.append("| Region | Leads | Response Rate | Win Rate |")
            lines.append("|--------|------:|--------------:|---------:|")
            for row in cross_field["geography"]:
                lines.append(
                    f"| {row['region']} | {row['total']} "
                    f"| {row['response_rate']}% | {row['win_rate']}% |"
                )
            lines.append("")

        # Time-to-Progress
        if cross_field.get("stage_transitions"):
            lines.append("### Time Between Stages\n")
            lines.append("| Transition | Sample | Avg Days | Median Days | Min | Max |")
            lines.append("|------------|-------:|---------:|------------:|----:|----:|")
            for row in cross_field["stage_transitions"]:
                lines.append(
                    f"| {row['from']} -> {row['to']} | {row['sample_size']} "
                    f"| {row['avg_days']} | {row['median_days']} "
                    f"| {row['min_days']} | {row['max_days']} |"
                )
            lines.append("")

    # Methodology
    lines.append("---\n")
    lines.append("## Methodology\n")
    lines.append(f"- Data source: Airtable Leads table")
    lines.append(f"- Total records analysed: {funnel['total_leads']}")
    lines.append(f"- Funnel stages: {' → '.join(FUNNEL_STAGES)}")
    lines.append(f"- Minimum segment size: {MIN_SEGMENT_SIZE} leads")
    lines.append(f"- Score bands: {', '.join(f'{l} ({lo}-{hi})' for l, lo, hi in SCORE_BANDS)}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_analytics(token_data: dict = None, notify_fn=None) -> dict:
    """Run analytics on current Airtable data. Returns computed metrics."""
    from dotenv import load_dotenv
    load_dotenv()

    airtable_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    leads_table_id = os.getenv("AIRTABLE_LEADS_ID")

    if not all([airtable_key, base_id, leads_table_id]):
        raise ValueError("AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_LEADS_ID must be set")

    notify = notify_fn or (lambda msg: logger.info(msg))

    # Fetch all leads
    records = get_airtable_records(airtable_key, base_id, leads_table_id)
    notify(f"*Lead Funnel* Fetched {len(records)} leads from Airtable")

    if len(records) < 5:
        notify("*Lead Funnel* Not enough data for analytics (< 5 leads)")
        return {"error": "Not enough data", "total_leads": len(records)}

    # Compute all metrics
    funnel = compute_funnel_metrics(records)
    score = compute_score_analytics(records)
    industries = compute_industry_metrics(records)
    cross_field = compute_cross_field_analytics(records)

    return {
        "funnel": funnel,
        "score": score,
        "industries": industries,
        "cross_field": cross_field,
        "record_count": len(records),
    }


def run_weekly(token_data: dict = None, notify_fn=None) -> dict:
    """Weekly summary: compute metrics, send Telegram message."""
    from dotenv import load_dotenv
    load_dotenv()

    notify = notify_fn or (lambda msg: logger.info(msg))
    notify("*Lead Funnel* Running weekly summary")

    metrics = run_analytics(token_data, notify)
    if "error" in metrics:
        return metrics

    telegram_token = os.getenv("LEAD_ANALYTICS_BOT_TOKEN")
    telegram_chat_id = os.getenv("LEAD_ANALYTICS_CHAT_ID")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_label = f"Weekly Summary — {today}"

    message = format_weekly_telegram(
        metrics["funnel"], metrics["score"], metrics["industries"], period_label,
        cross_field=metrics.get("cross_field"),
    )

    telegram_sent = False
    if telegram_token and telegram_chat_id:
        telegram_sent = send_telegram_message(telegram_token, telegram_chat_id, message)

    return {
        "status": "success",
        "report_type": "weekly",
        "telegram_sent": telegram_sent,
        "total_leads": metrics["record_count"],
        "overall_conversion": metrics["funnel"]["overall_rate"],
    }


def save_monthly_snapshot(token_data: dict, metrics: dict, period: str) -> bool:
    """
    Save monthly metrics snapshot to Automation Config spreadsheet.
    Creates 'Analytics Snapshots' tab if it doesn't exist.
    """
    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
    if not config_sheet_id:
        logger.warning("AUTOMATION_CONFIG_SHEET_ID not set, skipping snapshot save")
        return False

    try:
        gc = get_gspread_client(token_data)
        spreadsheet = gc.open_by_key(config_sheet_id)

        # Get or create the snapshots tab
        try:
            ws = spreadsheet.worksheet("Analytics Snapshots")
        except Exception:
            ws = spreadsheet.add_worksheet(title="Analytics Snapshots", rows=100, cols=15)
            ws.update("A1:L1", [[
                "Period", "Generated At", "Total Leads", "Overall Conversion",
                "Messaged", "Responded", "Meetings", "Proposals", "Won",
                "Exits", "Best Threshold", "Top Industry",
            ]])

        funnel = metrics["funnel"]
        sc = funnel["stage_counts"]
        ec = funnel.get("exit_counts", {})

        # Count new leads this month (by Lead Created At)
        top_ind = ""
        if metrics.get("industries") and len(metrics["industries"]) > 0:
            top_ind = metrics["industries"][0]["industry"]

        row = [
            period,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            str(funnel["total_leads"]),
            str(funnel["overall_rate"]),
            str(sc.get("Messaged", 0)),
            str(sc.get("Responded", 0)),
            str(sc.get("Meeting Booked", 0)),
            str(sc.get("Proposal Sent", 0)),
            str(sc.get("Won (active project)", 0)),
            str(sum(ec.values())),
            str(metrics["score"].get("best_threshold", "")),
            top_ind,
        ]

        ws.append_row(row, value_input_option="RAW")
        logger.info(f"Saved snapshot for {period}")
        return True

    except Exception as e:
        logger.error(f"Failed to save snapshot: {e}")
        return False


def load_previous_snapshot(token_data: dict) -> dict:
    """
    Load the most recent monthly snapshot from the config spreadsheet.
    Returns dict with previous metrics or None if no history.
    """
    config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
    if not config_sheet_id:
        return None

    try:
        gc = get_gspread_client(token_data)
        spreadsheet = gc.open_by_key(config_sheet_id)

        try:
            ws = spreadsheet.worksheet("Analytics Snapshots")
        except Exception:
            return None

        rows = ws.get_all_records()
        if not rows:
            return None

        # Last row is most recent
        prev = rows[-1]
        return {
            "period": prev.get("Period", ""),
            "total_leads": int(prev.get("Total Leads", 0) or 0),
            "overall_conversion": float(prev.get("Overall Conversion", 0) or 0),
            "messaged": int(prev.get("Messaged", 0) or 0),
            "responded": int(prev.get("Responded", 0) or 0),
            "meetings": int(prev.get("Meetings", 0) or 0),
            "proposals": int(prev.get("Proposals", 0) or 0),
            "won": int(prev.get("Won", 0) or 0),
            "exits": int(prev.get("Exits", 0) or 0),
        }

    except Exception as e:
        logger.error(f"Failed to load snapshot: {e}")
        return None


def compute_period_deltas(current: dict, previous: dict) -> dict:
    """Compute month-over-month changes."""
    if not previous:
        return None

    def delta_str(current_val, prev_val, suffix=""):
        diff = current_val - prev_val
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff}{suffix}"

    sc = current["funnel"]["stage_counts"]
    ec = current["funnel"].get("exit_counts", {})

    return {
        "prev_period": previous["period"],
        "total_leads": delta_str(current["funnel"]["total_leads"], previous["total_leads"]),
        "overall_conversion": delta_str(current["funnel"]["overall_rate"], previous["overall_conversion"], "%"),
        "messaged": delta_str(sc.get("Messaged", 0), previous["messaged"]),
        "responded": delta_str(sc.get("Responded", 0), previous["responded"]),
        "meetings": delta_str(sc.get("Meeting Booked", 0), previous["meetings"]),
        "proposals": delta_str(sc.get("Proposal Sent", 0), previous["proposals"]),
        "won": delta_str(sc.get("Won (active project)", 0), previous["won"]),
        "exits": delta_str(sum(ec.values()), previous["exits"]),
    }


def run_monthly(token_data: dict = None, notify_fn=None) -> dict:
    """Monthly report: compute metrics, create Google Doc, send Telegram."""
    from dotenv import load_dotenv
    load_dotenv()

    notify = notify_fn or (lambda msg: logger.info(msg))
    notify("*Lead Funnel* Running monthly report")

    metrics = run_analytics(token_data, notify)
    if "error" in metrics:
        return metrics

    telegram_token = os.getenv("LEAD_ANALYTICS_BOT_TOKEN")
    telegram_chat_id = os.getenv("LEAD_ANALYTICS_CHAT_ID")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    period_key = datetime.now(timezone.utc).strftime("%Y-%m")
    period_label = f"Monthly Report — {month_label}"

    # Load previous snapshot for comparison
    previous = load_previous_snapshot(token_data)
    deltas = compute_period_deltas(metrics, previous)

    # Generate full report
    report = format_monthly_report(
        metrics["funnel"], metrics["score"], metrics["industries"], period_label,
        cross_field=metrics.get("cross_field"), deltas=deltas,
    )

    # Create Google Doc
    doc_url = ""
    try:
        doc_title = f"Lead Funnel Analytics — {month_label}"
        doc_url = create_google_doc(token_data, doc_title, report)
        notify(f"*Lead Funnel* Google Doc created: {doc_url}")
    except Exception as e:
        logger.error(f"Google Doc creation failed: {e}")
        notify(f"*Lead Funnel* Google Doc failed: {e}")

    # Save snapshot for next month's comparison
    save_monthly_snapshot(token_data, metrics, period_key)

    # Trigger Manus deep-dive (async — results come back via webhook)
    manus_task_id = None
    manus_api_key = os.getenv("MANUS_API_KEY")
    if manus_api_key:
        try:
            manus_result = create_manus_analytics_task(manus_api_key, metrics, deltas)
            manus_task_id = manus_result.get("id") or manus_result.get("task_id")
            notify(f"*Lead Funnel* Manus deep-dive task created: `{manus_task_id}`")
        except Exception as e:
            logger.error(f"Manus task creation failed: {e}")
            notify(f"*Lead Funnel* Manus task failed (report still sent): {e}")
    else:
        logger.info("MANUS_API_KEY not set, skipping Manus deep-dive")

    # Send Telegram with link to doc
    telegram_sent = False
    if telegram_token and telegram_chat_id:
        tg_lines = [f"*Lead Funnel — {month_label}*\n"]
        if doc_url:
            tg_lines.append(f"[View Full Report]({doc_url})\n")
        tg_lines.append(f"Total leads: {metrics['record_count']}")
        tg_lines.append(f"Overall conversion: {metrics['funnel']['overall_rate']}%")

        # Add deltas
        if deltas:
            tg_lines.append(f"\n*vs {deltas['prev_period']}:*")
            tg_lines.append(f"  Leads: {deltas['total_leads']}")
            tg_lines.append(f"  Conversion: {deltas['overall_conversion']}")
            tg_lines.append(f"  Won: {deltas['won']}")

        if metrics["funnel"]["conversions"]:
            worst = min(metrics["funnel"]["conversions"], key=lambda x: x["rate"])
            tg_lines.append(f"\nBiggest drop-off: {worst['from']} -> {worst['to']} ({worst['rate']}%)")
        if metrics["score"].get("best_threshold"):
            tg_lines.append(
                f"Best score cutoff: >= {metrics['score']['best_threshold']} "
                f"({metrics['score']['best_lift']}x lift)"
            )
        tg_message = "\n".join(tg_lines)
        telegram_sent = send_telegram_message(telegram_token, telegram_chat_id, tg_message)

    return {
        "status": "success",
        "report_type": "monthly",
        "doc_url": doc_url,
        "telegram_sent": telegram_sent,
        "total_leads": metrics["record_count"],
        "overall_conversion": metrics["funnel"]["overall_rate"],
        "deltas": deltas,
        "manus_task_id": manus_task_id,
    }


# ---------------------------------------------------------------------------
# Manus AI integration for monthly deep-dive insights
# ---------------------------------------------------------------------------

MANUS_API_BASE = "https://api.manus.ai/v1"


def create_manus_analytics_task(api_key: str, metrics: dict, deltas: dict = None) -> dict:
    """
    Send lead funnel metrics to Manus for qualitative analysis.
    Returns the Manus task response (contains task_id).
    """
    # Build structured prompt
    prompt_parts = [
        "You are a B2B sales analytics consultant analysing lead funnel data for an AI automation agency (All In One Solutions).",
        "The agency helps businesses integrate AI and automation into their operations.",
        "",
        "Analyse the following lead funnel metrics and provide a detailed strategic report with:",
        "1. Executive Insights (3-5 key takeaways from the data)",
        "2. Funnel Analysis - where leads are being lost and hypotheses for why",
        "3. Industry-Specific Recommendations - which industries to double down on, which to deprioritise",
        "4. Lead Scoring Assessment - is the scoring model predictive? What improvements could be made?",
        "5. Cross-Field Insights - patterns across score, industry, rating, geography",
        "6. Actionable Next Steps - concrete actions to improve conversion rates",
    ]

    if deltas:
        prompt_parts.append(f"7. Month-over-Month Trend Analysis - compare current metrics to previous period ({deltas['prev_period']})")

    prompt_parts.extend([
        "",
        "Format your response as a well-structured Markdown document with headers and bullet points.",
        "Be specific and data-driven. Reference actual numbers from the data.",
        "",
        "DATA:",
        json.dumps({
            "funnel": metrics["funnel"],
            "score": metrics["score"],
            "industries": metrics["industries"],
            "cross_field": metrics.get("cross_field", {}),
            "deltas": deltas,
        }, indent=2, default=str),
    ])

    prompt = "\n".join(prompt_parts)

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


def handle_manus_completion(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Handle Manus task completion webhook for lead funnel insights.

    Extracts Manus insights, combines with Python quantitative report,
    creates a single Google Doc, sends Telegram notification.
    """
    from dotenv import load_dotenv
    load_dotenv()

    notify = slack_notify_fn or (lambda msg: logger.info(msg))

    telegram_token = os.getenv("LEAD_ANALYTICS_BOT_TOKEN")
    telegram_chat_id = os.getenv("LEAD_ANALYTICS_CHAT_ID")

    # Extract task details from Manus webhook payload
    event_type = payload.get("event_type", "")
    task_detail = payload.get("task_detail", {})
    task_id = task_detail.get("task_id", payload.get("task_id", "unknown"))
    stop_reason = task_detail.get("stop_reason", "")

    notify(f"*Lead Funnel Manus* Webhook received: {event_type} for task `{task_id}`")

    # Only process completed tasks
    if event_type != "task_stopped" or stop_reason != "finish":
        notify(f"*Lead Funnel Manus* Task not completed: {event_type}, {stop_reason}")
        return {"status": "skipped", "reason": f"event_type={event_type}, stop_reason={stop_reason}"}

    # Extract insights from Manus response
    message = task_detail.get("message", "")
    attachments = task_detail.get("attachments", [])

    manus_insights = ""
    for att in attachments:
        fname = att.get("file_name", "")
        if fname.endswith(".md") or fname.endswith(".txt") or fname.endswith(".markdown"):
            try:
                file_url = att.get("url", "")
                if file_url:
                    resp = requests.get(file_url, timeout=30)
                    resp.raise_for_status()
                    manus_insights = resp.text
                    break
            except Exception as e:
                logger.warning(f"Failed to download Manus attachment: {e}")

    if not manus_insights:
        manus_insights = message

    if not manus_insights:
        notify("*Lead Funnel Manus* No insights received from Manus")
        return {"status": "error", "error": "No Manus output received"}

    # Run fresh analytics to combine with Manus insights
    metrics = run_analytics(token_data, notify)
    if "error" in metrics:
        return metrics

    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    period_label = f"Monthly Report — {month_label} (with AI Insights)"

    previous = load_previous_snapshot(token_data)
    deltas = compute_period_deltas(metrics, previous)

    # Generate Python quantitative report
    quant_report = format_monthly_report(
        metrics["funnel"], metrics["score"], metrics["industries"], period_label,
        cross_field=metrics.get("cross_field"), deltas=deltas,
    )

    # Combine: Python tables + Manus insights
    combined_report = quant_report + "\n\n---\n\n## AI Strategic Insights (Manus)\n\n" + manus_insights

    # Create combined Google Doc
    doc_url = ""
    try:
        doc_title = f"Lead Funnel Analytics + AI Insights — {month_label}"
        doc_url = create_google_doc(token_data, doc_title, combined_report)
        notify(f"*Lead Funnel Manus* Combined report created: {doc_url}")
    except Exception as e:
        logger.error(f"Google Doc creation failed: {e}")
        notify(f"*Lead Funnel Manus* Google Doc failed: {e}")

    # Send Telegram
    telegram_sent = False
    if telegram_token and telegram_chat_id:
        tg_lines = [f"*Lead Funnel — {month_label} (AI Deep-Dive)*\n"]
        if doc_url:
            tg_lines.append(f"[View Full Report + AI Insights]({doc_url})\n")
        tg_lines.append(f"Total leads: {metrics['record_count']}")
        tg_lines.append(f"Overall conversion: {metrics['funnel']['overall_rate']}%")
        tg_lines.append(f"\nManus task: `{task_id}`")
        tg_message = "\n".join(tg_lines)
        telegram_sent = send_telegram_message(telegram_token, telegram_chat_id, tg_message)

    return {
        "status": "success",
        "report_type": "monthly_manus",
        "doc_url": doc_url,
        "telegram_sent": telegram_sent,
        "manus_task_id": str(task_id),
    }


# ---------------------------------------------------------------------------
# run() — webhook entry point (called by modal_webhook.py)
# ---------------------------------------------------------------------------

def run(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """
    Entry point for procedural webhook execution from modal_webhook.py.

    Payload options:
      {"action": "ingest", "sheet_id": "..."}  — ingest leads from sheet
      {"action": "weekly"}                      — run weekly summary
      {"action": "monthly"}                     — run monthly report
      {"action": "analytics"}                   — return raw metrics
      {"action": "manus_complete"}              — handle Manus completion webhook
    """
    action = payload.get("action", "")

    # Auto-detect Manus webhook payload (has event_type + task_detail)
    if not action and payload.get("event_type") and payload.get("task_detail"):
        action = "manus_complete"

    if not action:
        action = "analytics"

    if action == "ingest":
        sheet_id = payload.get("sheet_id")
        if not sheet_id:
            return {"error": "sheet_id required for ingest action"}
        return ingest_leads_from_sheet(sheet_id, token_data, slack_notify_fn)

    elif action == "weekly":
        return run_weekly(token_data, slack_notify_fn)

    elif action == "monthly":
        return run_monthly(token_data, slack_notify_fn)

    elif action == "analytics":
        return run_analytics(token_data, slack_notify_fn)

    elif action == "manus_complete":
        return handle_manus_completion(payload, token_data, slack_notify_fn)

    else:
        return {"error": f"Unknown action: {action}"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Lead Funnel Analytics")
    parser.add_argument("--weekly", action="store_true", help="Run weekly Telegram summary")
    parser.add_argument("--monthly", action="store_true", help="Run monthly Google Doc report")
    parser.add_argument("--ingest", action="store_true", help="Ingest leads from Google Sheet")
    parser.add_argument("--analytics", action="store_true", help="Print analytics to stdout")
    parser.add_argument("--sheet-id", help="Google Sheet ID for ingestion")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    if args.ingest:
        if not args.sheet_id:
            print("Error: --sheet-id required with --ingest")
            sys.exit(1)
        result = ingest_leads_from_sheet(args.sheet_id)
        print(json.dumps(result, indent=2))

    elif args.weekly:
        result = run_weekly()
        print(json.dumps(result, indent=2))

    elif args.monthly:
        result = run_monthly()
        print(json.dumps(result, indent=2))

    else:
        # Default: print analytics
        result = run_analytics()
        if "error" in result:
            print(f"Error: {result['error']}")
            sys.exit(1)
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
