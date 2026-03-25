#!/usr/bin/env python3
"""
Manus Router - Unified Manus webhook handler

Manus only supports one global webhook. This router receives all Manus
task completion events and dispatches to the correct handler by looking
up the task_id in each script's tracker sheet.
"""
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manus-router")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]


def get_gspread_client(token_data: dict):
    import gspread
    from google.oauth2.credentials import Credentials
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
    return gspread.authorize(creds)


def find_task_type(gc, task_id: str) -> str:
    """
    Check tracker sheets to determine which automation owns this task.
    Returns 'tech_radar', 'investment_research', or 'unknown'.
    """
    # Check tech radar tracker
    try:
        tracker_sheet_id = os.getenv("TECH_RADAR_TRACKER_SHEET_ID")
        if tracker_sheet_id:
            sheet = gc.open_by_key(tracker_sheet_id).sheet1
            for row in sheet.get_all_records():
                if str(row.get("manus_task_id", "")) == str(task_id):
                    return "tech_radar"
    except Exception as e:
        logger.warning(f"Could not check tech radar tracker: {e}")

    # Check investment research tracker tab
    try:
        config_sheet_id = os.getenv("AUTOMATION_CONFIG_SHEET_ID")
        if config_sheet_id:
            ws = gc.open_by_key(config_sheet_id).worksheet("Investment Research")
            for row in ws.get_all_records():
                if str(row.get("manus_task_id", "")) == str(task_id):
                    return "investment_research"
    except Exception as e:
        logger.warning(f"Could not check investment research tracker: {e}")

    logger.warning(f"Task {task_id} not found in any tracker, defaulting to tech_radar")
    return "tech_radar"


def run(payload: dict, token_data: dict, slack_notify_fn=None) -> dict:
    """Route Manus completion webhook to the correct handler."""
    import sys
    import importlib.util

    sys.path.insert(0, "/app")

    task_detail = payload.get("task_detail", {})
    task_id = task_detail.get("task_id", "unknown")

    notify = slack_notify_fn or (lambda msg: logger.info(msg))
    notify(f"*Manus Router* Webhook received for task `{task_id}`")

    gc = get_gspread_client(token_data)
    task_type = find_task_type(gc, task_id)

    notify(f"*Manus Router* Routing `{task_id}` → {task_type}")

    if task_type == "investment_research":
        script_path = "/app/execution/investment_research.py"
        module_name = "investment_research"
    else:
        script_path = "/app/execution/tech_radar_research.py"
        module_name = "tech_radar_research"

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run(payload, token_data, slack_notify_fn)
