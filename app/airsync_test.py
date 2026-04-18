#!/usr/bin/env python3
"""
Send a test AirSync Slack message using real Flysto data.

Usage:
    python3 airsync_test.py [LOG_ID]

Defaults to the most recent scraped log ID if not specified.
Sends to Jerry's DM so you can review formatting without noise in the channel.
"""
import json
import os
import sys

# Run from the app directory so relative paths (flysto.env, etc.) resolve correctly
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from flysto_pull import (
    load_dotenv,
    login,
    scrape_log_detail,
    _fmt_airsync_msg,
    _open_dm,
    _post,
    _load_pilots_cfg,
    _APP_ENV,
    _JERRY_ID,
)
from playwright.sync_api import sync_playwright

load_dotenv("flysto.env")
load_dotenv(_APP_ENV)

LOG_ID = sys.argv[1] if len(sys.argv) > 1 else os.getenv("FLYSTO_LOG_ID", "")
DATE   = sys.argv[2] if len(sys.argv) > 2 else "16 Apr"

if not LOG_ID:
    print("ERROR: no LOG_ID specified and FLYSTO_LOG_ID not set")
    sys.exit(1)

email    = os.getenv("FLYSTO_EMAIL")
password = os.getenv("FLYSTO_PASSWORD")
token    = os.getenv("N900JV_BOT_TOKEN", "")

if not email or not password:
    print("ERROR: FLYSTO_EMAIL / FLYSTO_PASSWORD not set")
    sys.exit(1)
if not token:
    print("ERROR: N900JV_BOT_TOKEN not set")
    sys.exit(1)

print(f"Scraping log detail for {LOG_ID} ({DATE})...")
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page    = browser.new_page()
    login(page, email, password)
    detail       = scrape_log_detail(page, LOG_ID)
    detail["id"]   = LOG_ID
    detail["date"] = DATE
    browser.close()

print("\n--- Raw flight data ---")
print(json.dumps(detail, indent=2))

pilots_cfg = _load_pilots_cfg()
msg        = _fmt_airsync_msg(detail, _JERRY_ID, pilots_cfg)

print("\n--- Formatted Slack message ---")
print(msg)

dm_channel = _open_dm(token, _JERRY_ID)
_post(token, dm_channel, msg)
print(f"\nSent to Jerry's DM ✓")
