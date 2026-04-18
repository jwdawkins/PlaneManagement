"""
FlySto flight log puller
Pulls flight log data from flysto.net and saves to CSV/JSON.

Uses Playwright to log in and scrape the rendered DOM, bypassing
the site's custom response encoding scheme.

Create a .env file with:
    FLYSTO_EMAIL=you@example.com
    FLYSTO_PASSWORD=yourpass
    FLYSTO_FROM_DATE=2026-01-01    # optional
    FLYSTO_OUTPUT=flight_logs.csv   # optional
    FLYSTO_AIRCRAFT=6rp5nv          # optional

Setup:
    pip install playwright
    playwright install chromium

Usage:
    python flysto_pull.py
"""

import json
import os
import re
import sys
from datetime import date, timedelta

from playwright.sync_api import sync_playwright


def load_dotenv(path="flysto.env"):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass


load_dotenv("flysto.env")

BASE_URL = "https://www.flysto.net"
AIRCRAFT = os.getenv("FLYSTO_AIRCRAFT", "6rp5nv")

APPROACH_SECTIONS = frozenset(["Heights", "Below 500'", "At 50'", "Flare", "Touchdown", "Landing performance"])


def login(page, email: str, password: str) -> None:
    page.goto(f"{BASE_URL}/login")
    page.wait_for_selector('input[type="email"]')
    page.fill('input[type="email"]', email)
    page.fill('input[type="password"]', password)
    # Click the submit button (not cookie/privacy buttons)
    page.click('button[type="button"]:not([class*="cookie"]):not([class*="privacy"])')
    page.wait_for_url(lambda url: "/login" not in url, timeout=15000)
    print("Logged in successfully.")


def scrape_flights(page, from_date: str) -> list:
    url = f"{BASE_URL}/logs?aircraft={AIRCRAFT}&from-date={from_date}"
    page.goto(url)
    page.wait_for_selector("main tr[data-key]", timeout=20000)
    # Give the page a moment to finish rendering all rows
    page.wait_for_timeout(1500)

    flights = page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('main tr[data-key]')).map(r => {
                const cells = Array.from(r.querySelectorAll('td')).map(c => c.innerText.trim());
                const fromTo = (cells[2] || '').split('\\n');
                const takeoffLanding = (cells[4] || '').split('\\n');
                return {
                    id:           r.getAttribute('data-key'),
                    date:         cells[1] || '',
                    from:         fromTo[0] || '',
                    to:           fromTo[1] || '',
                    crew:         cells[3] || '',
                    takeoffs:     takeoffLanding[0] || '',
                    landings:     takeoffLanding[1] || '',
                    approaches:   cells[5] || '',
                    brakes_off_on: cells[6] || '',
                    blocktime:    cells[7] || '',
                    airtime:      cells[8] || '',
                    fuel_usg:     (cells[9] || '').replace(' usg', ''),
                    tags:         cells[10] || ''
                };
            });
        }
    """)
    print(f"Scraped {len(flights)} flights.")
    return flights


def scrape_log_detail(page, log_id: str) -> dict:
    """
    Navigate to a log's approach page and scrape:
      - flags (landing flags like fuel quantity warnings)
      - approach score (overall % and points)
      - approach parameters (section, parameter, value, required)
    """
    # Grab flags from the main log page first
    page.goto(f"{BASE_URL}/logs/{log_id}")
    page.wait_for_timeout(2000)
    def _click_panel(label: str) -> None:
        page.evaluate(f"""
            () => {{
                const el = Array.from(document.querySelectorAll(
                    'button, [role="button"], [role="tab"], li, div, a'
                )).find(e => e.textContent.trim().startsWith({repr(label)}) && e.offsetParent !== null);
                if (el) el.click();
            }}
        """)

    # Expand collapsible cards before scraping
    _click_panel("Flags")
    page.wait_for_timeout(1000)
    _click_panel("Fuel details")
    page.wait_for_timeout(3000)
    page_data = page.evaluate("""
        () => {
            const body = document.body.innerText;
            const allLines = body.split('\\n').map(l => l.trim()).filter(Boolean);

            // --- Flags ---
            // Flags appear after "Takeoff at X" / "Landing at X" sub-headers anywhere in the body
            const flags = [];
            const seen = new Set();
            const stopWords = new Set([
                'Flight details', 'Crew, tags, remarks', 'Entire Flight', 'Times',
                'Startup', 'Shutdown', 'Flight log', 'Fuel details', 'Source log file',
            ]);
            const bodyLines = allLines;
            for (let i = 0; i < bodyLines.length; i++) {
                if (/^(Takeoff|Landing) at /.test(bodyLines[i])) {
                    for (let j = i + 1; j < Math.min(i + 10, bodyLines.length); j++) {
                        const fl = bodyLines[j];
                        if (!fl || stopWords.has(fl) || fl.startsWith('©')
                                || /^\\d{2}:\\d{2}z/.test(fl)
                                || /^(Takeoff|Landing) at /.test(fl)) break;
                        if (fl.length > 10 && fl.length < 200 && !seen.has(fl)) {
                            seen.add(fl);
                            flags.push(fl);
                        }
                    }
                }
            }

            // --- Flight stats ---
            const stats = {};
            for (let i = 0; i < allLines.length; i++) {
                const l = allLines[i];
                if (l === 'Startup') {
                    for (let j = i + 1; j < Math.min(i + 4, allLines.length); j++) {
                        if (/^\\d+ sec$/.test(allLines[j])) {
                            stats.startup_sec = parseInt(allLines[j]);
                            break;
                        }
                    }
                } else if (l === 'Start fuel') {
                    for (let j = i + 1; j < Math.min(i + 3, allLines.length); j++) {
                        if (/^[\\d.]+ usg$/.test(allLines[j])) {
                            stats.start_fuel_usg = parseFloat(allLines[j]);
                            break;
                        }
                    }
                } else if (l === 'End fuel') {
                    for (let j = i + 1; j < Math.min(i + 5, allLines.length); j++) {
                        if (/^[\\d.]+ usg$/.test(allLines[j]) && !stats.end_fuel_usg) {
                            stats.end_fuel_usg = parseFloat(allLines[j]);
                            break;
                        }
                    }
                }
            }

            // Extract Left/Right from the popup's End fuel section (after "Fuel usage")
            const fuelUsageIdx = body.indexOf('\\nFuel usage\\n');
            const popupEndFuelIdx = fuelUsageIdx > -1 ? body.indexOf('\\nEnd fuel\\n', fuelUsageIdx) : -1;
            if (popupEndFuelIdx > -1) {
                const popupLines = body.slice(popupEndFuelIdx, popupEndFuelIdx + 200)
                    .split('\\n').map(l => l.trim()).filter(Boolean);
                for (let j = 0; j < popupLines.length; j++) {
                    if (popupLines[j] === 'Left' && /^[\\d.]+$/.test(popupLines[j + 1] || ''))
                        stats.end_fuel_left_usg = parseFloat(popupLines[j + 1]);
                    else if (popupLines[j] === 'Right' && /^[\\d.]+$/.test(popupLines[j + 1] || ''))
                        stats.end_fuel_right_usg = parseFloat(popupLines[j + 1]);
                }
            }
            return { flags, stats };
        }
    """)
    extra_flags = page_data["flags"]
    flight_stats = page_data["stats"]

    page.goto(f"{BASE_URL}/logs/{log_id}/approaches/0")
    page.wait_for_url(lambda url: "approaches/0" in url, timeout=15000)

    has_approach = False
    try:
        score_btn = page.locator("button", has_text="Approach score").first
        score_btn.wait_for(timeout=10000)
        score_btn.click()
        page.wait_for_timeout(2000)
        has_approach = True
    except Exception:
        print(f"  No approach score found for {log_id}")

    raw = page.evaluate("""
        () => {
            const bodyText = document.body.innerText;

            // --- Overall approach score ---
            const scoreBtn = Array.from(document.querySelectorAll('button'))
                .find(b => b.innerText.includes('Approach score'));
            const scoreText = scoreBtn ? scoreBtn.innerText.trim() : '';
            const pctMatch  = scoreText.match(/(\\d+)%/);
            const ptsMatch  = scoreText.match(/(\\d+) of (\\d+) points/);

            // --- Table rows (structural, class-agnostic) ---
            const rows = Array.from(document.querySelectorAll('tr'))
                .map(r => Array.from(r.querySelectorAll('td')).map(c => {
                    const text = c.innerText.trim();
                    if (text) return text;
                    // SVG icon — capture path shape to identify check/X/!
                    const svg = c.querySelector('svg');
                    if (svg) {
                        const path = svg.querySelector('path, polyline, circle');
                        const d = path ? (path.getAttribute('d') || path.getAttribute('points') || '') : '';
                        return '__svg__' + d.slice(0, 40);
                    }
                    return '';
                }))
                .filter(cells => cells.length >= 1 && cells.some(c => c.length > 0));

            // --- Approach text as fallback ---
            const startIdx = bodyText.indexOf('\\nHeights\\n');
            const endMarkers = ['\\u00a9 MapTiler', '\\u00a9 OpenStreetMap', 'Altitude\\nSpeed'];
            let endIdx = bodyText.length;
            for (const m of endMarkers) {
                const i = bodyText.indexOf(m);
                if (i > -1 && i < endIdx) endIdx = i;
            }
            const approachText = startIdx > -1 ? bodyText.slice(startIdx, endIdx).trim() : '';

            return {
                score_pct:    pctMatch ? parseInt(pctMatch[1]) : null,
                score_earned: ptsMatch ? parseInt(ptsMatch[1]) : null,
                score_total:  ptsMatch ? parseInt(ptsMatch[2]) : null,
                rows,
                approach_text: approachText,
            };
        }
    """)

    approach_params = _parse_approach_rows(raw.get("rows", []))
    if not approach_params:
        approach_params = _parse_approach_text(raw.get("approach_text", ""))

    all_flags = [_clean(f) for f in dict.fromkeys(extra_flags)]

    return {
        "url":             f"{BASE_URL}/logs/{log_id}",
        "flags":           all_flags,
        "flight_stats":    flight_stats,
        "score_pct":       raw["score_pct"],
        "score_earned":    raw["score_earned"],
        "score_total":     raw["score_total"],
        "approach_params": approach_params,
    }


def _compute_pass_fail(value: str, required: str) -> str:
    """Derive PASS/FAIL by evaluating value against the required constraint."""
    if not value and not required:
        return "INFO"
    if not value or not required:
        return ""
    req_m = re.match(r"([<>]=?)\s*([\d.]+)", required)
    if not req_m:
        return ""
    op = req_m.group(1)
    req_num = float(req_m.group(2))
    # Qualitative "Low" means effectively 0 — passes any <= constraint
    if value.lower() == "low":
        return "PASS" if op == "<=" else "FAIL"
    # Value may be a range like "84-95" — use max for <= checks, min for >=
    range_m = re.match(r"([\d.]+)-([\d.]+)", value)
    if range_m:
        lo, hi = float(range_m.group(1)), float(range_m.group(2))
        val_num = hi if op == "<=" else lo
    else:
        num_m = re.search(r"[\d.]+", value)
        if not num_m:
            return ""
        val_num = float(num_m.group())
    if op == ">=" and val_num >= req_num:
        return "PASS"
    if op == "<=" and val_num <= req_num:
        return "PASS"
    return "FAIL"


def _parse_approach_rows(rows: list) -> list:
    """Parse raw tr/td cell arrays into structured approach params.

    Expected columns: [svg-icon, parameter, ≈value, required, points]
    """
    params = []
    for cells in rows:
        if not cells:
            continue
        # Layout: SVG icon at [0], param at [1], ≈value at [2], required at [3]
        if cells[0].startswith("__svg__"):
            cells = cells[1:]
        elif cells[0] == "":
            cells = cells[1:]

        if not cells:
            continue
        param = _clean(cells[0])
        value = _clean(cells[1].lstrip("≈")) if len(cells) > 1 else ""
        required = _clean(cells[2]) if len(cells) > 2 else ""

        if not param or param in APPROACH_SECTIONS or param == "Total:":
            continue
        params.append({
            "parameter": param,
            "value":     value,
            "required":  required,
            "result":    _compute_pass_fail(value, required),
        })
    return params


_UNICODE_MAP = str.maketrans({
    "\u2a7e": ">=",   # ⩾
    "\u2a7d": "<=",   # ⩽
    "\u2265": ">=",   # ≥
    "\u2264": "<=",   # ≤
    "\u2248": "~",    # ≈
    "\u00b0": "deg",  # °
})


def _clean(s: str) -> str:
    return s.translate(_UNICODE_MAP)


def _is_measurement(s: str) -> bool:
    return bool(re.search(r"\d", s)) and bool(re.search(r"[°'\"a-z%]", s, re.I))


def _parse_approach_text(text: str) -> list:
    """Best-effort parse of raw approach body text into structured params."""
    if not text:
        return []
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    params = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line in APPROACH_SECTIONS:
            i += 1
            continue
        # Pattern: param_name, ≈actual, required
        if i + 1 < len(lines) and lines[i + 1].startswith("≈"):
            actual = lines[i + 1].lstrip("≈").strip()
            required = ""
            if (i + 2 < len(lines)
                    and lines[i + 2] not in APPROACH_SECTIONS
                    and not lines[i + 2].startswith("≈")):
                required = lines[i + 2]
                i += 3
            else:
                i += 2
            params.append({"parameter": _clean(line), "value": _clean(actual), "required": _clean(required), "result": ""})
        elif i + 1 < len(lines) and _is_measurement(lines[i + 1]):
            # param_name followed by a plain measurement (no ≈)
            params.append({"parameter": _clean(line), "value": "", "required": _clean(lines[i + 1]), "result": ""})
            i += 2
        else:
            i += 1
    return params


def save_json(records: list, path: str = None) -> None:
    if path is None:
        path = os.getenv("FLYSTO_OUTPUT", "flight_logs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    print(f"Saved {len(records)} records to {path}")



def update_env(key: str, value: str, path: str = "flysto.env") -> None:
    """Update or append a key=value line in the .env file."""
    try:
        with open(path) as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []
    updated = False
    new_lines = []
    for line in lines:
        if "=" in line and line.split("=", 1)[0].strip() == key:
            new_lines.append(f"{key}={value}\n")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"{key}={value}\n")
    with open(path, "w") as f:
        f.writelines(new_lines)


def main():
    email   = os.getenv("FLYSTO_EMAIL")
    password = os.getenv("FLYSTO_PASSWORD")
    last_id  = os.getenv("FLYSTO_LOG_ID", "")

    if not email or not password:
        print("Error: FLYSTO_EMAIL and FLYSTO_PASSWORD must be set in your .env file.")
        sys.exit(1)
    if not last_id:
        print("Error: FLYSTO_LOG_ID must be set in your .env file.")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"Logging in as {email}...")
        login(page, email, password)

        # Scrape recent flight list (180-day window) — newest first
        from_date = (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")
        flights = scrape_flights(page, from_date)

        # Everything before last_id in the list is newer
        new_flights = []
        for f in flights:
            if f["id"] == last_id:
                break
            new_flights.append(f)

        if not new_flights:
            browser.close()
            print("No new flights found.")
            save_json([{"result": False}])
            return

        print(f"Found {len(new_flights)} new flight(s) — processing latest only.")

        # Only process the most recent (first in newest-first list)
        results = []
        for f in new_flights[:1]:
            print(f"Scraping detail for log {f['id']} ({f['date']})...")
            detail = scrape_log_detail(page, f["id"])
            detail["id"]   = f["id"]
            detail["date"] = f["date"]
            results.append(detail)

        browser.close()

        # Update .env so next run uses the newest flight as baseline
        newest_id = new_flights[0]["id"]
        update_env("FLYSTO_LOG_ID", newest_id)
        print(f"Updated FLYSTO_LOG_ID → {newest_id}")

        save_json(results)


if __name__ == "__main__":
    main()
