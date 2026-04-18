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


# ---------------------------------------------------------------------------
# AirSync — send Slack notification and update SQLite directly from liono
# ---------------------------------------------------------------------------
import sqlite3
import urllib.request
import urllib.error

_BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA_DIR        = os.path.join(_BASE_DIR, "data")
_PENDING_FILE    = os.path.join(_DATA_DIR, "airsync_pending.json")
_LIONO_DB        = os.path.join(_DATA_DIR, "logbook.db")
_LIONO_PILOTS    = os.path.join(_DATA_DIR, "pilots.json")
_APP_ENV         = os.path.join(_BASE_DIR, ".env")
_JERRY_ID        = "U0AHRJ7PHNC"
_AIRSYNC_TIMEOUT = 600  # 10 minutes


def _load_pending() -> dict | None:
    try:
        with open(_PENDING_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _clear_pending() -> None:
    try:
        os.remove(_PENDING_FILE)
    except FileNotFoundError:
        pass


def _slack_api(token: str, method: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"https://slack.com/api/{method}",
        data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _open_dm(token: str, user_id: str) -> str:
    result = _slack_api(token, "conversations.open", {"users": user_id})
    return result["channel"]["id"]


def _post(token: str, channel: str, text: str) -> None:
    result = _slack_api(token, "chat.postMessage", {"channel": channel, "text": text})
    if not result.get("ok"):
        print(f"Slack post failed: {result.get('error')}")


def _update_fuel_sqlite(flight: dict, slack_user: str, table: str) -> None:
    stats  = flight.get("flight_stats", {})
    l_fuel = stats.get("end_fuel_left_usg")
    r_fuel = stats.get("end_fuel_right_usg")
    if l_fuel is None and r_fuel is None:
        print("AirSync: no fuel data in Flysto response — SQLite not updated.")
        return
    from datetime import datetime as _dt
    con = sqlite3.connect(_LIONO_DB)
    now = _dt.now()
    if l_fuel is not None:
        con.execute(f"INSERT INTO {table}(date,type,valuen,number) VALUES(?,?,?,?)",
                    (now, 6, l_fuel, slack_user))
    if r_fuel is not None:
        con.execute(f"INSERT INTO {table}(date,type,valuen,number) VALUES(?,?,?,?)",
                    (now, 8, r_fuel, slack_user))
    con.commit()
    con.close()
    print(f"AirSync: fuel updated in SQLite — L={l_fuel} R={r_fuel}")


def _load_pilots_cfg() -> dict:
    try:
        with open(_LIONO_PILOTS) as f:
            return json.load(f)
    except Exception:
        return {}


def _fmt_airsync_msg(flight: dict, slack_user: str, pilots_cfg: dict) -> str:
    pilot_cfg   = pilots_cfg.get("pilots", {}).get(slack_user, {})
    pilot_name  = pilot_cfg.get("name", slack_user)
    is_jerry    = slack_user == _JERRY_ID
    date_str    = flight.get("date", "?")
    url         = flight.get("url", "")
    flags       = flight.get("flags", [])
    score_pct   = flight.get("score_pct")
    score_earned = flight.get("score_earned")
    score_total  = flight.get("score_total")
    params      = flight.get("approach_params", [])
    stats       = flight.get("flight_stats", {})
    l_fuel      = stats.get("end_fuel_left_usg")
    r_fuel      = stats.get("end_fuel_right_usg")

    header = f"\u2708\ufe0f *AirSync \u2014 N900JV \u2014 {date_str}*"
    if not is_jerry:
        header += f"  [{pilot_name}]"
    lines = [header]

    if flags:
        lines.append("")
        lines.append(":warning: *FLAGS*")
        for flag in flags:
            lines.append(f">{flag}")

    if score_pct is not None or params:
        lines.append("")
        score_str = ""
        if score_pct is not None:
            score_str = f"{score_pct}%"
            if score_earned is not None and score_total is not None:
                score_str += f"  ({score_earned}/{score_total} pts)"
        lines.append(f":clipboard: *APPROACH{' \u2014 ' + score_str if score_str else ''}*")
        for p in params:
            result = p.get("result", "")
            icon   = "\u2705" if result == "PASS" else ("\u274c" if result == "FAIL" else "\u2139\ufe0f")
            parts  = []
            if p.get("value"):
                parts.append(p["value"])
            if p.get("required"):
                parts.append(f"req {p['required']}")
            detail = "  _" + "  ".join(parts) + "_" if parts else ""
            lines.append(f">{icon} {p['parameter']}{detail}")

    if url:
        lines.append("")
        lines.append(f"<{url}|View on Flysto>")

    return "\n".join(lines)


def _airsync_notify(flight: dict, pending: dict, token: str) -> None:
    pilots_cfg = _load_pilots_cfg()
    slack_user = pending["slack_user"]
    channel_id = pending["channel_id"]
    is_jerry   = slack_user == _JERRY_ID

    msg = _fmt_airsync_msg(flight, slack_user, pilots_cfg)

    if is_jerry:
        _post(token, channel_id, msg)
    else:
        # DM Jerry with the full details
        dm_channel = _open_dm(token, _JERRY_ID)
        _post(token, dm_channel, msg)


def _airsync_notify_timeout(pending: dict, token: str) -> None:
    try:
        dm_channel = _open_dm(token, _JERRY_ID)
        _post(token, dm_channel,
              ":warning: *AirSync timed out* \u2014 no new flight found on Flysto after 10 minutes.")
    except Exception as e:
        print(f"AirSync: failed to send timeout DM: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Load flysto credentials
    load_dotenv("flysto.env")
    # Load main app .env for Slack tokens
    load_dotenv(_APP_ENV)

    # --- AirSync gate: only run if a log is pending ---
    pending = _load_pending()
    if pending is None:
        print("No AirSync pending — exiting.")
        return

    import time as _time
    created_at = pending.get("created_at", 0)
    age        = _time.time() - created_at
    token      = os.getenv("N900JV_BOT_TOKEN", "")

    if age > _AIRSYNC_TIMEOUT:
        print(f"AirSync: pending request timed out ({age:.0f}s old).")
        _airsync_notify_timeout(pending, token)
        _clear_pending()
        return

    # Normal path: check for new flight on Flysto
    email    = os.getenv("FLYSTO_EMAIL")
    password = os.getenv("FLYSTO_PASSWORD")
    last_id  = os.getenv("FLYSTO_LOG_ID", "")

    if not email or not password:
        print("Error: FLYSTO_EMAIL and FLYSTO_PASSWORD must be set.")
        sys.exit(1)
    if not last_id:
        print("Error: FLYSTO_LOG_ID must be set.")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()

        print(f"Logging in as {email}...")
        login(page, email, password)

        from_date   = (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")
        flights     = scrape_flights(page, from_date)

        new_flights = []
        for f in flights:
            if f["id"] == last_id:
                break
            new_flights.append(f)

        if not new_flights:
            browser.close()
            print("No new flights found yet — will retry next minute.")
            return   # leave pending file in place

        print(f"Found {len(new_flights)} new flight(s) — processing latest.")

        f        = new_flights[0]
        print(f"Scraping detail for log {f['id']} ({f['date']})...")
        detail   = scrape_log_detail(page, f["id"])
        detail["id"]   = f["id"]
        detail["date"] = f["date"]

        browser.close()

        # Advance the baseline ID so next cron run ignores this flight
        update_env("FLYSTO_LOG_ID", f["id"])
        print(f"Updated FLYSTO_LOG_ID → {f['id']}")

        # Update SQLite fuel directly on liono
        _update_fuel_sqlite(detail, pending["slack_user"], pending["table"])

        # Send Slack notification
        _airsync_notify(detail, pending, token)
        print("AirSync: Slack notification sent.")

        # Done — remove pending file so cron becomes a no-op again
        _clear_pending()
        print("AirSync: pending file cleared.")


if __name__ == "__main__":
    main()
