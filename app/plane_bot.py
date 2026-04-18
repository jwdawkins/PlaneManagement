"""
plane_bot.py
------------
Runs two Slack bots simultaneously — one for N900JV and one for N188CD.
Each bot listens for messages in its Slack app, passes them to the TBM
engine (tbm.py) pointed at the correct log table, and replies with
the result formatted using Slack mrkdwn.

Environment variables (set in docker-compose or .env):
    DB_PATH           Path to logbook.db         (default: /data/logbook.db)
    PILOTS_JSON       Path to pilots.json         (default: /data/pilots.json)

    N900JV_BOT_TOKEN  Slack bot token for N900JV
    N900JV_APP_TOKEN  Slack app-level token for N900JV
    N188CD_BOT_TOKEN  Slack bot token for N188CD
    N188CD_APP_TOKEN  Slack app-level token for N188CD

Usage:
    python3 plane_bot.py
"""

import json
import logging
import os
import re
import threading
import time

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from tbm import TBM

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AirSync — pending file path (written by bot, read by flysto_pull.py on liono)
# ---------------------------------------------------------------------------
AIRSYNC_PENDING = os.environ.get("AIRSYNC_PENDING", "/data/airsync_pending.json")

# ---------------------------------------------------------------------------
# Aircraft subclasses — only DB + TABLE differ; all logic lives in TBM
# ---------------------------------------------------------------------------
_DB = os.environ.get("DB_PATH", "/data/logbook.db")


class _N900JV(TBM):
    DB    = _DB
    TABLE = "logs_n900jv"


class _N188CD(TBM):
    DB    = _DB
    TABLE = "logs_n188cd"


# ---------------------------------------------------------------------------
# Slack mrkdwn formatters
# ---------------------------------------------------------------------------

def _val(pattern: str, text: str, default: str = "—") -> str:
    """Return first regex capture group from text, or default."""
    m = re.search(pattern, text)
    return m.group(1).strip() if m else default


def _fmt_status(raw: str, plane_name: str) -> dict:
    """Return a Block Kit payload for the status command."""
    oil    = _val(r"OIL:\s*([\d.]+)", raw)
    fuelp  = _val(r"FUEL:\s*([\d.]+)\n", raw)
    hobbs  = _val(r"HOBBS:\s*([\d.]+)", raw)
    days   = _val(r"HOBBS:.*?\[(\d+) days\]", raw)
    annual = _val(r"ANNUAL:\s*(\S+)", raw)
    l_fuel = _val(r"L:\s*(\d+) Gal", raw)
    r_fuel = _val(r"R:\s*(\d+) Gal", raw)

    details  = ":clipboard: *Details*\n"
    details += f">*Oil:* {oil} hrs  (AeroShell 560)\n"
    details += f">*Hobbs:* {hobbs}  [_{days} days ago_]\n"
    details += f">*Annual:* {annual}\n"
    details += f">*Fuel Price:* ${fuelp}/gal"

    fuel_bod  = ":fuelpump: *Fuel on Board*\n"
    fuel_bod += f">*L:* {l_fuel} gal\n"
    fuel_bod += f">*R:* {r_fuel} gal"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": details}},
        {"type": "section", "text": {"type": "mrkdwn", "text": fuel_bod}},
    ]

    # Recent squawks
    squawk_rows = []
    in_squawks = False
    for line in raw.splitlines():
        if "RECENT SQUAWKS" in line:
            in_squawks = True
            continue
        if in_squawks and line.strip():
            m = re.match(r"(\d{4}-\d{2}-\d{2})\s+(.+)", line.strip())
            if m:
                squawk_rows.append(f">*{m.group(1)}*   {m.group(2)}")
            else:
                squawk_rows.append(f">{line.strip()}")

    if squawk_rows:
        squawk_text = ":warning: *Recent Squawks*\n" + "\n".join(squawk_rows)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": squawk_text}})

    return {"blocks": blocks, "text": f"{plane_name} STATUS"}


def _fmt_log(raw: str, cmd_text: str = "") -> str:
    if raw.startswith("Flight Time:"):
        hrs = _val(r"Flight Time:\s*([\d.]+)", raw)
        tokens = cmd_text.strip().split()
        note = " ".join(tokens[4:]).strip() if len(tokens) > 4 else ""
        note_str = f" — _{note}_" if note else ""
        airsync_str = "\n_Fuel pending AirSync..._" if len(tokens) == 2 else ""
        return f":white_check_mark: *Flight logged* — *{hrs} hrs*{note_str}{airsync_str}"
    if raw.startswith("Invalid"):
        return f":warning: *{raw}*"
    return raw


def _fmt_pilot(raw: str) -> str:
    d30  = _val(r"Last 30 Days:\s*([\d.]+)", raw)
    d90  = _val(r"Last 90 Days:\s*([\d.]+)", raw)
    d365 = _val(r"Last 12 Months:\s*([\d.]+)", raw)
    tbm  = _val(r"TBM Time:\s*([\d.]+)", raw)
    tot  = _val(r"Total Time:\s*([\d.]+)", raw)

    msg  = f"*Last 30 Days:*   {d30} hrs\n"
    msg += f"*Last 90 Days:*   {d90} hrs\n"
    msg += f"*Last 12 Months:* {d365} hrs\n"
    msg += f"*TBM Time:*       {tbm} hrs\n"
    if tot != "—":
        msg += f"*Total Time:*     {tot} hrs\n"

    flights = re.findall(r"^([A-Z][a-z]{2} \d{2}) \[(\d+)\] (.*)$", raw, re.MULTILINE)
    if flights:
        msg += "\n*Recent Flights*\n"
        for date, fuel, note in flights:
            note_str = f"  _{note.strip()}_" if note.strip() else ""
            msg += f">{date}   {fuel} gal{note_str}\n"

    return msg.strip()


def _fmt_report(raw: str) -> str:
    this_month = _val(r"THIS MONTH:\s*([\d.]+)", raw)
    six_mo     = _val(r"6 MONTHS:\s*([\d.]+)", raw)
    twelve_mo  = _val(r"12 MONTHS:\s*([\d.]+)", raw)

    msg  = f"*This Month:* {this_month} hrs\n"
    msg += f"*6 Months:*   {six_mo} hrs\n"
    msg += f"*12 Months:*  {twelve_mo} hrs\n"

    # Dynamically parse each pilot block (name on its own line, indented entries follow)
    pilot_blocks = re.findall(r"^(\S+)\n((?:  .+\n?)+)", raw, re.MULTILINE)
    for pilot, block in pilot_blocks:
        msg += f"\n*{pilot}*\n"
        for line in block.strip().splitlines():
            msg += f">{line.strip()}\n"

    return msg.strip()


def _fmt_fuel(raw: str) -> str:
    l_cur   = _val(r"L:\s*(\d+) G", raw)
    r_cur   = _val(r"R:\s*(\d+) G", raw)
    topoff  = _val(r"TOPOFF:\s*(\d+)", raw)
    flight  = _val(r"FLIGHT:\s*(\d+)", raw)
    reserve = _val(r"RESERVE:\s*(\d+)", raw)

    msg  = ":fuelpump: *FUEL CALCULATOR*\n"
    msg += "*Current on Board:*\n"
    msg += f">L: *{l_cur} gal*\n"
    msg += f">R: *{r_cur} gal*\n\n"
    msg += f"*To top off:* {topoff} gal\n"
    msg += f"*Flight fuel:* {flight} gal\n"
    msg += f"*Reserve:* {reserve} gal\n"

    if "!! RESERVE !!" in raw:
        msg += "\n:rotating_light: *LOW RESERVE — CHECK FUEL LOAD*"

    return msg


def _fmt_squawk_report(raw: str) -> str:
    lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
    if not lines:
        return ":white_check_mark: No open squawks."

    msg = ":warning: *OPEN SQUAWKS*\n"
    for line in lines:
        m = re.match(r"([A-Z][a-z]{2} \d{2}) \[(\w+)\]: (.+)", line)
        if m:
            msg += f">*{m.group(1)}*  [{m.group(2)}]  {m.group(3)}\n"
        else:
            msg += f">{line}\n"
    return msg.strip()


def _fmt_help(plane_name: str, slack_id: str = "") -> str:
    from tbm import get_pilot
    cmds = [
        ("`status`",                          "Aircraft status"),
        ("`log [L] [R] [hobbs] [note]`",      "Log a flight"),
        ("`ferry [L] [R] [hobbs]`",           "Log a ferry flight"),
        ("`fuelp [price]`",                   "Update fuel price"),
        ("`oil`",                             "Oil added"),
        ("`squawk [msg]`",                    "Add a squawk"),
        ("`squawk`",                          "List open squawks"),
        ("`receipt [amt] [note]`",            "Log a receipt"),
        ("`annual [YYYY-MM-DD]`",             "Set next annual due date"),
        ("`pilot`",                           "Personal flight stats"),
        ("`report`",                          "Aircraft report"),
        ("`delete [log | receipt | squawk]`", "Delete the last entry"),
        ("`pick`",                            "Recommend aircraft by lowest usage"),
    ]
    p = get_pilot(slack_id)
    if p and p.get("owns"):
        cmds.append(("`usage`", "Aircraft usage balance"))
    msg = ""
    for cmd, desc in cmds:
        msg += f"{cmd}  {desc}\n"
    return msg.strip()


def format_for_slack(raw: str, cmd: str, plane_name: str, slack_id: str = ""):
    """Route raw TBM response through the correct Slack mrkdwn formatter."""
    if raw is None:
        return ":x: No response received from the database."
    raw  = str(raw)
    cmd0 = cmd.lower().split()[0] if cmd.strip() else ""

    if cmd0 == "status":
        return _fmt_status(raw, plane_name)
    if cmd0 in ("log", "ferry"):
        return _fmt_log(raw, cmd)
    if cmd0 == "pilot":
        return _fmt_pilot(raw)
    if cmd0 == "report":
        return _fmt_report(raw)
    if cmd0 == "fuel":
        return _fmt_fuel(raw)
    if cmd0 == "squawk":
        if len(cmd.strip().split()) == 1:
            return _fmt_squawk_report(raw)
        return f":warning: {raw}"
    if cmd0 == "annual":
        date = raw.replace("Annual set: ", "").strip()
        return f":calendar: Annual inspection set for *{date}*"
    if cmd0 == "oil":
        return f":droplet: {raw}"
    if cmd0 == "fuelp":
        return f":fuelpump: {raw}"
    if cmd0 == "receipt":
        return f":memo: {raw}"
    if cmd0 == "delete":
        return f":wastebasket: {raw}"
    if cmd0 == "usage":
        if raw == "Command not available.":
            return f":no_entry: {raw}"
        lines = []
        for line in raw.strip().splitlines():
            # e.g. "N900JV [90.5] - 90%"
            import re
            m = re.match(r"(\S+)\s+\[([\d.]+)\]\s+-\s+(\d+)%", line)
            if m:
                tail, hrs, pct = m.group(1), m.group(2), m.group(3)
                bar = "█" * (int(pct) // 10) + "░" * (10 - int(pct) // 10)
                lines.append(f"*{tail}*  {hrs} hrs  `{bar}` {pct}%")
            else:
                lines.append(line)
        return ":airplane: *Aircraft Usage Balance*\n" + "\n".join(lines)
    if cmd0 == "pick":
        if "is Preferred" in raw:
            # e.g. "N188CD is Preferred  [N900JV 52%  N188CD 48%]"
            import re
            m = re.match(r"(\S+) is Preferred\s+\[(.+)\]", raw.strip())
            if m:
                tail, summary = m.group(1), m.group(2)
                return f":white_check_mark: *{tail} is Preferred*\n_{summary}_"
        return f":airplane: {raw}"

    return _fmt_help(plane_name, slack_id)


# ---------------------------------------------------------------------------
# Factory: build a Slack App + SocketModeHandler for one aircraft
# ---------------------------------------------------------------------------
def build_handler(
    plane_name: str,
    PlaneClass,
    bot_token: str,
    app_token: str,
    peers: list = None,
    airsync: bool = False,
) -> SocketModeHandler:
    """
    peers:   list of TBM subclasses that should also receive a silent fuelp
             update whenever this bot processes a fuelp command.
    airsync: if True, write an AirSync pending file after every successful log
             so that flysto_pull.py (cron on liono) can pick it up.
    """

    slack_app = App(token=bot_token)

    @slack_app.message()
    def on_message(message, say):
        # Ignore bot messages (including our own replies)
        if message.get("bot_id"):
            return

        text = (message.get("text") or "").strip()
        if not text:
            return

        # Strip Slack entity wrappers; preserve plain user-typed content
        text = re.sub(r"<[@#!][^>]+>", "", text)
        text = re.sub(r"<https?://[^>]*>", "", text)
        text = re.sub(r"<[^|>]+\|([^>]+)>", r"\1", text)
        text = text.strip()

        slack_user = message.get("user", "")
        log.info("[%s] user=%s cmd=%r", plane_name, slack_user, text)

        try:
            plane = PlaneClass()

            # pilot / usage / pick — aggregate across all aircraft by injecting peer instances
            if text.lower().split()[0] in ("pilot", "usage", "pick") and peers:
                plane.peers = [P() for P in peers]

            raw_response = plane.process(text, slack_user)
            response     = format_for_slack(raw_response, text, plane_name, slack_user)

            # AirSync — write pending file after a successful log on N900JV.
            # flysto_pull.py (cron, runs on liono) checks this file each minute,
            # sends the Slack notification directly, and deletes the file when done.
            cmd0 = text.lower().split()[0]
            if airsync and cmd0 == "log" and raw_response.startswith("Flight Time:"):
                channel_id = message.get("channel", "")
                try:
                    with open(AIRSYNC_PENDING, "w") as _pf:
                        json.dump({
                            "channel_id": channel_id,
                            "slack_user": slack_user,
                            "table":      PlaneClass.TABLE,
                            "created_at": time.time(),
                        }, _pf)
                    log.info("[%s] AirSync: pending file written for user %s", plane_name, slack_user)
                except Exception:
                    log.exception("[%s] AirSync: failed to write pending file", plane_name)

            # fuelp — silently mirror the update to all peer aircraft tables
            if text.lower().split()[0] == "fuelp" and peers:
                for PeerClass in peers:
                    try:
                        PeerClass().process(text, slack_user)
                        log.info("[%s] fuelp mirrored to %s", plane_name, PeerClass.TABLE)
                    except Exception:
                        log.exception("[%s] Failed to mirror fuelp to %s", plane_name, PeerClass.TABLE)

        except Exception as exc:
            log.exception("[%s] Error processing command", plane_name)
            response = f":x: *Error:* {exc}"

        # _fmt_status returns a Block Kit dict; everything else is a plain string
        if isinstance(response, dict):
            say(**response)
        else:
            say(response)

    @slack_app.event("app_mention")
    def on_mention(event, say):
        # Treat @bot mentions exactly like direct messages
        on_message(event, say)

    return SocketModeHandler(slack_app, app_token)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    required = {
        "N900JV_BOT_TOKEN": os.environ.get("N900JV_BOT_TOKEN"),
        "N900JV_APP_TOKEN": os.environ.get("N900JV_APP_TOKEN"),
        "N188CD_BOT_TOKEN": os.environ.get("N188CD_BOT_TOKEN"),
        "N188CD_APP_TOKEN": os.environ.get("N188CD_APP_TOKEN"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    handlers = [
        ("N900JV", build_handler("N900JV", _N900JV, required["N900JV_BOT_TOKEN"], required["N900JV_APP_TOKEN"], peers=[_N188CD], airsync=True)),
        ("N188CD", build_handler("N188CD", _N188CD, required["N188CD_BOT_TOKEN"], required["N188CD_APP_TOKEN"], peers=[_N900JV])),
    ]

    threads = []
    for name, handler in handlers:
        t = threading.Thread(target=handler.start, name=name, daemon=True)
        threads.append(t)
        t.start()
        log.info("%s bot started", name)

    log.info("Both bots running. Press Ctrl+C to stop.")
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
