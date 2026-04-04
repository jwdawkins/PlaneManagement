"""
billing.py
----------
Generates a monthly billing report for a pilot across one or more aircraft
and optionally emails it via mailer.py.

Usage:
    python3 billing.py --pilot jerry [--test] [--send]

    --pilot  Pilot name (case-insensitive, matched against pilots.json)
    --test   Bill current month instead of the previous month (for testing)
    --send   Email the report to the pilot's address(es) from pilots.json

Environment variables (or .env):
    DB_PATH        Path to logbook.db       (default: /data/logbook.db)
    PILOTS_JSON    Path to pilots.json      (default: /data/pilots.json)
    MAIL_FROM      Sending Gmail address
    MAIL_APP_PASS  Google App Password
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env before any other imports so PILOTS_JSON / DB_PATH are set
# in time for tbm.py's module-level _load_pilots() call.
# ---------------------------------------------------------------------------
_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# Fall back to sibling data/ directory if PILOTS_JSON still not set
# (covers running directly from the app/ folder without a .env)
os.environ.setdefault(
    "PILOTS_JSON",
    str(Path(__file__).parent.parent / "data" / "pilots.json"),
)
os.environ.setdefault(
    "DB_PATH",
    str(Path(__file__).parent.parent / "data" / "logbook.db"),
)

# ---------------------------------------------------------------------------
# Allow imports from the same directory when run directly
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))

from dateutil.relativedelta import relativedelta
from mailer import send_to_pilot
from tbm import TBM, _PILOT_CFG, get_pilot_name

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DB   = os.environ.get("DB_PATH")
_RATE = int(_PILOT_CFG.get("config", {}).get("rate", 700))   # $/hr from pilots.json


# ---------------------------------------------------------------------------
# Aircraft registry — add new aircraft here
# ---------------------------------------------------------------------------
class _N900JV(TBM):
    DB    = _DB
    TABLE = "logs_n900jv"


class _N188CD(TBM):
    DB    = _DB
    TABLE = "logs_n188cd"


AIRCRAFT = {
    "N900JV": _N900JV,
    "N188CD": _N188CD,
}


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def _default_period() -> datetime:
    """First day of the previous month."""
    return (datetime.now().replace(day=1) - relativedelta(months=1))


def _parse_month(month_str: str) -> datetime:
    """
    Parse a month string into a datetime for the 1st of that month.
    Accepts:
        "April"         — April of the current year
        "April 2025"    — April 2025
    """
    parts = month_str.strip().split()
    if len(parts) == 1:
        return datetime.strptime(f"{parts[0]} {datetime.now().year}", "%B %Y")
    elif len(parts) == 2:
        return datetime.strptime(month_str.strip(), "%B %Y")
    raise ValueError(f"Invalid month format {month_str!r}. Use 'April' or 'April 2025'.")


def _date_where(period: datetime) -> str:
    """SQL WHERE clause covering the full calendar month of period."""
    start = period.strftime("%Y-%m-%d")
    end   = (period + relativedelta(months=1)).strftime("%Y-%m-%d")
    return f"date >= '{start}' AND date < '{end}'"


# ---------------------------------------------------------------------------
# Per-aircraft billing section
# ---------------------------------------------------------------------------
def _aircraft_section(inst: TBM, pilot: dict, date_where: str) -> tuple[str, float, float, float]:
    """
    Build the billing section for one aircraft.

    Returns:
        (text, flight_hrs, flight_charge, fuel_charge)
    """
    flight_type  = pilot["flight_type"]
    receipt_type = pilot["receipt_type"]
    cur          = inst.con.cursor()
    tail         = inst.TABLE.replace("logs_", "").upper()
    fuel_price   = float(inst.sqlReadLatest(14) or 0)

    # ── Flight time ───────────────────────────────────────────────────────
    cur.execute(
        f"SELECT sum(valuen) FROM {inst.TABLE} WHERE type=? AND {date_where}",
        (flight_type,),
    )
    flight_hrs = float(cur.fetchone()[0] or 0)
    flight_charge = flight_hrs * _RATE

    lines = []
    lines.append(f"{tail} FLIGHT TIME: {flight_hrs:.1f} hrs  [${flight_charge:,.2f}]")

    fuel_total_gal = 0.0

    if flight_hrs > 0:
        cur.execute(
            f"SELECT date, uid, valuen, note FROM {inst.TABLE} "
            f"WHERE type=? AND {date_where} ORDER BY uid ASC",
            (flight_type,),
        )
        flights = [
            (
                datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f"),
                row[1],   # flight-type uid
                float(row[2]),  # flight hrs
                row[3],   # note
            )
            for row in cur.fetchall()
        ]

        block_dep_fuel = None  # departure fuel for the current pilot "block"
        last_f_uid     = flights[-1][1]

        for flight_date, f_uid, f_hrs, note in flights:
            hobbs_uid  = f_uid + 3
            prev_hobbs = inst.getPreviousFlightUid(hobbs_uid)
            next_hobbs = inst.getNextFlightUid(hobbs_uid)

            # Show departure when the previous flight was a different pilot
            # (plane is being handed TO this pilot) or there is no previous flight.
            prev_pilot = inst.getFlightPilot(prev_hobbs) if prev_hobbs else None
            show_dep   = prev_pilot != flight_type

            # Show arrival when the next flight is a different pilot
            # (plane is being handed BACK), there is no next flight, OR this is
            # the last flight in the billing period (next flight may be outside window).
            next_pilot = inst.getFlightPilot(next_hobbs) if next_hobbs else None
            show_arr   = (next_pilot != flight_type) or (f_uid == last_f_uid)

            if show_dep:
                block_dep_fuel = inst.getFlightFuel(prev_hobbs) if prev_hobbs else None
                dep_str = f"{int(block_dep_fuel)}" if block_dep_fuel is not None else "--"
            else:
                dep_str = "--"

            arr_fuel = inst.getFlightFuel(hobbs_uid)
            arr_str  = f"{int(arr_fuel)}" if show_arr else "--"

            # Fuel used = departure of this block minus arrival, only when both are known
            fuel_str = ""
            if show_arr and block_dep_fuel is not None:
                fuel_used      = block_dep_fuel - arr_fuel
                if fuel_used > 0:
                    fuel_charge_fl  = fuel_used * fuel_price
                    fuel_total_gal += fuel_used
                    fuel_str = f"  [{int(fuel_used)}g / ${fuel_charge_fl:,.2f}]"
                block_dep_fuel = None  # reset for next block

            note_str = f"  — {note}" if note else ""
            lines.append(
                f"  {flight_date.strftime('%b %d')}  [{f_hrs:.1f}hrs]"
                f"  D:{dep_str}  A:{arr_str}{fuel_str}{note_str}"
            )

    fuel_charge    = fuel_total_gal * fuel_price
    subtotal       = flight_charge + fuel_charge

    lines.append(
        f"  Subtotal: ${flight_charge:,.2f} flight"
        + (f"  +  ${fuel_charge:,.2f} fuel ({int(fuel_total_gal)}g @ ${fuel_price:.2f}/gal)" if fuel_total_gal else "")
        + f"  =  ${subtotal:,.2f}"
    )

    # ── Receipts ──────────────────────────────────────────────────────────
    receipt_total = 0.0
    receipt_lines = []

    if receipt_type is not None:
        cur.execute(
            f"SELECT sum(valuen) FROM {inst.TABLE} WHERE type=? AND {date_where}",
            (receipt_type,),
        )
        receipt_total = float(cur.fetchone()[0] or 0)

        if receipt_total > 0:
            cur.execute(
                f"SELECT date, valuen, note FROM {inst.TABLE} "
                f"WHERE type=? AND {date_where} ORDER BY uid ASC",
                (receipt_type,),
            )
            for row in cur.fetchall():
                d        = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").strftime("%b %d")
                note_str = f"  — {row[2]}" if row[2] else ""
                receipt_lines.append(f"  {d}  ${float(row[1]):,.2f}{note_str}")

    if receipt_lines:
        lines.append(f"\n{tail} RECEIPTS: ${receipt_total:,.2f}")
        lines.extend(receipt_lines)

    return "\n".join(lines), flight_hrs, subtotal, receipt_total


# ---------------------------------------------------------------------------
# Squawks section (per aircraft)
# ---------------------------------------------------------------------------
def _squawks_section(inst: TBM, date_where: str) -> str:
    tail = inst.TABLE.replace("logs_", "").upper()
    cur  = inst.con.cursor()

    cur.execute(
        f"SELECT date, number, valuen FROM {inst.TABLE} "
        f"WHERE type=2 AND {date_where} ORDER BY date ASC",
    )
    rows = cur.fetchall()

    if not rows:
        return f"{tail} SQUAWKS: none"

    lines = [f"{tail} SQUAWKS:"]
    for row in rows:
        d          = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").strftime("%b %d")
        pilot_name = get_pilot_name(row[1])
        lines.append(f"  {d}  [{pilot_name}]: {row[2]}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main report builder
# ---------------------------------------------------------------------------
def build_report(pilot_name: str, aircraft: list, period: datetime = None) -> str:
    """
    Build a full billing report for the named pilot across the given aircraft list.

    pilot_name : case-insensitive name matching pilots.json
    aircraft   : list of TBM subclasses to bill against
    period     : datetime for the 1st of the target month; defaults to previous month
    """
    if period is None:
        period = _default_period()

    # Resolve pilot from config
    pilot = None
    for p in _PILOT_CFG["pilots"].values():
        if p["name"].lower() == pilot_name.lower():
            pilot = p
            break

    if pilot is None:
        return f"ERROR: Pilot '{pilot_name}' not found in pilots.json"

    date_where  = _date_where(period)
    month_label = period.strftime("%B %Y")

    separator = "─" * 48

    lines = [
        f"TBM Billing Report — {pilot['name'].title()} — {month_label}",
        separator,
        "",
    ]

    total_flight_hrs    = 0.0
    total_flight_charge = 0.0
    total_fuel_charge   = 0.0
    total_receipts      = 0.0
    squawk_sections     = []

    for PlaneClass in aircraft:
        inst = PlaneClass()

        section, f_hrs, subtotal, receipts = _aircraft_section(inst, pilot, date_where)
        lines.append(section)
        lines.append("")

        total_flight_hrs    += f_hrs
        total_flight_charge += f_hrs * _RATE
        total_fuel_charge   += subtotal - (f_hrs * _RATE) - receipts
        total_receipts      += receipts

        squawk_sections.append(_squawks_section(inst, date_where))

        inst.con.close()

    # ── Grand total ───────────────────────────────────────────────────────
    grand_total = total_flight_charge + total_fuel_charge + total_receipts
    lines.append(separator)
    lines.append(f"BALANCE DUE:  ${grand_total:,.2f}")
    lines.append(separator)

    # ── Squawks ───────────────────────────────────────────────────────────
    if any(squawk_sections):
        lines.append("")
        lines.extend(squawk_sections)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    # Load .env if present (local testing outside Docker)
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    parser = argparse.ArgumentParser(description="PlaneManagement billing report")
    parser.add_argument("--pilot",    required=True, help="Pilot name (e.g. jerry)")
    parser.add_argument("--aircraft", nargs="+",     help="Aircraft tail numbers (default: all)",
                        default=list(AIRCRAFT.keys()))
    parser.add_argument("--month",    default=None,
                        help="Month to bill: 'April' (current year) or 'April 2025' (defaults to previous month)")
    parser.add_argument("--send",     default=None, metavar="PILOT",
                        help="Email report to this pilot's addresses (e.g. --send jerry)")
    args = parser.parse_args()

    # Resolve billing period
    if args.month:
        try:
            period = _parse_month(args.month)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
    else:
        period = _default_period()

    # Validate aircraft
    unknown = [a for a in args.aircraft if a.upper() not in AIRCRAFT]
    if unknown:
        print(f"ERROR: Unknown aircraft: {unknown}.  Available: {list(AIRCRAFT.keys())}")
        sys.exit(1)

    plane_classes = [AIRCRAFT[a.upper()] for a in args.aircraft]
    report        = build_report(args.pilot, plane_classes, period=period)

    print(report)

    if args.send:
        month   = period.strftime("%B %Y")
        subject = f"TBM Billing Report — {args.pilot.title()} — {month}"
        results = send_to_pilot(args.send, subject, report)
        if results:
            for name, ok in results.items():
                print(f"\nEmail → {name}: {'SENT' if ok else 'FAILED'}")
        else:
            print(f"\nWARNING: No email addresses configured for '{args.send}'.")


if __name__ == "__main__":
    main()
