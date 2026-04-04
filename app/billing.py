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
# Allow imports from the same directory when run directly
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))

from dateutil.relativedelta import relativedelta
from mailer import send_to_pilot
from tbm import TBM, _PILOT_CFG, get_pilot_name

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RATE = 700          # $/hr — hourly billing rate
_DB  = os.environ.get("DB_PATH", "/data/logbook.db")


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
def _date_where(test: bool) -> str:
    if test:
        return "date >= date('now','start of month')"
    return (
        "date >= date('now','start of month','-1 month') "
        "AND date < date('now','start of month')"
    )


def _report_month(test: bool) -> str:
    if test:
        return datetime.now().strftime("%B %Y")
    return (datetime.now() - relativedelta(months=1)).strftime("%B %Y")


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
    flight_charge = flight_hrs * RATE

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

        for flight_date, f_uid, f_hrs, note in flights:
            # hobbs uid = flight-type uid + 3
            hobbs_uid  = f_uid + 3
            prev_hobbs = inst.getPreviousFlightUid(hobbs_uid)

            dep_fuel = inst.getFlightFuel(prev_hobbs) if prev_hobbs else None
            arr_fuel = inst.getFlightFuel(hobbs_uid)

            dep_str = f"{int(dep_fuel)}" if dep_fuel is not None else "--"
            arr_str = f"{int(arr_fuel)}"

            fuel_used = 0.0
            fuel_str  = ""
            if dep_fuel is not None and dep_fuel > arr_fuel:
                fuel_used      = dep_fuel - arr_fuel
                fuel_charge_fl = fuel_used * fuel_price
                fuel_total_gal += fuel_used
                fuel_str = f"  [{int(fuel_used)}g / ${fuel_charge_fl:,.2f}]"

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
def build_report(pilot_name: str, aircraft: list, test: bool = False) -> str:
    """
    Build a full billing report for the named pilot across the given aircraft list.

    pilot_name : case-insensitive name matching pilots.json
    aircraft   : list of TBM subclasses to bill against
    test       : if True, use current month; otherwise previous month
    """
    # Resolve pilot from config
    pilot = None
    for p in _PILOT_CFG["pilots"].values():
        if p["name"].lower() == pilot_name.lower():
            pilot = p
            break

    if pilot is None:
        return f"ERROR: Pilot '{pilot_name}' not found in pilots.json"

    date_where   = _date_where(test)
    month_label  = _report_month(test)

    separator = "─" * 48

    lines = [
        f"BILLING REPORT — {pilot['name'].upper()}",
        f"Period: {month_label}",
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
        total_flight_charge += f_hrs * RATE
        total_fuel_charge   += subtotal - (f_hrs * RATE) - receipts
        total_receipts      += receipts

        squawk_sections.append(_squawks_section(inst, date_where))

        inst.con.close()

    # ── Grand total ───────────────────────────────────────────────────────
    grand_total = total_flight_charge + total_fuel_charge + total_receipts
    lines.append(separator)
    lines.append(
        f"BALANCE DUE:  ${grand_total:,.2f}"
        f"  ({total_flight_hrs:.1f} hrs across {len(aircraft)} aircraft)"
    )
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
    parser.add_argument("--test",     action="store_true", help="Use current month instead of previous")
    parser.add_argument("--send",     action="store_true", help="Email report to pilot")
    args = parser.parse_args()

    # Validate aircraft
    unknown = [a for a in args.aircraft if a.upper() not in AIRCRAFT]
    if unknown:
        print(f"ERROR: Unknown aircraft: {unknown}.  Available: {list(AIRCRAFT.keys())}")
        sys.exit(1)

    plane_classes = [AIRCRAFT[a.upper()] for a in args.aircraft]
    report        = build_report(args.pilot, plane_classes, test=args.test)

    print(report)

    if args.send:
        month = _report_month(args.test)
        subject = f"Billing Report — {args.pilot.title()} — {month}"
        results = send_to_pilot(args.pilot, subject, report)
        if results:
            for name, ok in results.items():
                print(f"\nEmail → {name}: {'SENT' if ok else 'FAILED'}")
        else:
            print("\nWARNING: No email addresses configured for this pilot.")


if __name__ == "__main__":
    main()
