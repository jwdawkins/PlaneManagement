"""
tbm.py
------
Core engine for TBM aircraft logbook management.

Subclass TBM and set DB / TABLE class variables to target a specific
aircraft.  Pilot identity is resolved entirely from pilots.json —
no phone numbers or hardcoded IDs anywhere in this file.

Example subclass:
    class N900JV(TBM):
        DB    = "/data/logbook.db"
        TABLE = "logs_n900jv"
"""

import json
import os
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------------------------
# Pilot config — loaded once at module level
# ---------------------------------------------------------------------------
_PILOTS_PATH = os.environ.get(
    "PILOTS_JSON",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "pilots.json"),
)

def _load_pilots(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

_PILOT_CFG = _load_pilots(_PILOTS_PATH)


def get_pilot(slack_id: str) -> dict | None:
    """Return pilot config dict for a Slack user ID, or None if unknown."""
    return _PILOT_CFG["pilots"].get(slack_id)


def get_pilot_name(slack_id: str) -> str:
    """Return human-readable pilot name for a Slack user ID."""
    p = get_pilot(slack_id)
    return p["name"] if p else slack_id


# ---------------------------------------------------------------------------
# TBM engine
# ---------------------------------------------------------------------------
class TBM:
    DB    = "/data/logbook.db"
    TABLE = "logs_n900jv"       # override per aircraft subclass

    # ── init ─────────────────────────────────────────────────────────────────
    def __init__(self):
        self.con = sqlite3.connect(self.DB)
        self.peers = []   # list of TBM instances to aggregate with for pilot report

    # ── command router ───────────────────────────────────────────────────────
    def process(self, txt: str, slack_id: str) -> str:
        cmd = txt.lower().split()

        if not cmd:
            return self._help()

        c0 = cmd[0]

        if c0 == "status" and len(cmd) == 1:
            msg = self.status()

        elif c0 == "log" and (len(cmd) >= 4 or len(cmd) == 2):
            msg = self.log(cmd, slack_id)

        elif c0 == "ferry" and len(cmd) == 4:
            msg = self.log(cmd, "ferry")

        elif c0 == "pilot" and len(cmd) == 1:
            p = get_pilot(slack_id)
            if p is None:
                msg = "Unknown pilot — Slack ID not found in pilots.json"
            else:
                msg = self.pilotReport(p)

        elif c0 == "annual" and len(cmd) == 2:
            d = datetime.strptime(cmd[1], "%Y-%m-%d")
            msg = self.annual(d.strftime("%Y-%m-%d"), slack_id)

        elif c0 == "squawk" and len(cmd) == 1:
            msg = self.squawkreport()

        elif c0 == "squawk":
            msg = self.squawk(txt[7:].strip(), slack_id)

        elif c0 == "receipt":
            msg = self.receipt(txt[8:].strip(), slack_id)

        elif c0 == "report" and len(cmd) == 1:
            msg = self.report()

        elif c0 == "fuel" and len(cmd) == 2:
            msg = self.fuel(float(cmd[1]))

        elif c0 == "fuelp" and len(cmd) == 2:
            msg = self.fuelp(float(cmd[1]), slack_id)

        elif c0 == "oil" and len(cmd) == 1:
            msg = self.oil(slack_id)

        elif c0 == "delete" and len(cmd) == 2:
            msg = self.deleteEntry(cmd[1], slack_id)

        elif c0 == "last" and len(cmd) == 1:
            msg = str(self.getLastUsed())

        elif c0 == "usage" and len(cmd) == 1:
            p = get_pilot(slack_id)
            if p is None or not p.get("owns"):
                msg = "Command not available."
            else:
                msg = self.usage(slack_id)

        elif c0 == "pick" and len(cmd) == 1:
            msg = self.pick()

        else:
            msg = self._help(slack_id)

        # Ensure WAL is flushed and connection closed cleanly
        try:
            cur = self.con.cursor()
            self.con.commit()
            cur.execute("PRAGMA wal_checkpoint(FULL);")
            cur.execute("PRAGMA journal_mode=DELETE;")
            self.con.commit()
        finally:
            self.con.close()

        return msg

    def _help(self, slack_id: str = "") -> str:
        lines = (
            "status\n"
            "ferry [FUEL L] [FUEL R] [HOBBS]\n"
            "log [HOBBS]\n"
            "log [FUEL L] [FUEL R] [HOBBS] [NOTE]\n"
            "delete [log | receipt | squawk]\n"
            "receipt [AMT] [NOTE]\n"
            "oil\n"
            "fuel [GAL]\n"
            "fuelp [PRICE]\n"
            "squawk [MSG]\n"
            "squawk\n"
            "annual [YYYY-MM-DD]\n"
            "report\n"
            "pilot"
        )
        p = get_pilot(slack_id)
        if p and p.get("owns"):
            lines += "\nusage"
        lines += "\npick"
        return lines

    # ── status ───────────────────────────────────────────────────────────────
    def status(self) -> str:
        def _f(val, fmt, default="—"):
            try:
                return format(float(val), fmt) if val is not None else default
            except (TypeError, ValueError):
                return default

        oil_hrs = self.timeSinceLastOil()
        msg  = f"OIL: {oil_hrs:.1f} [AeroShell 560]\n"
        msg += f"FUEL: {_f(self.sqlReadLatest(14), '.2f')}\n\n"
        msg += f"HOBBS: {_f(self.sqlReadLatest(9), '.1f')} [{self.getLastUsed()} days]\n"
        msg += f"ANNUAL: {self.sqlReadLatest(10) or '—'}\n\n"
        msg += f"FUEL:\n"
        msg += f" L: {_f(self.sqlReadLatest(6), '.0f')} Gal\n"
        msg += f" R: {_f(self.sqlReadLatest(8), '.0f')} Gal\n\n"
        msg += "RECENT SQUAWKS\n"
        cur = self.con.cursor()
        for row in cur.execute(
            f"SELECT date, valuen FROM {self.TABLE} WHERE type=? ORDER BY uid DESC LIMIT 3",
            (2,),
        ):
            d = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            msg += f" {d.strftime('%Y-%m-%d')} {row[1]}\n"
        return msg

    # ── log a flight ─────────────────────────────────────────────────────────
    def log(self, cmd: list, slack_id: str) -> str:
        # Resolve pilot flight_type
        if slack_id == "ferry":
            flight_type = _PILOT_CFG["ferry_pilot"]["flight_type"]
        else:
            p = get_pilot(slack_id)
            if p is None:
                return "Unknown pilot — Slack ID not found in pilots.json"
            flight_type = p["flight_type"]

        if len(cmd) == 2:
            # log HOBBS — fuel placeholders; AirSync will fill in real values
            l_fuel = 0.0
            r_fuel = 0.0
            hobbs  = float(cmd[1])
            note   = ""
        else:
            l_fuel = float(cmd[1])
            r_fuel = float(cmd[2])
            hobbs  = float(cmd[3])
            note   = " ".join(cmd[4:])

        prev_hobbs = float(self.sqlReadLatest(9))
        flight_time = hobbs - prev_hobbs

        if flight_time > 10 or flight_time <= 0:
            return f"Invalid calculated flight time (must be 0–10) — got [{flight_time:.1f}]"

        now = datetime.now()
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (now, flight_type, f"{flight_time:.1f}", slack_id),
        )
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (now, 6, l_fuel, slack_id),
        )
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (now, 8, r_fuel, slack_id),
        )
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number,note) VALUES(?,?,?,?,?)",
            (now, 9, hobbs, slack_id, note),
        )
        return f"Flight Time: {flight_time:.1f}"

    # ── pilot report ─────────────────────────────────────────────────────────
    def pilotReport(self, pilot: dict) -> str:
        flight_type  = pilot["flight_type"]
        hobbs_offset = pilot.get("hobbs_offset", 0)
        name         = pilot["name"]

        # All instances to aggregate (self + any peers)
        all_instances = [self] + self.peers

        msg = f"PILOT FLIGHT REPORT — {name}\n\n"

        for label, days in [("Last 30 Days", 30), ("Last 90 Days", 90), ("Last 12 Months", 365)]:
            total = 0.0
            for inst in all_instances:
                cur = inst.con.cursor()
                cur.execute(
                    f"SELECT sum(valuen) FROM {inst.TABLE} WHERE type=? AND date>=date('now',?)",
                    (flight_type, f"-{days} day"),
                )
                total += float(cur.fetchone()[0] or 0)
            msg += f"{label}: {total:.1f}\n"

        total_time = 0.0
        per_aircraft = []
        for inst in all_instances:
            cur = inst.con.cursor()
            cur.execute(
                f"SELECT sum(valuen) FROM {inst.TABLE} WHERE type=?",
                (flight_type,),
            )
            t = float(cur.fetchone()[0] or 0)
            total_time += t
            per_aircraft.append(f"{inst.TABLE.replace('logs_', '').upper()}: {t:.1f}")

        msg += f"TBM Time: {total_time:.1f}  ({', '.join(per_aircraft)})\n"

        if hobbs_offset:
            msg += f"Total Time: {total_time + hobbs_offset:.1f}\n"

        # Recent flights (last month) — across all aircraft
        msg += "\n"
        cutoff = datetime.now() - relativedelta(months=1)
        flights = []
        for inst in all_instances:
            uid = inst.getLastFlightUid()
            while uid is not None:
                row_date_str = inst.getFlightDate(uid)
                d = datetime.strptime(row_date_str, "%Y-%m-%d %H:%M:%S.%f")
                if d < cutoff:
                    break
                if inst.getFlightPilot(uid) == flight_type:
                    prev_uid = inst.getPreviousFlightUid(uid)
                    fuel_used = 292 - int(inst.getFlightFuel(prev_uid)) if prev_uid else 0
                    details = inst.getFlightDetails(uid) or ""
                    aircraft = inst.TABLE.replace("logs_", "").upper()
                    flights.append((d, aircraft, fuel_used, details))
                uid = inst.getPreviousFlightUid(uid)

        # Sort all flights newest-first
        for d, aircraft, fuel_used, details in sorted(flights, key=lambda x: x[0], reverse=True):
            msg += f"{d.strftime('%b %d')} [{aircraft}] [{fuel_used}] {details}\n"

        return msg.strip()

    # ── usage (cross-aircraft balance) ───────────────────────────────────────
    def usage(self, slack_id: str) -> str:
        """
        For owners only (pilots with an 'owns' field in pilots.json).
        Shows non-owner flight time on each aircraft since March 2026,
        and the percentage balance between them.
        """
        CUTOFF = "2026-03-01"

        # All flight types belonging to real pilots
        all_flight_types = list({p["flight_type"] for p in _PILOT_CFG["pilots"].values()})

        all_instances = [self] + self.peers
        results = []

        for inst in all_instances:
            # Find the owner's flight_type for this aircraft
            owner_ft = None
            for p in _PILOT_CFG["pilots"].values():
                if p.get("owns") == inst.TABLE:
                    owner_ft = p["flight_type"]
                    break

            if owner_ft is None:
                continue  # no owner defined — skip

            # Sum time for every non-owner pilot flight type
            non_owner_types = [ft for ft in all_flight_types if ft != owner_ft]
            placeholders     = ",".join("?" * len(non_owner_types))
            cur = inst.con.cursor()
            cur.execute(
                f"SELECT sum(valuen) FROM {inst.TABLE} "
                f"WHERE type IN ({placeholders}) AND date >= ?",
                (*non_owner_types, CUTOFF),
            )
            hrs  = float(cur.fetchone()[0] or 0)
            tail = inst.TABLE.replace("logs_", "").upper()
            results.append((tail, hrs))

        if not results:
            return "No ownership data configured."

        total = sum(hrs for _, hrs in results)
        msg   = ""
        for tail, hrs in results:
            pct  = (hrs / total * 100) if total > 0 else 0
            msg += f"{tail} [{hrs:.1f}] - {pct:.0f}%\n"

        return msg.strip()

    # ── pick (recommend aircraft with lowest non-owner usage) ────────────────
    def pick(self) -> str:
        """
        Available to all pilots.
        Returns the aircraft with the lowest non-owner usage percentage,
        recommending it as the preferred choice.
        """
        CUTOFF = "2026-03-01"
        all_flight_types = list({p["flight_type"] for p in _PILOT_CFG["pilots"].values()})
        all_instances    = [self] + self.peers
        results          = []

        for inst in all_instances:
            owner_ft = None
            for p in _PILOT_CFG["pilots"].values():
                if p.get("owns") == inst.TABLE:
                    owner_ft = p["flight_type"]
                    break
            if owner_ft is None:
                continue

            non_owner_types = [ft for ft in all_flight_types if ft != owner_ft]
            placeholders    = ",".join("?" * len(non_owner_types))
            cur = inst.con.cursor()
            cur.execute(
                f"SELECT sum(valuen) FROM {inst.TABLE} "
                f"WHERE type IN ({placeholders}) AND date >= ?",
                (*non_owner_types, CUTOFF),
            )
            hrs  = float(cur.fetchone()[0] or 0)
            tail = inst.TABLE.replace("logs_", "").upper()
            results.append((tail, hrs))

        if not results:
            return "No ownership data configured."

        total    = sum(hrs for _, hrs in results)
        with_pct = [(tail, hrs, (hrs / total * 100) if total > 0 else 0)
                    for tail, hrs in results]

        preferred = min(with_pct, key=lambda x: x[2])
        summary   = "  ".join(f"{t} {p:.0f}%" for t, _, p in with_pct)
        return f"{preferred[0]} is Preferred  [{summary}]"

    # ── aircraft report ──────────────────────────────────────────────────────
    def report(self) -> str:
        # Collect pilot names and flight types from config
        pilots = [
            (p["name"], p["flight_type"])
            for p in _PILOT_CFG["pilots"].values()
        ]
        # Add "Others" bucket for ferry / unknown flights (flight_type 7)
        pilots.append(("Others", 7))

        # Deduplicate by flight_type
        seen = set()
        unique_pilots = []
        for name, ft in pilots:
            if ft not in seen:
                unique_pilots.append((name, ft))
                seen.add(ft)

        flight_types = [ft for _, ft in unique_pilots]
        type_placeholder = ",".join("?" * len(flight_types))

        cur = self.con.cursor()

        # Totals
        cur.execute(
            f"SELECT sum(valuen) FROM {self.TABLE} WHERE type IN ({type_placeholder}) AND date>=date('now','start of month')",
            flight_types,
        )
        this_month = float(cur.fetchone()[0] or 0)

        cur.execute(
            f"SELECT ifnull(sum(valuen),0) FROM {self.TABLE} WHERE type IN ({type_placeholder}) AND date>=date('now','-6 month')",
            flight_types,
        )
        six_mo = float(cur.fetchone()[0])

        cur.execute(
            f"SELECT ifnull(sum(valuen),0) FROM {self.TABLE} WHERE type IN ({type_placeholder}) AND date>=date('now','-12 month')",
            flight_types,
        )
        twelve_mo = float(cur.fetchone()[0])

        msg  = f"THIS MONTH: {this_month:.1f}\n"
        msg += f"6 MONTHS: {six_mo:.1f}\n"
        msg += f"12 MONTHS: {twelve_mo:.1f}\n\n"

        # Build month buckets (current + 2 previous)
        now = datetime.now()
        month_keys = []
        month_data = {}
        m = now
        for _ in range(3):
            k = m.strftime("%b %y")
            month_keys.append(k)
            month_data[k] = {ft: 0.0 for _, ft in unique_pilots}
            m -= relativedelta(months=1)

        for row in cur.execute(
            f"SELECT date, type, valuen FROM {self.TABLE} WHERE type IN ({type_placeholder}) ORDER BY uid DESC",
            flight_types,
        ):
            d_key = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").strftime("%b %y")
            if d_key in month_data:
                month_data[d_key][row[1]] = month_data[d_key].get(row[1], 0) + float(row[2])

        for name, ft in unique_pilots:
            msg += f"{name}\n"
            for k in month_keys:
                msg += f"  {k} - {month_data[k][ft]:.1f}\n"
            msg += "\n"

        return msg.strip()

    # ── fuel calculator ──────────────────────────────────────────────────────
    def fuel(self, gals: float) -> str:
        l_cur = int(self.sqlReadLatest(6))
        r_cur = int(self.sqlReadLatest(8))
        topoff = 292 - l_cur - r_cur
        reserve = max(0, 292 - gals)

        msg  = f"CURRENT FUEL:\n L: {l_cur} G\n R: {r_cur} G\n\n"
        msg += f"TOPOFF: {topoff}\n"
        msg += f"FLIGHT: {int(gals)}\nRESERVE: {int(reserve)}\n"
        if reserve < 80:
            msg += "\n!! RESERVE !!"
        return msg

    # ── fuel price ───────────────────────────────────────────────────────────
    def fuelp(self, price: float, slack_id: str) -> str:
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (datetime.now(), 14, f"{price:.2f}", slack_id),
        )
        return f"Fuel price updated: ${price:.2f}/gal"

    # ── oil ──────────────────────────────────────────────────────────────────
    def oil(self, slack_id: str) -> str:
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,number) VALUES(?,?,?)",
            (datetime.now(), 3, slack_id),
        )
        return "Oil added."

    # ── squawk ───────────────────────────────────────────────────────────────
    def squawk(self, msg: str, slack_id: str) -> str:
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (datetime.now(), 2, msg, slack_id),
        )
        return "Squawk added."

    def squawkreport(self) -> str:
        cur = self.con.cursor()
        msg = ""
        # Squawks since the last annual
        cur.execute(
            f"""SELECT date, valuen, number FROM {self.TABLE}
                WHERE type=2
                  AND uid>=(SELECT uid FROM {self.TABLE} WHERE type=10 ORDER BY date DESC LIMIT 1)
                ORDER BY date DESC""",
        )
        for row in cur.fetchall():
            d = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").strftime("%b %d")
            pilot_name = get_pilot_name(row[2])
            msg += f"  {d} [{pilot_name}]: {row[1]}\n"
        return msg

    # ── receipt ──────────────────────────────────────────────────────────────
    def receipt(self, msg: str, slack_id: str) -> str:
        p = get_pilot(slack_id)
        log_type = p["receipt_type"] if p else 12  # fallback to "Receipt Other"

        parts = msg.split(None, 1)
        if not parts:
            return "Invalid receipt format. Use: receipt [AMT] [NOTE]"
        amount = f"{float(parts[0]):.2f}"
        note   = parts[1] if len(parts) > 1 else None

        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number,note) VALUES(?,?,?,?,?)",
            (datetime.now(), log_type, amount, slack_id, note),
        )
        return "Receipt added."

    # ── annual ───────────────────────────────────────────────────────────────
    def annual(self, next_ann: str, slack_id: str) -> str:
        self.sqlWrite(
            f"INSERT INTO {self.TABLE}(date,type,valuen,number) VALUES(?,?,?,?)",
            (datetime.now(), 10, next_ann, slack_id),
        )
        return f"Annual set: {next_ann}"

    # ── delete ───────────────────────────────────────────────────────────────
    def deleteEntry(self, entry: str, slack_id: str) -> str:
        if entry == "log":
            uid = self.sqlUIDLatest(9)
            if uid is None:
                return "Nothing to delete."
            # A log entry is exactly 4 consecutive rows: pilot, fuel_L, fuel_R, hobbs
            self.sqlWrite(f"DELETE FROM {self.TABLE} WHERE uid BETWEEN ? AND ?", (uid - 3, uid))
            return "Log entry deleted."

        if entry == "squawk":
            uid = self.sqlUIDLatest(2)
            if uid is None:
                return "No squawk to delete."
            self.sqlWrite(f"DELETE FROM {self.TABLE} WHERE uid=?", (uid,))
            return "Squawk deleted."

        if entry == "receipt":
            p = get_pilot(slack_id)
            if p is None:
                return "Unknown pilot."
            uid = self.sqlUIDLatest(p["receipt_type"])
            if uid is None:
                return "No receipt to delete."
            self.sqlWrite(f"DELETE FROM {self.TABLE} WHERE uid=?", (uid,))
            return "Receipt deleted."

        return "Delete failed. Use: delete [log | receipt | squawk]"

    # ── helpers: flight traversal ─────────────────────────────────────────────
    def getFlightDate(self, uid: int) -> str:
        cur = self.con.cursor()
        cur.execute(f"SELECT date FROM {self.TABLE} WHERE uid=?", (uid,))
        return cur.fetchone()[0]

    def getLastFlightUid(self) -> int | None:
        cur = self.con.cursor()
        cur.execute(f"SELECT uid FROM {self.TABLE} WHERE type=9 ORDER BY date DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None

    def getPreviousFlightUid(self, uid: int) -> int | None:
        cur = self.con.cursor()
        cur.execute(
            f"SELECT uid FROM {self.TABLE} WHERE type=9 AND uid<? ORDER BY date DESC LIMIT 1",
            (uid,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def getNextFlightUid(self, uid: int) -> int | None:
        cur = self.con.cursor()
        cur.execute(
            f"SELECT uid FROM {self.TABLE} WHERE type=9 AND uid>? ORDER BY date ASC LIMIT 1",
            (uid,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def getFlightPilot(self, uid: int) -> int | None:
        """Return the flight_type (pilot type int) for the flight ending at hobbs uid."""
        cur = self.con.cursor()
        cur.execute(
            f"SELECT type FROM {self.TABLE} WHERE uid=? LIMIT 1",
            (uid - 3,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def getFlightDetails(self, uid: int) -> str | None:
        cur = self.con.cursor()
        cur.execute(f"SELECT note FROM {self.TABLE} WHERE uid=?", (uid,))
        row = cur.fetchone()
        return row[0] if row else None

    def getFlightFuel(self, uid: int) -> float:
        """Total fuel (L+R) at the hobbs row uid."""
        cur = self.con.cursor()
        cur.execute(
            f"""SELECT sum(valuen) FROM (
                    SELECT valuen FROM {self.TABLE}
                    WHERE (type=8 OR type=6) AND uid<=?
                    ORDER BY uid DESC LIMIT 2
                )""",
            (uid,),
        )
        return float(cur.fetchone()[0] or 0)

    def isFuelAwayFlight(self, uid: int) -> bool:
        cur = self.con.cursor()
        cur.execute(
            f"SELECT count(uid) FROM {self.TABLE} WHERE type=15 AND valuen=?",
            (uid,),
        )
        return cur.fetchone()[0] > 0

    def getLastUsed(self) -> int:
        cur = self.con.cursor()
        cur.execute(
            f"""SELECT CAST(julianday('now') - julianday(date) AS INTEGER)
                FROM {self.TABLE} WHERE type=9 ORDER BY date DESC LIMIT 1"""
        )
        row = cur.fetchone()
        return row[0] if row else 0

    def timeSinceLastOil(self) -> float:
        """Hobbs hours since the last oil-added entry."""
        cur = self.con.cursor()
        cur.execute(
            f"SELECT uid FROM {self.TABLE} WHERE type=3 ORDER BY uid DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return 0.0
        oil_uid = row[0]
        cur.execute(
            f"SELECT valuen FROM {self.TABLE} WHERE uid<? AND type=9 ORDER BY uid DESC LIMIT 1",
            (oil_uid,),
        )
        row = cur.fetchone()
        hobbs_at_oil = float(row[0]) if row else 0.0
        return float(self.sqlReadLatest(9)) - hobbs_at_oil

    # ── helpers: SQL ──────────────────────────────────────────────────────────
    def sqlWrite(self, sql: str, values: tuple) -> None:
        cur = self.con.cursor()
        cur.execute(sql, values)
        self.con.commit()

    def sqlReadLatest(self, log_type: int):
        cur = self.con.cursor()
        cur.execute(
            f"SELECT valuen FROM {self.TABLE} WHERE type=? ORDER BY uid DESC LIMIT 1",
            (log_type,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def sqlUIDLatest(self, log_type: int) -> int | None:
        cur = self.con.cursor()
        cur.execute(
            f"SELECT uid FROM {self.TABLE} WHERE type=? ORDER BY uid DESC LIMIT 1",
            (log_type,),
        )
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Quick smoke-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    class _TestPlane(TBM):
        DB    = sys.argv[1] if len(sys.argv) > 1 else "/data/logbook.db"
        TABLE = "logs_n900jv"

    plane = _TestPlane()
    cmd   = sys.argv[2] if len(sys.argv) > 2 else "status"
    user  = sys.argv[3] if len(sys.argv) > 3 else "U0AHRJ7PHNC"
    print(plane.process(cmd, user))
