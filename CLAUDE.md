# PlaneManagement — Project Context

## Overview
A Slack-based logbook system for two TBM aircraft managed by Jerry Dawkins.
Runs as a Docker container on a Linux machine called **liono** (`/home/jdawkins/planemanagement`).
GitHub repo: `https://github.com/jwdawkins/PlaneManagement`

---

## Aircraft & Pilots

| Pilot  | Slack ID       | flight_type | receipt_type | hobbs_offset | Owns        |
|--------|----------------|-------------|--------------|--------------|-------------|
| Jerry  | U0AHRJ7PHNC    | 0           | 5            | 1266         | logs_n900jv |
| Rodney | U0ARKQJU932    | 1           | 11           | 0            | —           |
| Mat    | U0AQR19CH44    | 16          | 17           | 0            | logs_n188cd |
| Ferry  | (ferry_pilot)  | 7           | null         | 0            | —           |

- **N900JV** — Jerry's plane, table `logs_n900jv`
- **N188CD** — Mat's plane, table `logs_n188cd`
- Both aircraft share one SQLite database: `/data/logbook.db` (Docker) = `/home/jdawkins/planemanagement/data/logbook.db` (liono)
- Hourly rate: **$700** (set in `data/pilots.json` → `config.rate`)
- Pilot config single source of truth: `data/pilots.json`

---

## File Structure

```
planemanagement/
├── Dockerfile
├── docker-compose.yml
├── .env                        # Slack tokens (gitignored)
├── .gitignore
├── data/
│   ├── pilots.json             # Pilot config — source of truth
│   ├── logbook.db              # SQLite database (gitignored)
│   ├── airsync_pending.json    # AirSync trigger file (written by bot, read by cron)
│   ├── flysto_cron.log         # AirSync cron output log
│   └── billing_cron.log        # Monthly billing cron output log
└── app/
    ├── plane_bot.py            # Slack bot (runs in Docker)
    ├── tbm.py                  # TBM engine / logbook logic
    ├── billing.py              # Standalone billing report script
    ├── mailer.py               # Gmail SMTP email sender
    ├── flysto_pull.py          # Flysto.net scraper + AirSync notifications
    ├── airsync_test.py         # Test script: sends AirSync DM for a given log ID
    ├── flysto.env              # Flysto credentials (gitignored)
    └── venv/                   # Python venv for liono scripts (not Docker)
```

---

## Docker Setup

```yaml
# docker-compose.yml
services:
  planemanagement:
    build: .
    image: planemanagement:local
    restart: unless-stopped
    env_file: .env
    environment:
      - PILOTS_JSON=/data/pilots.json
      - AIRSYNC_PENDING=/data/airsync_pending.json
    volumes:
      - ./data:/data
```

**Rebuild and restart:**
```bash
cd /home/jdawkins/planemanagement
docker compose down && docker compose build --no-cache && docker compose up -d
docker compose logs --tail=20
```

---

## Slack Bot Commands

Both bots (N900JV and N188CD) support all commands. Peer injection means
`pilot`, `usage`, and `pick` aggregate data across both aircraft.

| Command | Description |
|---------|-------------|
| `status` | Aircraft status (oil, hobbs, fuel, squawks) |
| `log HOBBS` | Log flight — fuel placeholders, AirSync fills in fuel |
| `log L R HOBBS [note]` | Log flight with fuel |
| `ferry L R HOBBS` | Log a ferry flight |
| `fuelp PRICE` | Update fuel price (mirrors to both aircraft) |
| `oil` | Record oil added |
| `squawk MSG` | Add a squawk |
| `squawk` | List open squawks |
| `receipt AMT [note]` | Log a receipt |
| `annual YYYY-MM-DD` | Set next annual date |
| `pilot` | Personal flight stats (aggregated across both aircraft) |
| `report` | Aircraft usage report |
| `delete log\|receipt\|squawk` | Delete last entry of that type |
| `usage` | Non-owner usage balance (owners only: Jerry, Mat) |
| `pick` | Recommend aircraft with lowest non-owner usage |

---

## SQLite Log Types

| type | description |
|------|-------------|
| 0    | Jerry flight time |
| 1    | Rodney flight time |
| 2    | Squawk |
| 3    | Oil added |
| 5    | Jerry receipt |
| 6    | Fuel Left (gallons) |
| 7    | Ferry/Other flight time |
| 8    | Fuel Right (gallons) |
| 9    | Hobbs reading |
| 10   | Annual date |
| 11   | Rodney receipt |
| 14   | Fuel price ($/gal) |
| 15   | Fuel-away flight marker |
| 16   | Mat flight time |
| 17   | Mat receipt |

---

## AirSync Feature

Integrates Flysto.net approach scores with Slack notifications after each flight.

**Flow:**
1. Pilot types `log HOBBS` or `log L R HOBBS` in Slack
2. Bot writes `data/airsync_pending.json` with channel_id, slack_user, table, created_at, fuel_logged flag
3. Cron runs `flysto_pull.py` every minute on liono
4. If no pending file → exits immediately (no-op)
5. If pending file > 10 min old → DMs Jerry timeout warning, clears file
6. If new flight found on Flysto:
   - Updates SQLite fuel (L/R) directly from Flysto end-of-flight values
   - Sends Slack Block Kit notification (route button, flags, approach params)
   - Clears pending file
7. If no new flight yet → exits, leaves pending file for next minute

**Notification routing:**
- **Jerry flies** → posts to the channel where `log` was typed
- **Rodney or Mat flies** → DMs Jerry + DMs the pilot
- **Fuel not synced** (2-arg log but Flysto had no fuel data) → warning shown in message

**Test a specific flight:**
```bash
cd /home/jdawkins/planemanagement/app
venv/bin/python airsync_test.py LOG_ID
# e.g.: venv/bin/python airsync_test.py 26xaqqd7
```

**Flysto credentials:** `app/flysto.env`
```
FLYSTO_EMAIL=jdawkins@gmail.com
FLYSTO_PASSWORD=...
FLYSTO_AIRCRAFT=6rp5nv
FLYSTO_LOG_ID=<last processed flight ID — updated automatically>
```

---

## Billing Script

Standalone script, run on liono using the venv.

```bash
cd /home/jdawkins/planemanagement/app

# Single pilot, previous month (default)
venv/bin/python billing.py --pilot rodney

# Multiple pilots
venv/bin/python billing.py --pilot jerry rodney mat

# Specific month
venv/bin/python billing.py --pilot jerry --month April
venv/bin/python billing.py --pilot rodney --month "March 2025"

# Specific aircraft only
venv/bin/python billing.py --pilot rodney --aircraft n900jv

# Generate and email
venv/bin/python billing.py --pilot rodney --month April --send rodney
```

---

## Cron Jobs (liono)

| Schedule | Job |
|----------|-----|
| Every minute | `flysto_pull.py` — AirSync scraper (exits immediately if no pending file) |
| 1st of month, 5AM | `billing.py --pilot rodney --aircraft n900jv n188cd --send rodney jerry` |
| 1st of month, 5AM | `billing.py --pilot rodney jerry --aircraft n188cd --send jerry mat` |
| 1st of month, 5AM | `billing.py --pilot mat --aircraft n900jv --send mat jerry` |
| Daily 3AM | `backup_logbook.sh` |

Logs:
- AirSync: `data/flysto_cron.log`
- Billing: `data/billing_cron.log`

---

## Environment Files

**`.env`** (Docker, gitignored) — Slack bot tokens:
```
N900JV_BOT_TOKEN=xoxb-...
N900JV_APP_TOKEN=xapp-...
N188CD_BOT_TOKEN=xoxb-...
N188CD_APP_TOKEN=xapp-...
DB_PATH=/data/logbook.db
```

**`app/flysto.env`** (liono only, gitignored):
```
FLYSTO_EMAIL=jdawkins@gmail.com
FLYSTO_PASSWORD=...
FLYSTO_AIRCRAFT=6rp5nv
FLYSTO_LOG_ID=<auto-updated>
```

---

## Key Design Decisions

- **Peer injection**: `tbm.py` instances have a `self.peers` list. `plane_bot.py` injects peer instances for cross-aircraft commands (`pilot`, `usage`, `pick`). `fuelp` silently mirrors to all peers.
- **AirSync architecture**: `flysto_pull.py` runs on liono (not Docker) because Playwright needs a real browser. It writes directly to the shared SQLite DB and calls the Slack API directly — no file polling.
- **pilots.json as source of truth**: All pilot identity, types, emails, and ownership resolved from `data/pilots.json`. Never hardcoded in Python except `JERRY_ID` in `flysto_pull.py`.
- **delete log fix**: Uses `BETWEEN uid-3 AND uid` (not `>= uid-3`) to avoid wiping entries added after the log.
- **WAL checkpoint**: tbm.py runs `PRAGMA wal_checkpoint(FULL)` and `PRAGMA journal_mode=DELETE` at end of every `process()` call to keep DB accessible to liono scripts.
