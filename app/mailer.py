"""
mailer.py
---------
Standalone email utility for PlaneManagement.

Reads pilot email addresses from pilots.json and sends email via a
Gmail account using a Google App Password.

Configuration (environment variables or .env file):
    MAIL_FROM        Sending Gmail address   e.g. alerts@gmail.com
    MAIL_APP_PASS    Google App Password     (16-char, no spaces)
    PILOTS_JSON      Path to pilots.json     (default: /data/pilots.json)

Usage (direct):
    python3 mailer.py --to jerry --subject "Test" --body "Hello"
    python3 mailer.py --to all   --subject "Test" --body "Hello"

Will be invoked by a cron job once tied to the database.
"""

import argparse
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_PILOTS_PATH = os.environ.get(
    "PILOTS_JSON",
    str(Path(__file__).parent.parent / "data" / "pilots.json"),
)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def _load_pilots(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Core send function
# ---------------------------------------------------------------------------
def send_email(
    to_addresses: list[str],
    subject: str,
    body: str,
    from_addr: str = None,
    app_password: str = None,
    html: bool = False,
) -> bool:
    """
    Send an email to one or more addresses via Gmail SMTP.

    Returns True on success, False on failure.
    """
    from_addr    = from_addr    or os.environ.get("MAIL_FROM")
    app_password = app_password or os.environ.get("MAIL_APP_PASS")

    if not from_addr:
        raise ValueError("MAIL_FROM environment variable is not set.")
    if not app_password:
        raise ValueError("MAIL_APP_PASS environment variable is not set.")
    if not to_addresses:
        log.warning("No recipients — skipping send.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addresses)

    mime_type = "html" if html else "plain"
    msg.attach(MIMEText(body, mime_type))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(from_addr, app_password)
            server.sendmail(from_addr, to_addresses, msg.as_string())
        log.info("Email sent to %s | subject: %r", to_addresses, subject)
        return True
    except smtplib.SMTPException as exc:
        log.error("Failed to send email: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Pilot address helpers
# ---------------------------------------------------------------------------
def get_pilot_emails(name_or_all: str, pilots_path: str = _PILOTS_PATH) -> dict[str, list[str]]:
    """
    Return a dict of { pilot_name: [email, ...] } for the given target.

    name_or_all:
        "all"        — every pilot with at least one email address
        "<name>"     — case-insensitive match on pilot name
    """
    cfg     = _load_pilots(pilots_path)
    pilots  = cfg["pilots"]
    target  = name_or_all.lower()
    result  = {}

    for p in pilots.values():
        emails = p.get("emails") or []
        if not emails:
            continue
        if target == "all" or p["name"].lower() == target:
            result[p["name"]] = emails

    return result


def send_to_pilot(
    name_or_all: str,
    subject: str,
    body: str,
    html: bool = False,
    pilots_path: str = _PILOTS_PATH,
) -> dict[str, bool]:
    """
    Send an email to a named pilot (or all pilots).

    Returns { pilot_name: success_bool }.
    """
    targets = get_pilot_emails(name_or_all, pilots_path)

    if not targets:
        log.warning("No email addresses found for target %r", name_or_all)
        return {}

    results = {}
    for pilot_name, addresses in targets.items():
        ok = send_email(addresses, subject, body, html=html)
        results[pilot_name] = ok

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="PlaneManagement mailer")
    parser.add_argument("--to",      required=True,  help="Pilot name or 'all'")
    parser.add_argument("--subject", required=True,  help="Email subject")
    parser.add_argument("--body",    required=True,  help="Email body text")
    parser.add_argument("--html",    action="store_true", help="Send body as HTML")
    args = parser.parse_args()

    results = send_to_pilot(args.to, args.subject, args.body, html=args.html)

    if not results:
        print("No emails sent — check pilot name and pilots.json email entries.")
    else:
        for name, ok in results.items():
            status = "OK" if ok else "FAILED"
            print(f"  {name}: {status}")


if __name__ == "__main__":
    # Load .env if present (for local testing without docker)
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    main()
