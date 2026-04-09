"""Email notification system for watched dockets.

Usage:
  python -m ujs notify              # Send daily digests
  python -m ujs notify --immediate  # Send immediate alerts
  python -m ujs notify --dry-run    # Preview without sending
"""

import html as html_mod
import os, smtplib, logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict

from ujs import db

logger = logging.getLogger("ujs.notify")

SITE_URL = os.environ.get("SITE_URL", "https://gavelsearch.com")


def _get_smtp_config():
    return {
        "host": os.environ.get("SMTP_HOST", ""),
        "port": int(os.environ.get("SMTP_PORT", "587")),
        "user": os.environ.get("SMTP_USER", ""),
        "pass": os.environ.get("SMTP_PASS", ""),
        "from": os.environ.get("SMTP_FROM", "alerts@gavelsearch.com"),
    }


def send_email(to, subject, html_body, text_body):
    """Send an email via SMTP. Returns True on success."""
    cfg = _get_smtp_config()
    if not cfg["host"] or not cfg["user"]:
        logger.warning("SMTP not configured — skipping email to %s", to)
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = to
    msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
            server.starttls()
            server.login(cfg["user"], cfg["pass"])
            server.send_message(msg)
        logger.info("Email sent to %s: %s", to, subject)
        return True
    except Exception as e:
        logger.error("Failed to send email to %s: %s", to, e)
        return False


def _render_email(user_email, changes_by_docket, unsubscribe_token):
    """Render notification email. Returns (subject, html, text)."""
    n_dockets = len(changes_by_docket)
    n_changes = sum(len(v) for v in changes_by_docket.values())
    subject = f"GavelSearch: {n_changes} update{'s' if n_changes != 1 else ''} on {n_dockets} watched docket{'s' if n_dockets != 1 else ''}"

    unsub_url = f"{SITE_URL}/unsubscribe/{unsubscribe_token}" if unsubscribe_token else ""

    # Plain text
    text_lines = [subject, "=" * 40, ""]
    for dn, changes in changes_by_docket.items():
        caption = changes[0].get("caption") or dn
        county = changes[0].get("county") or ""
        text_lines.append(f"{dn} — {caption}" + (f" ({county})" if county else ""))
        for c in changes:
            ct = c.get("change_type", "change")
            field = c.get("field_name", "")
            new_val = c.get("new_value", "")
            text_lines.append(f"  - {ct}: {field} → {new_val}" if field else f"  - {ct}: {new_val}")
        text_lines.append(f"  View: {SITE_URL}/chat?q={dn}")
        text_lines.append("")
    if unsub_url:
        text_lines.extend(["---", f"Unsubscribe: {unsub_url}"])
    text_body = "\n".join(text_lines)

    # HTML
    _e = html_mod.escape  # shorthand
    docket_rows = ""
    for dn, changes in changes_by_docket.items():
        caption = _e(changes[0].get("caption") or dn)
        county = _e(changes[0].get("county") or "")
        dn_safe = _e(dn)
        change_items = ""
        for c in changes:
            ct = _e(c.get("change_type", "change"))
            field = _e(c.get("field_name", ""))
            new_val = _e(c.get("new_value", ""))
            detail = f"{field}: {new_val}" if field else new_val
            change_items += f'<li style="color:#8fa8c8;font-size:13px;margin:4px 0">{ct} — {detail}</li>'

        docket_rows += f"""
        <div style="background:#132240;border:1px solid #1e3254;border-radius:8px;padding:16px;margin-bottom:12px">
          <div style="font-size:14px;font-weight:600;color:#e8edf5">{caption}</div>
          <div style="font-size:11px;color:#5a7aa0;margin-top:2px">{dn_safe}{(' — ' + county + ' County') if county else ''}</div>
          <ul style="list-style:none;padding:0;margin:10px 0 0 0">{change_items}</ul>
          <a href="{SITE_URL}/chat?q={dn_safe}" style="display:inline-block;margin-top:10px;font-size:12px;color:#c8a03a;text-decoration:none">View in GavelSearch →</a>
        </div>"""

    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#0f1e38;font-family:-apple-system,system-ui,sans-serif">
<div style="max-width:560px;margin:0 auto;padding:24px 16px">
  <div style="text-align:center;margin-bottom:24px">
    <span style="font-size:15px;font-weight:600;color:#e8edf5">Gavel</span><span style="font-size:15px;font-weight:600;color:#c8a03a">Search</span>
  </div>
  <div style="font-size:13px;color:#8fa8c8;text-align:center;margin-bottom:20px">
    {n_changes} update{'s' if n_changes != 1 else ''} on your watched dockets
  </div>
  {docket_rows}
  <div style="text-align:center;margin-top:24px;padding-top:16px;border-top:1px solid #1e3254">
    <a href="{SITE_URL}/chat" style="font-size:12px;color:#c8a03a;text-decoration:none;margin-right:16px">Open GavelSearch</a>
    <a href="{SITE_URL}/settings" style="font-size:12px;color:#5a7aa0;text-decoration:none;margin-right:16px">Manage Watches</a>
    {f'<a href="{unsub_url}" style="font-size:12px;color:#5a7aa0;text-decoration:none">Unsubscribe</a>' if unsub_url else ''}
  </div>
  <div style="text-align:center;margin-top:16px;font-size:10px;color:#4a6a99">
    GavelSearch — PA UJS Court Records — Not legal advice
  </div>
</div>
</body></html>"""

    return subject, html_body, text_body


def run_notifications(frequency='daily', dry_run=False):
    """Main entry point. Query pending changes and send grouped emails."""
    with db.connect() as conn:
        rows = db.get_pending_notifications(conn, frequency=frequency)

    if not rows:
        logger.info("No pending notifications for frequency=%s", frequency)
        return 0

    # Group by user
    by_user = defaultdict(lambda: {"email": "", "token": "", "dockets": defaultdict(list)})
    for r in rows:
        uid = r["user_id"]
        by_user[uid]["email"] = r["user_email"]
        by_user[uid]["token"] = r.get("unsubscribe_token") or ""
        by_user[uid]["dockets"][r["docket_number"]].append(r)

    sent = 0
    for uid, data in by_user.items():
        subject, html, text = _render_email(data["email"], dict(data["dockets"]), data["token"])

        if dry_run:
            logger.info("[DRY RUN] Would send to %s: %s (%d dockets)",
                        data["email"], subject, len(data["dockets"]))
        else:
            ok = send_email(data["email"], subject, html, text)
            if ok:
                with db.connect() as conn:
                    db.mark_notified(conn, uid, list(data["dockets"].keys()))
                sent += 1
            else:
                logger.error("Failed notification for user %s", uid)

    logger.info("Notifications complete: %d/%d users notified (frequency=%s, dry_run=%s)",
                sent, len(by_user), frequency, dry_run)
    return sent


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Send docket watch notifications")
    parser.add_argument("--immediate", action="store_true", help="Send immediate alerts instead of daily digest")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending emails")
    args = parser.parse_args()

    freq = "immediate" if args.immediate else "daily"
    run_notifications(frequency=freq, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
