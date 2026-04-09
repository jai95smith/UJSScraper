"""Job watchdog — kills stuck jobs older than 3 minutes.

Run via cron every 2 minutes:
  */2 * * * * cd /opt/ujs && /opt/ujs/.venv/bin/python -m ujs.modules.watchdog >> /opt/ujs/logs/watchdog.log 2>&1
"""

import logging
from ujs import db

logger = logging.getLogger("ujs.watchdog")


def run():
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE chat_jobs
            SET status = 'error', error = 'Timed out (watchdog)', completed_at = NOW()
            WHERE status = 'running' AND created_at < NOW() - INTERVAL '3 minutes'
        """)
        killed = cur.rowcount
        if killed:
            logger.info("Killed %d stuck jobs", killed)
        return killed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    n = run()
    if n:
        print(f"Killed {n} stuck jobs")
