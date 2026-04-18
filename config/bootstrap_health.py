from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)


def _get_database_url() -> str | None:
    return os.getenv("DATABASE_URL")


def check_db_health() -> bool:
    """Basic health check for the PostgreSQL backend."""
    url = _get_database_url()
    if not url:
        logger.error("DATABASE_URL is not set in environment; cannot perform DB health check")
        return False

    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.exception("Database health check failed: %s", e)
        return False
