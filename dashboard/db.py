import os
import sqlite3

from flask import current_app, g


def get_db():
    """
    Return a per-request SQLite connection stored in flask.g.

    Opens the database lazily on first call within a request context.
    Uses WAL journal mode and a 5-second busy timeout so the dashboard
    can read safely while the automator is writing.
    """
    if "db" not in g:
        db_path = current_app.config.get("DB_PATH", "/data/songs.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        g.db = conn
    return g.db


def close_db(e=None):
    """Pop and close the per-request database connection."""
    db = g.pop("db", None)
    if db is not None:
        db.close()
