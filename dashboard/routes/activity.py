"""
Pipeline Activity view — shows run history, live pipeline stage, and trigger buttons.

Reads from the pipeline_runs and settings tables that the automator scheduler
writes to each run. Settings configuration has moved to routes/settings.py.

Trigger endpoints use the DB-as-message-bus pattern: the dashboard writes a
flag to the settings table and the scheduler reads it in its sleep loop.
"""

import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, jsonify, render_template

from db import get_db

bp = Blueprint("activity", __name__)


def _get_source_types(db):
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


@bp.route("/activity")
def activity_page():
    """Render the activity page shell — JS fetches data async."""
    db = get_db()
    source_types = _get_source_types(db)
    return render_template("activity.html", source_types=source_types)


@bp.route("/activity/api")
def activity_api():
    """JSON API — pipeline run history, status, and live stage."""
    db = get_db()

    try:
        # Recent runs (last 20)
        runs = []
        try:
            rows = db.execute("""
                SELECT id, run_number, started_at, finished_at, duration_secs,
                       new_songs, downloads_queued, delivered, status,
                       headline, headline_level
                FROM pipeline_runs
                ORDER BY id DESC
                LIMIT 20
            """).fetchall()
            runs = [dict(r) for r in rows]
        except sqlite3.OperationalError:
            pass  # Table doesn't exist yet

        # Settings — read all rows into a dict for easy access
        settings = {}
        try:
            for row in db.execute("SELECT key, value FROM settings").fetchall():
                settings[row["key"]] = row["value"]
        except sqlite3.OperationalError:
            pass

        next_run_at = settings.get("next_run_at")
        scheduled_time = settings.get("scheduled_time", "00:00")

        # Pipeline status: check if any run is currently 'running'
        pipeline_status = "idle"
        try:
            running = db.execute(
                "SELECT COUNT(*) as c FROM pipeline_runs WHERE status = 'running'"
            ).fetchone()
            if running and running["c"] > 0:
                pipeline_status = "running"
        except sqlite3.OperationalError:
            pass

        # Live pipeline stage — written by the automator as it progresses
        pipeline_stage = settings.get("pipeline_stage", "")

        # Last run for the metric card
        last_run = runs[0] if runs else None

        return jsonify({
            "runs": runs,
            "next_run_at": next_run_at,
            "scheduled_time": scheduled_time,
            "pipeline_status": pipeline_status,
            "pipeline_stage": pipeline_stage,
            "last_run": last_run,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Trigger endpoints — DB-as-message-bus pattern
# ---------------------------------------------------------------------------

@bp.route("/activity/trigger-pipeline", methods=["POST"])
def trigger_pipeline():
    """Set the trigger_pipeline flag — scheduler picks it up within 10s."""
    db = get_db()
    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            ("trigger_pipeline", now_str, now_str),
        )
        db.commit()
        return jsonify({"ok": True, "message": "Pipeline run triggered — will start within 10 seconds"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500


@bp.route("/activity/trigger-delivery", methods=["POST"])
def trigger_delivery():
    """Set the trigger_delivery flag — scheduler runs delivery inline within 10s."""
    db = get_db()
    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            ("trigger_delivery", now_str, now_str),
        )
        db.commit()
        return jsonify({"ok": True, "message": "File transfer triggered — will start within 10 seconds"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500
