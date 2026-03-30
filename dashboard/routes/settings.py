"""
Settings page — pipeline configuration, file organisation, and folder paths.

Split out from activity.py so the Activity page focuses on run history and
live status while this page handles all user-configurable settings.
"""

import os
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from db import get_db

bp = Blueprint("settings", __name__)

# Path to the .env file mounted into the dashboard container
_ENV_FILE = "/config/.env"


def _read_env_value(key: str) -> str:
    """Read a single value from the .env file."""
    try:
        with open(_ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1]
    except FileNotFoundError:
        pass
    return ""


def _update_env_value(key: str, value: str) -> bool:
    """
    Update a single key=value pair in the .env file.

    Reads the whole file, replaces the matching line, writes it back.
    If the key doesn't exist, appends it. Returns True on success.
    """
    try:
        with open(_ENV_FILE) as f:
            lines = f.readlines()

        found = False
        new_lines = []
        for line in lines:
            if line.strip().startswith(f"{key}="):
                new_lines.append(f"{key}={value}\n")
                found = True
            else:
                new_lines.append(line)

        if not found:
            new_lines.append(f"{key}={value}\n")

        with open(_ENV_FILE, "w") as f:
            f.writelines(new_lines)
        return True
    except Exception:
        return False


def _get_source_types(db):
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


@bp.route("/settings")
def settings_page():
    """Render the settings page."""
    db = get_db()
    source_types = _get_source_types(db)

    # Read current settings for pre-filling the form
    settings = {}
    try:
        for row in db.execute("SELECT key, value FROM settings").fetchall():
            settings[row["key"]] = row["value"]
    except sqlite3.OperationalError:
        pass

    scheduled_time = settings.get("scheduled_time", "00:00")
    staging_dir = _read_env_value("DOWNLOAD_DIR") or settings.get("staging_dir", "/downloads")
    final_dir = _read_env_value("DEST_DIR") or settings.get("final_dir", "/dest")
    share_dir = _read_env_value("SHARE_DIR") or settings.get("share_dir", "")

    return render_template(
        "settings.html",
        source_types=source_types,
        scheduled_time=scheduled_time,
        staging_dir=staging_dir,
        final_dir=final_dir,
        share_dir=share_dir,
    )


@bp.route("/settings/save", methods=["POST"])
def settings_save():
    """Update pipeline settings from the dashboard form."""
    db = get_db()

    # --- Validate scheduled time ---
    scheduled_time = request.form.get("scheduled_time", "00:00").strip()

    import re as _re
    if not _re.match(r'^\d{2}:\d{2}$', scheduled_time):
        flash("Scheduled time must be in HH:MM format")
        return redirect(url_for("settings.settings_page"))

    try:
        h, m = (int(x) for x in scheduled_time.split(":"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
    except ValueError:
        flash("Invalid time — use 24-hour format (e.g. 00:00, 14:30)")
        return redirect(url_for("settings.settings_page"))

    # --- Read directory paths ---
    staging_dir = request.form.get("staging_dir", "").strip()
    final_dir = request.form.get("final_dir", "").strip()
    share_dir = request.form.get("share_dir", "").strip()

    # --- Save all settings ---
    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        upsert = (
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
        )

        db.execute(upsert, ("scheduled_time", scheduled_time, now_str))
        if staging_dir:
            db.execute(upsert, ("staging_dir", staging_dir, now_str))
        if final_dir:
            db.execute(upsert, ("final_dir", final_dir, now_str))
        if share_dir:
            db.execute(upsert, ("share_dir", share_dir, now_str))

        db.commit()

        # --- Update .env if folder paths changed ---
        restart_needed = False
        if staging_dir:
            old_staging = _read_env_value("DOWNLOAD_DIR")
            if old_staging != staging_dir:
                _update_env_value("DOWNLOAD_DIR", staging_dir)
                restart_needed = True
        if final_dir:
            old_final = _read_env_value("DEST_DIR")
            if old_final != final_dir:
                _update_env_value("DEST_DIR", final_dir)
                restart_needed = True
        if share_dir:
            old_share = _read_env_value("SHARE_DIR")
            if old_share != share_dir:
                _update_env_value("SHARE_DIR", share_dir)
                restart_needed = True

        msg = f"Settings saved — daily at {scheduled_time}"
        if restart_needed:
            msg += ". Folder paths updated — restart Docker to apply: docker compose up -d"
        flash(msg)
    except sqlite3.OperationalError as e:
        flash(f"Failed to save: {e}")

    return redirect(url_for("settings.settings_page"))


# ---------------------------------------------------------------------------
# Browse endpoint — directory picker for folder inputs
# ---------------------------------------------------------------------------

_HOST_MOUNT = "/host"
_HOST_HOME = os.environ.get("HOST_HOME", "")


def _host_to_container(host_path: str) -> str:
    """Translate a host path to its container-mount equivalent."""
    if _HOST_HOME and host_path.startswith(_HOST_HOME):
        return _HOST_MOUNT + host_path[len(_HOST_HOME):]
    return host_path


def _container_to_host(container_path: str) -> str:
    """Translate a container-mount path back to the host path the user sees."""
    if container_path.startswith(_HOST_MOUNT):
        return _HOST_HOME + container_path[len(_HOST_MOUNT):]
    return container_path


@bp.route("/settings/browse")
def browse_dirs():
    """
    List subdirectories at a given path for the folder picker modal.

    The user sends a real host path (e.g. /Users/yourname/Music).  We translate
    it to the container mount (/host/Music), list directories there, then
    return the host path back to the user so they never see '/host'.
    """
    raw_path = request.args.get("path", "")

    if not raw_path:
        raw_path = _HOST_HOME or "/"

    if _HOST_HOME and not raw_path.startswith(_HOST_HOME):
        raw_path = _HOST_HOME

    container_path = _host_to_container(raw_path)
    real_path = os.path.realpath(container_path)

    if not os.path.isdir(real_path):
        return jsonify({"current": raw_path, "parent": None, "dirs": []})

    host_current = _container_to_host(real_path)
    raw_parent = _container_to_host(os.path.dirname(real_path))
    if _HOST_HOME and (raw_parent == _HOST_HOME or not raw_parent.startswith(_HOST_HOME)):
        host_parent = _HOST_HOME if host_current != _HOST_HOME else None
    else:
        host_parent = raw_parent

    try:
        entries = sorted(os.listdir(real_path))
        dirs = [e for e in entries if not e.startswith(".") and os.path.isdir(os.path.join(real_path, e))]
    except PermissionError:
        dirs = []

    return jsonify({"current": host_current, "parent": host_parent, "dirs": dirs})
