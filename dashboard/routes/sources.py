import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, redirect, render_template, request, url_for

from db import get_db

TEXTFILES_DIR = Path("/data/textfiles")

bp = Blueprint("sources", __name__)


def _now_iso():
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _get_source_types(db):
    """Return all enabled source types for the sidebar. Returns [] on DB errors."""
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _build_config_json(source_type, form):
    """
    Build the config_json dict from type-specific form fields.
    Returns a JSON string ready for DB storage.
    """
    if source_type == "youtube":
        return json.dumps({"playlist_url": form.get("playlist_url", "").strip()})
    elif source_type == "spotify":
        return json.dumps({"playlist_url": form.get("playlist_url", "").strip()})
    elif source_type == "discogs":
        return json.dumps({
            "username": form.get("username", "").strip(),
            "list_type": form.get("list_type", "collection"),
        })
    elif source_type == "textfile":
        # Path is set after DB insert (needs source ID), so use placeholder
        return json.dumps({"mode": form.get("mode", "track")})
    return json.dumps({})


def _validate_form(source_type, name, form):
    """
    Validate required form fields.
    Returns a list of error strings (empty list = valid).
    """
    errors = []
    if not name:
        errors.append("Name is required.")
    if source_type == "youtube" and not form.get("playlist_url", "").strip():
        errors.append("Playlist URL is required for YouTube sources.")
    elif source_type == "spotify" and not form.get("playlist_url", "").strip():
        errors.append("Playlist URL is required for Spotify sources.")
    elif source_type == "discogs" and not form.get("username", "").strip():
        errors.append("Username is required for Discogs sources.")
    elif source_type == "textfile" and not form.get("songs_text", "").strip():
        errors.append("At least one song is required for Text File sources.")
    return errors


def _save_textfile(source_id, content, mode):
    """Write song lines to a managed text file and return the container path."""
    TEXTFILES_DIR.mkdir(parents=True, exist_ok=True)
    file_path = TEXTFILES_DIR / f"source-{source_id}.txt"
    file_path.write_text(content.strip() + "\n", encoding="utf-8")
    return str(file_path)


def _read_textfile(cfg):
    """Read content of a managed text file for editing."""
    path = cfg.get("path", "")
    if path and os.path.isfile(path):
        return Path(path).read_text(encoding="utf-8").strip()
    return ""


# ---------------------------------------------------------------------------
# Add source
# ---------------------------------------------------------------------------


@bp.route("/sources/add", methods=["GET"])
def source_add():
    """Render the add-source form (type-first, empty)."""
    db = get_db()
    source_types = _get_source_types(db)
    return render_template(
        "source_form.html",
        source_types=source_types,
        mode="add",
        source=None,
        errors=[],
        prefill_type=request.args.get("type", "youtube"),
        cfg={},
    )


@bp.route("/sources/add", methods=["POST"])
def source_add_post():
    """Create a new source from form data."""
    db = get_db()
    source_types = _get_source_types(db)

    source_type = request.form.get("type", "youtube")
    name = request.form.get("name", "").strip()
    errors = _validate_form(source_type, name, request.form)

    if errors:
        return render_template(
            "source_form.html",
            source_types=source_types,
            mode="add",
            source=None,
            errors=errors,
            prefill_type=source_type,
            form_data=request.form,
            cfg={},
        )

    config_json = _build_config_json(source_type, request.form)
    now = _now_iso()

    cursor = db.execute(
        "INSERT INTO sources (type, name, config_json, enabled, date_added, date_updated) VALUES (?, ?, ?, 1, ?, ?)",
        (source_type, name, config_json, now, now),
    )
    source_id = cursor.lastrowid

    # For textfile sources, save the textarea content to a managed file
    if source_type == "textfile":
        content = request.form.get("songs_text", "")
        mode = request.form.get("mode", "track")
        file_path = _save_textfile(source_id, content, mode)
        cfg = json.loads(config_json)
        cfg["path"] = file_path
        db.execute(
            "UPDATE sources SET config_json = ? WHERE id = ?",
            (json.dumps(cfg), source_id),
        )

    db.commit()

    return redirect(url_for("songs.source_view", source_type=source_type))


# ---------------------------------------------------------------------------
# Edit source
# ---------------------------------------------------------------------------


@bp.route("/sources/<int:source_id>/edit", methods=["GET"])
def source_edit(source_id):
    """Render the edit-source form, pre-populated with existing values."""
    db = get_db()
    source_types = _get_source_types(db)

    source = db.execute(
        "SELECT id, type, name, config_json, enabled FROM sources WHERE id = ?",
        (source_id,),
    ).fetchone()

    if source is None:
        return redirect(url_for("songs.no_sources"))

    # Parse config_json so the template can pre-populate type-specific fields
    try:
        cfg = json.loads(source["config_json"]) if source["config_json"] else {}
    except (ValueError, TypeError):
        cfg = {}

    # For textfile sources, read the file content for the textarea
    textfile_content = _read_textfile(cfg) if source["type"] == "textfile" else ""

    return render_template(
        "source_form.html",
        source_types=source_types,
        mode="edit",
        source=source,
        errors=[],
        prefill_type=source["type"],
        form_data=None,
        cfg=cfg,
        textfile_content=textfile_content,
    )


@bp.route("/sources/<int:source_id>/edit", methods=["POST"])
def source_edit_post(source_id):
    """Update an existing source from form data."""
    db = get_db()
    source_types = _get_source_types(db)

    # Fetch original to know the type (type is immutable after creation)
    source = db.execute(
        "SELECT id, type, name, config_json, enabled FROM sources WHERE id = ?",
        (source_id,),
    ).fetchone()

    if source is None:
        return redirect(url_for("songs.no_sources"))

    source_type = source["type"]  # type cannot be changed in edit mode
    name = request.form.get("name", "").strip()
    errors = _validate_form(source_type, name, request.form)

    if errors:
        try:
            cfg = json.loads(source["config_json"]) if source["config_json"] else {}
        except (ValueError, TypeError):
            cfg = {}
        return render_template(
            "source_form.html",
            source_types=source_types,
            mode="edit",
            source=source,
            errors=errors,
            prefill_type=source_type,
            form_data=request.form,
            cfg=cfg,
        )

    config_json = _build_config_json(source_type, request.form)
    now = _now_iso()

    # For textfile sources, update the managed file content
    if source_type == "textfile":
        content = request.form.get("songs_text", "")
        mode = request.form.get("mode", "track")
        file_path = _save_textfile(source_id, content, mode)
        cfg_dict = json.loads(config_json)
        cfg_dict["path"] = file_path
        config_json = json.dumps(cfg_dict)

    db.execute(
        "UPDATE sources SET name = ?, config_json = ?, date_updated = ? WHERE id = ?",
        (name, config_json, now, source_id),
    )
    db.commit()

    return redirect(url_for("songs.source_view", source_type=source_type))


# ---------------------------------------------------------------------------
# Toggle enabled/disabled
# ---------------------------------------------------------------------------


@bp.route("/sources/<int:source_id>/toggle", methods=["POST"])
def source_toggle(source_id):
    """Toggle enabled flag on a source."""
    db = get_db()

    source = db.execute(
        "SELECT id, type FROM sources WHERE id = ?", (source_id,)
    ).fetchone()

    if source is None:
        return redirect(url_for("songs.no_sources"))

    now = _now_iso()
    db.execute(
        "UPDATE sources SET enabled = NOT enabled, date_updated = ? WHERE id = ?",
        (now, source_id),
    )
    db.commit()

    return redirect(url_for("songs.source_view", source_type=source["type"]))


# ---------------------------------------------------------------------------
# Delete source
# ---------------------------------------------------------------------------


@bp.route("/sources/<int:source_id>/delete", methods=["POST"])
def source_delete(source_id):
    """
    Permanently delete a source.
    Songs belonging to this source are NOT deleted — they remain in the DB
    with their current status for historical reference.
    """
    db = get_db()

    source = db.execute(
        "SELECT id, type FROM sources WHERE id = ?", (source_id,)
    ).fetchone()

    if source is None:
        return redirect(url_for("songs.no_sources"))

    source_type = source["type"]
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db.execute("UPDATE sources SET deleted_at = ? WHERE id = ?", (now, source_id))
    db.commit()

    # After deletion, check if this type still has active sources — if not, go to index
    remaining = db.execute(
        "SELECT COUNT(*) AS cnt FROM sources WHERE type = ? AND deleted_at IS NULL", (source_type,)
    ).fetchone()

    if remaining and remaining["cnt"] > 0:
        return redirect(url_for("songs.source_view", source_type=source_type))
    return redirect(url_for("index"))
