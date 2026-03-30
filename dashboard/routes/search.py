"""
Manual Soulseek search routes — search for a song and enqueue downloads.

Flow:
  1. User clicks "Search" on a song row → GET /search/<song_id>
  2. Page loads instantly with a spinner, JS calls /search/<song_id>/api
  3. API endpoint searches slskd, returns JSON results
  4. JS renders the results table client-side
  5. User picks a result → POST /search/<song_id>/enqueue
"""

import json
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from db import get_db
import slskd_client

bp = Blueprint("search", __name__)


# Quality tier labels for display
TIER_LABELS = {0: "FLAC", 1: "Lossless", 2: "MP3 320", 3: "MP3", 4: "Other"}
LOSSLESS_EXTENSIONS = {"flac", "wav", "alac", "aiff", "aif"}
ACCEPTED_EXTENSIONS = {"flac", "wav", "alac", "aiff", "aif", "mp3", "ogg", "aac", "m4a", "wma", "opus"}

# File size bounds (same as automator/selector.py)
MIN_SIZE_BYTES = 1_000_000    # 1 MB
MAX_SIZE_BYTES = 200_000_000  # 200 MB


def _get_source_types(db):
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _format_tier(ext, bitrate):
    """Classify file quality. Returns tier int or None to reject non-audio files."""
    ext = (ext or "").lower()
    if ext not in ACCEPTED_EXTENSIONS:
        return None
    if ext == "flac":
        return 0
    if ext in LOSSLESS_EXTENSIONS:
        return 1
    if ext == "mp3" and bitrate and bitrate >= 320:
        return 2
    if ext == "mp3":
        return 3
    return 4


def _format_size(size_bytes):
    """Human-readable file size."""
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.1f} GB"
    if size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.1f} MB"
    if size_bytes >= 1_000:
        return f"{size_bytes / 1_000:.0f} KB"
    return f"{size_bytes} B"


def _get_basename(filename):
    """Extract filename from Windows-style Soulseek path."""
    return filename.rsplit("\\", 1)[-1]


def _get_parent_folder(filename):
    """Extract the parent folder name from a Windows-style Soulseek path."""
    parts = filename.rsplit("\\", 2)
    if len(parts) >= 2:
        return parts[-2]
    return ""


def _rank_results(responses):
    """
    Flatten peer responses into a ranked list of candidate files.

    Filters out files below quality floor or outside size bounds.
    Sorts by: tier ASC (best quality first), free slot DESC, speed DESC.
    """
    candidates = []

    for resp in responses:
        username = resp.get("username", "")
        upload_speed = resp.get("uploadSpeed", 0)
        has_free_slot = resp.get("hasFreeUploadSlot", False)

        for f in resp.get("files", []):
            filename = f.get("filename", "")
            ext = (f.get("extension") or "").lower()
            # Fallback: extract extension from filename if slskd didn't populate it
            if not ext and "." in filename:
                ext = filename.rsplit(".", 1)[-1].lower()
            bitrate = f.get("bitRate")
            size = f.get("size", 0)

            tier = _format_tier(ext, bitrate)
            if tier is None:
                continue

            if not (MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES):
                continue

            candidates.append({
                "username": username,
                "filename": filename,
                "basename": _get_basename(filename),
                "folder": _get_parent_folder(filename),
                "size": size,
                "size_display": _format_size(size),
                "format": ext.upper(),
                "bitrate": bitrate,
                "tier": tier,
                "tier_label": TIER_LABELS.get(tier, ext.upper()),
                "upload_speed": upload_speed,
                "speed_display": _format_size(upload_speed) + "/s" if upload_speed else "\u2014",
                "has_free_slot": has_free_slot,
            })

    # Sort: best quality first, then free slots, then fastest
    candidates.sort(key=lambda c: (c["tier"], not c["has_free_slot"], -c["upload_speed"]))
    return candidates


@bp.route("/search/<int:song_id>")
def search_song(song_id):
    """Show the search page instantly — JS fetches results async."""
    db = get_db()
    source_types = _get_source_types(db)

    song = db.execute(
        "SELECT rowid, artist, title, status, source_type, source_id FROM songs WHERE rowid = ?",
        (song_id,),
    ).fetchone()

    if song is None:
        return redirect(url_for("index"))

    # Build default query
    query = request.args.get("q", "").strip()
    if not query:
        parts = []
        if song["artist"]:
            parts.append(song["artist"])
        if song["title"]:
            parts.append(song["title"])
        query = " ".join(parts)

    configured = slskd_client.is_configured()

    return render_template(
        "search_results.html",
        source_types=source_types,
        song=song,
        query=query,
        configured=configured,
    )


@bp.route("/search/<int:song_id>/api")
def search_api(song_id):
    """JSON API — runs the actual slskd search. Called by JS on the page."""
    db = get_db()

    song = db.execute(
        "SELECT rowid, artist, title FROM songs WHERE rowid = ?",
        (song_id,),
    ).fetchone()

    if song is None:
        return jsonify({"error": "Song not found"}), 404

    if not slskd_client.is_configured():
        return jsonify({"error": "slskd not configured"}), 503

    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "No search query"}), 400

    try:
        raw_responses = slskd_client.search(query, timeout_ms=30000)
        results = _rank_results(raw_responses)
        return jsonify({"results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/search/<int:song_id>/enqueue", methods=["POST"])
def enqueue_song(song_id):
    """Enqueue a selected file for download and update song status."""
    db = get_db()

    song = db.execute(
        "SELECT rowid, artist, title, status, source_type, source_id, search_mode FROM songs WHERE rowid = ?",
        (song_id,),
    ).fetchone()

    if song is None:
        return redirect(url_for("index"))

    username = request.form.get("username", "")
    filename = request.form.get("filename", "")
    size = int(request.form.get("size", 0))

    if not all([username, filename, size]):
        return redirect(url_for("search.search_song", song_id=song_id))

    try:
        slskd_client.enqueue(username, filename, size)

        # Update song status to 'queued' and record the download details
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        db.execute(
            """UPDATE songs
               SET status = 'queued',
                   selected_filename = ?,
                   slsk_username = ?,
                   date_queued = ?
               WHERE rowid = ?""",
            (filename, username, now, song_id),
        )

        # For album songs (e.g. Discogs), also record in album_files so the
        # downloads page can match ALL enqueued tracks — not just the last one.
        # selected_filename can only hold one value, but album_files tracks many.
        if song["search_mode"] == "album":
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            try:
                db.execute(
                    """INSERT INTO album_files
                           (source_id, slsk_username, filename, size, extension, status, date_queued)
                       VALUES (?, ?, ?, ?, ?, 'queued', ?)""",
                    (song["source_id"], username, filename, size, ext, now),
                )
            except sqlite3.OperationalError:
                pass  # album_files table may not exist yet

        db.commit()

    except Exception as e:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": str(e)}), 500
        return redirect(url_for("search.search_song", song_id=song_id, error=str(e)))

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "message": f"Queued: {song['artist']} — {song['title']}"})
    return redirect(url_for("songs.source_view", source_type=song["source_type"]))


@bp.route("/search/<int:song_id>/folder")
def browse_folder(song_id):
    """Browse a peer's folder to show all files (not just search matches)."""
    username = request.args.get("username", "")
    directory = request.args.get("directory", "")

    if not username or not directory:
        return jsonify({"error": "username and directory are required"}), 400

    if not slskd_client.is_configured():
        return jsonify({"error": "slskd not configured"}), 503

    try:
        files = slskd_client.browse_folder(username, directory)
        result_files = []
        for f in files:
            fname = f["filename"]
            basename = fname.rsplit("\\", 1)[-1] if "\\" in fname else fname
            result_files.append({
                "filename": fname,
                "basename": basename,
                "size": f["size"],
                "size_display": _format_size(f["size"]),
                "extension": f.get("extension", ""),
            })
        return jsonify({"files": result_files, "count": len(result_files)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/search/<int:song_id>/enqueue-folder", methods=["POST"])
def enqueue_folder(song_id):
    """Enqueue all files in a folder and update song status."""
    db = get_db()

    song = db.execute(
        "SELECT rowid, artist, title, status, source_type, source_id, search_mode FROM songs WHERE rowid = ?",
        (song_id,),
    ).fetchone()

    if song is None:
        return jsonify({"error": "Song not found"}), 404

    data = request.get_json(silent=True) or {}
    username = data.get("username", "")
    files = data.get("files", [])

    if not username or not files:
        return jsonify({"error": "username and files are required"}), 400

    try:
        slskd_client.enqueue_many(username, files)

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Use the first file as the representative selected_filename
        db.execute(
            """UPDATE songs
               SET status = 'queued',
                   selected_filename = ?,
                   slsk_username = ?,
                   date_queued = ?
               WHERE rowid = ?""",
            (files[0]["filename"], username, now, song_id),
        )

        # Record each file in album_files for download tracking
        for f in files:
            fname = f["filename"]
            ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
            try:
                db.execute(
                    """INSERT INTO album_files
                           (source_id, slsk_username, filename, size, extension, status, date_queued)
                       VALUES (?, ?, ?, ?, ?, 'queued', ?)""",
                    (song["source_id"], username, fname, f["size"], ext, now),
                )
            except sqlite3.OperationalError:
                pass  # album_files table may not exist yet

        db.commit()

        return jsonify({"ok": True, "count": len(files),
                        "message": f"Queued {len(files)} files from {username}"})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
