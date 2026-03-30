"""
Downloads view — shows active and recent slskd transfers.

Pulls from the slskd transfers API and cross-references with the songs DB
to show which queued songs map to which downloads.
"""

import sqlite3

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from db import get_db
import slskd_client

bp = Blueprint("downloads", __name__)


def _get_source_types(db):
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _format_size(size_bytes):
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.1f} GB"
    if size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.1f} MB"
    if size_bytes >= 1_000:
        return f"{size_bytes / 1_000:.0f} KB"
    return f"{size_bytes} B"


def _get_basename(filename):
    return filename.rsplit("\\", 1)[-1]


def _classify_state(state_str):
    """
    Classify a slskd transfer state string into a simple category.

    slskd states are comma-separated flags like "Completed, Succeeded"
    or "Queued, Remotely" or "InProgress".
    """
    s = state_str.lower() if state_str else ""
    if "succeeded" in s:
        return "completed"
    if "errored" in s or "cancelled" in s or "rejected" in s or "timed out" in s:
        return "failed"
    if "inprogress" in s or "initializing" in s:
        return "downloading"
    if "queued" in s:
        return "queued"
    if "completed" in s:
        # "Completed" without "Succeeded" often means transfer done but verification pending
        return "completed"
    return "unknown"


def _flatten_downloads(raw_data):
    """Flatten the nested user/directory/file structure into a flat list."""
    downloads = []

    for user_entry in raw_data:
        username = user_entry.get("username", "")

        for directory in user_entry.get("directories", []):
            for f in directory.get("files", []):
                state_raw = f.get("state", "") or f.get("stateDescription", "")
                category = _classify_state(state_raw)
                size = f.get("size", 0)
                transferred = f.get("bytesTransferred", 0)
                percent = f.get("percentComplete", 0)
                speed = f.get("averageSpeed", 0)

                downloads.append({
                    "username": username,
                    "filename": f.get("filename", ""),
                    "basename": _get_basename(f.get("filename", "")),
                    "size": size,
                    "size_display": _format_size(size),
                    "transferred": transferred,
                    "transferred_display": _format_size(transferred),
                    "percent": round(percent, 1),
                    "speed": speed,
                    "speed_display": _format_size(speed) + "/s" if speed else "",
                    "state_raw": state_raw,
                    "category": category,
                    "error": f.get("exception", ""),
                    "requested_at": (f.get("requestedAt") or "")[:19].replace("T", " "),
                    "ended_at": (f.get("endedAt") or "")[:19].replace("T", " "),
                })

    # Sort: downloading first, then queued, then completed, then failed
    order = {"downloading": 0, "queued": 1, "completed": 2, "failed": 3, "unknown": 4}
    downloads.sort(key=lambda d: (order.get(d["category"], 9), d["requested_at"]), reverse=False)

    return downloads


@bp.route("/downloads")
def downloads_page():
    """Render the downloads page — JS fetches transfer data async."""
    db = get_db()
    source_types = _get_source_types(db)
    configured = slskd_client.is_configured()

    return render_template(
        "downloads.html",
        source_types=source_types,
        configured=configured,
    )


@bp.route("/downloads/api")
def downloads_api():
    """JSON API — fetch current transfers from slskd."""
    if not slskd_client.is_configured():
        return jsonify({"error": "slskd not configured"}), 503

    try:
        raw = slskd_client.get_downloads()
        downloads = _flatten_downloads(raw)

        # Cross-reference downloads with songs DB to enable Search/Retry links.
        # Three lookup strategies (tried in order):
        #   1. Exact match on songs.selected_filename
        #   2. Basename match against songs.selected_filename
        #   3. Basename match against album_files.filename (for album tracks)
        #      Uses the parent song's rowid so retry resets the whole album.
        db = get_db()
        try:
            exact_map = {}    # full filename → {rowid, source_type, date_queued, source_table_id}
            basename_map = {}  # basename → same dict (fallback)
            for row in db.execute(
                "SELECT rowid, selected_filename, source_type, date_queued, source_table_id "
                "FROM songs WHERE selected_filename IS NOT NULL"
            ).fetchall():
                info = {
                    "rowid": row["rowid"],
                    "source_type": row["source_type"] or "",
                    "date_queued": row["date_queued"] or "",
                    "source_table_id": row["source_table_id"],
                }
                exact_map[row["selected_filename"]] = info
                bn = row["selected_filename"].rsplit("\\", 1)[-1]
                basename_map[bn] = info

            # Prefetch source names for display
            source_names = {}
            try:
                for row in db.execute("SELECT id, name, type FROM sources").fetchall():
                    source_names[row["id"]] = row["name"]
            except sqlite3.OperationalError:
                pass

            # Also index album_files — map their basenames to the parent song info
            album_basename_map = {}  # basename → info dict
            try:
                for row in db.execute("""
                    SELECT af.filename, s.rowid AS song_rowid,
                           s.source_type, s.date_queued, s.source_table_id
                    FROM album_files af
                    JOIN songs s ON af.source_id = s.source_id
                """).fetchall():
                    bn = row["filename"].rsplit("\\", 1)[-1]
                    album_basename_map[bn] = {
                        "rowid": row["song_rowid"],
                        "source_type": row["source_type"] or "",
                        "date_queued": row["date_queued"] or "",
                        "source_table_id": row["source_table_id"],
                    }
            except sqlite3.OperationalError:
                pass  # album_files table may not exist yet

            for d in downloads:
                info = exact_map.get(d["filename"])
                if not info and d["basename"]:
                    info = basename_map.get(d["basename"])
                if not info and d["basename"]:
                    info = album_basename_map.get(d["basename"])

                if info:
                    d["song_rowid"] = info["rowid"]
                    d["source_type"] = info["source_type"]
                    d["date_queued"] = (info["date_queued"] or "")[:19].replace("T", " ")
                    sid = info["source_table_id"]
                    d["source_name"] = source_names.get(sid, "") if sid else ""
                else:
                    d["song_rowid"] = None
                    d["source_type"] = ""
                    d["date_queued"] = ""
                    d["source_name"] = ""
        except sqlite3.OperationalError:
            pass

        # Summary counts
        counts = {"downloading": 0, "queued": 0, "completed": 0, "failed": 0}
        for d in downloads:
            if d["category"] in counts:
                counts[d["category"]] += 1

        # Query DB for stuck songs that won't appear in slskd transfers.
        # These are songs the pipeline knows about but slskd has lost track of
        # (e.g. after container restart or transfer cleanup).
        db_stuck = []
        try:
            stuck_rows = db.execute("""
                SELECT rowid, source_id, artist, title, status, date_queued, selected_filename
                FROM songs
                WHERE status IN ('queued', 'stalled', 'stalled_waiting')
                   OR (status = 'downloaded' AND local_path IS NULL AND date_delivered IS NULL)
                ORDER BY date_queued DESC
            """).fetchall()

            # Build a set of filenames already visible in slskd transfers
            # so we don't double-show songs that are actively downloading
            slskd_filenames = {d["filename"] for d in downloads}

            for row in stuck_rows:
                # Skip if this song's file is already showing in slskd transfers
                if row["selected_filename"] and row["selected_filename"] in slskd_filenames:
                    continue
                db_stuck.append({
                    "rowid": row["rowid"],
                    "source_id": row["source_id"],
                    "artist": row["artist"],
                    "title": row["title"],
                    "status": row["status"],
                    "date_queued": (row["date_queued"] or "")[:19].replace("T", " "),
                })
        except sqlite3.OperationalError:
            pass  # Table might not have all columns yet

        counts["stuck"] = len(db_stuck)
        return jsonify({"downloads": downloads, "counts": counts, "db_stuck": db_stuck})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/downloads/<int:song_rowid>/retry", methods=["POST"])
def retry_download(song_rowid):
    """
    Reset a failed song back to 'new' so the pipeline re-searches it.

    Clears all download-related fields so it starts fresh, as if the song
    had just been discovered from the source.
    """
    db = get_db()
    try:
        row = db.execute(
            "SELECT source_id, artist, title FROM songs WHERE rowid = ?",
            (song_rowid,),
        ).fetchone()

        if not row:
            return jsonify({"ok": False, "error": "Song not found"}), 404

        db.execute("""
            UPDATE songs
            SET status = 'new',
                selected_filename = NULL,
                slsk_username = NULL,
                selected_format = NULL,
                selected_bitrate = NULL,
                date_queued = NULL,
                local_path = NULL,
                stall_check_bytes = NULL,
                stall_check_time = NULL,
                alt_source_count = 0
            WHERE rowid = ?
        """, (song_rowid,))
        db.commit()

        flash(f"Retrying: {row['artist']} — {row['title']}")
        return jsonify({"ok": True})

    except sqlite3.OperationalError as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/downloads/<int:song_rowid>/delete", methods=["POST"])
def delete_download(song_rowid):
    """
    Mark a song as 'removed' — permanently hides it from the downloads page.

    Uses the existing 'removed' terminal status so the song still appears
    in its source tab with a gray badge, but no longer clutters downloads.
    """
    db = get_db()
    try:
        row = db.execute(
            "SELECT source_id, artist, title FROM songs WHERE rowid = ?",
            (song_rowid,),
        ).fetchone()

        if not row:
            return jsonify({"ok": False, "error": "Song not found"}), 404

        db.execute(
            "UPDATE songs SET status = 'removed' WHERE rowid = ?",
            (song_rowid,),
        )
        db.commit()

        return jsonify({"ok": True})

    except sqlite3.OperationalError as e:
        return jsonify({"ok": False, "error": str(e)}), 500
