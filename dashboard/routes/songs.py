import json
import logging
import os
import sqlite3

import requests
from flask import Blueprint, jsonify, render_template, request

from db import get_db

log = logging.getLogger(__name__)

bp = Blueprint("songs", __name__)


def _get_source_types(db):
    """Return all enabled source types for the sidebar. Returns [] on DB errors."""
    try:
        return db.execute(
            "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type"
        ).fetchall()
    except sqlite3.OperationalError:
        return []


@bp.route("/sources/<source_type>")
def source_view(source_type):
    """Show metric cards and song table for a given source type."""
    db = get_db()

    source_types = _get_source_types(db)

    try:
        # All sources of this type for the horizontal tabs row.
        # Include disabled sources so they remain accessible (toggle / re-enable).
        sources = db.execute(
            "SELECT id, name, type, enabled FROM sources WHERE type = ? AND deleted_at IS NULL ORDER BY enabled DESC, name",
            (source_type,),
        ).fetchall()
    except sqlite3.OperationalError:
        sources = []

    # Active tab — default to first source.
    # Coerce to int so Jinja comparison with src.id (int from SQLite) works.
    raw_tab = request.args.get("tab")
    if raw_tab is not None:
        try:
            active_tab = int(raw_tab)
        except (ValueError, TypeError):
            active_tab = sources[0]["id"] if sources else None
    else:
        active_tab = sources[0]["id"] if sources else None

    # Filter params
    status_filter = request.args.get("status", "")
    search_query = request.args.get("q", "").strip()

    songs = []
    metrics = None
    all_statuses = []

    try:
        # Build the main song query.
        # When a specific source tab is selected AND the source_table_id column
        # exists, filter to just that source's songs. Otherwise, show all songs
        # for this source_type (backward-compatible).
        sql = """
            SELECT rowid, source_id, artist, title, status,
                   date_added, date_queued, date_downloaded
            FROM songs
            WHERE source_type = ?
        """
        params = [source_type]

        # Filter by specific source if a tab is selected
        if active_tab is not None:
            sql += " AND source_table_id = ?"
            params.append(active_tab)

        if status_filter:
            sql += " AND status = ?"
            params.append(status_filter)

        if search_query:
            sql += " AND (artist LIKE ? OR title LIKE ?)"
            like_val = f"%{search_query}%"
            params.extend([like_val, like_val])

        sql += " ORDER BY date_added DESC"

        songs = db.execute(sql, params).fetchall()

        # Metric counts — scoped to the active source tab
        metric_sql = """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status IN ('downloaded', 'delivered', 'upgraded') THEN 1 ELSE 0 END) AS downloaded,
                SUM(CASE WHEN status IN ('new', 'searching', 'queued', 'downloading', 'upgrade_queued') THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status IN ('not_found', 'no_match', 'stalled', 'stalled_waiting') THEN 1 ELSE 0 END) AS failed
            FROM songs
            WHERE source_type = ?
        """
        metric_params = [source_type]
        if active_tab is not None:
            metric_sql += " AND source_table_id = ?"
            metric_params.append(active_tab)

        metrics = db.execute(metric_sql, metric_params).fetchone()

        # Distinct statuses for the filter dropdown — also scoped
        status_sql = "SELECT DISTINCT status FROM songs WHERE source_type = ?"
        status_params = [source_type]
        if active_tab is not None:
            status_sql += " AND source_table_id = ?"
            status_params.append(active_tab)
        status_sql += " ORDER BY status"

        all_statuses = [
            row["status"]
            for row in db.execute(status_sql, status_params).fetchall()
        ]

    except sqlite3.OperationalError:
        # songs table not yet initialised
        pass

    return render_template(
        "source_view.html",
        source_type=source_type,
        source_types=source_types,
        sources=sources,
        active_tab=active_tab,
        songs=songs,
        metrics=metrics,
        all_statuses=all_statuses,
        status_filter=status_filter,
        search_query=search_query,
    )


@bp.route("/songs/<int:song_rowid>/tracklist")
def song_tracklist(song_rowid):
    """
    Return the tracklist for a Discogs album.

    Checks for a cached tracklist_json in the DB first. If not cached,
    fetches from the Discogs REST API using the release ID embedded in
    the source_id (format: "dc:<release_id>"), caches the result, and
    returns it.
    """
    db = get_db()

    try:
        row = db.execute(
            "SELECT source_id, source_type, artist, tracklist_json FROM songs WHERE rowid = ?",
            (song_rowid,),
        ).fetchone()
    except sqlite3.OperationalError:
        return jsonify({"error": "Database not ready"}), 500

    if not row:
        return jsonify({"error": "Song not found"}), 404

    if row["source_type"] != "discogs":
        return jsonify({"error": "Tracklist only available for Discogs albums"}), 400

    # Return cached tracklist if available
    if row["tracklist_json"]:
        return jsonify({"tracks": json.loads(row["tracklist_json"]), "artist": row["artist"]})

    # Extract release ID from source_id (format: "dc:12345")
    source_id = row["source_id"]
    if not source_id.startswith("dc:"):
        return jsonify({"error": "Invalid Discogs source_id format"}), 400
    release_id = source_id[3:]

    token = os.environ.get("DISCOGS_TOKEN", "").strip()
    if not token:
        return jsonify({"error": "DISCOGS_TOKEN not configured"}), 503

    try:
        resp = requests.get(
            f"https://api.discogs.com/releases/{release_id}",
            headers={
                "Authorization": f"Discogs token={token}",
                "User-Agent": "SoulSync/1.0",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        tracks = [
            {
                "position": t.get("position", ""),
                "title": t.get("title", ""),
                "duration": t.get("duration", ""),
            }
            for t in data.get("tracklist", [])
            if t.get("type_") == "track"  # Skip headings/indexes
        ]

        # Cache in DB
        tracklist_str = json.dumps(tracks)
        db.execute(
            "UPDATE songs SET tracklist_json = ? WHERE rowid = ?",
            (tracklist_str, song_rowid),
        )
        db.commit()

        return jsonify({"tracks": tracks, "artist": row["artist"]})

    except requests.RequestException as e:
        log.warning("Discogs API error for release %s: %s", release_id, e)
        return jsonify({"error": f"Discogs API error: {e}"}), 502


@bp.route("/no-sources")
def no_sources():
    """Shown when no sources are configured in the database."""
    db = get_db()
    source_types = _get_source_types(db)
    return render_template("no_sources.html", source_types=source_types)


@bp.route("/glossary")
def glossary():
    """Status glossary page — explains every possible song status."""
    db = get_db()
    source_types = _get_source_types(db)
    return render_template("glossary.html", source_types=source_types)
