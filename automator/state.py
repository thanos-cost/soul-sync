import json
import os
import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger(__name__)

DB_PATH = "/data/songs.db"


def _run_phase6_migration(conn: sqlite3.Connection) -> None:
    """
    Phase 6 schema migration — rename youtube_id to source_id and add
    source_type and search_mode columns.

    Strategy: recreate the songs table with the new schema, migrate all rows,
    then drop the old table. This is safer than ALTER TABLE RENAME COLUMN
    which is only supported in SQLite 3.25+ and has edge cases with indexes.

    Idempotent: checks for source_id column first and returns immediately
    if the migration has already run.
    """
    cursor = conn.execute("PRAGMA table_info(songs)")
    columns = [row[1] for row in cursor.fetchall()]

    if "source_id" in columns:
        # Migration already applied — nothing to do
        return

    if "youtube_id" not in columns:
        # Fresh database — no migration needed (CREATE TABLE already uses source_id)
        return

    log.info("Phase 6 migration: renaming youtube_id -> source_id and adding source columns")

    conn.execute("""
        CREATE TABLE songs_v6 (
            source_id           TEXT PRIMARY KEY,
            source_type         TEXT NOT NULL DEFAULT 'youtube',
            search_mode         TEXT NOT NULL DEFAULT 'track',
            raw_title           TEXT NOT NULL,
            artist              TEXT NOT NULL,
            title               TEXT NOT NULL,
            version             TEXT,
            status              TEXT NOT NULL DEFAULT 'new',
            date_added          TEXT NOT NULL,
            date_downloaded     TEXT,
            slsk_username       TEXT,
            selected_filename   TEXT,
            selected_format     TEXT,
            selected_bitrate    INTEGER,
            date_queued         TEXT,
            local_path          TEXT,
            date_delivered      TEXT,
            stall_check_bytes   INTEGER,
            stall_check_time    TEXT,
            alt_source_count    INTEGER DEFAULT 0,
            last_search_date    TEXT,
            search_attempts     INTEGER DEFAULT 0
        )
    """)

    conn.execute("""
        INSERT INTO songs_v6
        SELECT
            youtube_id          AS source_id,
            'youtube'           AS source_type,
            'track'             AS search_mode,
            raw_title,
            artist,
            title,
            version,
            status,
            date_added,
            date_downloaded,
            slsk_username,
            selected_filename,
            selected_format,
            selected_bitrate,
            date_queued,
            local_path,
            date_delivered,
            stall_check_bytes,
            stall_check_time,
            alt_source_count,
            last_search_date,
            search_attempts
        FROM songs
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM songs_v6").fetchone()[0]

    conn.execute("DROP TABLE songs")
    conn.execute("ALTER TABLE songs_v6 RENAME TO songs")

    # Recreate the index that was dropped along with the old table
    conn.execute("CREATE INDEX IF NOT EXISTS idx_songs_status ON songs(status)")

    conn.commit()
    log.info("Phase 6 migration complete — migrated %d row(s) to new schema", row_count)


def init_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    """
    Create the SQLite database, enable WAL mode, and ensure the songs table exists.

    The /data directory is a Docker volume mounted from ./data on the host,
    so the database persists across container restarts and is accessible via
    sqlite3 on the host machine for inspection.

    Phase 3 adds: schema migration for new columns (slsk_username, selected_filename,
    selected_format, selected_bitrate, date_queued). Migration runs safely on every
    startup — existing columns are silently ignored.

    Phase 4 adds: schema migration for delivery tracking, stall detection, and version
    metadata columns. Also runs safely on every startup.

    Phase 6 adds: renames youtube_id -> source_id, adds source_type and search_mode
    columns. Migrates all existing YouTube rows with source_type='youtube', search_mode='track'.

    Phase 8 adds: album_files child table for tracking individual files within an album
    download. Uses CREATE TABLE IF NOT EXISTS for idempotency.

    Parameters:
        conn — optional existing sqlite3.Connection (used in tests with :memory: DBs).
               When None (production), a new connection to DB_PATH is opened.

    Returns an open sqlite3.Connection.
    """
    if conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)

    conn.row_factory = sqlite3.Row  # Enables dict-style column access

    # WAL mode improves concurrency and is safer for long-running operations
    conn.execute("PRAGMA journal_mode=WAL")

    # Phase 6: fresh databases get the new schema directly (source_id, not youtube_id)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS songs (
            source_id           TEXT PRIMARY KEY,
            source_type         TEXT NOT NULL DEFAULT 'youtube',
            search_mode         TEXT NOT NULL DEFAULT 'track',
            raw_title           TEXT NOT NULL,
            artist              TEXT NOT NULL,
            title               TEXT NOT NULL,
            version             TEXT,
            status              TEXT NOT NULL DEFAULT 'new',
            date_added          TEXT NOT NULL,
            date_downloaded     TEXT
        )
    """)

    # Index on status so Phase 3 can efficiently query "WHERE status = 'new'"
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_songs_status ON songs(status)
    """)

    conn.commit()

    # Phase 3 schema migration — add new columns without dropping existing data.
    # Each ALTER TABLE is wrapped in try/except so it's safe to run on every startup.
    # SQLite does not support IF NOT EXISTS on ADD COLUMN, so we catch the error instead.
    _PHASE3_MIGRATIONS = [
        "ALTER TABLE songs ADD COLUMN slsk_username TEXT",
        "ALTER TABLE songs ADD COLUMN selected_filename TEXT",
        "ALTER TABLE songs ADD COLUMN selected_format TEXT",
        "ALTER TABLE songs ADD COLUMN selected_bitrate INTEGER",
        "ALTER TABLE songs ADD COLUMN date_queued TEXT",
    ]
    for stmt in _PHASE3_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists — safe to ignore on subsequent startups

    conn.commit()

    # Phase 4 schema migration — delivery tracking, stall detection, and version metadata.
    # Same pattern as Phase 3: each ALTER TABLE is wrapped in try/except for idempotency.
    _PHASE4_MIGRATIONS = [
        "ALTER TABLE songs ADD COLUMN version TEXT",
        "ALTER TABLE songs ADD COLUMN local_path TEXT",
        "ALTER TABLE songs ADD COLUMN date_delivered TEXT",
        "ALTER TABLE songs ADD COLUMN stall_check_bytes INTEGER",
        "ALTER TABLE songs ADD COLUMN stall_check_time TEXT",
        "ALTER TABLE songs ADD COLUMN alt_source_count INTEGER DEFAULT 0",
    ]
    for stmt in _PHASE4_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists — safe to ignore on subsequent startups

    conn.commit()

    # Phase 5 schema migration — search attempt tracking for reporting.
    # last_search_date: updated each time a song enters 'searching' status.
    # search_attempts: incremented each time a song enters 'searching' status.
    _PHASE5_MIGRATIONS = [
        "ALTER TABLE songs ADD COLUMN last_search_date TEXT",
        "ALTER TABLE songs ADD COLUMN search_attempts INTEGER DEFAULT 0",
    ]
    for stmt in _PHASE5_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists — safe to ignore on subsequent startups

    conn.commit()

    # Phase 6 schema migration — rename youtube_id -> source_id, add source columns.
    # Must run AFTER Phase 3-5 migrations so those columns exist in the old table
    # before we copy them into songs_v6.
    _run_phase6_migration(conn)

    # Phase 8 schema migration — album_files child table for tracking individual
    # files within an album download. Uses CREATE TABLE IF NOT EXISTS for idempotency
    # (same pattern as the songs table) — safe to run on every startup.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS album_files (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id       TEXT NOT NULL,
            slsk_username   TEXT NOT NULL,
            filename        TEXT NOT NULL,
            size            INTEGER NOT NULL,
            extension       TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'queued',
            date_queued     TEXT NOT NULL,
            date_downloaded TEXT,
            local_path      TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_album_files_source_id ON album_files(source_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_album_files_status ON album_files(status)")
    conn.commit()

    # Phase 9 schema migration — sources table for dashboard-backed source configuration.
    # Replaces sources.yaml as the authoritative source store so the dashboard can perform
    # CRUD operations and the pipeline reads the results on the next run.
    # Uses CREATE TABLE IF NOT EXISTS for idempotency (same pattern as other tables).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            type         TEXT NOT NULL,
            name         TEXT NOT NULL,
            config_json  TEXT NOT NULL DEFAULT '{}',
            enabled      INTEGER NOT NULL DEFAULT 1,
            date_added   TEXT NOT NULL,
            date_updated TEXT,
            deleted_at   TEXT
        )
    """)
    conn.commit()

    # Migration: add deleted_at column for soft-delete support.
    # When a source is deleted, we set deleted_at instead of removing the row,
    # so orphaned songs can still resolve their source name for delivery.
    try:
        conn.execute("ALTER TABLE sources ADD COLUMN deleted_at TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Phase 9.1 schema migration — link songs to their specific source entry.
    # source_table_id is a foreign key to sources.id — it tells the dashboard
    # which exact source (e.g. "My Favourites" vs "Chill Vibes") a song came from.
    # Nullable because songs added before this migration lack the link.
    _PHASE9_1_MIGRATIONS = [
        "ALTER TABLE songs ADD COLUMN source_table_id INTEGER",
    ]
    for stmt in _PHASE9_1_MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Tracklist cache for Discogs albums — stores JSON array of {position, title, duration}
    # fetched from the Discogs API so the dashboard can show track-level detail.
    # Failed peers tracking — stores JSON array of usernames that failed for this song
    # so retries always skip peers that already rejected/failed (peer exclusion).
    for stmt in [
        "ALTER TABLE songs ADD COLUMN tracklist_json TEXT",
        "ALTER TABLE songs ADD COLUMN failed_peers_json TEXT",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()

    # Self-heal: unstick album rows whose search_attempts climbed past the cap
    # while their status was 'new'. The earlier retry path reset such albums
    # back to 'new' without resetting search_attempts, so get_pending_albums()
    # filtered them out forever (status=new, attempts>=10 = invisible). We
    # clear the budget and peer exclusions so they re-enter the loop fresh.
    # Idempotent: after the fix, the retry path keeps these in sync, so this
    # statement matches zero rows on subsequent startups.
    conn.execute("""
        UPDATE songs
        SET search_attempts = 0,
            failed_peers_json = NULL
        WHERE search_mode = 'album'
          AND status = 'new'
          AND search_attempts >= 10
    """)
    conn.commit()

    # Backfill: when there's exactly one source per type, link existing songs.
    # This is safe and idempotent — only updates NULL source_table_id rows.
    try:
        conn.execute("""
            UPDATE songs
            SET source_table_id = (
                SELECT id FROM sources
                WHERE sources.type = songs.source_type
                LIMIT 1
            )
            WHERE source_table_id IS NULL
              AND source_type IN (
                  SELECT type FROM sources
                  GROUP BY type HAVING COUNT(*) = 1
              )
        """)
    except sqlite3.OperationalError:
        pass  # sources table might not exist yet

    conn.commit()

    # Phase 10 schema migration — pipeline_runs and settings tables for the
    # Activity page. pipeline_runs records each scheduler run with stats so
    # the dashboard can show history. settings stores tunable values (like
    # scheduled_time) that the dashboard can edit and the scheduler reads.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_number       INTEGER NOT NULL,
            started_at       TEXT NOT NULL,
            finished_at      TEXT,
            duration_secs    REAL,
            new_songs        INTEGER DEFAULT 0,
            downloads_queued INTEGER DEFAULT 0,
            delivered        INTEGER DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'running',
            headline         TEXT,
            headline_level   TEXT DEFAULT 'info'
        )
    """)

    # Headline columns for pipeline_runs — added for activity dashboard summaries.
    # Same idempotent pattern as other migrations: try ADD COLUMN, ignore if exists.
    for stmt in [
        "ALTER TABLE pipeline_runs ADD COLUMN headline TEXT",
        "ALTER TABLE pipeline_runs ADD COLUMN headline_level TEXT DEFAULT 'info'",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # Seed default settings so there's always a value for each key.
    # INSERT OR IGNORE means existing values survive container restarts.
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    default_seeds = [
        ("scheduled_time", "00:00"),
        ("staging_dir", os.environ.get("DOWNLOAD_DIR", "/downloads")),
        ("final_dir", os.environ.get("DEST_DIR", "/dest")),
    ]
    for key, value in default_seeds:
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, now),
        )

    conn.commit()

    return conn


# ---------------------------------------------------------------------------
# Phase 10: Pipeline run logging + settings helpers
# ---------------------------------------------------------------------------

def log_run_start(conn: sqlite3.Connection, run_number: int) -> int:
    """
    Record the start of a pipeline run. Returns the new run ID.

    Called by the scheduler before run_pipeline() executes. The matching
    log_run_finish() or log_run_failed() call fills in the rest.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cursor = conn.execute(
        "INSERT INTO pipeline_runs (run_number, started_at, status) VALUES (?, ?, 'running')",
        (run_number, now),
    )
    conn.commit()
    return cursor.lastrowid


def log_run_finish(conn: sqlite3.Connection, run_id: int, stats: dict) -> None:
    """
    Record successful completion of a pipeline run with stats.

    stats dict may contain: new_songs, downloads_queued, delivered,
    headline, headline_level.
    Duration is computed from started_at.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = conn.execute(
        "SELECT started_at FROM pipeline_runs WHERE id = ?", (run_id,)
    ).fetchone()
    duration = None
    if row:
        try:
            start = datetime.fromisoformat(row["started_at"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(now.replace("Z", "+00:00"))
            duration = (end - start).total_seconds()
        except (ValueError, TypeError):
            pass

    conn.execute("""
        UPDATE pipeline_runs
        SET finished_at = ?, duration_secs = ?, new_songs = ?, downloads_queued = ?,
            delivered = ?, status = 'completed', headline = ?, headline_level = ?
        WHERE id = ?
    """, (
        now,
        duration,
        stats.get("new_songs", 0),
        stats.get("downloads_queued", 0),
        stats.get("delivered", 0),
        stats.get("headline"),
        stats.get("headline_level", "info"),
        run_id,
    ))
    conn.commit()


def log_run_failed(conn: sqlite3.Connection, run_id: int) -> None:
    """Mark a pipeline run as failed (crashed)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "UPDATE pipeline_runs SET finished_at = ?, status = 'failed', "
        "headline = 'Pipeline crashed — check logs', headline_level = 'error' "
        "WHERE id = ?",
        (now, run_id),
    )
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    """Read a single setting value, returning default if not found."""
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a setting value (insert or update)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        (key, value, now),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Phase 9: Sources table CRUD helpers
# ---------------------------------------------------------------------------

def get_enabled_sources(conn: sqlite3.Connection) -> list[dict]:
    """
    Return all enabled source rows ordered by id.

    Used by load_sources() to build adapter instances from the database.
    Returns a list of dicts with keys: id, type, name, config_json, enabled,
    date_added, date_updated.
    """
    cursor = conn.execute("""
        SELECT * FROM sources
        WHERE enabled = 1 AND deleted_at IS NULL
        ORDER BY id ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_all_sources(conn: sqlite3.Connection) -> list[dict]:
    """
    Return ALL source rows (enabled and disabled) ordered by id.

    Used by the dashboard to show all sources regardless of enabled state.
    Excludes soft-deleted sources.
    """
    cursor = conn.execute("SELECT * FROM sources WHERE deleted_at IS NULL ORDER BY id ASC")
    return [dict(row) for row in cursor.fetchall()]


def add_source(conn: sqlite3.Connection, type: str, name: str, config_json: str) -> int:
    """
    Insert a new source row into the sources table.

    Parameters:
        conn        — open sqlite3.Connection
        type        — source type: 'youtube', 'textfile', 'spotify', or 'discogs'
        name        — human-readable label shown in the dashboard
        config_json — JSON string with type-specific config fields

    Returns the new row's id.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cursor = conn.execute("""
        INSERT INTO sources (type, name, config_json, enabled, date_added)
        VALUES (?, ?, ?, 1, ?)
    """, (type, name, config_json, now))
    conn.commit()
    return cursor.lastrowid


def update_source(
    conn: sqlite3.Connection,
    id: int,
    name: str | None = None,
    config_json: str | None = None,
    enabled: int | None = None,
) -> None:
    """
    Update one or more fields on an existing source row.

    Only provided (non-None) fields are updated. Always sets date_updated.

    Parameters:
        conn        — open sqlite3.Connection
        id          — sources.id of the row to update
        name        — new human-readable label (optional)
        config_json — new JSON config blob (optional)
        enabled     — 1 to enable, 0 to disable (optional)
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    set_parts = ["date_updated = ?"]
    params: list = [now]

    if name is not None:
        set_parts.append("name = ?")
        params.append(name)
    if config_json is not None:
        set_parts.append("config_json = ?")
        params.append(config_json)
    if enabled is not None:
        set_parts.append("enabled = ?")
        params.append(enabled)

    params.append(id)
    conn.execute(f"UPDATE sources SET {', '.join(set_parts)} WHERE id = ?", params)
    conn.commit()


def delete_source(conn: sqlite3.Connection, id: int) -> None:
    """
    Soft-delete a source by setting deleted_at timestamp.

    The row is kept so orphaned songs can still resolve their source name
    for delivery folder routing. Soft-deleted sources are excluded from
    all dashboard queries and pipeline runs.

    Parameters:
        conn — open sqlite3.Connection
        id   — sources.id of the row to soft-delete
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute("UPDATE sources SET deleted_at = ? WHERE id = ?", (now, id))
    conn.commit()


def toggle_source(conn: sqlite3.Connection, id: int, enabled: int) -> None:
    """
    Enable or disable a source without changing any other fields.

    Convenience wrapper around update_source() for the dashboard toggle button.

    Parameters:
        conn    — open sqlite3.Connection
        id      — sources.id of the row to toggle
        enabled — 1 to enable, 0 to disable
    """
    update_source(conn, id, enabled=enabled)


def get_known_ids(conn: sqlite3.Connection) -> set[str]:
    """
    Return the set of all source_ids already in the database.

    Used to skip known songs before calling Haiku, saving API costs.
    """
    cursor = conn.execute("SELECT source_id FROM songs")
    return {row[0] for row in cursor.fetchall()}


# ---------------------------------------------------------------------------
# Peer exclusion helpers
# ---------------------------------------------------------------------------

def add_failed_peer(conn: sqlite3.Connection, source_id: str, username: str) -> None:
    """
    Record a peer that failed for this song so future retries skip them.

    Appends username to the song's failed_peers_json list. Idempotent —
    won't add duplicates.
    """
    row = conn.execute(
        "SELECT failed_peers_json FROM songs WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    if not row:
        return

    peers = json.loads(row["failed_peers_json"]) if row["failed_peers_json"] else []
    if username not in peers:
        peers.append(username)
        conn.execute(
            "UPDATE songs SET failed_peers_json = ? WHERE source_id = ?",
            (json.dumps(peers), source_id),
        )
        conn.commit()
        log.debug("Added failed peer %r for song %s (total: %d)", username, source_id, len(peers))


def get_failed_peers(conn: sqlite3.Connection, source_id: str) -> set[str]:
    """
    Return the set of peer usernames that have previously failed for this song.

    Used before searching to build the exclusion filter — any peer in this set
    will be filtered out of search results so we never retry a known-bad peer.
    """
    row = conn.execute(
        "SELECT failed_peers_json FROM songs WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    if not row or not row["failed_peers_json"]:
        return set()
    return set(json.loads(row["failed_peers_json"]))


def add_songs(conn: sqlite3.Connection, songs: list[dict]) -> int:
    """
    Insert new songs into the database, silently skipping any duplicates.

    Uses INSERT OR IGNORE so if a song was already stored (by source_id primary
    key), it is not overwritten. This makes the operation fully idempotent.

    Parameters:
        songs: list of dicts with keys {source_id, source_type, search_mode,
               raw_title, artist, title, version (optional)}

    Returns the number of rows actually inserted.
    """
    if not songs:
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = [
        (
            song["source_id"],
            song.get("source_type", "youtube"),
            song.get("search_mode", "track"),
            song["raw_title"],
            song["artist"],
            song["title"],
            song.get("version"),  # Phase 4: version field (None if not present)
            "new",
            now,
            song.get("source_table_id"),  # Phase 9.1: link to sources.id
        )
        for song in songs
    ]

    before = conn.total_changes
    conn.executemany("""
        INSERT OR IGNORE INTO songs
            (source_id, source_type, search_mode, raw_title, artist, title, version, status, date_added, source_table_id)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

    inserted = conn.total_changes - before
    return inserted


def mark_removed(conn: sqlite3.Connection, source_ids: set[str]):
    """
    Mark songs as 'removed' when they are no longer in the source.

    Only marks songs that have NOT been downloaded yet — once a file is
    downloaded, we keep it regardless of source changes (per project decision).

    Logs the count of songs marked removed.
    """
    if not source_ids:
        return

    placeholders = ",".join("?" * len(source_ids))
    cursor = conn.execute(f"""
        UPDATE songs
        SET status = 'removed'
        WHERE source_id IN ({placeholders})
          AND status NOT IN ('downloaded')
    """, list(source_ids))
    conn.commit()

    count = cursor.rowcount
    log.info("Marked %d song(s) as removed (no longer in source)", count)


def get_pending_songs(conn: sqlite3.Connection) -> list[dict]:
    """
    Return songs that are ready to be searched on Soulseek.

    # Status 'new' is Phase 2's insert default — equivalent to 'pending' in
    # the conceptual lifecycle described in CONTEXT.md. Do NOT query for
    # status='pending' — no such value exists in the database.

    Returns a list of dicts with: source_id, artist, title, raw_title.
    """
    cursor = conn.execute("""
        SELECT source_id, artist, title, raw_title
        FROM songs
        WHERE status = 'new'
          AND (search_mode = 'track' OR search_mode IS NULL)
        ORDER BY date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def update_song_status(
    conn: sqlite3.Connection,
    source_id: str,
    status: str,
    metadata: dict | None = None,
) -> None:
    """
    Transition a song to a new status and optionally record selection metadata.

    Valid statuses (full lifecycle):
      'new'             — inserted by Phase 2, not yet searched (initial state)
      'searching'       — search in progress
      'queued'          — download enqueued in slskd
      'downloaded'      — file confirmed downloaded
      'not_found'       — all query variants returned zero search results
      'no_match'        — results found but none passed quality/filename filters
      'removed'         — song no longer in source (set by mark_removed)
      'delivered'       — file moved to destination folder
      'stalled'         — download declared stalled, alt source queued
      'upgraded'        — FLAC found for previously MP3-downloaded song
      'stalled_waiting' — alt_source_count >= 3, no new sources, just wait
      'upgrade_queued'  — FLAC download queued, replacing existing MP3
      'downloading'     — album files enqueued in slskd, waiting for all to complete

    The metadata dict may contain any subset of these keys to update alongside
    the status column:
      slsk_username     — Soulseek username of the peer providing the file
      selected_filename — full path as provided in the slskd search response
      selected_format   — file extension (e.g., 'flac', 'mp3')
      selected_bitrate  — bitrate in kbps (may be None for lossless)
      date_queued       — ISO 8601 UTC timestamp when download was enqueued
      version           — remix/live/acoustic qualifier from parser
      local_path        — staging file path after download completes
      date_delivered    — ISO 8601 UTC timestamp when delivered
      stall_check_bytes — bytesTransferred at last stall check
      stall_check_time  — ISO 8601 UTC timestamp of last stall check
      alt_source_count  — how many alternative sources tried

    Parameters:
        conn      — open sqlite3.Connection
        source_id — primary key of the song to update
        status    — new status value (one of the valid statuses above)
        metadata  — optional dict of selection metadata to record alongside status
    """
    VALID_STATUSES = {
        "new", "searching", "queued", "downloaded",
        "not_found", "no_match", "removed",
        # Phase 4 statuses
        "delivered", "stalled", "upgraded", "stalled_waiting", "upgrade_queued",
        # Phase 8 statuses
        "downloading",  # Album files enqueued in slskd, waiting for all to complete
    }
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status {status!r}. Must be one of: {sorted(VALID_STATUSES)}")

    METADATA_COLUMNS = {
        "slsk_username", "selected_filename", "selected_format",
        "selected_bitrate", "date_queued", "date_downloaded",
        # Phase 4 metadata columns
        "version", "local_path", "date_delivered",
        "stall_check_bytes", "stall_check_time", "alt_source_count",
    }

    if metadata:
        # Build SET clause for status + any provided metadata columns
        extra_cols = {k: v for k, v in metadata.items() if k in METADATA_COLUMNS}
        set_parts = ["status = ?"]
        params = [status]
        for col, val in extra_cols.items():
            set_parts.append(f"{col} = ?")
            params.append(val)
        params.append(source_id)
        conn.execute(
            f"UPDATE songs SET {', '.join(set_parts)} WHERE source_id = ?",
            params,
        )
    elif status == "searching":
        # When transitioning to 'searching', also increment search_attempts and
        # record the timestamp. This powers the pending songs report (Phase 5).
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE songs
               SET status = ?,
                   last_search_date = ?,
                   search_attempts = COALESCE(search_attempts, 0) + 1
               WHERE source_id = ?""",
            (status, now, source_id),
        )
    else:
        conn.execute(
            "UPDATE songs SET status = ? WHERE source_id = ?",
            (status, source_id),
        )

    conn.commit()
    log.debug("Updated song %s -> status=%r (metadata=%r)", source_id, status, metadata)


def get_songs_by_status(conn: sqlite3.Connection, status: str) -> list[dict]:
    """
    Return all songs with the given status.

    General-purpose query used by the download loop and status-reporting utilities.
    Returns a list of row dicts with all columns.
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE status = ?
        ORDER BY date_added ASC
    """, (status,))
    return [dict(row) for row in cursor.fetchall()]


def get_downloaded_mp3s(conn: sqlite3.Connection) -> list[dict]:
    """
    Return songs that were downloaded as MP3 — candidates for quality upgrade checks.

    Phase 4 retry logic re-searches these songs for FLAC availability. If a FLAC
    is found, the old MP3 is replaced.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE status = 'downloaded'
          AND selected_format = 'mp3'
        ORDER BY date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_not_found_songs(conn: sqlite3.Connection, cooldown_days: int = 7) -> list[dict]:
    """
    Return track-mode songs that have never been found on Soulseek — candidates
    for retry. Albums use a separate retry path (get_not_found_albums) because
    they have a search_attempts budget that needs to reset on each cooldown.

    Phase 4 retry logic re-searches these, but with a cooldown to avoid hammering
    Soulseek for the same songs every run. Songs searched within the last
    cooldown_days are skipped — they'll be retried after the cooldown expires.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE status = 'not_found'
          AND (search_mode = 'track' OR search_mode IS NULL)
          AND (last_search_date IS NULL
               OR last_search_date < datetime('now', ? || ' days'))
        ORDER BY date_added ASC
    """, (str(-cooldown_days),))
    return [dict(row) for row in cursor.fetchall()]


def get_stalled_songs(conn: sqlite3.Connection, cooldown_days: int = 7) -> list[dict]:
    """
    Return track-mode songs stuck in stalled/stalled_waiting — candidates for
    auto-retry. Albums never enter these states (they go through 'downloading'
    instead), so this query is track-only.

    With peer exclusion (failed_peers_json), retrying is safe because the system
    always picks a different peer. Songs searched within the last cooldown_days
    are skipped to avoid hammering Soulseek.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE status IN ('stalled', 'stalled_waiting')
          AND (search_mode = 'track' OR search_mode IS NULL)
          AND (last_search_date IS NULL
               OR last_search_date < datetime('now', ? || ' days'))
        ORDER BY date_added ASC
    """, (str(-cooldown_days),))
    return [dict(row) for row in cursor.fetchall()]


def get_no_match_songs(conn: sqlite3.Connection, cooldown_days: int = 7) -> list[dict]:
    """
    Return track-mode songs where results were found but none passed quality
    filters. Albums never reach 'no_match' (folder selection produces a result
    or None, which goes to the search_attempts path), so this query is track-only.

    Re-searched periodically because Soulseek users come and go — a song that
    had no quality match last week may have a new FLAC share today. Songs
    searched within the last cooldown_days are skipped to avoid wasted effort.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE status = 'no_match'
          AND (search_mode = 'track' OR search_mode IS NULL)
          AND (last_search_date IS NULL
               OR last_search_date < datetime('now', ? || ' days'))
        ORDER BY date_added ASC
    """, (str(-cooldown_days),))
    return [dict(row) for row in cursor.fetchall()]


def get_not_found_albums(conn: sqlite3.Connection, cooldown_days: int = 7) -> list[dict]:
    """
    Return album-mode entries currently marked not_found and past the cooldown.

    Unlike track retries, an album retry must reset both the attempts budget
    (search_attempts) and the peer exclusions (failed_peers_json) so the next
    cycle gets a fresh shot. Use reset_album_for_retry() to perform the reset.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT *
        FROM songs
        WHERE search_mode = 'album'
          AND status = 'not_found'
          AND (last_search_date IS NULL
               OR last_search_date < datetime('now', ? || ' days'))
        ORDER BY date_added ASC
    """, (str(-cooldown_days),))
    return [dict(row) for row in cursor.fetchall()]


def reset_album_for_retry(conn: sqlite3.Connection, source_id: str) -> None:
    """
    Reset an album to the search-eligible state with a fresh budget.

    Sets status='new', search_attempts=0, failed_peers_json=NULL so the album
    re-enters get_pending_albums() and explores all available peers from
    scratch. Called by the retry phase after an album's not_found cooldown
    has elapsed.
    """
    conn.execute("""
        UPDATE songs
        SET status = 'new',
            search_attempts = 0,
            failed_peers_json = NULL
        WHERE source_id = ?
    """, (source_id,))
    conn.commit()


def get_search_attempts(conn: sqlite3.Connection, source_id: str) -> int:
    """
    Return the current search_attempts count for a song. NULL is returned as 0.

    Used by the album loop after the auto-increment in update_song_status('searching')
    so we can decide whether the budget is exhausted without performing a second
    increment.
    """
    row = conn.execute(
        "SELECT search_attempts FROM songs WHERE source_id = ?", (source_id,)
    ).fetchone()
    if not row:
        return 0
    return row["search_attempts"] or 0


def get_songs_for_delivery(conn: sqlite3.Connection) -> list[dict]:
    """
    Return songs that have been downloaded (or upgraded) and are ready to move
    to the destination folder.

    A song is deliverable when:
      - status is 'downloaded' (normal first delivery) OR 'upgraded' (FLAC
        replacement for a previously MP3-delivered song)
      - local_path is set (file exists on the staging volume)
      - date_delivered IS NULL (not yet delivered in its current status)

    Note: upgraded songs may have a non-NULL date_delivered from the original
    MP3 delivery. The query selects them regardless of date_delivered so the
    FLAC can be delivered and the old MP3 can be cleaned up. Only 'downloaded'
    songs use the date_delivered IS NULL guard.

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT songs.*, sources.name AS source_name
        FROM songs
        LEFT JOIN sources ON songs.source_table_id = sources.id
        WHERE (
            (songs.status = 'downloaded' AND songs.date_delivered IS NULL)
            OR songs.status = 'upgraded'
        )
          AND songs.local_path IS NOT NULL
        ORDER BY songs.date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_pending_albums(conn: sqlite3.Connection) -> list[dict]:
    """
    Return album-mode songs ready to be searched on Soulseek.

    Only returns entries with search_mode='album', status='new', and fewer
    than 10 search attempts. After 10 failed searches the album is considered
    permanently not found and excluded from future runs.

    Returns a list of dicts with: source_id, artist, title, search_attempts.
    """
    cursor = conn.execute("""
        SELECT source_id, artist, title, search_attempts
        FROM songs
        WHERE search_mode = 'album'
          AND status = 'new'
          AND (search_attempts IS NULL OR search_attempts < 10)
        ORDER BY date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_downloading_albums(conn: sqlite3.Connection) -> list[dict]:
    """
    Return album-mode songs currently in the 'downloading' state.

    These are albums where files have been enqueued in slskd and we are
    waiting for all of them to complete before delivering. The pipeline
    checks these each run to see if all files are now downloaded.

    Returns a list of dicts with: source_id, artist, title, slsk_username.
    """
    cursor = conn.execute("""
        SELECT source_id, artist, title, slsk_username
        FROM songs
        WHERE search_mode = 'album'
          AND status = 'downloading'
        ORDER BY date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


def insert_album_files(
    conn: sqlite3.Connection,
    source_id: str,
    username: str,
    files: list[dict],
) -> int:
    """
    Insert one album_files row per file from a selected album folder.

    Called immediately after enqueue_album_folder() succeeds, creating the
    child records that will be monitored for completion. Each file dict must
    contain: filename, size, extension.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id (FK to songs table)
        username  — Soulseek peer username the files are being downloaded from
        files     — list of file dicts from the directory["files"] browse result

    Returns the number of rows inserted.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [
        (
            source_id,
            username,
            f["filename"],
            f["size"],
            f.get("extension", "").lower(),
            "queued",
            now,
        )
        for f in files
    ]
    before = conn.total_changes
    conn.executemany("""
        INSERT INTO album_files
            (source_id, slsk_username, filename, size, extension, status, date_queued)
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return conn.total_changes - before


def all_album_files_complete(conn: sqlite3.Connection, source_id: str) -> bool:
    """
    Return True if all album_files for source_id have been downloaded.

    Completion requires at least one file (guards against empty album_files)
    AND every file having status='downloaded'.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id to check

    Returns True when total > 0 AND total == downloaded count.
    """
    row = conn.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status = 'downloaded' THEN 1 ELSE 0 END) AS done
        FROM album_files
        WHERE source_id = ?
    """, (source_id,)).fetchone()
    total = row["total"] or 0
    done = row["done"] or 0
    return total > 0 and total == done


def any_album_files_failed(conn: sqlite3.Connection, source_id: str) -> bool:
    """
    Return True if any album_files row for source_id has status='failed'.

    Used to detect when a partial download has permanently failed, triggering
    a retry from a different peer.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id to check
    """
    row = conn.execute("""
        SELECT COUNT(*) AS failed_count
        FROM album_files
        WHERE source_id = ? AND status = 'failed'
    """, (source_id,)).fetchone()
    return (row["failed_count"] or 0) > 0


def get_album_files_by_source(conn: sqlite3.Connection, source_id: str) -> list[dict]:
    """
    Return all album_files rows for a given source_id.

    Used by the polling loop to check current transfer state for each file
    in a downloading album.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id

    Returns a list of row dicts with all album_files columns.
    """
    cursor = conn.execute("""
        SELECT *
        FROM album_files
        WHERE source_id = ?
        ORDER BY id ASC
    """, (source_id,))
    return [dict(row) for row in cursor.fetchall()]


def reset_album_files(conn: sqlite3.Connection, source_id: str) -> None:
    """
    Delete all album_files rows for a source_id and reset the parent song for retry.

    Called when a partial album download has permanently failed and we want to
    retry from a different peer. Removes all child records and resets the parent
    song to status='new' so it will be picked up by get_pending_albums() again.
    Also increments alt_source_count to track how many peers have been tried.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id to reset
    """
    conn.execute("DELETE FROM album_files WHERE source_id = ?", (source_id,))
    conn.execute("""
        UPDATE songs
        SET status = 'new',
            alt_source_count = COALESCE(alt_source_count, 0) + 1
        WHERE source_id = ?
    """, (source_id,))
    conn.commit()
    log.info("Reset album files and parent song for retry: %s", source_id)


def update_album_file_status(
    conn: sqlite3.Connection,
    file_id: int,
    status: str,
    metadata: dict | None = None,
) -> None:
    """
    Update a single album_files row's status and optional metadata.

    Valid statuses: 'queued', 'downloaded', 'failed'

    Parameters:
        conn     — open sqlite3.Connection
        file_id  — album_files.id of the row to update
        status   — new status value
        metadata — optional dict with: date_downloaded, local_path
    """
    VALID_FILE_STATUSES = {"queued", "downloaded", "failed"}
    if status not in VALID_FILE_STATUSES:
        raise ValueError(
            f"Invalid album file status {status!r}. Must be one of: {sorted(VALID_FILE_STATUSES)}"
        )

    METADATA_COLUMNS = {"date_downloaded", "local_path"}

    if metadata:
        extra_cols = {k: v for k, v in metadata.items() if k in METADATA_COLUMNS}
        set_parts = ["status = ?"]
        params = [status]
        for col, val in extra_cols.items():
            set_parts.append(f"{col} = ?")
            params.append(val)
        params.append(file_id)
        conn.execute(
            f"UPDATE album_files SET {', '.join(set_parts)} WHERE id = ?",
            params,
        )
    else:
        conn.execute(
            "UPDATE album_files SET status = ? WHERE id = ?",
            (status, file_id),
        )
    conn.commit()
    log.debug("Updated album_file id=%s -> status=%r", file_id, status)


def mark_album_not_found(conn: sqlite3.Connection, source_id: str) -> None:
    """
    Mark an album as permanently not found after exceeding 10 search attempts.

    Convenience wrapper — transitions the parent song to 'not_found' so it is
    excluded from future pipeline runs and shows up in the pending report as
    a long-standing failure.

    Parameters:
        conn      — open sqlite3.Connection
        source_id — parent song source_id to mark
    """
    conn.execute(
        "UPDATE songs SET status = 'not_found' WHERE source_id = ?",
        (source_id,),
    )
    conn.commit()
    log.info("Marked album as not_found after max search attempts: %s", source_id)


def get_albums_for_delivery(conn: sqlite3.Connection) -> list[dict]:
    """
    Return album entries that are ready to be delivered.

    An album is deliverable when:
      - search_mode is 'album'
      - status is 'downloaded' (all files complete)
      - local_path is set (folder exists on staging volume)
      - date_delivered IS NULL (not yet delivered)

    Returns all columns, ordered by date_added ASC (oldest first).
    """
    cursor = conn.execute("""
        SELECT songs.*, sources.name AS source_name
        FROM songs
        LEFT JOIN sources ON songs.source_table_id = sources.id
        WHERE songs.search_mode = 'album'
          AND songs.status = 'downloaded'
          AND songs.local_path IS NOT NULL
          AND songs.date_delivered IS NULL
        ORDER BY songs.date_added ASC
    """)
    return [dict(row) for row in cursor.fetchall()]


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s: %(message)s")

    print("Running state.py standalone test...\n")

    # Wipe and recreate for a clean test
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    conn = init_db()
    print(f"Database created at {DB_PATH}")

    # Insert a test song
    test_songs = [
        {
            "source_id": "dQw4w9WgXcQ",
            "source_type": "youtube",
            "search_mode": "track",
            "raw_title": "Rick Astley - Never Gonna Give You Up (Official Video)",
            "artist": "Rick Astley",
            "title": "Never Gonna Give You Up",
        }
    ]
    inserted = add_songs(conn, test_songs)
    print(f"Inserted {inserted} song(s)")

    # Verify idempotency — second insert should not add a duplicate
    inserted_again = add_songs(conn, test_songs)
    print(f"Re-inserted same song: {inserted_again} added (expected 0 — idempotent)")

    # Query it back
    cursor = conn.execute("SELECT source_id, artist, title, status, date_added FROM songs")
    rows = cursor.fetchall()
    print(f"\nDatabase contents ({len(rows)} row(s)):")
    for row in rows:
        print(f"  [{row['source_id']}] {row['artist']} — {row['title']} | {row['status']} | {row['date_added']}")

    # Test get_pending_songs — should return the song with status='new'
    pending = get_pending_songs(conn)
    print(f"\nget_pending_songs(): {len(pending)} song(s) (expected 1)")
    for song in pending:
        print(f"  {song['artist']} — {song['title']} | source_id={song['source_id']}")

    # Test update_song_status — transition through the lifecycle
    print("\nTesting status transitions:")

    update_song_status(conn, "dQw4w9WgXcQ", "searching")
    row = conn.execute("SELECT status FROM songs WHERE source_id = 'dQw4w9WgXcQ'").fetchone()
    print(f"  After searching: {row['status']}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    update_song_status(conn, "dQw4w9WgXcQ", "queued", metadata={
        "slsk_username": "somepeer",
        "selected_filename": "C:\\Music\\Rick Astley - Never Gonna Give You Up.flac",
        "selected_format": "flac",
        "selected_bitrate": None,
        "date_queued": now,
    })
    row = conn.execute(
        "SELECT status, slsk_username, selected_format, date_queued FROM songs WHERE source_id = 'dQw4w9WgXcQ'"
    ).fetchone()
    print(f"  After queued: status={row['status']}, peer={row['slsk_username']}, format={row['selected_format']}")

    # Test get_songs_by_status
    queued = get_songs_by_status(conn, "queued")
    print(f"\nget_songs_by_status('queued'): {len(queued)} song(s) (expected 1)")

    # Test mark_removed — now logs instead of prints
    mark_removed(conn, {"dQw4w9WgXcQ"})
    row = conn.execute("SELECT status FROM songs WHERE source_id = 'dQw4w9WgXcQ'").fetchone()
    print(f"Status after mark_removed: {row['status']}")
    print("  Note: 'queued' status is protected — mark_removed should have skipped it")

    # Verify schema has Phase 3 columns
    cursor = conn.execute("PRAGMA table_info(songs)")
    columns = [row["name"] for row in cursor.fetchall()]
    phase3_cols = {"slsk_username", "selected_filename", "selected_format", "selected_bitrate", "date_queued"}
    missing = phase3_cols - set(columns)
    if missing:
        print(f"\n[FAIL] Missing Phase 3 columns: {missing}")
    else:
        print(f"\n[OK] All Phase 3 columns present: {sorted(phase3_cols)}")

    # Verify schema has Phase 4 columns
    phase4_cols = {"version", "local_path", "date_delivered", "stall_check_bytes", "stall_check_time", "alt_source_count"}
    missing4 = phase4_cols - set(columns)
    if missing4:
        print(f"\n[FAIL] Missing Phase 4 columns: {missing4}")
    else:
        print(f"[OK] All Phase 4 columns present: {sorted(phase4_cols)}")

    # Verify Phase 6 schema columns
    phase6_cols = {"source_id", "source_type", "search_mode"}
    missing6 = phase6_cols - set(columns)
    if missing6:
        print(f"\n[FAIL] Missing Phase 6 columns: {missing6}")
    else:
        print(f"[OK] All Phase 6 columns present: {sorted(phase6_cols)}")
    assert "youtube_id" not in columns, "youtube_id column should not exist after Phase 6 migration"
    print("[OK] youtube_id column absent — migration complete")

    # Verify Phase 4 statuses are accepted
    try:
        for s in ("delivered", "stalled", "upgraded", "stalled_waiting", "upgrade_queued"):
            update_song_status(conn, "dQw4w9WgXcQ", s)
        print("[OK] All Phase 4 status values accepted by update_song_status()")
    except ValueError as e:
        print(f"[FAIL] Status validation error: {e}")

    # Verify new query helpers are callable
    mp3s = get_downloaded_mp3s(conn)
    not_found = get_not_found_songs(conn)
    deliverable = get_songs_for_delivery(conn)
    print(f"[OK] New query helpers callable: get_downloaded_mp3s={len(mp3s)}, get_not_found_songs={len(not_found)}, get_songs_for_delivery={len(deliverable)}")

    conn.close()
    print("\nDone.")
