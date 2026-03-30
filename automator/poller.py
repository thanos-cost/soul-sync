"""
poller.py — Download completion detection module.

Polls slskd for finished downloads and updates the database with completion
status and local file paths. Called by the scheduler after each pipeline run.

The slskd transfers API nests files inside directories inside user entries:
    [{"username": "...", "directories": [{"files": [...]}]}]

Succeeded files have state "Completed, Succeeded". Any state starting with
"Completed," is terminal (Completed, Cancelled / Completed, TimedOut / etc).
"""

import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import slskd_client
from state import (
    update_song_status,
    get_downloading_albums,
    get_album_files_by_source,
    update_album_file_status,
    all_album_files_complete,
    any_album_files_failed,
    reset_album_files,
    add_failed_peer,
    get_failed_peers,
)

log = logging.getLogger(__name__)

STALL_TIMEOUT_HOURS = 24   # Declare a download stalled if no byte progress in this many hours
ALT_SOURCE_CAP = 3         # Maximum number of alternative sources to try per song
ALBUM_STALL_TIMEOUT_DAYS = 7  # Declare an album download stalled if no byte progress in this many days


# ---------------------------------------------------------------------------
# Local file finder
# ---------------------------------------------------------------------------

def _strip_slskd_suffix(filename: str) -> str:
    """
    Strip slskd's duplicate-download suffix from a filename.

    When slskd downloads the same file multiple times (e.g. from stall retries),
    it appends a unique numeric suffix to avoid collisions:
        track.flac → track_639083147689283554.flac

    This strips that suffix so we can match the file back to the original.
    """
    return re.sub(r'_\d{15,}(?=\.\w+$)', '', filename)


def find_local_file(filename: str, downloads_dir: str = "/downloads") -> str | None:
    """
    Find a downloaded file on disk by matching against its basename.

    The slskd API reports Windows-style paths (e.g. C:\\Music\\track.flac).
    We extract the basename and walk the downloads directory to find the
    actual file, since slskd may nest files in subdirectories.

    Two-pass matching:
      1. Exact basename match (fastest, most common case)
      2. Suffix-stripped match — handles slskd duplicate suffixes like
         track_639083147689283554.flac when looking for track.flac

    Parameters:
        filename      — Windows-style path as returned by the slskd API
        downloads_dir — root directory to search (default: /downloads volume)

    Returns the full local path string if found, None otherwise.
    """
    # Extract just the filename from a Windows-style path
    basename = filename.rsplit("\\", 1)[-1]
    if not basename:
        return None

    try:
        # Pass 1: exact match
        for path in Path(downloads_dir).rglob("*"):
            if path.is_file() and path.name == basename:
                return str(path)

        # Pass 2: match after stripping slskd duplicate suffixes from disk files
        for path in Path(downloads_dir).rglob("*"):
            if path.is_file() and _strip_slskd_suffix(path.name) == basename:
                return str(path)
    except Exception as exc:
        log.error("find_local_file() failed while scanning %s: %s", downloads_dir, exc)

    return None


# ---------------------------------------------------------------------------
# Recovery: fix downloaded songs with NULL local_path
# ---------------------------------------------------------------------------

def recover_missing_local_paths(conn) -> int:
    """
    Scan for downloaded songs stuck with NULL local_path and try to find their files.

    These songs completed downloading (status='downloaded') but the poller didn't
    find the file on disk at the time — maybe a timing issue, or the file was
    in an unexpected location. Without local_path, the delivery step skips them,
    so they sit in staging forever.

    For each stuck song: look for the file using selected_filename. If found,
    update local_path so delivery can pick it up next cycle.

    Returns the count of songs recovered.
    """
    cursor = conn.execute("""
        SELECT source_id, artist, title, selected_filename
        FROM songs
        WHERE status = 'downloaded'
          AND local_path IS NULL
          AND date_delivered IS NULL
          AND selected_filename IS NOT NULL
    """)
    stuck = [dict(row) for row in cursor.fetchall()]

    if not stuck:
        return 0

    log.info("Recovery: checking %d downloaded song(s) with NULL local_path", len(stuck))

    recovered = 0
    for song in stuck:
        local_path = find_local_file(song["selected_filename"])
        if local_path:
            conn.execute(
                "UPDATE songs SET local_path = ? WHERE source_id = ?",
                (local_path, song["source_id"]),
            )
            conn.commit()
            log.info(
                "Recovery: found local_path for %s - %s -> %s",
                song["artist"], song["title"], local_path,
            )
            recovered += 1
        else:
            log.warning(
                "Recovery: file still not found for %s - %s (filename=%s)",
                song["artist"], song["title"],
                song["selected_filename"].rsplit("\\", 1)[-1] if song["selected_filename"] else "?",
            )

    if recovered:
        log.info("Recovery: found local_path for %d song(s)", recovered)

    return recovered


# ---------------------------------------------------------------------------
# Recovery: queued songs whose files already exist on disk
# ---------------------------------------------------------------------------

def recover_queued_with_files(conn) -> int:
    """
    Recover songs stuck at 'queued' whose files are already on disk.

    This happens when slskd transfer records get cleared (e.g. user clears
    completed downloads in the UI) before the poller sees the "Completed,
    Succeeded" state. The file is physically downloaded but the song stays
    queued forever because the poller never gets a chance to transition it.

    For each stuck song: check if the file exists using selected_filename.
    If found, mark as 'downloaded' with local_path so delivery picks it up.

    Returns the count of songs recovered.
    """
    cursor = conn.execute("""
        SELECT source_id, artist, title, selected_filename
        FROM songs
        WHERE status = 'queued'
          AND selected_filename IS NOT NULL
    """)
    stuck = [dict(row) for row in cursor.fetchall()]

    if not stuck:
        return 0

    log.info("Recovery (queued): checking %d song(s) with selected_filename", len(stuck))

    recovered = 0
    now_str = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    for song in stuck:
        local_path = find_local_file(song["selected_filename"])
        if local_path:
            conn.execute(
                """UPDATE songs
                   SET status = 'downloaded',
                       local_path = ?,
                       date_downloaded = ?
                 WHERE source_id = ?""",
                (local_path, now_str, song["source_id"]),
            )
            conn.commit()
            log.info(
                "Recovery (queued): %s - %s -> downloaded (%s)",
                song["artist"], song["title"], local_path,
            )
            recovered += 1

    if recovered:
        log.info("Recovery (queued): promoted %d song(s) to downloaded", recovered)

    return recovered


# ---------------------------------------------------------------------------
# Download polling
# ---------------------------------------------------------------------------

def poll_downloads(client) -> tuple[list[dict], list[dict]]:
    """
    Poll slskd for completed downloads.

    Iterates the nested transfers structure (user -> directories -> files)
    and separates succeeded files from other terminal-state files.

    State strings used by slskd:
      "Completed, Succeeded"  — download finished successfully
      "Completed, *"          — any other terminal state (cancelled, timed out, etc.)
      Anything else           — still in progress (Queued, Initializing, InProgress, etc.)

    Returns:
        (succeeded_list, all_terminal_list)

        Each entry in succeeded_list is a dict with:
            username, filename, size, id
    """
    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)

        succeeded = []
        terminal = []

        for user_entry in all_downloads:
            username = user_entry.get("username", "")
            for directory in user_entry.get("directories", []):
                for file in directory.get("files", []):
                    state = file.get("state", "")

                    if state == "Completed, Succeeded":
                        succeeded.append({
                            "username": username,
                            "filename": file.get("filename", ""),
                            "size": file.get("size", 0),
                            "id": file.get("id", ""),
                        })
                        terminal.append(file)
                    elif state.startswith("Completed,"):
                        terminal.append(file)

        log.debug(
            "poll_downloads(): %d succeeded, %d total terminal",
            len(succeeded), len(terminal),
        )
        return succeeded, terminal

    except Exception as exc:
        log.error(
            "poll_downloads() failed — raw response logged below. "
            "The response shape may differ from what was expected. Error: %s",
            exc,
        )
        try:
            raw = client.transfers.get_all_downloads(includeRemoved=False)
            log.error("Raw get_all_downloads() response: %r", raw)
        except Exception:
            pass
        return [], []


# ---------------------------------------------------------------------------
# Poll and update DB
# ---------------------------------------------------------------------------

def poll_and_update(client, conn) -> int:
    """
    Poll slskd for completed downloads and update the database.

    For each succeeded download:
      1. Match against queued songs in the DB by (slsk_username, selected_filename)
      2. If found and still in 'queued' status: find the local file path, then
         update status to 'downloaded' with date and local_path recorded

    Returns the count of songs newly marked as downloaded this cycle.
    """
    succeeded, _ = poll_downloads(client)

    if not succeeded:
        log.debug("Poller: no succeeded downloads this cycle")
        return 0

    log.info("Poller: found %d succeeded download(s) from slskd", len(succeeded))

    # Load all queued/upgrade_queued songs from DB for matching.
    # upgrade_queued songs are FLAC replacements for previously MP3-downloaded songs.
    cursor = conn.execute("""
        SELECT source_id, artist, title, slsk_username, selected_filename, selected_format, status
        FROM songs
        WHERE status IN ('queued', 'upgrade_queued')
          AND slsk_username IS NOT NULL
          AND selected_filename IS NOT NULL
    """)
    queued_songs = [dict(row) for row in cursor.fetchall()]

    if not queued_songs:
        log.debug("Poller: no queued songs in DB to match against")
        return 0

    # Build lookup by (username, filename) for O(1) matching
    queued_lookup: dict[tuple, dict] = {
        (song["slsk_username"], song["selected_filename"]): song
        for song in queued_songs
    }

    # Fallback lookup by basename only — handles cases where alt-source retry
    # overwrote (slsk_username, selected_filename) but the OLD peer's download
    # completed. Multiple songs may share a basename, so we store a list.
    basename_lookup: dict[str, list[dict]] = {}
    for song in queued_songs:
        bn = song["selected_filename"].rsplit("\\", 1)[-1]
        basename_lookup.setdefault(bn, []).append(song)

    marked_downloaded = 0

    for dl in succeeded:
        key = (dl["username"], dl["filename"])
        song = queued_lookup.get(key)

        if not song:
            # Exact match failed — try basename fallback.
            # This catches downloads from peers whose username/filename was
            # overwritten in the DB by a subsequent alt-source retry.
            dl_basename = dl["filename"].rsplit("\\", 1)[-1]
            dl_basename_clean = _strip_slskd_suffix(dl_basename)
            candidates = basename_lookup.get(dl_basename, [])
            if not candidates:
                candidates = basename_lookup.get(dl_basename_clean, [])
            if len(candidates) == 1:
                song = candidates[0]
                log.info(
                    "Poller: matched %s by basename fallback (peer=%s)",
                    dl_basename, dl["username"],
                )
            elif len(candidates) > 1:
                # Ambiguous — multiple queued songs share this basename.
                # Skip to avoid matching the wrong song.
                log.warning(
                    "Poller: basename %s matches %d queued songs — skipping ambiguous match",
                    dl_basename, len(candidates),
                )

        if not song:
            continue

        source_id = song["source_id"]
        artist = song["artist"]
        title = song["title"]
        fmt = song.get("selected_format", "")
        song_status = song.get("status", "queued")

        # Find the file on disk (slskd doesn't reliably report local paths via API)
        local_path = find_local_file(dl["filename"])
        if not local_path:
            log.warning(
                "Download succeeded but file not found on disk: %s (will still mark complete)",
                dl["filename"].rsplit("\\", 1)[-1],
            )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if song_status == "upgrade_queued":
            # This is a FLAC upgrade for a previously MP3-downloaded song.
            # Transition to 'upgraded' — the delivery module will deliver the
            # FLAC and delete the old MP3 from the destination on the next batch run.
            update_song_status(conn, source_id, "upgraded", metadata={
                "date_downloaded": now,
                "local_path": local_path,
            })
            log.info(
                "Upgrade download complete: %s - %s (%s)%s",
                artist, title, fmt,
                f" -> {local_path}" if local_path else " (local path not found)",
            )
        else:
            update_song_status(conn, source_id, "downloaded", metadata={
                "date_downloaded": now,
                "local_path": local_path,
            })
            log.info(
                "Download complete: %s - %s (%s)%s",
                artist, title, fmt,
                f" -> {local_path}" if local_path else " (local path not found)",
            )

        marked_downloaded += 1

    log.info("Poller: %d download(s) completed this cycle", marked_downloaded)
    return marked_downloaded


# ---------------------------------------------------------------------------
# Stall detection and alternative source queuing
# ---------------------------------------------------------------------------

def check_stalled_downloads(client, conn, log: logging.Logger) -> int:
    """
    Detect downloads that have made no byte progress in 24+ hours and queue
    alternative sources for them.

    Two stall triggers:
      1. Bytes unchanged since last check AND stall_check_time is > 24 hours ago
      2. Transfer has a non-Succeeded "Completed," state (failed terminal state)

    When stalled:
      - If alt_source_count < ALT_SOURCE_CAP (3): search for an alternative
        peer (excluding the current stalled peer), enqueue the new source,
        and increment alt_source_count
      - If alt_source_count >= ALT_SOURCE_CAP: set status to 'stalled_waiting'
        and do nothing further — a human may need to intervene

    Stall detection is advisory — it cannot cancel the existing slskd transfer
    (the slskd API does not support per-transfer cancel in all versions). The
    old transfer will time out naturally; the new source runs in parallel.

    Returns the count of stalled downloads handled this cycle.
    """
    # Load all songs currently being downloaded (status='queued')
    cursor = conn.execute("""
        SELECT
            source_id, artist, title,
            slsk_username, selected_filename, selected_format,
            stall_check_bytes, stall_check_time, alt_source_count
        FROM songs
        WHERE status = 'queued'
          AND slsk_username IS NOT NULL
          AND selected_filename IS NOT NULL
    """)
    queued_songs = [dict(row) for row in cursor.fetchall()]

    if not queued_songs:
        log.debug("Stall check: no queued songs to inspect")
        return 0

    # Get all current transfers from the slskd API
    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)
    except Exception as exc:
        log.error("Stall check: could not fetch transfers from slskd: %s", exc)
        return 0

    # Build a flat lookup of active/terminal transfers keyed by (username, filename)
    # Each value contains the state and bytes transferred
    transfer_lookup: dict[tuple, dict] = {}
    for user_entry in all_downloads:
        username = user_entry.get("username", "")
        for directory in user_entry.get("directories", []):
            for file in directory.get("files", []):
                fname = file.get("filename", "")
                key = (username, fname)
                transfer_lookup[key] = {
                    "state": file.get("state", ""),
                    "bytesTransferred": file.get("bytesTransferred", 0),
                }

    now_utc = datetime.now(timezone.utc)
    stall_count = 0

    for song in queued_songs:
        source_id = song["source_id"]
        artist = song["artist"]
        title = song["title"]
        username = song["slsk_username"]
        filename = song["selected_filename"]
        alt_count = song.get("alt_source_count") or 0

        key = (username, filename)
        transfer = transfer_lookup.get(key)

        stalled = False
        stall_reason = ""

        if transfer is None:
            # Transfer not found in API — may have been removed or never registered.
            # Don't act on this yet; could be a timing issue on first check.
            log.debug(
                "Stall check: transfer not found for %s - %s (peer=%s)",
                artist, title, username,
            )
            continue

        state = transfer["state"]
        bytes_now = transfer.get("bytesTransferred", 0) or 0

        if state.startswith("Completed,") and state != "Completed, Succeeded":
            # Transfer hit a terminal failure state — treat as immediate stall
            stalled = True
            stall_reason = f"terminal failure state: {state!r}"

        elif not state.startswith("Completed,"):
            # Transfer is still active — check byte progress
            stall_check_bytes = song.get("stall_check_bytes")
            stall_check_time_str = song.get("stall_check_time")

            if stall_check_bytes is None:
                # First time we're checking this transfer — record the baseline
                update_song_status(conn, source_id, "queued", metadata={
                    "stall_check_bytes": bytes_now,
                    "stall_check_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
                log.debug(
                    "Stall check: baseline set for %s - %s (bytes=%d)",
                    artist, title, bytes_now,
                )
                continue

            if bytes_now != stall_check_bytes:
                # Bytes have changed — progress is happening, update the baseline
                update_song_status(conn, source_id, "queued", metadata={
                    "stall_check_bytes": bytes_now,
                    "stall_check_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
                log.debug(
                    "Stall check: progress on %s - %s (%d -> %d bytes)",
                    artist, title, stall_check_bytes, bytes_now,
                )
                continue

            # Bytes unchanged — check how long it's been stalled
            if stall_check_time_str:
                try:
                    stall_since = datetime.fromisoformat(
                        stall_check_time_str.replace("Z", "+00:00")
                    )
                    stalled_hours = (now_utc - stall_since).total_seconds() / 3600
                    if stalled_hours >= STALL_TIMEOUT_HOURS:
                        stalled = True
                        stall_reason = (
                            f"no byte progress for {stalled_hours:.1f}h "
                            f"(bytes={bytes_now}, since={stall_check_time_str})"
                        )
                except Exception:
                    log.warning(
                        "Stall check: could not parse stall_check_time %r for %s - %s",
                        stall_check_time_str, artist, title,
                    )

        if not stalled:
            continue

        log.warning(
            "Stalled download: %s - %s from %s, %s",
            artist, title, username, stall_reason,
        )
        stall_count += 1

        # Record this peer as failed so future retries skip them
        add_failed_peer(conn, source_id, username)

        # Check the alternative source cap
        if alt_count >= ALT_SOURCE_CAP:
            update_song_status(conn, source_id, "stalled_waiting")
            log.warning(
                "%s - %s has %d+ stalled sources — setting stalled_waiting",
                artist, title, ALT_SOURCE_CAP,
            )
            continue

        # Try to find an alternative source, excluding the stalled peer
        _queue_alternative_source(client, conn, song, username, now_utc, log)

    return stall_count


def _queue_alternative_source(
    client,
    conn,
    song: dict,
    stalled_username: str,
    now_utc: datetime,
    log: logging.Logger,
) -> None:
    """
    Search for an alternative download source for a stalled song and enqueue it.

    Excludes the stalled peer to avoid re-downloading from the same offline user.
    Uses the same quality preference as the original download — if the original
    was FLAC, prefer FLAC; fall back to lower tiers only if unavailable.

    Updates the DB to reflect the new source or marks the song as 'stalled'
    if no alternative is found.
    """
    # Import here to avoid module-level circular dependency.
    # poller.py is imported lazily inside scheduler.py's loop.
    from main import build_query_rounds
    from selector import select_best

    source_id = song["source_id"]
    artist = song["artist"]
    title = song["title"]
    alt_count = song.get("alt_source_count") or 0
    original_format = song.get("selected_format", "")

    # Build the full exclusion set — all peers that have ever failed for this song
    excluded_peers = get_failed_peers(conn, source_id)
    excluded_peers.add(stalled_username)  # Include current peer in case it wasn't recorded yet

    try:
        rounds = build_query_rounds(artist, title)
        best_alt = None

        for round_num, rnd in enumerate(rounds, 1):
            if best_alt:
                break

            match_title = rnd["match_title"]
            for query in rnd["queries"]:
                log.info(
                    "  [Alt source Round %d] Searching for alternative: %r (excluding %d peer(s))",
                    round_num, query, len(excluded_peers),
                )
                responses = slskd_client.search(client, query)

                if not responses:
                    time.sleep(2)
                    continue

                # Filter out ALL previously failed peers from responses
                filtered_responses = [
                    r for r in responses
                    if r.get("username", "") not in excluded_peers
                ]

                result = select_best(filtered_responses, artist, match_title)

                if result:
                    # Prefer same quality tier as original: if original was lossless,
                    # only accept lossless. Fall back to any tier if same quality unavailable.
                    from selector import LOSSLESS_EXTENSIONS
                    original_is_lossless = original_format in LOSSLESS_EXTENSIONS
                    result_is_lossless = result.get("format", "") in LOSSLESS_EXTENSIONS

                    if original_is_lossless and not result_is_lossless:
                        # Stalled download was lossless — keep searching for lossless alt
                        log.info(
                            "  Alt source: skipping lower-quality result (%s) for lossless original",
                            result.get("format"),
                        )
                        time.sleep(2)
                        continue

                    best_alt = result
                    break

                time.sleep(2)

        if best_alt:
            slskd_client.enqueue(
                client,
                username=best_alt["username"],
                filename=best_alt["filename"],
                size=best_alt["size"],
            )

            update_song_status(conn, source_id, "queued", metadata={
                "slsk_username": best_alt["username"],
                "selected_filename": best_alt["filename"],
                "selected_format": best_alt["format"],
                "selected_bitrate": best_alt["bitrate"],
                "date_queued": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "stall_check_bytes": None,
                "stall_check_time": None,
                "alt_source_count": alt_count + 1,
            })

            log.info(
                "Alt source queued for %s - %s: %s (alt #%d)",
                artist, title, best_alt["username"], alt_count + 1,
            )
        else:
            update_song_status(conn, source_id, "stalled")
            log.warning(
                "No alternative source found for stalled %s - %s — marking stalled",
                artist, title,
            )

    except Exception:
        log.error(
            "Error queuing alternative source for %s - %s",
            artist, title, exc_info=True,
        )


# ---------------------------------------------------------------------------
# Album download polling
# ---------------------------------------------------------------------------

def poll_album_downloads(client, conn) -> int:
    """
    Poll slskd for completed album file downloads and update album completion state.

    Album downloads are tracked in the album_files table (not via selected_filename
    on the songs row). This is a SEPARATE polling path from poll_and_update().

    For each album in 'downloading' status:
      1. Load all album_files rows for the album.
      2. Cross-reference with slskd succeeded downloads — update any completed files.
      3. Also detect terminal-failure states and mark those files as 'failed'.
      4. After processing all files, check album-level completion:
         - All files complete: transition parent song to 'downloaded'.
         - Any file failed: call reset_album_files() to retry from a different peer.

    Returns the count of albums that transitioned to 'downloaded' this cycle.
    """
    albums = get_downloading_albums(conn)
    if not albums:
        log.debug("Album poller: no albums currently downloading")
        return 0

    log.info("Album poller: checking %d downloading album(s)", len(albums))

    # Fetch all current downloads from slskd once — reused for all albums
    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)
    except Exception as exc:
        log.error("Album poller: failed to fetch downloads from slskd: %s", exc)
        return 0

    # Build a flat lookup keyed by (username, filename) for O(1) matching
    # Value contains the download state
    transfer_lookup: dict[tuple, dict] = {}
    for user_entry in all_downloads:
        username = user_entry.get("username", "")
        for directory in user_entry.get("directories", []):
            for file in directory.get("files", []):
                fname = file.get("filename", "")
                key = (username, fname)
                transfer_lookup[key] = {
                    "state": file.get("state", ""),
                    "bytesTransferred": file.get("bytesTransferred", 0),
                }

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    albums_completed = 0

    for album in albums:
        source_id = album["source_id"]
        artist = album.get("artist", "")
        title = album.get("title", "")

        # Load all album_files rows for this album
        album_files = get_album_files_by_source(conn, source_id)
        if not album_files:
            log.debug("Album poller: no album_files rows for %s — skipping", source_id)
            continue

        # Update status of each file that has completed
        for af in album_files:
            if af["status"] != "queued":
                # Already processed (downloaded or failed) — skip
                continue

            username = af["slsk_username"]
            filename = af["filename"]
            file_id = af["id"]

            key = (username, filename)
            transfer = transfer_lookup.get(key)

            if transfer is None:
                # Not yet visible in slskd — may still be pending
                continue

            state = transfer["state"]

            if state == "Completed, Succeeded":
                # File downloaded successfully — find it on disk
                local_path = find_local_file(filename)
                if not local_path:
                    log.warning(
                        "Album file succeeded but not found on disk: %s (marking downloaded anyway)",
                        filename.rsplit("\\", 1)[-1],
                    )
                update_album_file_status(conn, file_id, "downloaded", metadata={
                    "date_downloaded": now,
                    "local_path": local_path,
                })
                log.debug(
                    "Album file complete: %s%s",
                    filename.rsplit("\\", 1)[-1],
                    f" -> {local_path}" if local_path else "",
                )

            elif state.startswith("Completed,"):
                # Terminal failure state (Cancelled, TimedOut, etc.)
                update_album_file_status(conn, file_id, "failed")
                log.warning(
                    "Album file failed (state=%r): %s from peer %s",
                    state, filename.rsplit("\\", 1)[-1], username,
                )

        # After processing all files, check album-level completion
        if all_album_files_complete(conn, source_id):
            # Determine the staging folder path from the local_path of downloaded files
            downloaded_files = get_album_files_by_source(conn, source_id)
            local_paths = [
                af["local_path"] for af in downloaded_files
                if af.get("local_path")
            ]

            if local_paths:
                # All files share a common parent directory (the album staging folder)
                staging_folder = str(Path(local_paths[0]).parent)
            else:
                staging_folder = None

            update_song_status(conn, source_id, "downloaded", metadata={
                "local_path": staging_folder,
                "date_downloaded": now,
            })
            log.info(
                "Album complete: %s - %s (staging=%s)",
                artist, title, staging_folder or "(unknown)",
            )
            albums_completed += 1

        elif any_album_files_failed(conn, source_id):
            # Record the failed peer before resetting so the next search skips them
            failed_username = album.get("slsk_username")
            if failed_username:
                add_failed_peer(conn, source_id, failed_username)
            reset_album_files(conn, source_id)
            log.warning(
                "Album has failed file(s) — resetting for retry from different peer: %s - %s",
                artist, title,
            )

    log.info("Album poller: %d album(s) completed this cycle", albums_completed)
    return albums_completed


def check_stalled_albums(client, conn, log: logging.Logger) -> int:
    """
    Detect album downloads that have made no byte progress in 7+ days and reset them.

    For each album in 'downloading' status, compares total bytes across all
    album_files against the previously recorded stall_check_bytes. If unchanged
    for ALBUM_STALL_TIMEOUT_DAYS (7), the album is reset for retry from a
    different peer using reset_album_files().

    Returns the count of stalled albums handled this cycle.
    """
    albums = get_downloading_albums(conn)
    if not albums:
        log.debug("Album stall check: no albums currently downloading")
        return 0

    # Fetch all downloads for byte-progress inspection
    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)
    except Exception as exc:
        log.error("Album stall check: failed to fetch downloads from slskd: %s", exc)
        return 0

    # Build a lookup of current bytes by (username, filename)
    transfer_bytes: dict[tuple, int] = {}
    for user_entry in all_downloads:
        username = user_entry.get("username", "")
        for directory in user_entry.get("directories", []):
            for file in directory.get("files", []):
                fname = file.get("filename", "")
                key = (username, fname)
                transfer_bytes[key] = file.get("bytesTransferred", 0) or 0

    now_utc = datetime.now(timezone.utc)
    stall_count = 0

    for album in albums:
        source_id = album["source_id"]
        artist = album.get("artist", "")
        title = album.get("title", "")

        # Load the parent song row to check stall metadata
        row = conn.execute(
            "SELECT stall_check_bytes, stall_check_time FROM songs WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if not row:
            continue

        stall_check_bytes = row["stall_check_bytes"]
        stall_check_time_str = row["stall_check_time"]

        # Sum bytes across all queued album_files for this album
        album_files = get_album_files_by_source(conn, source_id)
        total_bytes_now = sum(
            transfer_bytes.get((af["slsk_username"], af["filename"]), 0)
            for af in album_files
            if af["status"] == "queued"
        )

        if stall_check_bytes is None:
            # First stall check — record the baseline
            update_song_status(conn, source_id, "downloading", metadata={
                "stall_check_bytes": total_bytes_now,
                "stall_check_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            log.debug(
                "Album stall check: baseline set for %s - %s (bytes=%d)",
                artist, title, total_bytes_now,
            )
            continue

        if total_bytes_now != stall_check_bytes:
            # Progress detected — update the baseline
            update_song_status(conn, source_id, "downloading", metadata={
                "stall_check_bytes": total_bytes_now,
                "stall_check_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            log.debug(
                "Album stall check: progress on %s - %s (%d -> %d bytes)",
                artist, title, stall_check_bytes, total_bytes_now,
            )
            continue

        # Bytes unchanged — check how long it's been stalled
        if not stall_check_time_str:
            continue

        try:
            stall_since = datetime.fromisoformat(
                stall_check_time_str.replace("Z", "+00:00")
            )
            stalled_days = (now_utc - stall_since).total_seconds() / 86400
            if stalled_days >= ALBUM_STALL_TIMEOUT_DAYS:
                log.warning(
                    "Album stalled for %.1f days — resetting for retry: %s - %s",
                    stalled_days, artist, title,
                )
                # Record the stalled peer before resetting
                stalled_username = album.get("slsk_username")
                if stalled_username:
                    add_failed_peer(conn, source_id, stalled_username)
                reset_album_files(conn, source_id)
                stall_count += 1
            else:
                log.debug(
                    "Album stall check: no progress for %.1f days (threshold=%dd): %s - %s",
                    stalled_days, ALBUM_STALL_TIMEOUT_DAYS, artist, title,
                )
        except Exception:
            log.warning(
                "Album stall check: could not parse stall_check_time %r for %s - %s",
                stall_check_time_str, artist, title,
            )

    log.info("Album stall check: %d stalled album(s) handled this cycle", stall_count)
    return stall_count


# ---------------------------------------------------------------------------
# Untracked download cleanup
# ---------------------------------------------------------------------------

def cancel_untracked_downloads(client, conn) -> int:
    """
    Cancel and remove slskd transfers that don't match any song or album_file in the DB.

    These are "ghost" downloads — files that slskd is downloading (or has downloaded)
    that our pipeline never asked for. They waste bandwidth and clutter the staging
    folder. Common causes: unknown slskd behavior, manual UI accidents, or stale
    transfers left over from alt-source retries where the DB record was overwritten.

    Only removes transfers in terminal states (Completed, Succeeded, Errored, etc.)
    to avoid cancelling downloads that might be mid-transfer and just haven't been
    matched yet.

    Returns the count of transfers removed.
    """
    # Build the set of all filenames we've intentionally tracked
    tracked = set()
    for row in conn.execute(
        "SELECT selected_filename FROM songs WHERE selected_filename IS NOT NULL"
    ).fetchall():
        tracked.add(row["selected_filename"])
    for row in conn.execute(
        "SELECT filename FROM album_files"
    ).fetchall():
        tracked.add(row["filename"])

    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)
    except Exception as exc:
        log.error("cancel_untracked_downloads: failed to fetch transfers: %s", exc)
        return 0

    removed = 0
    for user_entry in all_downloads:
        username = user_entry.get("username", "")
        for directory in user_entry.get("directories", []):
            for f in directory.get("files", []):
                fname = f.get("filename", "")
                state = (f.get("state") or "").lower()
                file_id = f.get("id", "")

                if fname in tracked:
                    continue  # This is one of ours

                # Only remove terminal transfers — don't cancel in-progress ones
                # that might just be slow to appear in the DB
                if "completed" not in state and "errored" not in state:
                    continue

                if file_id and username:
                    try:
                        client.transfers.cancel_download(username, file_id, remove=True)
                        removed += 1
                    except Exception:
                        pass

    if removed:
        log.info("Cleanup: removed %d untracked transfer(s) from slskd", removed)

    return removed


def cleanup_orphan_files(conn, downloads_dir: str = "/downloads") -> int:
    """
    Delete files from the staging folder that don't match any DB record.

    Builds a set of all known basenames (from songs.selected_filename and
    album_files.filename), then walks the downloads directory and deletes
    any file whose basename (after stripping slskd duplicate suffixes)
    doesn't appear in the known set.

    This catches:
      - Files from untracked downloads (slskd pulled files we never asked for)
      - Duplicate-suffix copies from stall retries where the original was already
        matched and delivered

    Skips .DS_Store and other hidden files. Cleans up empty directories afterward.

    Returns the count of files deleted.
    """
    import os

    # Build set of known basenames from DB — only for songs that still need
    # their files (not yet delivered/removed/upgraded). Delivered songs' files
    # are orphans and should be cleaned up.
    KEEP_STATUSES = ("new", "searching", "queued", "downloaded", "downloading",
                     "upgrade_queued", "not_found", "no_match", "stalled", "stalled_waiting")
    known_basenames = set()
    for row in conn.execute(
        "SELECT selected_filename FROM songs "
        "WHERE selected_filename IS NOT NULL AND status IN ({})".format(
            ",".join("?" for _ in KEEP_STATUSES)),
        KEEP_STATUSES,
    ).fetchall():
        known_basenames.add(row["selected_filename"].rsplit("\\", 1)[-1])
    for row in conn.execute(
        "SELECT filename FROM album_files WHERE status NOT IN ('delivered', 'removed')"
    ).fetchall():
        known_basenames.add(row["filename"].rsplit("\\", 1)[-1])

    # Also include local_path basenames for songs still pending delivery
    for row in conn.execute(
        "SELECT local_path FROM songs "
        "WHERE local_path IS NOT NULL AND status IN ({})".format(
            ",".join("?" for _ in KEEP_STATUSES)),
        KEEP_STATUSES,
    ).fetchall():
        known_basenames.add(os.path.basename(row["local_path"]))
    for row in conn.execute(
        "SELECT local_path FROM album_files "
        "WHERE local_path IS NOT NULL AND status NOT IN ('delivered', 'removed')"
    ).fetchall():
        known_basenames.add(os.path.basename(row["local_path"]))

    deleted = 0
    empty_dirs = []

    for root, dirs, files in os.walk(downloads_dir):
        for f in files:
            if f.startswith("."):
                continue

            # Check exact match first, then suffix-stripped match
            if f in known_basenames:
                continue
            clean = _strip_slskd_suffix(f)
            if clean in known_basenames:
                continue

            # This file doesn't match anything in the DB — delete it
            filepath = os.path.join(root, f)
            try:
                os.remove(filepath)
                deleted += 1
                log.debug("Cleanup: deleted orphan file %s", filepath)
            except OSError as exc:
                log.warning("Cleanup: could not delete %s: %s", filepath, exc)

    # Clean up empty directories left behind
    if deleted:
        for root, dirs, files in os.walk(downloads_dir, topdown=False):
            if root == downloads_dir:
                continue
            try:
                if not os.listdir(root):
                    os.rmdir(root)
                    log.debug("Cleanup: removed empty directory %s", root)
            except OSError:
                pass

    if deleted:
        log.info("Cleanup: deleted %d orphan file(s) from staging", deleted)

    return deleted
