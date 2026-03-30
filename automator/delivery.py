"""
delivery.py — Batch delivery module.

Moves completed downloads from staging (/downloads) to the destination folder,
keeping original Soulseek filenames so mismatches are visible.

Files are organised by source name (e.g. My Playlist/Artist - track.flac).

Artist comes from the DB when available, otherwise extracted from the
Soulseek folder path (e.g. music\\Artist\\Album\\track.flac).

Size verification guards against partial transfers through Docker mounts.
"""

import os
import shutil
import logging
from pathlib import Path
from datetime import datetime, timezone

from state import get_songs_for_delivery, update_song_status, get_albums_for_delivery
from poller import find_local_file

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filename building
# ---------------------------------------------------------------------------

def _sanitise(s: str) -> str:
    """Replace characters that cause filesystem issues."""
    return s.replace("/", "-").replace("\\", "-").replace(":", "-")


# ---------------------------------------------------------------------------
# Staging cleanup
# ---------------------------------------------------------------------------

def _cleanup_empty_parents(folder: Path, stop_at: Path) -> None:
    """
    Remove empty directories walking upward from folder, stopping at stop_at.

    After a file is moved out of an album folder like /downloads/Album/track.flac,
    the Album folder is left empty. This walks up and removes each empty folder
    until it hits /downloads (which we never delete) or a non-empty folder.
    """
    current = folder
    while current != stop_at and current.is_relative_to(stop_at):
        try:
            current.rmdir()  # Only succeeds if the directory is empty
            log.debug("Cleaned up empty folder: %s", current)
            current = current.parent
        except OSError:
            break  # Folder not empty or permission issue — stop walking


# ---------------------------------------------------------------------------
# Organisation mode routing
# ---------------------------------------------------------------------------

def _extract_artist_from_path(selected_filename: str) -> str:
    """
    Try to extract artist name from the Soulseek file path.

    Soulseek paths typically look like:
        music\\Artist Name\\Album (Year)\\01 - Track.flac
        shared\\Artist Name\\Album\\Track.flac

    We walk the path segments and pick the segment after the first
    "root" folder (the sharer's top-level directory). This heuristic
    works for the vast majority of Soulseek shares.
    """
    parts = Path(selected_filename.replace("\\", "/")).parts

    # Need at least: root_folder / artist / album / file
    if len(parts) >= 4:
        return parts[-3]  # Artist is typically two levels above the file

    return ""


def _extract_album_from_path(selected_filename: str) -> str:
    """
    Extract album folder name from a Soulseek file path.

    Soulseek paths typically look like:
        music\\Artist Name\\Album (Year)\\01 - Track.flac

    The album is the directory immediately above the file.
    """
    parts = Path(selected_filename.replace("\\", "/")).parts

    # Need at least: something / album / file
    if len(parts) >= 3:
        return parts[-2]

    return ""


def _resolve_dest_dir(final_dir: str, source_name: str = "") -> Path:
    """
    Pick the destination subdirectory — always grouped by source name.

    Result: final_dir / source_name / filename
    Songs without a source go into "Unsorted".
    """
    base = Path(final_dir)
    subfolder = _sanitise(source_name) if source_name else "Unsorted"
    return base / subfolder


# ---------------------------------------------------------------------------
# Move to destination
# ---------------------------------------------------------------------------

def move_to_dest(
    staging_path: str,
    artist: str,
    selected_filename: str,
    dest_dir: str,
    source_name: str = "",
) -> bool:
    """
    Move a file from staging to the destination folder, keeping its original Soulseek name.

    Files are organised into subfolders by source name:
      dest/My Jazz Playlist/Artist - track.flac

    Size verification catches silent partial-write failures through Docker mounts.
    """
    try:
        src = Path(staging_path)

        if not src.exists():
            log.error("move_to_dest: source file not found: %s", staging_path)
            return False

        src_size = os.path.getsize(staging_path)

        # Use DB artist, or extract from Soulseek path if empty
        effective_artist = artist.strip() if artist else ""
        if not effective_artist and selected_filename:
            effective_artist = _extract_artist_from_path(selected_filename)
        if not effective_artist:
            effective_artist = "Unknown Artist"

        # Filename: prefix with artist name if not already present
        original_filename = src.name
        safe_original = _sanitise(original_filename)
        safe_artist = _sanitise(effective_artist)
        if safe_original.lower().startswith(effective_artist.lower()):
            delivered_filename = safe_original
        else:
            delivered_filename = f"{safe_artist} - {safe_original}"

        # Route to source-name subfolder
        dest_dir = _resolve_dest_dir(dest_dir, source_name)
        dest_path = dest_dir / delivered_filename

        dest_dir.mkdir(parents=True, exist_ok=True)

        shutil.move(str(src), str(dest_path))

        # Verify integrity
        dest_size = os.path.getsize(str(dest_path))
        if dest_size != src_size:
            log.error(
                "move_to_dest: size mismatch after move — src=%d bytes, dest=%d bytes: %s",
                src_size, dest_size, dest_path,
            )
            return False

        log.info("Delivered: %s -> %s", src.name, dest_path)

        staging_root = Path("/downloads")
        _cleanup_empty_parents(src.parent, staging_root)

        return True

    except Exception as exc:
        log.error("move_to_dest failed for %s: %s", staging_path, exc, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Delivery batch
# ---------------------------------------------------------------------------

def _delete_old_mp3(date_delivered: str, local_path: str, dest_dir: str) -> None:
    """
    Delete the old MP3 file from the destination after a FLAC upgrade has been delivered.

    Since files are now stored with their original Soulseek names, we infer the
    old MP3 path from the original local_path filename and the delivery date.
    Searches the month folder for a matching .mp3 file.

    Failure is non-fatal — the FLAC upgrade is already delivered.
    """
    try:
        month_folder = date_delivered[:7]  # "2026-02" from "2026-02-20T..."
        old_stem = Path(local_path).stem  # filename without extension
        month_dir = Path(dest_dir) / month_folder

        if not month_dir.exists():
            return

        # Search for the old MP3 in any artist subfolder
        for mp3_path in month_dir.rglob(f"{old_stem}.mp3"):
            mp3_path.unlink()
            log.info("Upgrade complete: deleted old MP3 %s", mp3_path)
            return

        log.debug("Upgrade: old MP3 not found for %s (may have been moved manually)", old_stem)
    except Exception as exc:
        log.error("Failed to delete old MP3 for %s: %s", local_path, exc)


def run_delivery_batch(conn, dest_dir: str) -> int:
    """
    Move all completed downloads to the destination folder.

    Queries the DB for songs that are:
      - status='downloaded' OR status='upgraded'
      - local_path is set (file known to exist on disk)
      - date_delivered IS NULL (not yet delivered)

    For each song:
      - Calls move_to_dest() for each file
      - On success: updates status to 'delivered' with timestamp
      - For status='upgraded': after delivery, deletes the old MP3

    Returns the count of files successfully delivered.
    """
    songs = get_songs_for_delivery(conn)

    if not songs:
        log.info("Delivery batch: no files ready for delivery")
        return 0

    log.info("Delivery batch: %d file(s) ready for delivery to %s", len(songs), dest_dir)

    delivered_count = 0

    for song in songs:
        source_id = song["source_id"]
        artist = song.get("artist", "")
        title = song.get("title", "Unknown Title")
        local_path = song.get("local_path")
        selected_filename = song.get("selected_filename", "")
        source_name = song.get("source_name", "")

        # Dynamically locate the file on disk rather than trusting the stored
        # local_path. slskd may rename files with dedup suffixes on retries,
        # making the stored path stale. find_local_file() handles suffix
        # stripping so it finds the actual file regardless of renaming.
        actual_path = None
        if local_path and Path(local_path).exists():
            actual_path = local_path
        elif selected_filename:
            actual_path = find_local_file(selected_filename)
            if actual_path:
                log.info(
                    "Delivery: found %s - %s via dynamic lookup (stored path was stale)",
                    artist, title,
                )

        if not actual_path:
            log.warning(
                "Delivery: file not found for %s - %s — resetting to 'new' "
                "for re-download",
                artist, title,
            )
            update_song_status(conn, source_id, "new", metadata={
                "local_path": None,
                "selected_filename": None,
                "selected_format": None,
                "slsk_username": None,
                "date_queued": None,
            })
            continue

        success = move_to_dest(
            actual_path, artist, selected_filename, dest_dir,
            source_name=source_name,
        )

        if success:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            update_song_status(conn, source_id, "delivered", metadata={
                "date_delivered": now,
            })
            delivered_count += 1

            # For upgraded songs: delete the old MP3 that was previously delivered
            if song.get("status") == "upgraded":
                old_date_delivered = song.get("date_delivered")
                if old_date_delivered:
                    _delete_old_mp3(old_date_delivered, local_path, dest_dir)
                else:
                    log.warning(
                        "Upgrade delivery: no date_delivered for %s - %s — cannot delete old MP3",
                        artist, title,
                    )
        else:
            log.warning(
                "Delivery failed for %s - %s — will retry next batch",
                artist, title,
            )

    log.info(
        "Delivery batch: %d/%d file(s) delivered",
        delivered_count, len(songs),
    )
    return delivered_count


# ---------------------------------------------------------------------------
# Album folder delivery
# ---------------------------------------------------------------------------

def deliver_album_folder(
    staging_folder: str,
    artist: str,
    album: str,
    dest_dir: str,
    source_name: str = "",
) -> bool:
    """
    Move an entire album folder from staging to the destination folder.

    Albums go into: dest_dir / source_name / Artist - Album /

    Uses shutil.copytree + shutil.rmtree instead of shutil.move for safety across
    filesystem boundaries (Docker volumes may use different underlying filesystems).
    """
    try:
        src = Path(staging_folder)

        if not src.exists():
            log.error("deliver_album_folder: staging folder not found: %s", staging_folder)
            return False

        safe_artist = _sanitise(artist.strip()) if artist else "Unknown Artist"
        safe_album = _sanitise(album.strip()) if album else "Unknown Album"

        parent_dir = _resolve_dest_dir(dest_dir, source_name)
        folder_name = f"{safe_artist} - {safe_album}"
        dest_path = parent_dir / folder_name

        if dest_path.exists():
            log.warning(
                "deliver_album_folder: destination already exists — skipping: %s",
                dest_path,
            )
            return False

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the entire tree first, then remove the source
        # copytree + rmtree is safer than move across filesystem boundaries
        shutil.copytree(str(src), str(dest_path))
        shutil.rmtree(str(src))

        # Verify at least one file made it to the destination
        dest_files = list(dest_path.rglob("*"))
        if not any(f.is_file() for f in dest_files):
            log.error(
                "deliver_album_folder: no files in destination after copy: %s",
                dest_path,
            )
            return False

        log.info(
            "Album delivered: %s -> %s",
            src.name,
            dest_path,
        )

        # Clean up any empty parent directories left behind in staging
        staging_root = Path("/downloads")
        _cleanup_empty_parents(src.parent, staging_root)

        return True

    except Exception as exc:
        log.error(
            "deliver_album_folder failed for %s: %s",
            staging_folder, exc, exc_info=True,
        )
        return False


def run_album_delivery_batch(conn, dest_dir: str) -> int:
    """
    Move all completed album downloads to the destination folder.

    Queries the DB for album entries with status='downloaded', a local_path set
    (the staging folder), and date_delivered IS NULL. For each:
      - Extracts artist and album name from the title field (format: "Artist - Album")
      - Calls deliver_album_folder() with source_name for folder routing
      - On success: updates status to 'delivered' with timestamp

    Returns the count of albums successfully delivered.
    """
    albums = get_albums_for_delivery(conn)

    if not albums:
        log.info("Album delivery batch: no albums ready for delivery")
        return 0

    log.info(
        "Album delivery batch: %d album(s) ready for delivery to %s",
        len(albums), dest_dir,
    )

    delivered_count = 0

    for album in albums:
        source_id = album["source_id"]
        title = album.get("title", "")
        local_path = album.get("local_path")
        source_name = album.get("source_name", "")

        if not local_path:
            log.warning(
                "Album delivery: local_path not set for %r — skipping",
                title,
            )
            continue

        # Title is stored as "Artist - Album" — split to get components
        if " - " in title:
            artist, album_name = title.split(" - ", 1)
        else:
            artist = ""
            album_name = title

        success = deliver_album_folder(
            local_path, artist, album_name, dest_dir,
            source_name=source_name,
        )

        if success:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            update_song_status(conn, source_id, "delivered", metadata={
                "date_delivered": now,
            })
            delivered_count += 1
        else:
            log.warning(
                "Album delivery failed for %r — will retry next batch",
                title,
            )

    log.info(
        "Album delivery batch: %d/%d album(s) delivered",
        delivered_count, len(albums),
    )
    return delivered_count
