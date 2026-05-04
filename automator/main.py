import os
import re
import sys
import time
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from state import (
    init_db, add_songs, get_pending_songs,
    update_song_status, get_not_found_songs, get_no_match_songs, get_stalled_songs,
    get_not_found_albums, reset_album_for_retry,
    get_downloaded_mp3s, get_known_ids, set_setting, get_failed_peers,
)
import slskd_client
from selector import select_best, LOSSLESS_EXTENSIONS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = "/data/logs"


def setup_logging() -> logging.Logger:
    """
    Configure logging with two handlers:
      - StreamHandler (stdout) for Docker container logs
      - FileHandler (timestamped file in /data/logs/) for persistent records

    All modules use logging.getLogger(__name__), so configuring root here
    propagates to slskd_client, selector, and state automatically.

    Idempotent: if root logger already has handlers, skips re-configuration.
    This allows scheduler.py to call setup_logging() once on daemon startup
    without re-attaching duplicate handlers on each pipeline run.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        # Already configured — return the main logger without re-adding handlers
        return logging.getLogger("main")

    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    log_path = os.path.join(LOG_DIR, f"run-{timestamp}.log")

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path),
        ],
    )

    root_log = logging.getLogger("main")
    root_log.info("Logging initialised — writing to %s", log_path)
    return root_log


# ---------------------------------------------------------------------------
# Database summary (converted to logging)
# ---------------------------------------------------------------------------

def log_db_summary(conn, log: logging.Logger):
    """Log a count of songs by status so we can see the database state at a glance."""
    cursor = conn.execute("""
        SELECT status, COUNT(*) as count
        FROM songs
        GROUP BY status
        ORDER BY status
    """)
    rows = cursor.fetchall()
    if not rows:
        log.info("Database is empty")
        return

    log.info("Database summary:")
    for row in rows:
        log.info("  %s: %d", row[0], row[1])


# ---------------------------------------------------------------------------
# Multi-source sync phase (replaces run_youtube_sync)
# ---------------------------------------------------------------------------


def run_source_sync(conn, sources: list, log: logging.Logger) -> tuple[int, list[str]]:
    """
    Run all source adapters in sequence, collecting songs from each.

    Each adapter is wrapped in its own try/except — a failure in one source
    does NOT abort other sources (ARCH-03: per-source error isolation).

    Sources are add-only inputs: new songs get added to the DB, but songs
    removed from a source (e.g. deleted from a playlist) stay in the DB
    and continue through the pipeline.

    Returns a tuple: (count of new songs added, list of source names that returned 0 songs).
    """
    log.info("=== Source Sync Phase ===")
    log.info("Running %d source adapter(s)", len(sources))

    # Pre-load known song IDs so adapters can skip already-parsed entries.
    # This saves API tokens — e.g. YouTube adapter won't re-send known songs to Haiku.
    known_ids = get_known_ids(conn)
    log.info("Known songs in DB: %d", len(known_ids))

    all_songs: list[dict] = []
    failed_sources: list[str] = []
    zero_sources: list[str] = []

    for adapter in sources:
        source_name = adapter.source_name
        set_setting(conn, "pipeline_stage", f"Fetching {source_name}...")
        log.info("--- Fetching from source: %s ---", source_name)

        try:
            # Pass known_ids as a keyword arg — adapters that accept it will
            # use it to filter before calling expensive APIs. Adapters that
            # don't accept it (e.g. TextFileAdapter) simply ignore it via **kwargs.
            try:
                songs = adapter.fetch_songs(known_ids=known_ids)
            except TypeError:
                # Adapter doesn't accept known_ids — call without it
                songs = adapter.fetch_songs()
            # Phase 9.1: tag each song with source_table_id so the dashboard
            # can filter by specific source (not just source_type)
            stid = getattr(adapter, '_source_table_id', None)
            if stid is not None:
                for song in songs:
                    song["source_table_id"] = stid
            all_songs.extend(songs)
            log.info("Source %s returned %d song(s)", source_name, len(songs))

            if len(songs) == 0:
                # Only warn if this source has no existing songs in the DB.
                # A source with existing songs returning 0 just means "no new
                # songs" — that's steady state, not a configuration problem.
                stid = getattr(adapter, '_source_table_id', None)
                existing = 0
                if stid is not None:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM songs WHERE source_table_id = ?",
                        (stid,),
                    ).fetchone()
                    existing = row[0] if row else 0

                if existing == 0:
                    zero_sources.append(source_name)
                    log.warning(
                        "Source %s returned 0 songs — check configuration "
                        "(missing API token? wrong URL? empty playlist?)",
                        source_name,
                    )
                else:
                    log.info(
                        "Source %s returned 0 new songs (%d existing in DB)",
                        source_name, existing,
                    )
        except Exception:
            log.warning(
                "Source %s raised an exception — skipping this source",
                source_name,
                exc_info=True,
            )
            failed_sources.append(source_name)

    if failed_sources:
        log.warning(
            "Source failures this run: %s",
            ", ".join(failed_sources),
        )

    if not all_songs:
        log.info("No songs collected from any source")
        return 0, zero_sources

    log.info("Total songs collected from all sources: %d", len(all_songs))
    new_songs_added = add_songs(conn, all_songs)
    log.info("New songs added to database: %d", new_songs_added)

    return new_songs_added, zero_sources


# ---------------------------------------------------------------------------
# Soulseek download loop
# ---------------------------------------------------------------------------

QUEUE_CAP = 1000        # Safety net: stop queueing if slskd already has 1000+ non-terminal transfers
SEARCH_DELAY_S = 2      # Seconds to sleep between search calls (be a polite API user)
MAX_CONSECUTIVE_ERRORS = 3  # Abort loop early if slskd appears down (circuit breaker)


def _strip_paren_symbols(text: str) -> str:
    """Remove parenthesis/bracket characters but keep the content inside."""
    return text.replace("(", "").replace(")", "").replace("[", "").replace("]", "")


def _strip_paren_content(text: str) -> str:
    """Remove parenthesised/bracketed groups AND their content entirely."""
    stripped = re.sub(r'\s*[\(\[][^)\]]*[\)\]]', '', text)
    return stripped.strip()


def build_query_rounds(artist: str, title: str) -> list[dict]:
    """
    Build progressively simpler search rounds.

    Each round has:
      - queries: list of search strings to try on Soulseek
      - match_title: the title to use for filename matching (relaxes with each round)

    As rounds progress, BOTH the search query AND the filename matching
    get simpler. This prevents the case where a simpler search finds results
    but the selector still rejects them using the full strict title.

    Example for artist="Move D", title="The Golden Pudelizer (Pandemix Live Jams Vol. 4 B1)":

      Round 1: search "Move D The Golden Pudelizer Pandemix Live Jams Vol 4 B1"
               match against "The Golden Pudelizer (Pandemix Live Jams Vol. 4 B1)"
      Round 2: search "Move D The Golden Pudelizer"
               match against "The Golden Pudelizer"    <- relaxed matching too

    Note: Title-only searches (Round 3) were removed in Phase 5 because they
    produced false-positive matches from unrelated artists on Soulseek.
    """
    full_title = _strip_paren_symbols(title).strip()
    full_title = " ".join(full_title.split())

    core_title = _strip_paren_content(title).strip()

    rounds = []

    # Round 1: artist + full title — match against full title
    if artist:
        rounds.append({
            "queries": [f"{artist} {full_title}", f"{artist} - {full_title}"],
            "match_title": title,
        })
    else:
        rounds.append({
            "queries": [full_title],
            "match_title": title,
        })

    # Round 2: artist + core title — match against core title only
    if core_title != full_title:
        if artist:
            rounds.append({
                "queries": [f"{artist} {core_title}", f"{artist} - {core_title}"],
                "match_title": core_title,
            })
        else:
            rounds.append({
                "queries": [core_title],
                "match_title": core_title,
            })

    return rounds


def run_download_loop(conn, client, log: logging.Logger) -> int:
    """
    Search pending songs on Soulseek, select quality results, and enqueue downloads.

    For each song with status='new':
      1. Try up to 3 query variants
      2. Use selector to pick the best FLAC/lossless/MP3-320 result
      3. Enqueue via slskd and update status to 'queued'
      4. If no results at all: mark 'not_found'
      5. If results exist but none pass quality filters: mark 'no_match'

    All pending songs are searched every run. A QUEUE_CAP (1000) safety net
    stops queueing new downloads if slskd already has too many non-terminal
    transfers. Each song's search is wrapped in try/except — a failure on
    one song does not abort the rest of the batch.

    Returns the count of downloads queued this run.
    """
    log.info("=== Soulseek Download Loop ===")

    # Check queue cap — safety net to prevent flooding slskd
    active = slskd_client.count_active_downloads(client)
    remaining_slots = max(0, QUEUE_CAP - active)
    log.info("Active transfers in slskd: %d, queue cap slots remaining: %d", active, remaining_slots)

    # Get all pending songs (status='new')
    pending = get_pending_songs(conn)
    if not pending:
        log.info("No pending songs to search")
        return 0

    log.info("Found %d pending song(s) to search", len(pending))

    # Counters for end-of-run summary
    searched = 0
    queued = 0
    not_found = 0
    no_match = 0
    cap_skipped = 0
    consecutive_errors = 0

    for song in pending:
        source_id = song["source_id"]
        artist = song["artist"]
        title = song["title"]
        raw_title = song["raw_title"]

        log.info("Searching: %s - %s", artist, title)

        # Mark as 'searching' before we hit the network
        update_song_status(conn, source_id, "searching")

        try:
            # Build progressively simpler query rounds — both search queries
            # and filename matching relax together as rounds progress
            rounds = build_query_rounds(artist, title)

            best_result = None
            any_responses = False

            for round_num, rnd in enumerate(rounds, 1):
                if best_result:
                    break

                match_title = rnd["match_title"]

                for query in rnd["queries"]:
                    log.info("  [Round %d] Query: %r", round_num, query)
                    responses = slskd_client.search(client, query)

                    if responses:
                        any_responses = True

                    result = select_best(responses, artist, match_title)

                    if result:
                        best_result = result
                        log.info(
                            "  Match found: %s / %s (format=%s)",
                            result["username"],
                            result["filename"].rsplit("\\", 1)[-1],
                            result["format"],
                        )
                        break  # Stop at first good result

                    time.sleep(SEARCH_DELAY_S)

            searched += 1
            consecutive_errors = 0  # reset on any successful search cycle

            # Handle outcome
            if best_result:
                if remaining_slots <= 0:
                    # Queue cap reached — reset to 'new' so it will be queued next run
                    update_song_status(conn, source_id, "new")
                    cap_skipped += 1
                    log.info(
                        "  Match found but queue cap reached — deferring: %s — %s",
                        artist, title,
                    )
                else:
                    # Enqueue the download
                    slskd_client.enqueue(
                        client,
                        username=best_result["username"],
                        filename=best_result["filename"],
                        size=best_result["size"],
                    )

                    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    update_song_status(conn, source_id, "queued", metadata={
                        "slsk_username": best_result["username"],
                        "selected_filename": best_result["filename"],
                        "selected_format": best_result["format"],
                        "selected_bitrate": best_result["bitrate"],
                        "date_queued": now,
                    })

                    log.info(
                        "  Queued: %s — %s (format=%s, peer=%s)",
                        artist, title, best_result["format"], best_result["username"],
                    )
                    queued += 1
                    remaining_slots -= 1

            elif not any_responses:
                # All query variants returned empty responses — song is not on Soulseek
                update_song_status(conn, source_id, "not_found")
                log.warning("  Not found on Soulseek: %s - %s", artist, title)
                not_found += 1

            else:
                # Results existed but none passed quality/filename filters
                update_song_status(conn, source_id, "no_match")
                log.warning(
                    "  No quality match: %s - %s (results found but none passed filters)",
                    artist, title,
                )
                no_match += 1

        except Exception:
            consecutive_errors += 1
            log.error(
                "  Error searching %s - %s — skipping",
                artist, title, exc_info=True,
            )
            # Reset status to 'new' so it can be retried on next run
            try:
                update_song_status(conn, source_id, "new")
            except Exception:
                pass

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log.error(
                    "Aborting track download loop — %d consecutive errors (slskd may be down)",
                    consecutive_errors,
                )
                break

    # End-of-run summary
    log.info(
        "Download loop complete — searched=%d, queued=%d, not_found=%d, no_match=%d%s",
        searched, queued, not_found, no_match,
        f", cap_deferred={cap_skipped}" if cap_skipped else "",
    )

    return queued


# ---------------------------------------------------------------------------
# Album download loop (Phase 8)
# ---------------------------------------------------------------------------

def run_album_download_loop(conn, client, log: logging.Logger) -> int:
    """
    Search pending albums on Soulseek, select folder-level results, and enqueue downloads.

    For each album entry with search_mode='album' and status='new':
      1. Mark as 'searching'.
      2. Split title into artist + album name.
      3. Run album search (search -> browse -> select best folder).
      4. If found: enqueue all files from the best folder and insert album_files rows.
      5. If not found: increment search_attempts; after 10 failures, mark as not_found.

    Each album's search is wrapped in try/except — a failure on one album does not
    abort the rest of the batch.

    Returns the count of albums successfully enqueued this run.
    """
    from album_search import run_album_search, enqueue_album_folder
    from state import (
        get_pending_albums,
        insert_album_files,
        mark_album_not_found,
        get_search_attempts,
    )

    log.info("=== Album Download Loop ===")

    pending = get_pending_albums(conn)
    if not pending:
        log.info("No pending albums to search")
        return 0

    log.info("Found %d pending album(s) to search", len(pending))

    albums_enqueued = 0
    consecutive_errors = 0

    for album in pending:
        source_id = album["source_id"]
        title = album.get("title", "")
        db_artist = album.get("artist", "")

        # Mark as 'searching' before network activity
        update_song_status(conn, source_id, "searching")

        # Determine artist and album name.
        # Newer adapters (Discogs) store artist and title separately in the DB.
        # Older entries may have "Artist - Album" combined in the title column.
        # Prefer the DB artist column when available; fall back to splitting title.
        if db_artist:
            artist = db_artist
            album_name = title
        elif " - " in title:
            artist, album_name = title.split(" - ", 1)
        else:
            artist = ""
            album_name = title
            log.warning("Album title has no ' - ' separator and no artist: %r", title)

        log.info("Searching album: %s - %s", artist, album_name)

        # Load any peers that previously failed for this album so we skip them
        failed_peers = get_failed_peers(conn, source_id)
        if failed_peers:
            log.info("Excluding %d previously failed peer(s) for %s", len(failed_peers), source_id)

        try:
            result = run_album_search(client, artist, album_name, log, excluded_peers=failed_peers or None)

            if result is None:
                # No matching folder found. update_song_status('searching') above
                # already incremented search_attempts; read the current value to
                # decide whether the budget is exhausted.
                consecutive_errors = 0  # not a connection error, reset breaker
                attempts = get_search_attempts(conn, source_id)
                if attempts >= 10:
                    mark_album_not_found(conn, source_id)
                    log.warning(
                        "Album not found after %d attempts — marking not_found: %s - %s",
                        attempts, artist, album_name,
                    )
                else:
                    # Reset to 'new' so it will be retried on the next run
                    update_song_status(conn, source_id, "new")
                    log.info(
                        "Album search: no result for %s - %s (attempt %d/10)",
                        artist, album_name, attempts,
                    )
                continue

            # Found a result — enqueue all files from the selected folder
            username = result["username"]
            files = result["files"]

            enqueue_ok = enqueue_album_folder(client, username, files, log)

            if enqueue_ok:
                # Insert album_files rows and transition to 'downloading'
                inserted = insert_album_files(conn, source_id, username, files)
                update_song_status(conn, source_id, "downloading", metadata={
                    "slsk_username": username,
                })
                log.info(
                    "Album enqueued: %s - %s from peer %s (%d file(s) inserted)",
                    artist, album_name, username, inserted,
                )
                albums_enqueued += 1
                consecutive_errors = 0  # reset on success
            else:
                # Enqueue failed — reset to 'new' for retry
                update_song_status(conn, source_id, "new")
                log.warning(
                    "Album enqueue failed for %s - %s — resetting for retry",
                    artist, album_name,
                )

        except Exception:
            consecutive_errors += 1
            log.error(
                "Error processing album %s - %s — skipping",
                artist, album_name, exc_info=True,
            )
            # Reset to 'new' so it can be retried on next run
            try:
                update_song_status(conn, source_id, "new")
            except Exception:
                pass

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log.error(
                    "Aborting album download loop — %d consecutive errors (slskd may be down)",
                    consecutive_errors,
                )
                break

        time.sleep(SEARCH_DELAY_S)

    log.info("Album download loop complete — %d album(s) enqueued", albums_enqueued)
    return albums_enqueued


# ---------------------------------------------------------------------------
# Retry and quality upgrade logic (Phase 4 — reliability layer)
# ---------------------------------------------------------------------------

def run_retry_searches(conn, client, log: logging.Logger) -> None:
    """
    Run two retry scenarios before the main download loop:

    1. Re-search not_found songs (SLSK-06):
       Songs that previously returned zero results are reset to 'new' so
       the existing download loop picks them up again this run. Soulseek
       availability changes over time — this gives them another chance.

    2. MP3-to-FLAC quality upgrade check (SLSK-05):
       Songs already downloaded as MP3 are re-searched for lossless copies.
       If a FLAC/lossless result is found, it is enqueued and the song's
       status is set to 'upgrade_queued'. The poller will later detect
       completion and set status to 'upgraded'; the delivery module will
       then deliver the FLAC and delete the old MP3.

    This function must be called BEFORE run_download_loop() so that
    not_found songs are already reset to 'new' when the loop processes them.
    """
    log.info("=== Retry and Quality Upgrade Phase ===")

    # --- Part 1: Reset not_found songs for re-search (7-day cooldown) ---
    # Only retry songs whose last search was >7 days ago. This prevents hammering
    # Soulseek for the same unavailable songs every single run.
    COOLDOWN_DAYS = 7
    not_found_songs = get_not_found_songs(conn, cooldown_days=COOLDOWN_DAYS)
    if not_found_songs:
        for song in not_found_songs:
            update_song_status(conn, song["source_id"], "new")
        log.info(
            "Retry: resetting %d not_found song(s) for re-search (cooldown %dd)",
            len(not_found_songs), COOLDOWN_DAYS,
        )
    else:
        log.info("Retry: no not_found songs eligible for re-search (cooldown %dd)", COOLDOWN_DAYS)

    # --- Part 1b: Reset no_match songs for re-search (7-day cooldown) ---
    no_match_songs = get_no_match_songs(conn, cooldown_days=COOLDOWN_DAYS)
    if no_match_songs:
        for song in no_match_songs:
            update_song_status(conn, song["source_id"], "new")
        log.info(
            "Retry: resetting %d no_match song(s) for re-search (cooldown %dd)",
            len(no_match_songs), COOLDOWN_DAYS,
        )
    else:
        log.info("Retry: no no_match songs eligible for re-search (cooldown %dd)", COOLDOWN_DAYS)

    # --- Part 1c: Reset stalled/stalled_waiting songs for re-search (7-day cooldown) ---
    # With peer exclusion (failed_peers_json), retrying stalled songs is safe — the system
    # always picks a different peer. We clear stall metadata but preserve failed_peers_json
    # so the retry skips all previously failed peers.
    stalled_songs = get_stalled_songs(conn, cooldown_days=COOLDOWN_DAYS)
    if stalled_songs:
        for song in stalled_songs:
            update_song_status(conn, song["source_id"], "new", metadata={
                "stall_check_bytes": None,
                "stall_check_time": None,
                "alt_source_count": 0,
            })
        log.info(
            "Retry: resetting %d stalled song(s) for re-search (cooldown %dd, failed_peers preserved)",
            len(stalled_songs), COOLDOWN_DAYS,
        )
    else:
        log.info("Retry: no stalled songs eligible for re-search (cooldown %dd)", COOLDOWN_DAYS)

    # --- Part 1d: Reset not_found albums for re-search (7-day cooldown) ---
    # Albums use a 10-attempt budget per cycle. When the cooldown elapses we
    # restore them to status='new' AND zero search_attempts so they get a fresh
    # budget on the next run, and clear failed_peers so we re-explore the peer
    # pool. Without the budget reset they would be filtered out by
    # get_pending_albums() (status='new' AND search_attempts<10) and never
    # search again.
    not_found_albums = get_not_found_albums(conn, cooldown_days=COOLDOWN_DAYS)
    if not_found_albums:
        for album in not_found_albums:
            reset_album_for_retry(conn, album["source_id"])
        log.info(
            "Retry: resetting %d not_found album(s) for re-search "
            "(cooldown %dd, attempts and failed_peers cleared)",
            len(not_found_albums), COOLDOWN_DAYS,
        )
    else:
        log.info("Retry: no not_found albums eligible for re-search (cooldown %dd)", COOLDOWN_DAYS)

    # --- Part 2: Quality upgrade check for MP3 downloads ---
    mp3_songs = get_downloaded_mp3s(conn)
    if not mp3_songs:
        log.info("Quality upgrade check: no MP3 downloads to check")
        return

    log.info("Quality upgrade check: checking %d MP3 download(s) for FLAC upgrades", len(mp3_songs))

    upgrades_queued = 0

    for song in mp3_songs:
        source_id = song["source_id"]
        artist = song["artist"]
        title = song["title"]

        try:
            rounds = build_query_rounds(artist, title)
            best_lossless = None
            any_responses = False

            for round_num, rnd in enumerate(rounds, 1):
                if best_lossless:
                    break

                match_title = rnd["match_title"]

                for query in rnd["queries"]:
                    log.info(
                        "  [Upgrade Round %d] Searching for FLAC: %r",
                        round_num, query,
                    )
                    responses = slskd_client.search(client, query)

                    if responses:
                        any_responses = True

                    # Use select_best to find a candidate, then check if it's lossless.
                    # We don't modify selector.py — instead we filter after the call.
                    result = select_best(responses, artist, match_title)

                    if result and result["format"] in LOSSLESS_EXTENSIONS:
                        best_lossless = result
                        log.info(
                            "  Upgrade found: %s - %s — %s from %s (was MP3)",
                            artist, title, result["format"].upper(), result["username"],
                        )
                        break

                    time.sleep(SEARCH_DELAY_S)

            if best_lossless:
                # Enqueue the FLAC download
                slskd_client.enqueue(
                    client,
                    username=best_lossless["username"],
                    filename=best_lossless["filename"],
                    size=best_lossless["size"],
                )

                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                update_song_status(conn, source_id, "upgrade_queued", metadata={
                    "slsk_username": best_lossless["username"],
                    "selected_filename": best_lossless["filename"],
                    "selected_format": best_lossless["format"],
                    "selected_bitrate": best_lossless["bitrate"],
                    "date_queued": now,
                })
                upgrades_queued += 1

        except Exception:
            log.error(
                "  Error during upgrade search for %s - %s — skipping",
                artist, title, exc_info=True,
            )

    log.info(
        "Quality upgrade check: %d MP3(s) checked, %d upgrade(s) queued",
        len(mp3_songs), upgrades_queued,
    )


# ---------------------------------------------------------------------------
# Pipeline entry point (callable from scheduler)
# ---------------------------------------------------------------------------

def run_pipeline() -> dict:
    """
    Run a single pipeline cycle: source sync + Soulseek download loop.

    Sources are loaded from sources.yaml (via load_sources()). If sources.yaml
    is empty or missing but YOUTUBE_PLAYLIST_URL is set, falls back to using
    that env var directly — this ensures existing Docker setups keep working
    without requiring immediate migration to sources.yaml.

    Returns a summary dict with counts:
        {"new_songs": int, "downloads_queued": int}

    This is the function called by scheduler.py on each scheduled or manual run.
    It does NOT call load_dotenv() or setup_logging() — those are the scheduler's
    responsibility when running as a daemon. When run standalone via main(), they
    are called before run_pipeline().
    """
    log = logging.getLogger("main")

    conn = init_db()
    log.info("Database ready at /data/songs.db")

    new_songs = 0
    downloads_queued = 0
    albums_queued = 0

    # Headline tracking — collects events during the run to pick the
    # single most important summary message at the end.
    source_sync_failed = False
    zero_song_sources: list[str] = []
    download_loop_failed = False
    searched_count = 0

    # Phase 1: Source sync — always runs, slskd not required
    set_setting(conn, "pipeline_stage", "Syncing sources...")
    try:
        from sources import load_sources

        # Phase 9: pass conn so load_sources() reads from the sources table.
        # Falls back to sources.yaml automatically when the table is empty.
        sources = load_sources(conn=conn)

        if not sources:
            log.warning(
                "No sources configured — add sources via the dashboard. "
                "Download loop will still run for existing DB songs."
            )

        new_songs, zero_song_sources = run_source_sync(conn, sources, log)

    except Exception:
        log.error("Source sync phase failed:", exc_info=True)
        source_sync_failed = True
        # Don't abort — still attempt the download loop for previously synced songs

    # Phase 2: Soulseek download loop — only if API key is configured
    api_key = os.environ.get("SLSKD_API_KEY")
    if not api_key:
        log.warning(
            "SLSKD_API_KEY not set — skipping download loop. "
            "Add it to .env to enable Soulseek searching."
        )
    else:
        try:
            client = slskd_client.make_client()
            # Retry phase runs first: resets not_found songs and checks for upgrades.
            # This ensures not_found songs are already 'new' when run_download_loop runs.
            set_setting(conn, "pipeline_stage", "Retrying not_found songs...")
            run_retry_searches(conn, client, log)
            # Count how many songs will be searched (for headline context)
            searched_count = len(get_pending_songs(conn))
            set_setting(conn, "pipeline_stage", f"Searching Soulseek ({searched_count} songs)...")
            downloads_queued = run_download_loop(conn, client, log)
        except Exception:
            log.error("Download loop failed:", exc_info=True)
            download_loop_failed = True

        # Phase 3: Album download loop — searches for folder-level results
        try:
            set_setting(conn, "pipeline_stage", "Searching albums...")
            from album_search import run_album_search, enqueue_album_folder  # noqa: F401
            albums_queued = run_album_download_loop(conn, client, log)
            log.info("Album loop: %d album(s) enqueued", albums_queued)
        except Exception:
            log.error("Album download loop failed:", exc_info=True)

    # Final DB state snapshot
    log_db_summary(conn, log)

    conn.close()

    # Build headline — priority order (highest wins):
    # 1. Pipeline crashed / phase failed (red)
    # 2. Source returned 0 songs (amber)
    # 3. Songs searched but 0 queued (amber)
    # 4. Downloads queued (green)
    # 5. New songs parsed (green)
    # 6. Nothing new — steady state (info)
    headline = "All clear — no new songs or downloads"
    headline_level = "info"

    if source_sync_failed or download_loop_failed:
        headline = "Pipeline error — check logs for details"
        headline_level = "error"
    elif zero_song_sources:
        names = ", ".join(zero_song_sources[:3])
        headline = f"{names} returned 0 songs — check configuration"
        headline_level = "warning"
    elif searched_count > 0 and downloads_queued == 0:
        headline = f"Searched {searched_count} songs, none found on Soulseek"
        headline_level = "warning"
    elif downloads_queued > 0:
        headline = f"Queued {downloads_queued} new download{'s' if downloads_queued != 1 else ''}"
        headline_level = "success"
    elif new_songs > 0:
        headline = f"Parsed {new_songs} new song{'s' if new_songs != 1 else ''}"
        headline_level = "success"

    log.info("Run headline: [%s] %s", headline_level, headline)

    return {
        "new_songs": new_songs,
        "downloads_queued": downloads_queued,
        "albums_queued": albums_queued,
        "headline": headline,
        "headline_level": headline_level,
    }


# ---------------------------------------------------------------------------
# Standalone entry point (backward compatibility)
# ---------------------------------------------------------------------------

def main():
    load_dotenv()  # Fallback for local dev; Docker injects env vars directly
    setup_logging()
    result = run_pipeline()
    log = logging.getLogger("main")
    log.info("Run complete — new_songs=%d, downloads_queued=%d", result["new_songs"], result["downloads_queued"])


if __name__ == "__main__":
    main()
