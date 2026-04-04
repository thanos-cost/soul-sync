"""
Album search engine for Soulseek folder-level downloads.

This module handles the full album search flow:
  1. Build a query from an "Artist - Album" string
  2. Search Soulseek to discover peers who have the album
  3. Browse the top 5 peers' folder structures
  4. Select the best matching folder using FLAC-preferred-but-completeness-wins logic
  5. Batch-enqueue all files from the winning folder

This mirrors the pattern of selector.py (track mode) but operates at folder level.
All external API calls are wrapped in try/except with descriptive logging.
"""

import re
import time
import logging

import slskd_api

log = logging.getLogger(__name__)

# Audio file extensions considered when counting files in a folder
AUDIO_EXTENSIONS = {"flac", "mp3", "wav", "alac", "aiff", "aif", "m4a", "ogg"}

# Number of peer browse calls to make before selecting the best folder
MAX_PEERS_TO_BROWSE = 5

# How long slskd runs the album search (ms) — slightly longer than track search
# to allow more peer responses, since album searches return fewer results per peer
SEARCH_TIMEOUT_MS = 15000

# Per-browse call timeout in seconds — large libraries are slow to traverse
BROWSE_TIMEOUT_S = 45


def build_album_query(title: str) -> str:
    """
    Build a Soulseek search query from a stored "Artist - Album" string.

    The stored format uses " - " as separator (e.g., "Radiohead - OK Computer").
    We strip the dash when searching, since Soulseek peers name folders
    inconsistently ("Radiohead OK Computer", "Radiohead - OK Computer", etc.)
    and a plain "Artist Album" query matches all of them.

    Parameters:
        title — stored title string, typically "Artist - Album"

    Returns a search query string with no dash separator.
    """
    if " - " in title:
        parts = title.split(" - ", 1)
        return f"{parts[0]} {parts[1]}"
    # No separator found — return raw string as fallback
    return title


# Common English words that inflate match scores without indicating a real match.
# "the lemonheads - it's a shame about ray" matched "it's about time" via these.
# Note: music-meaningful words (ep, vol, remix, mix, edit) are intentionally kept
# OUT of this list — they carry real signal for vinyl/electronic releases where
# album names are often short ("Alien", "Cheebah EP").
STOP_WORDS = {
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "by", "is", "it",
    "its", "s", "and", "or", "not", "no", "with", "from", "as", "but", "be",
    "was", "are", "has", "had", "have", "my", "your", "all",
    "lp", "cd", "pt", "part", "feat", "ft",
    "deluxe", "remaster", "remastered", "edition",
}


def _normalize_for_matching(text: str) -> set[str]:
    """
    Normalize a folder or album name into a set of words for fuzzy matching.

    Strips common path separators, punctuation, and extra whitespace so that
    folder names like "OK_Computer", "OK Computer", and "ok-computer" all
    produce the same word set.

    Parameters:
        text — any string (folder name, album name, artist name)

    Returns a set of lowercase alphanumeric words.
    """
    # Replace common path separators and punctuation with spaces
    text = re.sub(r"[_\-./\\()\[\]]", " ", text.lower())
    # Strip anything that isn't alphanumeric or space
    text = re.sub(r"[^a-z0-9\s]", "", text)
    # Split into words (handles multiple spaces)
    return set(text.split())


def _meaningful_words(text: str) -> set[str]:
    """
    Like _normalize_for_matching but filters out stop words.
    Only meaningful words (artist names, album keywords) survive.
    """
    return _normalize_for_matching(text) - STOP_WORDS


def score_directory(dir_name: str, artist: str, album: str) -> tuple[bool, int]:
    """
    Score a directory path against a target artist + album.

    Two-part check:
      1. Artist gate: all meaningful artist words must appear somewhere in the
         FULL path (not just the leaf folder), because many peers organize as
         Artist\\Album. If the artist doesn't match, the folder is rejected
         regardless of album score.
      2. Album score: count of meaningful overlapping words between the leaf
         folder name and the album name (stop words excluded).

    Parameters:
        dir_name — full Windows-style path (e.g., "C:\\Music\\Radiohead\\OK Computer")
        artist   — target artist name
        album    — target album name

    Returns (artist_matched: bool, album_score: int).
    """
    # Artist check uses the FULL path (artist is often a parent folder)
    path_words = _normalize_for_matching(dir_name)
    artist_words = _meaningful_words(artist)

    # If artist has meaningful words, ALL must be present in the path
    artist_matched = artist_words.issubset(path_words) if artist_words else True

    # Album score uses only the leaf folder name to avoid false positives
    leaf = dir_name.rsplit("\\", 1)[-1]
    leaf_words = _meaningful_words(leaf)
    album_words = _meaningful_words(album)
    album_score = len(leaf_words & album_words) if album_words else 0

    return (artist_matched, album_score)


def _count_audio_files(directory: dict) -> int:
    """
    Count the number of audio files in a browse directory dict.

    Parameters:
        directory — a directory dict from slskd browse response,
                    containing a "files" list

    Returns the count of files whose extension is in AUDIO_EXTENSIONS.
    """
    count = 0
    for file in directory.get("files", []):
        ext = file.get("extension", "").lower()
        if ext in AUDIO_EXTENSIONS:
            count += 1
    return count


def _folder_has_flac(directory: dict) -> bool:
    """
    Return True if any file in the directory has a FLAC extension.

    Parameters:
        directory — a directory dict from slskd browse response

    Returns True if at least one file is FLAC, False otherwise.
    """
    for file in directory.get("files", []):
        if file.get("extension", "").lower() == "flac":
            return True
    return False


def _find_matching_folders(
    browse_result: dict,
    artist: str,
    album: str,
) -> list[dict]:
    """
    Find folders in a peer's browse result that match the target album.

    A folder must pass TWO gates:
      1. Artist gate — all meaningful artist words must appear in the full path
      2. Album score — at least min_score meaningful album words must overlap
         with the leaf folder name (stop words excluded)

    min_score is dynamic: if the album name has only 1 meaningful word (e.g.
    "Alien", "Bassline"), we accept score=1. For albums with 2+ meaningful
    words, we require score=2. The artist gate already prevents false positives.

    Parameters:
        browse_result — raw browse response dict from slskd users.browse()
        artist        — target artist name
        album         — target album name

    Returns a list of dicts sorted by score (desc) then audio count (desc):
        [{"directory": dir_dict, "score": int, "audio_count": int}, ...]
    """
    # Dynamic threshold: accept score=1 for single-word album names
    album_words = _meaningful_words(album)
    min_score = min(len(album_words), 2) if album_words else 1

    candidates = []
    directories = browse_result.get("directories", [])

    for directory in directories:
        dir_name = directory.get("name", "")
        artist_matched, album_score = score_directory(dir_name, artist, album)

        # Gate 1: artist must match
        if not artist_matched:
            continue

        # Gate 2: enough meaningful album words must overlap
        if album_score < min_score:
            continue

        audio_count = _count_audio_files(directory)
        if audio_count < 1:
            continue
        candidates.append({
            "directory": directory,
            "score": album_score,
            "audio_count": audio_count,
        })

    # Sort by score descending, then audio_count descending
    candidates.sort(key=lambda c: (-c["score"], -c["audio_count"]))
    return candidates


def select_best_folder(candidate_folders: list[dict]) -> dict | None:
    """
    Select the best album folder from candidates across multiple peers.

    Implements the FLAC-preferred-but-completeness-wins algorithm:
      1. Find the best MP3-only folder (most audio files among non-FLAC folders)
      2. Find the best FLAC folder (most audio files among FLAC folders)
      3. FLAC wins only if its audio count >= the best MP3 count
      4. On tie, FLAC is preferred

    This avoids sacrificing album completeness just to get FLAC — a 12-track
    MP3 album beats an 8-track FLAC folder every time.

    Parameters:
        candidate_folders — list of dicts: [{"username": str, "directory": dict}, ...]

    Returns the winning {"username": str, "directory": dict} or None if empty.
    """
    if not candidate_folders:
        return None

    # Annotate each candidate with audio count and FLAC flag
    annotated = []
    for candidate in candidate_folders:
        directory = candidate["directory"]
        audio_count = _count_audio_files(directory)
        has_flac = _folder_has_flac(directory)
        annotated.append({
            "username": candidate["username"],
            "directory": directory,
            "audio_count": audio_count,
            "has_flac": has_flac,
        })

    # Separate into FLAC and MP3-only pools
    flac_folders = [c for c in annotated if c["has_flac"]]
    mp3_folders = [c for c in annotated if not c["has_flac"]]

    best_flac = max(flac_folders, key=lambda c: c["audio_count"]) if flac_folders else None
    best_mp3 = max(mp3_folders, key=lambda c: c["audio_count"]) if mp3_folders else None

    # Decision: FLAC wins only if it has >= files as the best MP3 option
    if best_flac and best_mp3:
        if best_flac["audio_count"] >= best_mp3["audio_count"]:
            winner = best_flac
            log.info(
                "Folder selection: FLAC wins (flac=%d files vs mp3=%d files)",
                best_flac["audio_count"],
                best_mp3["audio_count"],
            )
        else:
            winner = best_mp3
            log.info(
                "Folder selection: MP3 wins on completeness (mp3=%d files vs flac=%d files)",
                best_mp3["audio_count"],
                best_flac["audio_count"],
            )
    elif best_flac:
        winner = best_flac
        log.info("Folder selection: FLAC wins (no MP3 candidates)")
    elif best_mp3:
        winner = best_mp3
        log.info("Folder selection: MP3 wins (no FLAC candidates)")
    else:
        return None

    log.info(
        "Selected folder: %s from peer %s (%d audio files, has_flac=%s)",
        winner["directory"].get("name", ""),
        winner["username"],
        winner["audio_count"],
        winner["has_flac"],
    )

    return {"username": winner["username"], "directory": winner["directory"]}


def search_album(
    client: slskd_api.SlskdClient,
    artist: str,
    album: str,
    log: logging.Logger,
    excluded_peers: set[str] | None = None,
) -> list[dict]:
    """
    Search Soulseek for an album and return the top peer usernames.

    Submits a search for "Artist Album" and waits for results using the same
    poll-until-complete pattern as slskd_client.search(). Deduplicates peers
    and sorts by availability (free upload slot first) then speed.

    Parameters:
        client         — authenticated SlskdClient
        artist         — artist name
        album          — album name
        log            — logger instance from the calling module
        excluded_peers — set of usernames to skip (previously failed peers)

    Returns a list of up to MAX_PEERS_TO_BROWSE dicts: [{"username": str}, ...]
    Returns an empty list if no results are found or an error occurs.
    """
    # For multi-artist releases (e.g. "Joutro Mundo, JKriv"), use only the first
    # artist in the search query. Including all artists makes the query too specific
    # and returns 0 results. The full artist string is still used for folder matching.
    search_artist = artist.split(",")[0].strip() if "," in artist else artist
    query = build_album_query(f"{search_artist} - {album}")
    log.info("Album search: %r (query=%r)", f"{artist} - {album}", query)

    try:
        response = client.searches.search_text(
            searchText=query,
            searchTimeout=SEARCH_TIMEOUT_MS,
            filterResponses=True,
            maximumPeerQueueLength=50,
            minimumPeerUploadSpeed=0,
        )
        search_id = response["id"]
    except Exception as exc:
        log.error("Failed to submit album search for %r: %s", query, exc)
        return []

    # Wait for slskd to start collecting peer responses
    time.sleep(5)

    # Poll until search completes or safety timeout is hit
    safety_limit_seconds = (SEARCH_TIMEOUT_MS / 1000) + 10
    start = time.time()

    while True:
        try:
            state = client.searches.state(search_id, includeResponses=False)
        except Exception as exc:
            log.warning("Failed to poll search state for %s: %s", search_id, exc)
            break

        if state["state"] != "InProgress":
            log.debug("Album search %s completed with state: %s", search_id, state["state"])
            break

        elapsed = time.time() - start
        if elapsed > safety_limit_seconds:
            log.warning(
                "Album search safety timeout hit after %.1fs for query %r — fetching partial results",
                elapsed,
                query,
            )
            break

        time.sleep(1)

    try:
        results = client.searches.search_responses(search_id)
    except Exception as exc:
        log.error("Failed to fetch album search results for %s: %s", search_id, exc)
        return []

    # Delete the search from slskd immediately after fetching results.
    # Prevents search accumulation that can block new searches and
    # disconnect slskd from the Soulseek network.
    try:
        client.searches.delete(search_id)
    except Exception:
        pass  # Cleanup failure is non-fatal — scheduler bulk cleanup is the safety net

    log.info("Album search %r returned %d peer response(s)", query, len(results))

    if not results:
        return []

    # Deduplicate by username — keep the response with best stats per peer
    seen_usernames: dict[str, dict] = {}
    for peer_response in results:
        username = peer_response.get("username", "")
        if not username:
            continue
        # Keep the first occurrence (search responses are already sorted by slskd)
        if username not in seen_usernames:
            seen_usernames[username] = peer_response

    # Filter out excluded peers (previously failed for this song)
    if excluded_peers:
        before_count = len(seen_usernames)
        seen_usernames = {
            u: r for u, r in seen_usernames.items() if u not in excluded_peers
        }
        filtered_count = before_count - len(seen_usernames)
        if filtered_count:
            log.info("Album search: excluded %d previously failed peer(s)", filtered_count)

    # Sort: free upload slot first, then by upload speed descending
    unique_peers = list(seen_usernames.values())
    unique_peers.sort(
        key=lambda r: (not r.get("hasFreeUploadSlot", False), -r.get("uploadSpeed", 0))
    )

    # Return top MAX_PEERS_TO_BROWSE as simple username dicts
    top_peers = [{"username": p["username"]} for p in unique_peers[:MAX_PEERS_TO_BROWSE]]
    log.info(
        "Album search: selected %d peer(s) to browse: %s",
        len(top_peers),
        [p["username"] for p in top_peers],
    )
    return top_peers


def browse_and_select(
    client: slskd_api.SlskdClient,
    peer_usernames: list[str],
    artist: str,
    album: str,
    log: logging.Logger,
    excluded_peers: set[str] | None = None,
) -> dict | None:
    """
    Browse each peer's folder structure and select the best album folder.

    For each username, calls the slskd browse endpoint to retrieve the peer's
    complete share structure. Finds folders matching the target album and collects
    the top-scoring folder from each peer as a candidate. The best folder across
    all candidates is selected using FLAC-preferred-but-completeness-wins logic.

    Parameters:
        client         — authenticated SlskdClient
        peer_usernames — list of dicts: [{"username": str}, ...]
        artist         — target artist name
        album          — target album name
        log            — logger instance from the calling module
        excluded_peers — set of usernames to skip (belt-and-suspenders with search_album filtering)

    Returns {"username": str, "directory": dict} for the winning folder,
    or None if no matching folder was found across all peers.
    """
    all_candidates = []

    for peer_entry in peer_usernames[:MAX_PEERS_TO_BROWSE]:
        username = peer_entry["username"]

        # Belt-and-suspenders: skip excluded peers even if search_album missed them
        if excluded_peers and username in excluded_peers:
            log.debug("Skipping excluded peer in browse: %s", username)
            continue

        log.info("Browsing peer: %s", username)

        try:
            browse_result = client.users.browse(username)
        except Exception as exc:
            log.warning("Failed to browse peer %r: %s — skipping", username, exc)
            continue

        if not browse_result:
            log.debug("Empty browse result for peer %s", username)
            continue

        matching_folders = _find_matching_folders(browse_result, artist, album)

        if not matching_folders:
            log.debug("No matching folders found for %r - %r at peer %s", artist, album, username)
            continue

        # Take only the top-scoring folder from this peer
        best_from_peer = matching_folders[0]
        log.info(
            "Found matching folder at peer %s: %s (score=%d, audio_files=%d)",
            username,
            best_from_peer["directory"].get("name", ""),
            best_from_peer["score"],
            best_from_peer["audio_count"],
        )
        all_candidates.append({
            "username": username,
            "directory": best_from_peer["directory"],
        })

    if not all_candidates:
        log.info(
            "No matching album folders found across %d peer(s) for %r - %r",
            len(peer_usernames),
            artist,
            album,
        )
        return None

    log.info(
        "Selecting best folder from %d candidate(s) across peers",
        len(all_candidates),
    )
    return select_best_folder(all_candidates)


def enqueue_album_folder(
    client: slskd_api.SlskdClient,
    username: str,
    files: list[dict],
    log: logging.Logger,
) -> bool:
    """
    Batch-enqueue all files from a selected album folder for download.

    Sends a single request to slskd to enqueue all files from the winning
    folder. This is more efficient than N individual enqueue calls and ensures
    all files are tracked under the same peer in slskd.

    Parameters:
        client   — authenticated SlskdClient
        username — Soulseek username of the peer to download from
        files    — list of file dicts from directory["files"]
        log      — logger instance from the calling module

    Returns True on success, False if the enqueue call failed.
    """
    if not files:
        log.warning("enqueue_album_folder() called with empty file list")
        return False

    file_list = [
        {"filename": f["filename"], "size": f["size"]}
        for f in files
    ]

    log.info(
        "Enqueueing %d file(s) from peer %s",
        len(file_list),
        username,
    )

    try:
        client.transfers.enqueue(username=username, files=file_list)
        log.info("Successfully enqueued %d file(s) from %s", len(file_list), username)
        return True
    except Exception as exc:
        log.error(
            "Failed to enqueue album files from peer %s: %s",
            username,
            exc,
        )
        return False


def run_album_search(
    client: slskd_api.SlskdClient,
    artist: str,
    album: str,
    log: logging.Logger,
    excluded_peers: set[str] | None = None,
) -> dict | None:
    """
    Orchestrate the full album search flow: search -> browse -> select.

    This is the main entry point called by the pipeline. It coordinates all
    steps and returns a ready-to-enqueue result, or None if no suitable album
    folder was found.

    Parameters:
        client         — authenticated SlskdClient
        artist         — target artist name
        album          — target album name
        log            — logger instance from the calling module
        excluded_peers — set of usernames to skip (previously failed peers)

    Returns a dict on success:
        {
          "username": str,       — peer to download from
          "directory": dict,     — full directory dict from browse result
          "files": list[dict],   — all files in the selected directory
        }

    Returns None if:
      - No peers had search results
      - No matching folders found after browsing
    """
    log.info("Starting album search for: %r - %r", artist, album)
    if excluded_peers:
        log.info("Excluding %d previously failed peer(s)", len(excluded_peers))

    # Step 1: Search for peers who have the album
    peer_usernames = search_album(client, artist, album, log, excluded_peers=excluded_peers)
    if not peer_usernames:
        log.info("Album search returned no peers for %r - %r", artist, album)
        return None

    # Step 2: Browse peers and select the best matching folder
    winner = browse_and_select(client, peer_usernames, artist, album, log, excluded_peers=excluded_peers)
    if not winner:
        log.info("No matching album folder found across all browsed peers")
        return None

    files = winner["directory"].get("files", [])
    log.info(
        "Album search complete: %r - %r found at peer %s (%d files)",
        artist,
        album,
        winner["username"],
        len(files),
    )

    return {
        "username": winner["username"],
        "directory": winner["directory"],
        "files": files,
    }


if __name__ == "__main__":
    """
    Quick smoke test for the album search module (no network required).

    Tests the pure functions that don't require a live slskd connection.
    """
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s: %(message)s")

    print("=== album_search.py smoke tests ===\n")

    # Test build_album_query
    print("build_album_query() tests:")
    assert build_album_query("Radiohead - OK Computer") == "Radiohead OK Computer", "FAIL: with dash"
    assert build_album_query("Radiohead OK Computer") == "Radiohead OK Computer", "FAIL: no dash"
    print("  [OK] build_album_query('Radiohead - OK Computer') =", build_album_query("Radiohead - OK Computer"))
    print("  [OK] build_album_query('Radiohead OK Computer') =", build_album_query("Radiohead OK Computer"))

    # Test score_directory
    print("\nscore_directory() tests:")
    matched, score = score_directory("C:\\Music\\Radiohead\\OK Computer", "Radiohead", "OK Computer")
    print(f"  score for 'OK Computer' folder: matched={matched}, score={score}")
    assert matched, "FAIL: artist should match"
    assert score >= 2, f"FAIL: expected >= 2, got {score}"
    print("  [OK] artist matched and score >= 2")

    # Test that the Lemonheads bug is fixed
    matched2, score2 = score_directory(
        "C:\\Music\\The Lemonheads\\It's a Shame About Ray",
        "Yukihiro Fukutomi", "It's About Time",
    )
    print(f"  score for Lemonheads vs Fukutomi: matched={matched2}, score={score2}")
    assert not matched2, "FAIL: artist should NOT match (Fukutomi != Lemonheads)"
    print("  [OK] Lemonheads correctly rejected when searching for Fukutomi")

    # Test select_best_folder with empty input
    print("\nselect_best_folder() tests:")
    assert select_best_folder([]) is None, "FAIL: empty list should return None"
    print("  [OK] select_best_folder([]) returns None")

    # Test FLAC-preferred-but-completeness-wins logic
    mock_candidates = [
        {
            "username": "peer_mp3",
            "directory": {
                "name": "C:\\Music\\Radiohead\\OK Computer",
                "files": [
                    {"filename": "01.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "02.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "03.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "04.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "05.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "06.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "07.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "08.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "09.mp3", "size": 10_000_000, "extension": "mp3"},
                    {"filename": "10.mp3", "size": 10_000_000, "extension": "mp3"},
                ],
            },
        },
        {
            "username": "peer_flac",
            "directory": {
                "name": "C:\\Music\\Radiohead\\OK Computer",
                "files": [
                    {"filename": "01.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "02.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "03.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "04.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "05.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "06.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "07.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "08.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "09.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "10.flac", "size": 30_000_000, "extension": "flac"},
                ],
            },
        },
    ]
    result = select_best_folder(mock_candidates)
    assert result is not None, "FAIL: expected a winner"
    assert result["username"] == "peer_flac", f"FAIL: FLAC should win tie, got {result['username']}"
    print("  [OK] FLAC wins when file counts are equal")

    # Test that MP3 wins when it's more complete
    mock_flac_incomplete = [
        mock_candidates[0],  # 10-track MP3
        {
            "username": "peer_flac_5tracks",
            "directory": {
                "name": "C:\\Music\\Radiohead\\OK Computer",
                "files": [
                    {"filename": "01.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "02.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "03.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "04.flac", "size": 30_000_000, "extension": "flac"},
                    {"filename": "05.flac", "size": 30_000_000, "extension": "flac"},
                ],
            },
        },
    ]
    result2 = select_best_folder(mock_flac_incomplete)
    assert result2 is not None, "FAIL: expected a winner"
    assert result2["username"] == "peer_mp3", f"FAIL: MP3 should win (10 files > 5 FLAC), got {result2['username']}"
    print("  [OK] MP3 wins on completeness (10 MP3 files > 5 FLAC files)")

    print("\n[OK] All smoke tests passed.")
