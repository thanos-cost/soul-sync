"""
Quality ranking and filename matching for Soulseek search results.

Format priority (tier 0 = best):
  0 — FLAC
  1 — Other lossless (WAV, ALAC, AIFF, AIF)
  2 — MP3 320kbps
  3 — MP3 (any bitrate)
  4 — Other audio formats (AAC, OGG, M4A, OPUS, WMA)

All recognised audio formats are accepted. When multiple candidates exist,
higher-quality tiers are preferred. Size outliers (< 1MB or > 200MB) are
still rejected. Filename must contain artist AND title.

The selector works on the raw dicts returned by slskd_client.search().
"""

import logging

log = logging.getLogger(__name__)

# Format quality constants
LOSSLESS_EXTENSIONS = {"flac", "wav", "alac", "aiff", "aif"}
ACCEPTED_EXTENSIONS = {"flac", "wav", "alac", "aiff", "aif", "mp3", "ogg", "aac", "m4a", "wma", "opus"}

# File size bounds — Claude's discretion per plan:
# 1MB minimum eliminates corrupt or placeholder files.
# 200MB maximum is a generous cap; a typical 5-min FLAC is 20-40MB.
MIN_SIZE_BYTES = 1_000_000    # 1 MB
MAX_SIZE_BYTES = 200_000_000  # 200 MB


def format_tier(extension: str, bitrate: int | None) -> int | None:
    """
    Return the quality tier for a file, or None to reject non-audio files.

    Lower tier numbers mean higher quality:
      0 — FLAC
      1 — Other lossless (WAV, ALAC, AIFF, AIF)
      2 — MP3 at 320kbps
      3 — MP3 (any bitrate)
      4 — Other accepted audio (AAC, OGG, M4A, OPUS, WMA)
      None — Not a recognised audio format

    IMPORTANT: Bitrate is only checked for MP3. FLAC and other lossless formats
    frequently have bitRate=None or bitRate=0 in slskd responses — this is normal
    (lossless files have variable bitrates by definition). Checking bitrate for
    lossless would incorrectly reject them.
    """
    ext = extension.lower()

    if ext not in ACCEPTED_EXTENSIONS:
        return None

    if ext == "flac":
        return 0

    if ext in LOSSLESS_EXTENSIONS:
        return 1

    if ext == "mp3":
        if bitrate is not None and bitrate >= 320:
            return 2
        return 3

    return 4


def get_basename(filename: str) -> str:
    """
    Extract the filename portion from a Windows-style Soulseek path.

    Soulseek peers run on Windows; filenames are full paths with backslash
    separators, e.g.: C:\\Music\\Radiohead\\Creep.flac

    Using rsplit("\\", 1) handles both:
      - Full paths: "C:\\Music\\Artist\\Song.flac" -> "Song.flac"
      - Bare filenames already without path: "Song.flac" -> "Song.flac"
    """
    return filename.rsplit("\\", 1)[-1]


def _normalize(text: str) -> str:
    """
    Strip punctuation, extra spaces, and lowercase for fuzzy matching.

    Soulseek filenames are messy — dashes, apostrophes, dots, and underscores
    are used inconsistently. Comparing normalised strings catches matches that
    exact substring search would miss, e.g.:
      "Can't You See" matches "Cant You See"
      "Room To Breath" matches "Room_To_Breath"
    """
    import re as _re
    # Replace common separators with space, strip remaining non-alphanumeric
    text = _re.sub(r"[_\-./\\]", " ", text.lower())
    text = _re.sub(r"[^a-z0-9\s]", "", text)
    return " ".join(text.split())  # collapse whitespace


def _words(text: str) -> set[str]:
    """Split normalised text into a set of words."""
    return set(_normalize(text).split())


def filename_matches(
    filename: str,
    artist: str,
    title: str,
) -> bool:
    """
    Check that the file path reflects the song's identity using word-level
    matching. Every word from the artist and title must appear somewhere in
    the file path — but they don't need to be contiguous or in order.

    This handles real Soulseek naming where extra info (track numbers, years,
    album names, featured artists) is interspersed in the path:
      "03. Move D - The Golden Pudelizer (feat. Rüftata110).flac"
      matches title "The Golden Pudelizer" — all words present.

    Checks the full path (folder + filename), not just the basename — the
    artist is often only in the folder name.
    """
    path_words = _words(filename)

    if artist:
        artist_words = _words(artist)
        if not artist_words.issubset(path_words):
            return False

    title_words = _words(title)
    if not title_words.issubset(path_words):
        return False

    return True


def select_best(
    responses: list,
    artist: str,
    title: str,
    min_size_bytes: int = MIN_SIZE_BYTES,
    max_size_bytes: int = MAX_SIZE_BYTES,
) -> dict | None:
    """
    Select the highest-quality matching file from a list of search responses.

    Filters each file through three gates:
      1. format_tier() — must not be None (meets quality floor)
      2. Size bounds — between min_size_bytes and max_size_bytes
      3. filename_matches() — artist + title present in file path

    Surviving candidates are sorted by:
      1. Tier ascending (lower tier = higher quality, e.g., FLAC before MP3)
      2. has_free_slot descending (free slot = peer is ready to upload now)
      3. upload_speed descending (faster upload = faster download)

    Returns a dict with: {username, filename, size, format, bitrate}
    Returns None if no candidate passes all filters.
    """
    candidates = []

    for response in responses:
        username = response.get("username", "")
        upload_speed = response.get("uploadSpeed", 0)
        has_free_slot = response.get("hasFreeUploadSlot", False)

        for file in response.get("files", []):
            fname = file.get("filename", "")
            ext = file.get("extension", "").lower()
            # Fallback: extract extension from filename if slskd didn't populate it
            if not ext and "." in fname:
                ext = fname.rsplit(".", 1)[-1].lower()
            bitrate = file.get("bitRate")  # May be None for lossless files
            size = file.get("size", 0)

            # Gate 1: Quality floor
            tier = format_tier(ext, bitrate)
            if tier is None:
                continue

            # Gate 2: Size bounds (reject outliers)
            if not (min_size_bytes <= size <= max_size_bytes):
                continue

            # Gate 3: Filename must match song identity
            if not filename_matches(fname, artist, title):
                continue

            candidates.append({
                "username": username,
                "filename": fname,
                "size": size,
                "format": ext,
                "bitrate": bitrate,
                "tier": tier,
                "upload_speed": upload_speed,
                "has_free_slot": has_free_slot,
            })

    log.info(
        "select_best(%r, %r): %d candidate(s) from %d peer response(s)",
        artist,
        title,
        len(candidates),
        len(responses),
    )

    if not candidates:
        log.debug("No candidates passed all filters for %r - %r", artist, title)
        return None

    # Sort: tier ASC (lower=better), free slot DESC (available first), speed DESC
    candidates.sort(key=lambda c: (c["tier"], not c["has_free_slot"], -c["upload_speed"]))

    best = candidates[0]
    log.info(
        "Selected: %s / %s (format=%s, bitrate=%s, tier=%d, free_slot=%s)",
        best["username"],
        get_basename(best["filename"]),
        best["format"],
        best["bitrate"],
        best["tier"],
        best["has_free_slot"],
    )

    # Return the public-facing dict — strip internal sorting fields
    return {
        "username": best["username"],
        "filename": best["filename"],
        "size": best["size"],
        "format": best["format"],
        "bitrate": best["bitrate"],
    }


if __name__ == "__main__":
    """
    Demonstrate the selector with a mock search response.

    Soulseek filenames commonly follow the "Artist - Title.ext" convention at
    the file level. Filenames that put the artist only in a folder name (like
    C:\\Radiohead\\Creep.flac) will fail the artist check — this is intentional,
    as it protects against downloading wrong songs from artist-named folders.

    Covers the key scenarios:
      - FLAC wins over MP3 320
      - MP3 128 is rejected (below floor)
      - Size outliers are rejected
      - Filename mismatch is rejected (different song from same artist)
    """
    # Simulate a real slskd search response for "Radiohead Creep"
    # Filenames use the common "Artist - Title.ext" Soulseek convention
    mock_responses = [
        {
            "username": "peer_mp3_128",
            "uploadSpeed": 1_000_000,
            "hasFreeUploadSlot": True,
            "files": [
                {
                    "filename": "C:\\Music\\Radiohead - Creep.mp3",
                    "size": 5_000_000,
                    "extension": "mp3",
                    "bitRate": 128,  # Below 320kbps floor — should be rejected
                }
            ],
        },
        {
            "username": "peer_mp3_320",
            "uploadSpeed": 500_000,
            "hasFreeUploadSlot": False,
            "files": [
                {
                    "filename": "C:\\Music\\Radiohead - Creep.mp3",
                    "size": 12_000_000,
                    "extension": "mp3",
                    "bitRate": 320,  # Passes quality floor — tier 2
                }
            ],
        },
        {
            "username": "peer_flac",
            "uploadSpeed": 800_000,
            "hasFreeUploadSlot": True,
            "files": [
                {
                    "filename": "C:\\Music\\Radiohead - Creep.flac",
                    "size": 28_000_000,
                    "extension": "flac",
                    "bitRate": None,  # Normal for FLAC — must NOT be rejected
                },
                {
                    "filename": "C:\\Music\\Radiohead - Together Forever.flac",
                    "size": 25_000_000,
                    "extension": "flac",
                    "bitRate": None,
                    # ^ Different song — filename check rejects this (no "Creep")
                }
            ],
        },
        {
            "username": "peer_tiny_corrupt",
            "uploadSpeed": 2_000_000,
            "hasFreeUploadSlot": True,
            "files": [
                {
                    "filename": "C:\\Music\\Radiohead - Creep.flac",
                    "size": 500_000,  # Below 1MB minimum — likely corrupt placeholder
                    "extension": "flac",
                    "bitRate": None,
                }
            ],
        },
    ]

    print("=== selector.py demonstration ===\n")

    # Test format_tier
    print("format_tier() tests:")
    tests = [
        ("flac", None, 0),     # FLAC with no bitrate — should be accepted
        ("flac", 0, 0),        # FLAC with bitRate=0 — should still be accepted
        ("wav", None, 1),      # Other lossless — tier 1
        ("mp3", 320, 2),       # MP3 at floor — tier 2
        ("mp3", 128, None),    # MP3 below floor — reject
        ("aac", 256, None),    # Non-MP3 lossy — reject
    ]
    for ext, br, expected in tests:
        result = format_tier(ext, br)
        status = "OK" if result == expected else "FAIL"
        print(f"  [{status}] format_tier({ext!r}, {br}) = {result} (expected {expected})")

    print()

    # Test filename_matches
    # Checks full path (folder + file) with normalised comparison
    print("filename_matches() tests (word-level subset matching):")
    match_tests = [
        ("C:\\Music\\Radiohead - Creep.flac", "Radiohead", "Creep", True),
        ("C:\\Music\\Radiohead - Together Forever.flac", "Radiohead", "Creep", False),
        ("C:\\Music\\Radiohead\\01 - Creep.flac", "Radiohead", "Creep", True),
        ("C:\\Various\\Unknown Artist - Creep.mp3", "Radiohead", "Creep", False),
        ("C:\\Music\\Radiohead - Creep (Acoustic).flac", "Radiohead", "Creep (Acoustic)", True),
        ("C:\\Music\\Radiohead - Creep.flac", "Radiohead", "Creep (Acoustic)", False),
        ("C:\\Music\\Rick Wade - Can't You See.flac", "Rick Wade", "Can't You See", True),
        # Move D — words scattered across folder + filename
        ("@@zezim\\torrenty_business\\Move D - 2022\\03. Move D - The Golden Pudelizer (feat. X).flac",
         "Move D", "The Golden Pudelizer", True),
    ]
    for fname, artist, title, expected in match_tests:
        result = filename_matches(fname, artist, title)
        status = "OK" if result == expected else "FAIL"
        base = get_basename(fname)
        print(f"  [{status}] filename_matches({base!r}, {artist!r}, {title!r}) = {result}")

    print()

    # Test select_best — FLAC should win over MP3 320
    print("select_best() test (Radiohead - Creep):")
    result = select_best(mock_responses, "Radiohead", "Creep")
    if result:
        print(f"  Selected: {result['username']} / {get_basename(result['filename'])}")
        print(f"  Format: {result['format']}, Bitrate: {result['bitrate']}, Size: {result['size']:,} bytes")
        expected_winner = "peer_flac"
        status = "OK" if result["username"] == expected_winner else f"UNEXPECTED (expected {expected_winner})"
        print(f"  Winner check: [{status}]")
    else:
        print("  [FAIL] No result returned — expected FLAC from peer_flac")

    print("\nDone.")
