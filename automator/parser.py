import logging
import os

from anthropic import Anthropic
from pydantic import BaseModel

log = logging.getLogger(__name__)


class SongParseResult(BaseModel):
    """Structured output from Claude Haiku for a single YouTube title."""
    artist: str     # Empty string if artist cannot be determined
    title: str      # The song title, cleaned of noise words
    version: str    # Remix/live/acoustic qualifier, empty string if none


_client = Anthropic(max_retries=3)

_SYSTEM_PROMPT = """\
You are a music metadata extractor. Given a YouTube video title, extract the artist,
song title, and version qualifier.

Rules:
- Most titles follow "Artist - Title" format. Parse accordingly.
- Also handle "Title by Artist" format.
- Strip noise words from the title: "Official Video", "Official Audio", "Music Video",
  "Official Lyric Video", "Lyrics", "Lyric Video", "(HD)", "(HQ)", "(4K)",
  year tags like "(2024)" or "[2024]", and similar boilerplate.
- Version qualifiers like "Remix", "DJ Snake Remix", "Live at [venue]", "Acoustic",
  "Remastered", "Radio Edit" belong in the `version` field, NOT in the `title` field.
  The `title` field should contain only the clean song title without these qualifiers.
  Examples:
    "Flume - Say It (Illenium Remix)" -> artist="Flume", title="Say It", version="Illenium Remix"
    "Radiohead - Creep (Live at Glastonbury 2003)" -> artist="Radiohead", title="Creep", version="Live at Glastonbury 2003"
    "Arctic Monkeys - Do I Wanna Know?" -> artist="Arctic Monkeys", title="Do I Wanna Know?", version=""
- Featured artists: normalize "ft.", "feat.", "featuring" all to "feat." and keep
  in the title field (not the artist field). E.g. "Get Lucky feat. Pharrell Williams".
- If the artist cannot be determined from the title alone, use an empty string.
- If there is no version qualifier, use an empty string for `version`.
"""

_CONTEXT_SYSTEM_PROMPT = """\
You are a music metadata extractor. Given a YouTube video title plus additional context
(channel name and video description), extract the artist, song title, and version qualifier.

Use the channel name and description to help identify the artist and title when the video
title alone is ambiguous or incomplete.

Rules:
- Most titles follow "Artist - Title" format. Parse accordingly.
- Also handle "Title by Artist" format.
- Strip noise words from the title: "Official Video", "Official Audio", "Music Video",
  "Official Lyric Video", "Lyrics", "Lyric Video", "(HD)", "(HQ)", "(4K)",
  year tags like "(2024)" or "[2024]", and similar boilerplate.
- Version qualifiers like "Remix", "DJ Snake Remix", "Live at [venue]", "Acoustic",
  "Remastered", "Radio Edit" belong in the `version` field, NOT in the `title` field.
  The `title` field should contain only the clean song title without these qualifiers.
  Examples:
    "Flume - Say It (Illenium Remix)" -> artist="Flume", title="Say It", version="Illenium Remix"
    "Radiohead - Creep (Live at Glastonbury 2003)" -> artist="Radiohead", title="Creep", version="Live at Glastonbury 2003"
    "Arctic Monkeys - Do I Wanna Know?" -> artist="Arctic Monkeys", title="Do I Wanna Know?", version=""
- Featured artists: normalize "ft.", "feat.", "featuring" all to "feat." and keep
  in the title field (not the artist field). E.g. "Get Lucky feat. Pharrell Williams".
- If the artist cannot be determined even with the extra context, use an empty string.
- If there is no version qualifier, use an empty string for `version`.
"""


def parse_title(raw_title: str) -> SongParseResult | None:
    """
    Send a raw YouTube title to Claude Haiku and return a structured parse result.

    Returns None if the API call fails after retries, so the caller can skip this entry.
    """
    try:
        response = _client.messages.parse(
            model="claude-haiku-4-5-20251001",
            max_tokens=128,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": raw_title}],
            output_format=SongParseResult,
        )
        return response.parsed_output
    except Exception as e:
        log.error("API call failed for %r — %s", raw_title, e)
        return None


def parse_title_with_context(
    raw_title: str,
    channel_name: str,
    description: str,
) -> SongParseResult | None:
    """
    Parse a raw YouTube title using additional video metadata as context.

    Used in two-pass enrichment when the title alone is ambiguous — channel name
    and description often reveal the artist name that the title omits.

    Returns None if the API call fails after retries.
    """
    user_content = f"""Video title: {raw_title}
Channel name: {channel_name}
Description: {description[:500] if description else '(none)'}"""

    try:
        response = _client.messages.parse(
            model="claude-haiku-4-5-20251001",
            max_tokens=128,
            system=_CONTEXT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
            output_format=SongParseResult,
        )
        return response.parsed_output
    except Exception as e:
        log.error("API call (with context) failed for %r — %s", raw_title, e)
        return None


def parse_titles(entries: list[dict]) -> list[dict]:
    """
    Parse a list of {source_id, raw_title} entries through Claude Haiku.

    Every entry in your playlist is treated as wanted — no is_song filtering.
    Skips only entries where the API call itself fails.

    Returns a list of dicts: {source_id, raw_title, artist, title, version}
    """
    total = len(entries)
    results = []

    for i, entry in enumerate(entries, start=1):
        raw_title = entry["raw_title"]
        log.info("Parsing %d/%d: %s", i, total, raw_title)

        result = parse_title(raw_title)

        if result is None:
            log.warning("SKIPPED (API failure): %s", raw_title)
            continue

        log.info("  -> %s — %s%s", result.artist, result.title,
                 f" ({result.version})" if result.version else "")
        results.append({
            "source_id": entry["source_id"],
            "raw_title": raw_title,
            "artist": result.artist,
            "title": result.title,
            "version": result.version,
        })

    log.info("Result: %d/%d entries parsed", len(results), total)
    return results


def enrich_incomplete(entries: list[dict], fetch_metadata_fn) -> list[dict]:
    """
    Two-pass enrichment: re-parse entries with empty artist or title using full video metadata.

    Some YouTube titles don't contain the artist name — the channel name or
    description often does. For entries where Haiku returned an empty artist or
    empty title, we fetch full video metadata and re-parse with that extra context.

    Parameters:
        entries:           list of dicts from parse_titles() — each has source_id, raw_title,
                           artist, title, version
        fetch_metadata_fn: callable(source_id) -> {channel_name, description} | None
                           Injected so this function doesn't depend directly on youtube.py
                           (makes it testable without network calls).

    Returns the same list with incomplete entries updated where enrichment succeeded.
    """
    incomplete = [e for e in entries if not e.get("artist") or not e.get("title")]

    if not incomplete:
        return entries

    log.info("Two-pass enrichment: %d entries with empty artist or title", len(incomplete))
    enriched_count = 0

    for entry in incomplete:
        source_id = entry["source_id"]
        raw_title = entry["raw_title"]

        log.info("  Enriching: %s (%s)", raw_title, source_id)

        metadata = fetch_metadata_fn(source_id)
        if metadata is None:
            log.warning("  Could not fetch metadata for %s — skipping", source_id)
            continue

        channel_name = metadata.get("channel_name", "")
        description = metadata.get("description", "")

        result = parse_title_with_context(raw_title, channel_name, description)
        if result is None:
            log.warning("  Re-parse failed for %s — keeping original", source_id)
            continue

        # Only update if enrichment actually improved something
        if result.artist or result.title:
            entry["artist"] = result.artist
            entry["title"] = result.title
            entry["version"] = result.version
            log.info("  Enriched: %s — %s%s", result.artist, result.title,
                     f" ({result.version})" if result.version else "")
            enriched_count += 1

    log.info("Two-pass enrichment complete: %d/%d entries improved", enriched_count, len(incomplete))
    return entries


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s: %(message)s")

    test_titles = [
        {"source_id": "test001", "raw_title": "Arctic Monkeys - Do I Wanna Know? (Official Video)"},
        {"source_id": "test002", "raw_title": "Radiohead - Creep (Live at Glastonbury 2003)"},
        {"source_id": "test003", "raw_title": "lofi hip hop radio - beats to relax/study to"},
        {"source_id": "test004", "raw_title": "Daft Punk - Get Lucky ft. Pharrell Williams"},
        {"source_id": "test005", "raw_title": "Move D • The Golden Pudelizer (Pandemix Live Jams Vol. 4  B1)"},
        {"source_id": "test006", "raw_title": "Flume - Say It (Illenium Remix)"},
    ]

    print("Running parser.py standalone test with 6 titles...\n")
    songs = parse_titles(test_titles)

    print("\nParsed songs (with version field):")
    for song in songs:
        version_str = f" [{song['version']}]" if song.get("version") else ""
        print(f"  [{song['source_id']}] {song['artist']} — {song['title']}{version_str}")

    print("\nChecking version field presence in all results:")
    for song in songs:
        assert "version" in song, f"MISSING version key in {song['source_id']}"
    print("  All results have 'version' key — OK")
