"""
sources/textfile.py — TextFileAdapter: reads a plain-text file of "Artist - Title" lines.

File format:
  - One song per line in "Artist - Title" format
  - Lines starting with # are comments (skipped)
  - Blank lines are skipped
  - Lines without " - " are sent to Claude Haiku for intelligent extraction
    (track mode only — album mode requires the " - " separator)

Source IDs are hash-based (not line-number-based) so they remain stable when
the file is edited, reordered, or reformatted.

Two modes are supported via the `mode` config key:
  - "track" (default): existing behavior — parses "Artist - Title" lines and
    falls back to Haiku for malformed entries.
  - "album": parses "Artist - Album" lines. Lines without " - " are logged and
    skipped (no Haiku fallback — album format is too structured for AI recovery).
    Album entries are stored with search_mode='album' and excluded from the
    track-search pipeline until Phase 8.

Design note: Using Haiku as fallback for malformed lines is a user decision
documented in the research phase. The adapter is permissive — it recovers
what it can and logs what it skips.
"""

import hashlib
import logging
import os

from anthropic import Anthropic
from pydantic import BaseModel

log = logging.getLogger(__name__)

_client = Anthropic(max_retries=3)

_SYSTEM_PROMPT = """\
You are a music metadata extractor. Given a text line that is supposed to represent
a song, extract the artist name, song title, and any version qualifier (remix, live, etc.).

The line may be malformed — it might be missing the " - " separator, use unusual dashes,
have extra parentheses, or contain noise like track numbers.

Rules:
- Strip leading track numbers (e.g. "1.", "01.", "Track 1 -") from the beginning.
- Most entries follow "Artist - Title" format. Parse accordingly.
- Also handle "Title by Artist" format.
- Strip noise: extra parentheses content that is not meaningful, unusual punctuation.
- Version qualifiers like "Remix", "Live", "Acoustic", "Remastered", "Radio Edit"
  belong in the `version` field, NOT in the `title` field.
- Featured artists: normalize "ft.", "feat.", "featuring" to "feat." and keep in title.
- If the artist cannot be determined, use an empty string.
- If there is no version qualifier, use an empty string for `version`.
- apostrophe variants (', `, ') should be normalized to standard apostrophe.
"""


class TextLineParseResult(BaseModel):
    """Structured output from Claude Haiku for a single text file line."""
    artist: str    # Empty string if artist cannot be determined
    title: str     # The clean song title
    version: str   # Remix/live/acoustic qualifier, empty string if none


class TextFileAdapter:
    """
    Source adapter that reads a plain-text file with one song per line.

    Supported line format: "Artist - Title"
    Lines without " - " are sent to Claude Haiku for intelligent parsing.
    Lines Haiku cannot parse are skipped with a log warning.
    """

    def __init__(self, config: dict):
        """
        Parameters:
            config — dict parsed from sources.yaml entry, e.g.:
                     {"type": "textfile", "path": "/config/tracks.txt"}
                     {"type": "textfile", "path": "/config/albums.txt", "mode": "album"}

        The `mode` key defaults to "track" for backward compatibility with
        existing sources.yaml entries that have no `mode` key.
        """
        self._path = config["path"]
        self._mode = config.get("mode", "track")

    @property
    def source_name(self) -> str:
        """
        Returns a stable identifier for this source instance.

        Example: "textfile:tracks.txt"
        """
        return f"textfile:{os.path.basename(self._path)}"

    def fetch_songs(self) -> list[dict]:
        """
        Read the text file and return fully-parsed SongEntry dicts.

        Returns a list of dicts matching the SongEntry shape:
            source_id, source_type, search_mode, raw_title, artist, title, version

        Returns [] if the file cannot be read. Per-line errors are logged and skipped.
        """
        log.info("[%s] Reading file: %s", self.source_name, self._path)

        try:
            with open(self._path, encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            log.error("[%s] File not found: %s", self.source_name, self._path)
            return []
        except Exception as exc:
            log.error("[%s] Failed to read file %s: %s", self.source_name, self._path, exc)
            return []

        songs = []

        for line_num, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()

            # Skip blank lines and comment lines
            if not line or line.startswith("#"):
                continue

            raw_title = line  # Preserve the original for raw_title field

            # Normalize en dashes to standard hyphens so "Artist – Title"
            # is split the same way as "Artist - Title" (avoids Haiku
            # fallback which strips remix/version info from the title).
            line = line.replace(" – ", " - ")

            if self._mode == "album":
                # Album mode: expects "Artist - Album Name" format.
                # No Haiku fallback — album format is too structured for AI recovery.
                if " - " not in line:
                    log.warning(
                        "[%s] Line %d: album entry has no ' - ' separator — skipping: %r",
                        self.source_name, line_num, line,
                    )
                    continue

                artist, _album_name = line.split(" - ", 1)
                artist = artist.strip()

                songs.append({
                    "source_id":   line,          # Full "Artist - Album" string as human-readable ID
                    "source_type": "textfile",
                    "search_mode": "album",
                    "raw_title":   line,
                    "artist":      artist,
                    "title":       line,           # Per CONTEXT.md: title stores "Artist - Album"
                    "version":     "",
                })

            else:
                # Track mode (default): existing behavior is completely unchanged.
                if " - " in line:
                    # Standard format: split on first " - " only
                    artist, remainder = line.split(" - ", 1)
                    artist = artist.strip()
                    title = remainder.strip()
                    version = ""
                else:
                    # Non-standard format: send to Haiku for intelligent extraction
                    log.info(
                        "[%s] Line %d has no ' - ' separator — sending to Haiku: %r",
                        self.source_name, line_num, line,
                    )
                    result = self._parse_with_haiku(line)
                    if result is None:
                        log.warning(
                            "[%s] Line %d: Haiku could not parse %r — skipping",
                            self.source_name, line_num, line,
                        )
                        continue
                    artist = result.artist
                    title = result.title
                    version = result.version

                # Skip entries with no usable title
                if not title:
                    log.warning(
                        "[%s] Line %d: no title extracted from %r — skipping",
                        self.source_name, line_num, raw_title,
                    )
                    continue

                source_id = self._make_source_id(artist, title)

                songs.append({
                    "source_id":   source_id,
                    "source_type": "textfile",
                    "search_mode": "track",
                    "raw_title":   raw_title,
                    "artist":      artist,
                    "title":       title,
                    "version":     version,
                })

        log.info("[%s] fetch_songs complete — %d songs loaded", self.source_name, len(songs))
        return songs

    def _parse_with_haiku(self, line: str) -> TextLineParseResult | None:
        """
        Send a malformed line to Claude Haiku for intelligent artist/title extraction.

        Returns a TextLineParseResult on success, or None if the API call fails
        or the model cannot extract meaningful data.
        """
        try:
            response = _client.messages.parse(
                model="claude-haiku-4-5-20251001",
                max_tokens=128,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": line}],
                output_format=TextLineParseResult,
            )
            return response.parsed_output
        except Exception as exc:
            log.error(
                "[%s] Haiku parse failed for %r: %s",
                self.source_name, line, exc,
            )
            return None

    def _make_source_id(self, artist: str, title: str) -> str:
        """
        Generate a stable, hash-based source ID for a text file song.

        The hash is computed from normalized artist + normalized title only
        (NOT the file path). This means the same "Artist - Title" in two
        different text file sources produces the same source_id, preventing
        duplicate downloads across sources.

        Format: "tf:" + first 12 hex chars of SHA-256
        Example: "tf:a3f9e1c2b8d4"
        """
        payload = f"{artist.lower().strip()}:{title.lower().strip()}"
        digest = hashlib.sha256(payload.encode()).hexdigest()[:12]
        return f"tf:{digest}"
