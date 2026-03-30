"""
sources/youtube.py — YouTubeAdapter: wraps existing youtube.py and parser.py.

This adapter is a thin composition layer — it does NOT rewrite youtube.py or parser.py.
Instead, it calls them in the right sequence and adds the multi-source fields
(source_type, search_mode) that the pipeline needs.

The adapter receives a config dict from load_sources() (parsed from sources.yaml):
    {"type": "youtube", "playlist_url": "https://..."}

Design note: Parsing (Haiku) happens INSIDE fetch_songs() — this keeps the pipeline
orchestrator source-agnostic. By the time songs leave the adapter, they are fully
parsed and enriched, ready for add_songs().
"""

import logging
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)


class YouTubeAdapter:
    """
    Source adapter that fetches and parses a YouTube playlist.

    Wraps:
      - youtube.fetch_playlist()      — yt-dlp playlist extraction
      - parser.parse_titles()         — Claude Haiku title parsing
      - parser.enrich_incomplete()    — two-pass enrichment with video metadata
      - youtube.fetch_video_metadata() — metadata for enrichment pass

    Returns fully-parsed SongEntry dicts with source_type='youtube'.
    """

    def __init__(self, config: dict):
        """
        Parameters:
            config — dict parsed from sources.yaml entry, e.g.:
                     {"type": "youtube", "playlist_url": "https://..."}
        """
        self._playlist_url = config["playlist_url"]

    @property
    def source_name(self) -> str:
        """
        Returns a stable identifier for this source instance.

        Extracts the playlist ID from the URL to produce a name like:
            "youtube:PLxxxxxxxxxxxxxx"

        Falls back to "youtube:unknown" if the URL has no list= parameter.
        """
        qs = parse_qs(urlparse(self._playlist_url).query)
        playlist_id = qs.get("list", ["unknown"])[0]
        return f"youtube:{playlist_id}"

    def fetch_songs(self, known_ids: set[str] | None = None) -> list[dict]:
        """
        Fetch the YouTube playlist, parse titles with Haiku, and enrich incomplete entries.

        Parameters:
            known_ids — optional set of source_ids already in the DB. Entries matching
                        these IDs are skipped before calling Haiku, saving API tokens.

        Returns a list of dicts matching the SongEntry shape:
            source_id, source_type, search_mode, raw_title, artist, title, version

        Returns [] on any failure — errors are logged, not raised.
        The pipeline orchestrator handles empty returns gracefully.
        """
        # Lazy imports: youtube.py and parser.py live in the automator/ root.
        # These exist from v1.0 and are NOT modified by this adapter.
        from youtube import fetch_playlist, fetch_video_metadata
        from parser import parse_titles, enrich_incomplete

        log.info("[%s] Fetching playlist: %s", self.source_name, self._playlist_url)

        entries = fetch_playlist(self._playlist_url)
        if not entries:
            log.warning("[%s] No entries returned from playlist", self.source_name)
            return []

        log.info("[%s] Fetched %d entries", self.source_name, len(entries))

        # Filter out entries we already have in the DB — saves Haiku API calls.
        # A 200-track playlist with 5 new songs = 5 API calls instead of 200.
        if known_ids:
            new_entries = [e for e in entries if e.get("source_id") not in known_ids]
            skipped = len(entries) - len(new_entries)
            if skipped:
                log.info(
                    "[%s] Skipped %d known songs, parsing %d new entries",
                    self.source_name, skipped, len(new_entries),
                )
            entries = new_entries

        if not entries:
            log.info("[%s] All entries already known — nothing to parse", self.source_name)
            return []

        # parse_titles expects [{source_id, raw_title}, ...] — exactly what fetch_playlist returns
        parsed = parse_titles(entries)

        # Two-pass enrichment: re-parse entries with empty artist/title using video metadata
        parsed = enrich_incomplete(parsed, fetch_video_metadata)

        # Tag all entries with multi-source fields
        for song in parsed:
            song["source_type"] = "youtube"
            song["search_mode"] = "track"

        log.info("[%s] fetch_songs complete — %d songs ready", self.source_name, len(parsed))
        return parsed
