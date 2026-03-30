"""
sources/base.py — Core interface contracts for all input source adapters.

Defines the two building blocks every source must provide:
  - SongEntry: the shape of a single song record as it leaves any source adapter
  - SourceAdapter: the Protocol (interface) that every adapter must implement

This file contains NO implementations — only type definitions.
Concrete adapters live in separate modules (sources/youtube.py, sources/textfile.py, etc.)
"""

from typing import Protocol, TypedDict


class SongEntry(TypedDict):
    """
    A single song record as returned by any source adapter.

    source_id   — unique identifier within this source (e.g. YouTube video ID,
                  or a hash of "Artist - Title" for text file entries)
    source_type — identifies the source system: "youtube", "textfile", etc.
    search_mode — how to search for this song on Soulseek: "track" (default)
                  or "album" (future: album-mode search)
    raw_title   — the original title string from the source, before any parsing
    artist      — extracted artist name (empty string if not determinable)
    title       — extracted song title (clean, without version qualifiers)
    version     — remix/live/acoustic qualifier (empty string if none)
    """
    source_id:   str
    source_type: str
    search_mode: str
    raw_title:   str
    artist:      str
    title:       str
    version:     str


class SourceAdapter(Protocol):
    """
    Protocol that every input source adapter must implement.

    A Protocol is Python's way of defining an interface contract — any class
    that has these attributes/methods is considered a valid SourceAdapter,
    without needing to explicitly inherit from this class.

    Think of it like a job description: any adapter that can do these things
    gets to participate in the pipeline.
    """

    @property
    def source_name(self) -> str:
        """
        Auto-generated human-readable identifier for this source instance.

        Examples:
            "youtube:PLxxxxxxxxxxxxxx"
            "textfile:tracks.txt"

        Used in log messages and error reporting to identify which source
        produced a song or which source failed.
        """
        ...

    def fetch_songs(self) -> list[SongEntry]:
        """
        Fetch all songs from this source.

        Returns a list of SongEntry dicts ready for insertion into the database.
        On failure, implementations should log the error and return an empty list
        rather than raising — the pipeline continues with other sources.
        """
        ...
