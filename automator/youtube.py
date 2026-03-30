import logging
import os
from urllib.parse import parse_qs, urlparse

import yt_dlp

log = logging.getLogger(__name__)


def _normalize_playlist_url(url: str) -> str:
    """Convert any YouTube URL with a list= parameter to a clean playlist URL.

    YouTube URLs come in two forms:
      - https://www.youtube.com/playlist?list=PLxxx  (clean — works with yt-dlp)
      - https://www.youtube.com/watch?v=abc&list=PLxxx  (watch page — yt-dlp may ignore the playlist)

    This ensures we always use the clean form.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    list_id = params.get("list", [None])[0]
    if list_id:
        return f"https://www.youtube.com/playlist?list={list_id}"
    return url


def fetch_playlist(playlist_url: str) -> list[dict]:
    """
    Fetch all entries from a YouTube playlist using yt-dlp's extract_flat mode.

    Returns a list of dicts with keys:
      - source_id: the video ID (e.g. "dQw4w9WgXcQ")
      - raw_title: the video title as it appears on YouTube

    On failure, logs the error and returns an empty list so the pipeline
    can continue processing songs already in the database.
    """
    ydl_opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
    }

    playlist_url = _normalize_playlist_url(playlist_url)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(playlist_url, download=False)
    except Exception as e:
        print(f"[youtube] ERROR: failed to fetch playlist — {e}")
        return []

    if not info or "entries" not in info:
        print("[youtube] ERROR: no entries returned from playlist")
        return []

    entries = []
    for entry in info["entries"]:
        if entry and entry.get("id") and entry.get("title"):
            entries.append({
                "source_id": entry["id"],
                "raw_title": entry["title"],
            })

    return entries


def fetch_video_metadata(video_id: str) -> dict | None:
    """
    Fetch full metadata for a single YouTube video using yt-dlp.

    Used during two-pass parser enrichment — when a flat playlist fetch doesn't
    give enough context to identify the artist, we fetch the full video page to
    get the channel name and description.

    Returns a dict with:
      - channel_name: str — the uploader/channel name (empty string if unavailable)
      - description: str — video description (empty string if unavailable)

    Returns None on any failure so the caller can gracefully skip enrichment.
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        log.warning("fetch_video_metadata: failed for %s — %s", video_id, e)
        return None

    if not info:
        log.warning("fetch_video_metadata: no info returned for %s", video_id)
        return None

    return {
        "channel_name": info.get("uploader", ""),
        "description": info.get("description", ""),
    }


if __name__ == "__main__":
    url = os.environ.get("YOUTUBE_PLAYLIST_URL")
    if not url:
        print("ERROR: YOUTUBE_PLAYLIST_URL environment variable not set")
        raise SystemExit(1)

    print(f"Fetching playlist: {url}")
    results = fetch_playlist(url)
    print(f"\nFound {len(results)} entries:\n")
    for entry in results:
        print(f"  [{entry['source_id']}] {entry['raw_title']}")
