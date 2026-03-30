"""
sources/spotify.py — SpotifyAdapter: fetches playlist tracks via the Spotify Web API.

Uses the spotipy library with Authorization Code flow. Spotify now requires
user authentication even for public playlists (changed late 2024), so we use
a one-time browser login to get a refresh token.

Setup (one-time):
    docker compose exec automator python -m sources.spotify_auth

This opens a URL you paste into your browser, log in, and paste the redirect
URL back. The token is saved to /data/.spotify_cache and auto-refreshes.

Each source instance holds a playlist URL. The adapter extracts the playlist ID,
fetches all tracks (handling pagination), and returns SongEntry dicts with
artist + title already cleanly separated (Spotify provides structured metadata).

Config dict from sources table:
    {"playlist_url": "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"}
"""

import logging
import os
import re

log = logging.getLogger(__name__)

# Spotify playlist IDs are 22-character base62 strings
_PLAYLIST_ID_RE = re.compile(r'playlist[/:]([A-Za-z0-9]{22})')

# Token cache lives on the persistent /data volume so it survives container restarts
_CACHE_PATH = "/data/.spotify_cache"

# Redirect URI for the OAuth flow — localhost is fine since the user just copies
# the URL back into the terminal. No web server needs to be running.
_REDIRECT_URI = "http://127.0.0.1:8888/callback"

# Minimal scope — we only need to read playlists
_SCOPE = "playlist-read-private playlist-read-collaborative"


def _extract_playlist_id(url_or_id: str) -> str | None:
    """
    Extract a Spotify playlist ID from a URL or URI.

    Accepts:
      - https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M
      - https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M?si=xxxx
      - spotify:playlist:37i9dQZF1DXcBWIGoYBM5M
      - 37i9dQZF1DXcBWIGoYBM5M  (bare ID)

    Returns the 22-char playlist ID, or None if not parseable.
    """
    url_or_id = url_or_id.strip()

    # Bare ID
    if re.fullmatch(r'[A-Za-z0-9]{22}', url_or_id):
        return url_or_id

    m = _PLAYLIST_ID_RE.search(url_or_id)
    return m.group(1) if m else None


def _make_spotify_client():
    """
    Create an authenticated Spotify client using the Authorization Code flow.

    Returns a spotipy.Spotify instance, or None if credentials are missing
    or the token cache doesn't exist (user hasn't run the auth setup).
    """
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        log.error("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET env vars "
                  "are required. Set them in your .env file.")
        return None

    try:
        import spotipy
        from spotipy.oauth2 import SpotifyOAuth
    except ImportError:
        log.error("spotipy is not installed. Add it to requirements.txt.")
        return None

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=_REDIRECT_URI,
        scope=_SCOPE,
        cache_path=_CACHE_PATH,
        open_browser=False,
    )

    # Check if we have a cached token
    token_info = auth_manager.cache_handler.get_cached_token()
    if not token_info:
        log.error(
            "No Spotify token found. Run the one-time setup:\n"
            "  docker compose exec automator python -m sources.spotify_auth"
        )
        return None

    return spotipy.Spotify(auth_manager=auth_manager)


class SpotifyAdapter:
    """
    Source adapter that fetches tracks from a Spotify playlist.

    Uses Authorization Code flow via spotipy. Supports both public and
    private playlists. Requires a one-time browser login to set up the
    token (see module docstring).

    Spotify provides structured metadata (artist, track name, album) so no
    title parsing or Haiku enrichment is needed — a big advantage over
    YouTube where titles are freeform text.
    """

    def __init__(self, config: dict):
        """
        Parameters:
            config — dict parsed from sources table config_json, e.g.:
                     {"playlist_url": "https://open.spotify.com/playlist/..."}
        """
        self._playlist_url = config.get("playlist_url", "")
        self._playlist_id = _extract_playlist_id(self._playlist_url)

    @property
    def source_name(self) -> str:
        """Returns 'spotify:<playlist_id>' for logging."""
        return f"spotify:{self._playlist_id or 'unknown'}"

    def fetch_songs(self) -> list[dict]:
        """
        Fetch all tracks from the Spotify playlist.

        Returns a list of SongEntry dicts. Handles Spotify's pagination
        (100 tracks per page) transparently.

        Returns [] on any failure — errors are logged, not raised.
        """
        if not self._playlist_id:
            log.error("[%s] Could not extract playlist ID from: %s",
                      self.source_name, self._playlist_url)
            return []

        sp = _make_spotify_client()
        if sp is None:
            return []

        log.info("[%s] Fetching playlist: %s", self.source_name, self._playlist_url)

        try:
            songs = []
            # Spotify paginates at 100 items per request
            # No fields filter — Spotify changed their response structure
            # (track data moved from "track" to "item" key) and the old
            # fields filter silently strips the data. The extra payload
            # is negligible for playlists.
            results = sp.playlist_items(
                self._playlist_id,
                additional_types=["track"],
            )

            while results:
                for item in results.get("items", []):
                    # Spotify changed their API: track data moved from "track"
                    # to "item" key. The old "track" key is now a boolean.
                    # Support both formats for forward/backward compatibility.
                    track = item.get("track")
                    if isinstance(track, bool) or track is None:
                        track = item.get("item")
                    if not track or not track.get("id"):
                        continue  # Skip local files or unavailable tracks

                    track_id = track["id"]
                    title = track.get("name", "").strip()
                    # Join multiple artists: "Artist1, Artist2"
                    artists = track.get("artists", [])
                    artist = ", ".join(a["name"] for a in artists if a.get("name"))

                    if not title:
                        continue

                    songs.append({
                        "source_id":   f"sp:{track_id}",
                        "source_type": "spotify",
                        "search_mode": "track",
                        "raw_title":   f"{artist} - {title}" if artist else title,
                        "artist":      artist,
                        "title":       title,
                        "version":     "",
                    })

                # Follow pagination
                if results.get("next"):
                    results = sp.next(results)
                else:
                    results = None

        except Exception as exc:
            log.error("[%s] Spotify API error: %s", self.source_name, exc)
            return []

        log.info("[%s] fetch_songs complete — %d songs loaded",
                 self.source_name, len(songs))
        return songs
