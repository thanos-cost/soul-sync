"""
sources/discogs.py — DiscogsAdapter: fetches collection or wantlist releases via the Discogs API.

Uses python3-discogs-client with a personal user token. The token is read from
the DISCOGS_TOKEN environment variable, set once per installation in .env.

Each source instance holds a Discogs username and list type (collection or wantlist).
Discogs is album-oriented — releases contain artist + album title, so the adapter
returns SongEntry dicts with search_mode="album" by default.

Config dict from sources table:
    {"username": "my_discogs_user", "list_type": "collection"}
    {"username": "my_discogs_user", "list_type": "wantlist"}
"""

import logging
import os

log = logging.getLogger(__name__)


class DiscogsAdapter:
    """
    Source adapter that fetches releases from a Discogs collection or wantlist.

    Uses the Discogs API v2 via python3-discogs-client. Reads basic_information
    directly from the paginated response to avoid per-release API calls (staying
    well within the 60 req/min rate limit).

    Public collections/wantlists are accessible with any valid token. Private
    ones require the collection owner's token.
    """

    def __init__(self, config: dict):
        """
        Parameters:
            config — dict parsed from sources table config_json, e.g.:
                     {"username": "some_user", "list_type": "collection"}
        """
        self._username = config.get("username", "").strip()
        self._list_type = config.get("list_type", "collection").strip().lower()

    @property
    def source_name(self) -> str:
        """Returns 'discogs:<username>/<list_type>' for logging."""
        return f"discogs:{self._username}/{self._list_type}"

    def fetch_songs(self) -> list[dict]:
        """
        Fetch all releases from the Discogs collection or wantlist.

        Returns a list of SongEntry dicts with search_mode="album".
        Reads basic_information inline from the API response to avoid
        extra per-release API calls.

        Returns [] on any failure — errors are logged, not raised.
        """
        if not self._username:
            log.error("[%s] No username configured", self.source_name)
            return []

        if self._list_type not in ("collection", "wantlist"):
            log.error("[%s] Invalid list_type %r — must be 'collection' or 'wantlist'",
                      self.source_name, self._list_type)
            return []

        token = os.environ.get("DISCOGS_TOKEN", "").strip()
        if not token:
            log.error("[%s] DISCOGS_TOKEN env var is required. Generate one at "
                      "https://www.discogs.com/settings/developers", self.source_name)
            return []

        try:
            import discogs_client
        except ImportError:
            log.error("[%s] python3-discogs-client is not installed. "
                      "Add it to requirements.txt.", self.source_name)
            return []

        log.info("[%s] Fetching %s for user: %s",
                 self.source_name, self._list_type, self._username)

        try:
            d = discogs_client.Client("SoulSync/1.0", user_token=token)
            user = d.user(self._username)

            songs = []

            if self._list_type == "collection":
                # Folder 0 = "All" (contains every release across all folders)
                items = user.collection_folders[0].releases
            else:
                items = user.wantlist

            for item in items:
                # Read from basic_information directly to avoid extra API calls.
                # The library's lazy loading would hit the API per-release otherwise.
                info = item.data.get("basic_information", {})

                release_id = str(info.get("id", "") or item.data.get("id", ""))
                title = info.get("title", "").strip()
                artists = info.get("artists", [])
                artist = ", ".join(
                    a.get("name", "") for a in artists if a.get("name")
                )

                if not release_id or not title:
                    continue

                songs.append({
                    "source_id":   f"dc:{release_id}",
                    "source_type": "discogs",
                    "search_mode": "album",
                    "raw_title":   f"{artist} - {title}" if artist else title,
                    "artist":      artist,
                    "title":       title,
                    "version":     "",
                })

        except Exception as exc:
            log.error("[%s] Discogs API error: %s", self.source_name, exc)
            return []

        log.info("[%s] fetch_songs complete — %d releases loaded",
                 self.source_name, len(songs))
        return songs
