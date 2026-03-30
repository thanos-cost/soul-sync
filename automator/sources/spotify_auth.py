"""
One-time Spotify authorization setup.

Run inside the automator container:
    docker compose exec -it automator python -m sources.spotify_auth

This creates a token cache at /data/.spotify_cache that the SpotifyAdapter
reads on every pipeline run. The token auto-refreshes, so you only need
to do this once.
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv("/config/.env")


def main():
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("ERROR: SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in .env")
        sys.exit(1)

    from spotipy.oauth2 import SpotifyOAuth

    from sources.spotify import _CACHE_PATH, _REDIRECT_URI, _SCOPE

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=_REDIRECT_URI,
        scope=_SCOPE,
        cache_path=_CACHE_PATH,
        open_browser=False,
    )

    # Check if already authenticated
    token_info = auth_manager.cache_handler.get_cached_token()
    if token_info:
        print("Already authenticated! Token cache exists at", _CACHE_PATH)
        print("To re-authenticate, delete the cache file and run again:")
        print(f"  docker compose exec automator rm {_CACHE_PATH}")
        return

    # Get the authorization URL
    auth_url = auth_manager.get_authorize_url()

    print()
    print("=" * 60)
    print("  Spotify Authorization Setup")
    print("=" * 60)
    print()
    print("BEFORE YOU START — check these in your Spotify Developer")
    print("Dashboard (https://developer.spotify.com/dashboard):")
    print()
    print("  a) Your app's Redirect URI must be exactly:")
    print(f"     {_REDIRECT_URI}")
    print()
    print("  b) Your Spotify email must be added under")
    print("     'User Management' (required for dev mode apps)")
    print()
    print("-" * 60)
    print()
    print("1. Open this URL in your browser:")
    print()
    print(f"   {auth_url}")
    print()
    print("2. Log in to Spotify and click 'Agree'")
    print()
    print("3. You'll be redirected to a page that says")
    print("   'This site can't be reached' — that's normal!")
    print(f"   The URL will start with: {_REDIRECT_URI}?code=...")
    print()
    print("4. Copy the ENTIRE redirect URL and paste it below:")
    print()

    redirect_url = input("Paste the redirect URL here: ").strip()

    if not redirect_url:
        print("No URL provided. Aborting.")
        sys.exit(1)

    if "error=" in redirect_url:
        print()
        print("ERROR: Spotify returned an error in the redirect URL.")
        print("Common fixes:")
        print("  - Add your email under 'User Management' in the Spotify app")
        print(f"  - Ensure the Redirect URI is exactly: {_REDIRECT_URI}")
        print("  - Wait a minute after saving changes — Spotify can be slow")
        sys.exit(1)

    try:
        code = auth_manager.parse_response_code(redirect_url)
        auth_manager.get_access_token(code)
        print()
        print("Success! Token saved to", _CACHE_PATH)
        print("Your Spotify playlists will be fetched on the next pipeline run.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
