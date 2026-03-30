"""
Lightweight slskd API client for the dashboard.

Talks directly to the slskd REST API over HTTP — no slskd_api library needed.
This keeps the dashboard's dependencies minimal (just Flask + requests).
"""

import os
import time
import logging

import requests

log = logging.getLogger(__name__)


def _base_url():
    return os.environ.get("SLSKD_HOST", "http://slskd:5030")


def _api_key():
    return os.environ.get("SLSKD_API_KEY", "")


def _headers():
    return {"X-API-Key": _api_key(), "Content-Type": "application/json"}


def is_configured():
    """Return True if slskd connection details are set."""
    return bool(_api_key())


def server_status():
    """
    Check if slskd is connected to the Soulseek server.

    Returns a dict with 'isConnected', 'isLoggedIn', and 'state' keys.
    Returns None if the API is unreachable.
    """
    try:
        resp = requests.get(
            f"{_base_url()}/api/v0/server",
            headers=_headers(),
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def search(query, timeout_ms=15000):
    """
    Submit a search to slskd and wait for results.

    Returns a list of response dicts, each with:
      username, uploadSpeed, hasFreeUploadSlot, files[]
    Each file has: filename, size, extension, bitRate, bitDepth, sampleRate
    """
    base = _base_url()
    headers = _headers()

    # Pre-flight: check slskd is connected. Without this, searches
    # silently fail with 0 results or throw 409 errors.
    status = server_status()
    if status and not status.get("isLoggedIn"):
        state_str = status.get("state", "Unknown")
        raise ConnectionError(
            f"slskd is not connected to Soulseek (state: {state_str}). "
            "Check if another Soulseek client is using the same account."
        )

    # Submit search
    resp = requests.post(
        f"{base}/api/v0/searches",
        headers=headers,
        json={
            "searchText": query,
            "searchTimeout": timeout_ms,
            "filterResponses": True,
            "maximumPeerQueueLength": 50,
            "minimumPeerUploadSpeed": 0,
        },
    )
    resp.raise_for_status()
    search_data = resp.json()
    search_id = search_data["id"]

    # Wait for results — slskd needs time to collect peer responses.
    # Obscure songs need longer; 5s gives slower peers time to respond.
    time.sleep(5)

    safety_limit = (timeout_ms / 1000) + 10
    start = time.time()

    while True:
        state_resp = requests.get(
            f"{base}/api/v0/searches/{search_id}",
            headers=headers,
            params={"includeResponses": False},
        )
        state_resp.raise_for_status()
        state = state_resp.json()

        if state.get("state") != "InProgress":
            break

        if time.time() - start > safety_limit:
            log.warning("Search timeout for %r — fetching partial results", query)
            break

        time.sleep(1)

    # Fetch responses
    results_resp = requests.get(
        f"{base}/api/v0/searches/{search_id}/responses",
        headers=headers,
    )
    results_resp.raise_for_status()
    results = results_resp.json()

    # Delete the search from slskd immediately after fetching results.
    # slskd keeps every search in memory forever — without cleanup,
    # thousands accumulate, blocking new searches and eventually
    # disconnecting slskd from the Soulseek network.
    try:
        requests.delete(
            f"{base}/api/v0/searches/{search_id}",
            headers=headers,
            timeout=5,
        )
    except Exception:
        pass  # Cleanup failure is non-fatal

    return results


def get_downloads():
    """Fetch all current downloads from slskd."""
    base = _base_url()
    headers = _headers()

    resp = requests.get(
        f"{base}/api/v0/transfers/downloads",
        headers=headers,
    )
    resp.raise_for_status()
    return resp.json()


def enqueue(username, filename, size):
    """
    Queue a file for download from a Soulseek peer.

    Both filename and size are required by the slskd API.
    """
    base = _base_url()
    headers = _headers()

    resp = requests.post(
        f"{base}/api/v0/transfers/downloads/{username}",
        headers=headers,
        json=[{"filename": filename, "size": size}],
    )
    resp.raise_for_status()
    return True


def enqueue_many(username, files):
    """
    Batch-enqueue multiple files from the same peer.

    files: list of dicts with 'filename' and 'size' keys.
    Uses the same endpoint as enqueue — it already accepts a list.
    """
    base = _base_url()
    headers = _headers()

    payload = [{"filename": f["filename"], "size": f["size"]} for f in files]

    resp = requests.post(
        f"{base}/api/v0/transfers/downloads/{username}",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return True


def browse_folder(username, directory):
    """
    Browse a peer's shared directory to get the full file listing.

    Returns a list of file dicts: [{filename, size, extension}, ...]
    Uses slskd's browse API — a single GET that fetches the peer's entire
    share tree (can take 30-60s for large libraries), then filters to the
    requested directory.
    """
    base = _base_url()
    headers = _headers()

    resp = requests.get(
        f"{base}/api/v0/users/{username}/browse",
        headers=headers,
        timeout=120,
    )
    resp.raise_for_status()
    browse_data = resp.json()

    directories = browse_data.get("directories", [])
    if not directories:
        log.warning("Browse returned no directories for user %s", username)
        return []

    # Normalize the target directory for comparison
    target = directory.rstrip("\\").lower()

    # Search through the directory tree for our target
    for d in directories:
        dir_name = (d.get("name") or "").rstrip("\\").lower()
        if dir_name == target:
            files = []
            for f in d.get("files", []):
                fname = f.get("filename", "")
                ext = (f.get("extension") or "").lower()
                if not ext and "." in fname:
                    ext = fname.rsplit(".", 1)[-1].lower()
                files.append({
                    "filename": directory.rstrip("\\") + "\\" + fname,
                    "size": f.get("size", 0),
                    "extension": ext,
                })
            return files

    log.warning("Directory %r not found for user %s", directory, username)
    return []
