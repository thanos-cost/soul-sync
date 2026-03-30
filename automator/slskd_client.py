"""
slskd API wrapper — search, poll, enqueue, and count active downloads.

This module is a thin layer over the slskd-api library (v0.2.1). It handles
the async search flow (submit → wait → poll → fetch results) and provides
a clean interface for the download loop in main.py.

All functions use logging.getLogger(__name__) — configure logging in main.py
before importing this module.
"""

import os
import time
import logging

import slskd_api

log = logging.getLogger(__name__)


def make_client() -> slskd_api.SlskdClient:
    """
    Create and return an authenticated slskd API client.

    Reads connection config from environment variables:
      SLSKD_HOST    — base URL of the slskd instance (default: http://slskd:5030)
      SLSKD_API_KEY — required; raises RuntimeError if not set

    The default host uses the Docker Compose service name, which is how the
    automator container reaches slskd. Never use localhost — that resolves to
    the automator container itself, not slskd.
    """
    host = os.environ.get("SLSKD_HOST", "http://slskd:5030")
    api_key = os.environ.get("SLSKD_API_KEY")

    if not api_key:
        raise RuntimeError(
            "SLSKD_API_KEY environment variable not set. "
            "Add it to your .env file and ensure docker-compose.yml passes it to the automator."
        )

    log.info("Connecting to slskd at %s", host)
    return slskd_api.SlskdClient(host=host, api_key=api_key)


def search(client: slskd_api.SlskdClient, query: str, timeout_ms: int = 10000) -> list:
    """
    Submit a search and wait for results. Returns a list of search response dicts.

    slskd searches are asynchronous — this function handles the full cycle:
      1. Check slskd is connected to the Soulseek server
      2. Submit the search request
      3. Sleep 5 seconds (slskd needs time before its state is queryable)
      4. Poll until the search state is no longer "InProgress" or until timeout
      5. Fetch and return the complete responses
      6. Delete the search from slskd to prevent queue buildup

    Each response dict contains: username, uploadSpeed, hasFreeUploadSlot, files.
    Each file dict contains: filename, size, extension, bitRate, bitDepth, sampleRate.

    Parameters:
        client     — authenticated SlskdClient from make_client()
        query      — search string (e.g. "Radiohead Creep")
        timeout_ms — how long slskd itself runs the search (milliseconds)
                     Safety timeout adds 10 extra seconds on top of this.
    """
    log.info("Searching: %r", query)

    # Pre-flight: check that slskd is connected to the Soulseek server.
    # Without this, searches silently fail with 0 results or 409 errors.
    try:
        server_state = client.server.state()
        if not server_state.get("isLoggedIn"):
            state_str = server_state.get("state", "Unknown")
            log.error(
                "slskd is not connected to Soulseek (state: %s) — "
                "search will fail. Check if another Soulseek client "
                "is using the same account.",
                state_str,
            )
            return []
    except Exception:
        log.debug("Could not check server state — proceeding with search", exc_info=True)

    response = client.searches.search_text(
        searchText=query,
        searchTimeout=timeout_ms,
        filterResponses=True,
        maximumPeerQueueLength=50,
        minimumPeerUploadSpeed=0,
    )
    search_id = response["id"]

    # Initial wait — slskd needs time to begin collecting peer responses before
    # its state endpoint returns meaningful data.
    time.sleep(5)

    # Poll until search is complete or safety timeout is hit
    safety_limit_seconds = (timeout_ms / 1000) + 10
    start = time.time()

    while True:
        state = client.searches.state(search_id, includeResponses=False)

        if state["state"] != "InProgress":
            log.debug("Search %s completed with state: %s", search_id, state["state"])
            break

        elapsed = time.time() - start
        if elapsed > safety_limit_seconds:
            log.warning(
                "Search safety timeout hit after %.1fs for query %r — fetching partial results",
                elapsed,
                query,
            )
            break

        time.sleep(1)

    results = client.searches.search_responses(search_id)
    log.info("Search %r returned %d peer response(s)", query, len(results))

    # Delete the search from slskd immediately after fetching results.
    # slskd keeps every search in memory forever. If we don't clean up,
    # thousands accumulate, new searches get stuck in "Queued" state,
    # and slskd eventually disconnects from the Soulseek network.
    try:
        client.searches.delete(search_id)
    except Exception:
        pass  # Cleanup failure is non-fatal

    return results


def enqueue(
    client: slskd_api.SlskdClient,
    username: str,
    filename: str,
    size: int,
) -> bool:
    """
    Queue a single file for download from a specific peer.

    Both filename AND size are required by the slskd API to locate and queue
    the correct file. Omitting size causes a silent failure or HTTP 400.

    Parameters:
        client   — authenticated SlskdClient from make_client()
        username — Soulseek username of the peer sharing the file
        filename — full Windows-style path as returned by the search response
        size     — file size in bytes as returned by the search response

    Returns the raw result from the slskd API (typically True on success).
    """
    log.info("Enqueueing download: %s / %s (%d bytes)", username, filename, size)

    result = client.transfers.enqueue(
        username=username,
        files=[{"filename": filename, "size": size}],
    )
    return result


def count_active_downloads(client: slskd_api.SlskdClient) -> int:
    """
    Return the number of downloads that are currently queued or in progress.

    Downloads in terminal states (Succeeded, Completed, Errored, Cancelled)
    are not counted. This count is used to enforce the concurrent download cap
    in the download loop.

    # Response shape inferred — validate against live slskd if count seems wrong.
    The get_all_downloads() response nests files inside directories inside user
    entries. This structure is LOW confidence from research. The try/except below
    logs the raw structure on error so you can diagnose shape mismatches without
    crashing the pipeline.

    Terminal states that are NOT counted as active:
        Succeeded, Completed, Errored, Cancelled
    """
    TERMINAL_STATES = {"Succeeded", "Completed", "Errored", "Cancelled"}

    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)

        active = 0
        for user_entry in all_downloads:
            for directory in user_entry.get("directories", []):
                for file in directory.get("files", []):
                    state = file.get("state", "")
                    if state not in TERMINAL_STATES:
                        active += 1

        log.debug("Active downloads (non-terminal): %d", active)
        return active

    except Exception as exc:
        log.error(
            "count_active_downloads() failed — raw response logged below. "
            "The response shape may differ from what was expected. Error: %s",
            exc,
        )
        try:
            # Log the raw structure so we can diagnose the actual shape
            raw = client.transfers.get_all_downloads(includeRemoved=False)
            log.error("Raw get_all_downloads() response: %r", raw)
        except Exception:
            pass

        # Return 0 so the pipeline doesn't crash — callers will be conservative
        return 0


def remove_succeeded_downloads(client: slskd_api.SlskdClient) -> int:
    """
    Remove only succeeded transfers from slskd's list, keeping failures visible.

    Failed downloads stay in slskd so the user can see them on the Downloads
    page and retry via the dashboard. Only transfers in a "Succeeded" state
    are removed.

    Returns the number of transfers removed.
    """
    removed = 0
    try:
        all_downloads = client.transfers.get_all_downloads(includeRemoved=False)

        for user_entry in all_downloads:
            username = user_entry.get("username", "")
            for directory in user_entry.get("directories", []):
                for f in directory.get("files", []):
                    state = (f.get("state") or "").lower()
                    if "succeeded" in state:
                        file_id = f.get("id", "")
                        if file_id and username:
                            try:
                                client.transfers.cancel_download(username, file_id, remove=True)
                                removed += 1
                            except Exception:
                                pass  # Individual removal failure is non-fatal

        if removed:
            log.info("Removed %d succeeded download(s) from slskd — failed ones kept for retry", removed)
    except Exception as exc:
        log.warning("Failed to clean up succeeded downloads: %s", exc)

    return removed


if __name__ == "__main__":
    """
    Manual test: create a client and print connection status.

    Run from inside the automator container:
        docker compose exec automator python slskd_client.py

    Requires SLSKD_HOST and SLSKD_API_KEY to be set (via .env or environment).
    """
    import sys
    from dotenv import load_dotenv

    load_dotenv()

    print("slskd_client.py — manual connection test")
    print("-" * 40)

    try:
        client = make_client()
        print(f"Client created for host: {os.environ.get('SLSKD_HOST', 'http://slskd:5030')}")

        # Try a lightweight API call to confirm connectivity
        active = count_active_downloads(client)
        print(f"Active downloads: {active}")
        print("Connection: OK")

    except RuntimeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        print("Is slskd running? Is SLSKD_API_KEY correct?", file=sys.stderr)
        sys.exit(1)
