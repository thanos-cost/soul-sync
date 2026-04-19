"""
scheduler.py — Daemon entry point for the automator container.

Converts the run-and-exit pipeline into a long-running daemon that:
  - Runs the pipeline immediately on startup
  - Waits until the configured daily run time (default: 00:00 local)
  - Accepts SIGUSR1 to trigger an immediate run without waiting
  - Delivers completed downloads to destination folder after every run
  - Retries once automatically after a pipeline crash before sleeping
  - Logs each run to pipeline_runs table for the Activity dashboard
  - Reads scheduled_time dynamically from settings table each loop

Usage (via Docker CMD):
    python scheduler.py

Manual trigger from host:
    docker kill --signal=USR1 automator

The exec-form CMD ensures Python is PID 1 inside the container, which is
required for SIGUSR1 to reach the process directly.
"""

import os
import signal
import logging
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from main import setup_logging, run_pipeline
from state import init_db, log_run_start, log_run_finish, log_run_failed, get_setting, set_setting
import slskd_client

log = logging.getLogger("scheduler")


# ---------------------------------------------------------------------------
# Search cleanup
# ---------------------------------------------------------------------------

def _cleanup_searches(client):
    """
    Delete ALL searches from slskd — safety net after each pipeline cycle.

    Each individual search() call now deletes its own search immediately after
    fetching results. This bulk cleanup catches anything that slipped through
    (e.g. searches from interrupted runs, dashboard searches, etc.).

    slskd keeps every search in memory forever. Without cleanup, thousands
    accumulate, new searches get stuck in "Queued" state, and slskd eventually
    disconnects from the Soulseek network.
    """
    import requests
    base = os.environ.get("SLSKD_HOST", "http://slskd:5030")
    headers = {"X-API-Key": os.environ.get("SLSKD_API_KEY", ""), "Content-Type": "application/json"}

    try:
        r = requests.get(f"{base}/api/v0/searches", headers=headers, timeout=10)
        r.raise_for_status()
        searches = r.json()
    except Exception:
        return

    if not searches:
        return

    deleted = 0
    for s in searches:
        try:
            requests.delete(f"{base}/api/v0/searches/{s['id']}", headers=headers, timeout=5)
            deleted += 1
        except Exception:
            pass

    if deleted:
        log.info("Search cleanup: deleted %d search(es) from slskd", deleted)


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------

_run_now: bool = False


def _handle_sigusr1(signum, frame):
    """
    Set the _run_now flag when SIGUSR1 is received.

    The sleep loop checks this flag every 10 seconds and breaks early,
    triggering an immediate pipeline run without waiting for the interval.
    """
    global _run_now
    _run_now = True
    log.info("SIGUSR1 received — will start next run immediately")


# ---------------------------------------------------------------------------
# Main daemon loop
# ---------------------------------------------------------------------------

def main():
    load_dotenv()
    setup_logging()

    global _run_now

    # Read configuration from environment
    dest_dir = os.environ.get("DEST_DIR")

    log.info(
        "Scheduler starting — dest_dir=%s",
        dest_dir or "(not set)",
    )

    # Register SIGUSR1 handler for manual trigger
    signal.signal(signal.SIGUSR1, _handle_sigusr1)
    log.info("SIGUSR1 handler registered — trigger with: docker kill --signal=USR1 automator")

    # Set up DB connection and slskd client once — reused across all runs
    conn = init_db()

    # Clean up stale "running" rows left behind by prior container crashes.
    # When the container is killed mid-run, log_run_finish() never fires, leaving
    # pipeline_runs rows permanently stuck as "running". Mark them as "crashed".
    try:
        stale = conn.execute(
            "UPDATE pipeline_runs SET status = 'failed', finished_at = ? "
            "WHERE status = 'running'",
            (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),),
        )
        if stale.rowcount > 0:
            conn.commit()
            log.info("Cleaned up %d stale 'running' pipeline_runs from prior crash", stale.rowcount)
    except Exception:
        log.debug("Could not clean up stale runs — continuing", exc_info=True)

    # Reset songs stuck in "searching" — these were mid-search when the container
    # was killed. Move them back to "new" so the next run picks them up.
    try:
        stuck = conn.execute(
            "UPDATE songs SET status = 'new' WHERE status = 'searching'"
        )
        if stuck.rowcount > 0:
            conn.commit()
            log.info("Reset %d song(s) stuck in 'searching' back to 'new'", stuck.rowcount)
    except Exception:
        log.debug("Could not reset stuck searches — continuing", exc_info=True)

    # Resume run_number from where the DB left off (not reset to 0 each restart)
    try:
        row = conn.execute("SELECT MAX(run_number) as max_run FROM pipeline_runs").fetchone()
        run_number = (row["max_run"] or 0)
    except Exception:
        run_number = 0

    api_key = os.environ.get("SLSKD_API_KEY")
    client = None
    if api_key:
        try:
            client = slskd_client.make_client()
        except Exception:
            log.warning("Could not create slskd client — polling and delivery will be skipped", exc_info=True)
    else:
        log.warning("SLSKD_API_KEY not set — polling and delivery will be skipped")

    while True:
        _run_now = False
        run_number += 1

        # Read scheduled time from DB each loop — allows the dashboard to change it
        scheduled_time = get_setting(conn, "scheduled_time", "00:00")

        log.info("=== Starting Run #%d (scheduled=%s) ===", run_number, scheduled_time)

        # Log run start to pipeline_runs table
        run_id = log_run_start(conn, run_number)

        # Run the pipeline with one automatic retry on crash
        result = {"new_songs": 0, "downloads_queued": 0}
        run_failed = False
        set_setting(conn, "pipeline_stage", "Starting pipeline...")
        try:
            result = run_pipeline()
        except Exception:
            log.error("Pipeline run #%d failed — retrying once", run_number, exc_info=True)
            try:
                result = run_pipeline()
            except Exception:
                log.error(
                    "Pipeline run #%d failed on retry — moving to sleep phase",
                    run_number, exc_info=True,
                )
                run_failed = True

        # Track delivery counts for the run stats
        delivered_count = 0

        # Poll for completed downloads and record local paths
        if client is not None:
            set_setting(conn, "pipeline_stage", "Checking downloads...")
            try:
                from poller import poll_and_update
                completed = poll_and_update(client, conn)
                log.info("Poller: %d download(s) marked complete this cycle", completed)
            except Exception:
                log.error("Poller failed — continuing (poller failure is non-fatal)", exc_info=True)

            # Recover downloaded songs with NULL local_path (stuck in staging)
            try:
                from poller import recover_missing_local_paths
                recovered = recover_missing_local_paths(conn)
                if recovered:
                    log.info("Recovery: fixed local_path for %d song(s)", recovered)
            except Exception:
                log.error("Local path recovery failed — continuing (non-fatal)", exc_info=True)

            # Recover queued songs whose files already exist on disk
            try:
                from poller import recover_queued_with_files
                recovered_q = recover_queued_with_files(conn)
                if recovered_q:
                    log.info("Recovery: promoted %d queued song(s) to downloaded", recovered_q)
            except Exception:
                log.error("Queued recovery failed — continuing (non-fatal)", exc_info=True)

            # Check for stalled downloads and queue alternative sources
            try:
                from poller import check_stalled_downloads
                stalled = check_stalled_downloads(client, conn, log)
                if stalled:
                    log.info("Stall check: %d download(s) handled this cycle", stalled)
            except Exception:
                log.error("Stall check failed — continuing (non-fatal)", exc_info=True)

            # Poll for completed album downloads
            try:
                from poller import poll_album_downloads
                album_completed = poll_album_downloads(client, conn)
                if album_completed:
                    log.info("Album poller: %d album(s) completed this cycle", album_completed)
            except Exception:
                log.error("Album poller failed — continuing (non-fatal)", exc_info=True)

            # Check for stalled album downloads (7-day threshold)
            try:
                from poller import check_stalled_albums
                stalled_albums = check_stalled_albums(client, conn, log)
                if stalled_albums:
                    log.info("Album stall check: %d album(s) handled this cycle", stalled_albums)
            except Exception:
                log.error("Album stall check failed — continuing (non-fatal)", exc_info=True)

            # Clear succeeded transfers from slskd — keeps the transfer list clean
            # while preserving failed ones so the user can see and retry them.
            # Must run AFTER polling so we don't lose track of completions.
            try:
                slskd_client.remove_succeeded_downloads(client)
            except Exception:
                log.debug("Failed to clear succeeded downloads — non-fatal", exc_info=True)

            # Cancel untracked downloads — removes slskd transfers not in our DB.
            # Must run AFTER remove_succeeded (which clears tracked succeeded ones)
            # so we only cancel truly untracked transfers.
            try:
                from poller import cancel_untracked_downloads
                cancel_untracked_downloads(client, conn)
            except Exception:
                log.debug("Untracked download cleanup failed — non-fatal", exc_info=True)

        # Delivery batch — move completed downloads to destination every run
        if dest_dir:
            set_setting(conn, "pipeline_stage", "Delivering files...")
            try:
                from delivery import run_delivery_batch
                delivered = run_delivery_batch(conn, dest_dir)
                if delivered:
                    log.info("Delivery batch: %d file(s) delivered", delivered)
                    delivered_count += delivered
            except Exception:
                log.error("Delivery batch failed — will retry next run", exc_info=True)
            # Album delivery batch — move completed album folders to destination
            try:
                from delivery import run_album_delivery_batch
                albums_delivered = run_album_delivery_batch(conn, dest_dir)
                if albums_delivered:
                    log.info("Album delivery: %d album(s) delivered", albums_delivered)
                    delivered_count += albums_delivered
            except Exception:
                log.error("Album delivery failed — will retry next run", exc_info=True)
        else:
            log.debug("DEST_DIR not set - skipping delivery")

        # Clean up orphan files from staging — deletes files that don't match
        # any DB record (e.g. duplicate-suffix copies from stall retries,
        # or files from untracked downloads).
        # Runs AFTER delivery so we never delete a file that's about to be moved.
        try:
            from poller import cleanup_orphan_files
            cleanup_orphan_files(conn)
        except Exception:
            log.debug("Orphan file cleanup failed — non-fatal", exc_info=True)

        # Clean up completed searches from slskd — prevents the search queue
        # from growing indefinitely (thousands of stale searches can block new
        # ones and eventually disconnect slskd from the Soulseek network).
        if client is not None:
            try:
                _cleanup_searches(client)
            except Exception:
                log.debug("Search cleanup failed — non-fatal", exc_info=True)

        # Log run completion or failure
        if run_failed:
            log_run_failed(conn, run_id)
        else:
            log_run_finish(conn, run_id, {
                "new_songs": result.get("new_songs", 0),
                "downloads_queued": result.get("downloads_queued", 0),
                "delivered": delivered_count,
                "headline": result.get("headline"),
                "headline_level": result.get("headline_level", "info"),
            })

        # Clear pipeline stage — run is done, back to idle
        set_setting(conn, "pipeline_stage", "")

        # Calculate next run time — next occurrence of scheduled_time
        try:
            hour, minute = (int(x) for x in scheduled_time.split(":"))
        except (ValueError, AttributeError):
            hour, minute = 0, 0

        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            # Scheduled time already passed today — wait until tomorrow
            next_run += __import__("datetime").timedelta(days=1)

        next_run_iso = next_run.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        set_setting(conn, "next_run_at", next_run_iso)

        wait_seconds = (next_run - now).total_seconds()
        next_run_str = next_run.strftime("%H:%M")

        log.info(
            "Run #%d complete: %d new songs found, %d downloads queued, %d delivered, next run at %s (in %.1fh)",
            run_number,
            result.get("new_songs", 0),
            result.get("downloads_queued", 0),
            delivered_count,
            next_run_str,
            wait_seconds / 3600,
        )

        # Sleep until next_run by wall-clock time, not accumulated time.asleep().
        # If the host (e.g. macOS laptop) sleeps, the Docker VM pauses and an
        # elapsed-seconds counter would freeze — causing the scheduler to drift
        # days behind the schedule. Comparing datetime.now() to next_run means
        # we fire immediately on wake if the target time has already passed.
        while datetime.now() < next_run:
            time.sleep(10)
            if _run_now:
                break

            # Check DB trigger flags (set by the dashboard)
            try:
                tp = get_setting(conn, "trigger_pipeline", "")
                if tp:
                    set_setting(conn, "trigger_pipeline", "")
                    _run_now = True
                    log.info("Dashboard trigger_pipeline flag detected — will start run immediately")
                    break

                td = get_setting(conn, "trigger_delivery", "")
                if td:
                    set_setting(conn, "trigger_delivery", "")
                    log.info("Dashboard trigger_delivery flag detected — running delivery batch inline")
                    if dest_dir:
                        try:
                            from delivery import run_delivery_batch, run_album_delivery_batch
                            d = run_delivery_batch(conn, dest_dir)
                            a = run_album_delivery_batch(conn, dest_dir)
                            log.info("Triggered delivery complete: %d track(s), %d album(s)", d, a)
                        except Exception:
                            log.error("Triggered delivery failed", exc_info=True)
                    # Don't break — resume sleeping after delivery
            except Exception:
                log.debug("Error checking trigger flags — continuing sleep", exc_info=True)

        if _run_now:
            log.info("Manual trigger — starting run immediately")


if __name__ == "__main__":
    main()
