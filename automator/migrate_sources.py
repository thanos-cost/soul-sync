"""
migrate_sources.py — One-time migration from sources.yaml to the sources table.

Reads the existing sources.yaml and inserts each entry as a row in the sources
table in songs.db. Once entries exist in the table, the pipeline reads from
there instead of the YAML file.

The migration is idempotent — running it again when rows already exist prints
a message and exits without inserting duplicates.

Usage:
    python migrate_sources.py                  # uses defaults
    SOURCES_CONFIG=/path/to/sources.yaml python migrate_sources.py

Can also be called programmatically from load_sources() as an auto-migration:
    from migrate_sources import migrate_yaml_to_db
    migrated = migrate_yaml_to_db(conn, config_path)
"""

import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone

import yaml

log = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = "/config/sources.yaml"


def _name_for_entry(entry: dict) -> str:
    """
    Derive a human-readable name for a source entry.

    Used as the 'name' column in the sources table — shown as the tab label
    in the dashboard.
    """
    source_type = entry.get("type", "").strip().lower()

    if source_type == "youtube":
        url = entry.get("playlist_url", "")
        # Extract playlist ID from URL for a friendlier label
        # e.g. "https://www.youtube.com/playlist?list=PLxxx" -> "YouTube: PLxxx"
        if "list=" in url:
            playlist_id = url.split("list=")[-1].split("&")[0]
            return f"YouTube: {playlist_id}"
        return "YouTube Playlist"

    elif source_type == "textfile":
        path = entry.get("path", "")
        mode = entry.get("mode", "track")
        basename = os.path.basename(path) if path else "tracks.txt"
        if mode == "album":
            return f"Albums: {basename}"
        return f"Tracks: {basename}"

    return f"Source ({source_type})"


def _config_json_for_entry(entry: dict) -> str:
    """
    Build the config_json blob for a source entry.

    Only includes type-specific fields — the 'type' key itself is stored
    separately in the sources.type column, not in config_json.

    Field names match sources.yaml exactly so existing adapters don't
    need any changes when reading from config_json.
    """
    source_type = entry.get("type", "").strip().lower()

    if source_type == "youtube":
        config = {"playlist_url": entry.get("playlist_url", "")}

    elif source_type == "textfile":
        config: dict = {"path": entry.get("path", "")}
        if "mode" in entry:
            config["mode"] = entry["mode"]

    else:
        # Unknown type — store all non-type fields as-is
        config = {k: v for k, v in entry.items() if k != "type"}

    return json.dumps(config)


def migrate_yaml_to_db(
    conn: sqlite3.Connection,
    config_path: str | None = None,
    silent: bool = False,
) -> int:
    """
    Migrate sources.yaml entries into the sources table.

    Idempotent: if the sources table already has rows, returns 0 immediately
    without inserting any duplicates.

    Parameters:
        conn        — open sqlite3.Connection with the sources table created
        config_path — path to sources.yaml. Defaults to SOURCES_CONFIG env var
                      or /config/sources.yaml.
        silent      — if True, suppress print() output (used for auto-migration)

    Returns the number of sources inserted (0 if already migrated).
    """
    resolved_path = config_path or os.environ.get("SOURCES_CONFIG") or _DEFAULT_CONFIG_PATH

    # Check if already migrated
    existing_count = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    if existing_count > 0:
        if not silent:
            print(f"Sources already migrated ({existing_count} row(s) in sources table). Nothing to do.")
        return 0

    # Read sources.yaml
    try:
        with open(resolved_path) as f:
            yaml_config = yaml.safe_load(f)
    except FileNotFoundError:
        if not silent:
            print(f"sources.yaml not found at {resolved_path} — nothing to migrate.")
        log.warning("migrate_sources: sources.yaml not found at %s", resolved_path)
        return 0
    except Exception as exc:
        log.error("migrate_sources: failed to read %s: %s", resolved_path, exc)
        if not silent:
            print(f"Error reading sources.yaml: {exc}")
        return 0

    source_list = yaml_config.get("sources") if yaml_config else None
    if not source_list:
        if not silent:
            print(f"sources.yaml at {resolved_path} has no 'sources' list — nothing to migrate.")
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted = 0

    for entry in source_list:
        source_type = entry.get("type", "").strip().lower()
        if not source_type:
            log.warning("migrate_sources: skipping entry with no type: %r", entry)
            continue

        name = _name_for_entry(entry)
        config_json = _config_json_for_entry(entry)

        conn.execute("""
            INSERT INTO sources (type, name, config_json, enabled, date_added)
            VALUES (?, ?, ?, 1, ?)
        """, (source_type, name, config_json, now))
        inserted += 1

        if not silent:
            print(f"  Migrated: [{source_type}] {name}")

    conn.commit()

    if not silent:
        print(f"\nMigration complete — {inserted} source(s) inserted into sources table.")

    log.info("migrate_sources: inserted %d source(s) from %s", inserted, resolved_path)
    return inserted


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s: %(message)s")

    # Import state for init_db() — handles /data directory creation
    sys.path.insert(0, os.path.dirname(__file__))
    from state import init_db  # type: ignore[import]

    config_path = os.environ.get("SOURCES_CONFIG") or _DEFAULT_CONFIG_PATH

    print(f"SoulSync: migrate sources.yaml -> sources table")
    print(f"Config path: {config_path}")
    print()

    conn = init_db()
    migrated = migrate_yaml_to_db(conn, config_path=config_path, silent=False)
    conn.close()

    if migrated > 0:
        sys.exit(0)
    elif migrated == 0:
        # Either already migrated or nothing to migrate — both are success states
        sys.exit(0)
