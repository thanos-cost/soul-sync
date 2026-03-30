"""
sources/__init__.py — Source adapter registry and factory.

Public API:
    load_sources(config_path, conn)  — return list of SourceAdapter instances
    SourceAdapter                    — the Protocol interface (re-exported from base)
    SongEntry                        — the TypedDict for song records (re-exported from base)

Usage:
    from sources import load_sources, SourceAdapter, SongEntry

    # DB-backed (Phase 9+): pass an open connection
    adapters = load_sources(conn=conn)

    # Backward-compatible (pre-Phase 9): reads sources.yaml
    adapters = load_sources()

    for adapter in adapters:
        songs = adapter.fetch_songs()
"""

import json
import logging
import os
import sqlite3

import yaml

from sources.base import SourceAdapter, SongEntry

__all__ = ["load_sources", "SourceAdapter", "SongEntry"]

log = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = "/config/sources.yaml"


def _build_adapter_from_config(config: dict) -> SourceAdapter | None:
    """
    Instantiate a source adapter from a config dict.

    The config dict must have a 'type' key. All other keys are type-specific
    and match the fields in sources.yaml exactly (e.g. playlist_url, path, mode).

    Returns the adapter instance, or None if the type is unknown.
    """
    source_type = config.get("type", "").strip().lower()

    if source_type == "youtube":
        from sources.youtube import YouTubeAdapter  # type: ignore[import]
        return YouTubeAdapter(config)

    elif source_type == "textfile":
        from sources.textfile import TextFileAdapter  # type: ignore[import]
        return TextFileAdapter(config)

    elif source_type == "spotify":
        from sources.spotify import SpotifyAdapter  # type: ignore[import]
        return SpotifyAdapter(config)

    elif source_type == "discogs":
        from sources.discogs import DiscogsAdapter  # type: ignore[import]
        return DiscogsAdapter(config)

    else:
        log.warning(
            "Unknown source type %r — skipping. "
            "Supported types: youtube, textfile, spotify, discogs",
            source_type,
        )
        return None


def _load_from_db(conn: sqlite3.Connection) -> list[SourceAdapter]:
    """
    Read enabled source rows from the sources table and return adapter instances.

    Each row's config_json is parsed back into a dict, then type is added so
    _build_adapter_from_config() can dispatch correctly. The field names in
    config_json match sources.yaml exactly, so adapters need no changes.
    """
    from state import get_enabled_sources  # local import to avoid circular dependency

    rows = get_enabled_sources(conn)
    if not rows:
        return []

    adapters: list[SourceAdapter] = []
    for row in rows:
        try:
            config = json.loads(row["config_json"])
        except (json.JSONDecodeError, KeyError) as exc:
            log.error("Failed to parse config_json for source id=%s: %s", row.get("id"), exc)
            continue

        config["type"] = row["type"]
        adapter = _build_adapter_from_config(config)
        if adapter is not None:
            # Phase 9.1: stamp the source table ID so run_source_sync
            # can tag every song with the DB source it came from.
            adapter._source_table_id = row.get("id")
            adapters.append(adapter)

    log.info("Loaded %d source adapter(s) from sources table", len(adapters))
    return adapters


def _load_from_yaml(config_path: str | None = None) -> list[SourceAdapter]:
    """
    Read sources.yaml and return adapter instances.

    This is the pre-Phase 9 path — used as fallback when the sources table
    is empty or no DB connection is provided.
    """
    resolved_path = config_path or os.environ.get("SOURCES_CONFIG") or _DEFAULT_CONFIG_PATH

    try:
        with open(resolved_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        log.warning(
            "sources.yaml not found at %s — no sources loaded. "
            "Create this file to configure input sources.",
            resolved_path,
        )
        return []
    except Exception as exc:
        log.error("Failed to read sources config at %s: %s", resolved_path, exc)
        return []

    source_list = config.get("sources") if config else None
    if not source_list:
        log.warning("sources.yaml at %s has no 'sources' list — no adapters loaded", resolved_path)
        return []

    adapters: list[SourceAdapter] = []
    for entry in source_list:
        adapter = _build_adapter_from_config(entry)
        if adapter is not None:
            adapters.append(adapter)

    log.info("Loaded %d source adapter(s) from %s", len(adapters), resolved_path)
    return adapters


def _auto_migrate_if_needed(conn: sqlite3.Connection, config_path: str | None = None) -> None:
    """
    If the sources table is empty but sources.yaml exists, run the migration inline.

    This ensures users don't need a manual migration step — the first pipeline
    run after Phase 9 is deployed automatically populates the sources table
    from whatever sources.yaml they already have configured.
    """
    resolved_path = config_path or os.environ.get("SOURCES_CONFIG") or _DEFAULT_CONFIG_PATH
    if not os.path.exists(resolved_path):
        return

    try:
        from migrate_sources import migrate_yaml_to_db  # type: ignore[import]
        migrated = migrate_yaml_to_db(conn, resolved_path, silent=True)
        if migrated:
            log.info(
                "Auto-migrated %d source(s) from %s to sources table",
                migrated, resolved_path,
            )
    except Exception as exc:
        log.warning("Auto-migration from sources.yaml failed: %s", exc)


def load_sources(
    config_path: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[SourceAdapter]:
    """
    Return instantiated source adapters.

    Resolution order:
      1. If conn is provided and the sources table has rows → read from DB.
      2. If conn is provided but table is empty and sources.yaml exists →
         auto-migrate sources.yaml into the DB, then read from DB.
      3. Fall back to reading sources.yaml directly (backward compatibility).
      4. If sources.yaml is also missing → return [].

    The YOUTUBE_PLAYLIST_URL env var fallback has been moved into run_pipeline()
    in main.py to keep this function's concerns focused on adapter loading.

    Parameters:
        config_path — path to sources.yaml (used for fallback and auto-migration).
                      Defaults to env var SOURCES_CONFIG, then /config/sources.yaml.
        conn        — open sqlite3.Connection. When provided, the sources table
                      is queried first (Phase 9+ path).

    Returns a list of adapter instances.
    """
    if conn is not None:
        db_adapters = _load_from_db(conn)
        if db_adapters:
            return db_adapters

        # Sources table is empty — try auto-migrating from sources.yaml
        _auto_migrate_if_needed(conn, config_path)

        # Re-query after potential migration
        db_adapters = _load_from_db(conn)
        if db_adapters:
            return db_adapters

        # Table still empty even after migration attempt — fall through to YAML
        log.info("Sources table empty — falling back to sources.yaml")

    return _load_from_yaml(config_path)
