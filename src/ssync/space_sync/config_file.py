"""TOML config file discovery and loading for the ssync CLI."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Final

_LOG_LEVEL_CHOICES: Final[frozenset[str]] = frozenset(
    {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
)

# TOML section names (top-level tables).
_ALLOWED_SECTIONS: Final[frozenset[str]] = frozenset(
    {"global", "receive", "send", "sync", "server", "monitor"}
)

_GLOBAL_KEYS: Final[frozenset[str]] = frozenset({"log_level"})

_RECEIVE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "bind_host",
        "bind_port",
        "output_dir",
        "monitor_ipc_socket",
        "feedback",
        "keep_part_files_on_complete",
        "status_repeat",
        "periodic_repair_request_s",
        "periodic_repair_min_seen_chunks",
        "max_repair_chunks_per_request",
        "adaptive_leading_hole_boost",
        "leading_hole_start_threshold_chunks",
        "leading_hole_min_span_chunks",
        "leading_hole_boost_multiplier",
        "leading_hole_max_repair_chunks_per_request",
        "repair_request_cooldown_s",
        "repair_request_inflight_timeout_s",
        "transfer_inactivity_timeout_s",
        "socket_rcvbuf_bytes",
        "journal_flush_interval_s",
        "beacon_interval_s",
        "forward_stream_quiet_s",
        "pre_metadata_max_pending_bytes",
        "pre_metadata_max_pending_bytes_per_transfer",
        "pre_metadata_max_pending_transfers",
        "pre_metadata_ttl_s",
    }
)

_SEND_KEYS: Final[frozenset[str]] = frozenset(
    {
        "dest_host",
        "dest_port",
        "chunk_size",
        "manifest_repeats",
        "feedback",
        "feedback_wait_s",
        "max_repair_rounds",
        "max_feedback_idle_timeouts",
        "drop_every_nth_data",
        "drop_rate",
        "inter_packet_delay_s",
        "max_data_rate_bps",
        "midstream_repair_max_rounds_per_poll",
        "midstream_repair_max_chunks_per_poll",
        "repair_duplicate_suppression_s",
        "repair_queue_max_pending_requests",
        "repair_worker_max_chunks_per_burst",
        "initial_pass_repair_max_chunks_per_burst",
        "repair_worker_poll_interval_s",
        "beacon_interval_s",
        "periodic_metadata_interval_s",
        "periodic_metadata_every_n_chunks",
        "revisit_incomplete_passes",
        "revisit_max_rounds_per_pass",
        "primary_feedback_max_rounds",
        "primary_feedback_max_seconds",
        "json_output",
    }
)

_MONITOR_KEYS: Final[frozenset[str]] = frozenset(
    {
        "output_dir",
        "refresh_interval_s",
        "monitor_ipc_socket",
    }
)

_SERVER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "bind_host",
        "bind_port",
        "root_dir",
        "monitor_ipc_socket",
        "feedback",
        "keep_part_files_on_complete",
        "status_repeat",
        "periodic_repair_request_s",
        "periodic_repair_min_seen_chunks",
        "max_repair_chunks_per_request",
        "adaptive_leading_hole_boost",
        "leading_hole_start_threshold_chunks",
        "leading_hole_min_span_chunks",
        "leading_hole_boost_multiplier",
        "leading_hole_max_repair_chunks_per_request",
        "repair_request_cooldown_s",
        "repair_request_inflight_timeout_s",
        "transfer_inactivity_timeout_s",
        "socket_rcvbuf_bytes",
        "journal_flush_interval_s",
        "beacon_interval_s",
        "forward_stream_quiet_s",
        "pre_metadata_max_pending_bytes",
        "pre_metadata_max_pending_bytes_per_transfer",
        "pre_metadata_max_pending_transfers",
        "pre_metadata_ttl_s",
    }
)

_SYNC_KEYS: Final[frozenset[str]] = frozenset(
    {
        "destinations",
        "recursive",
        "dry_run",
        "verbose",
        "include",
        "exclude",
        "delete",
        "skip_unchanged",
        "checksum",
        "dest_port",
        "chunk_size",
        "manifest_repeats",
        "feedback",
        "feedback_wait_s",
        "max_repair_rounds",
        "max_feedback_idle_timeouts",
        "drop_every_nth_data",
        "drop_rate",
        "inter_packet_delay_s",
        "max_data_rate_bps",
        "midstream_repair_max_rounds_per_poll",
        "midstream_repair_max_chunks_per_poll",
        "repair_duplicate_suppression_s",
        "repair_queue_max_pending_requests",
        "repair_worker_max_chunks_per_burst",
        "initial_pass_repair_max_chunks_per_burst",
        "repair_worker_poll_interval_s",
        "beacon_interval_s",
        "periodic_metadata_interval_s",
        "periodic_metadata_every_n_chunks",
        "revisit_incomplete_passes",
        "revisit_max_rounds_per_pass",
        "primary_feedback_max_rounds",
        "primary_feedback_max_seconds",
        "state_file",
        "open_loop_max_rounds",
        "json_output",
    }
)

_PATH_KEYS: Final[frozenset[str]] = frozenset(
    {"output_dir", "root_dir", "monitor_ipc_socket", "state_file"}
)

_APPEND_LIST_KEYS: Final[frozenset[str]] = frozenset(
    {"destinations", "include", "exclude"}
)

_STRING_KEYS: Final[frozenset[str]] = frozenset({"bind_host", "dest_host"})

_BOOL_KEYS: Final[frozenset[str]] = frozenset(
    {
        "adaptive_leading_hole_boost",
        "checksum",
        "delete",
        "dry_run",
        "feedback",
        "json_output",
        "keep_part_files_on_complete",
        "recursive",
        "skip_unchanged",
    }
)

_PORT_KEYS: Final[frozenset[str]] = frozenset({"bind_port", "dest_port"})

_POSITIVE_INT_KEYS: Final[frozenset[str]] = frozenset({"chunk_size"})

_NONNEGATIVE_INT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "drop_every_nth_data",
        "initial_pass_repair_max_chunks_per_burst",
        "leading_hole_boost_multiplier",
        "leading_hole_max_repair_chunks_per_request",
        "leading_hole_min_span_chunks",
        "leading_hole_start_threshold_chunks",
        "manifest_repeats",
        "max_feedback_idle_timeouts",
        "max_repair_chunks_per_request",
        "max_repair_rounds",
        "max_data_rate_bps",
        "midstream_repair_max_chunks_per_poll",
        "midstream_repair_max_rounds_per_poll",
        "open_loop_max_rounds",
        "periodic_metadata_every_n_chunks",
        "periodic_repair_min_seen_chunks",
        "pre_metadata_max_pending_bytes",
        "pre_metadata_max_pending_bytes_per_transfer",
        "pre_metadata_max_pending_transfers",
        "primary_feedback_max_rounds",
        "repair_queue_max_pending_requests",
        "repair_worker_max_chunks_per_burst",
        "revisit_incomplete_passes",
        "revisit_max_rounds_per_pass",
        "socket_rcvbuf_bytes",
        "status_repeat",
        "verbose",
    }
)

_NONNEGATIVE_FLOAT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "beacon_interval_s",
        "feedback_wait_s",
        "inter_packet_delay_s",
        "journal_flush_interval_s",
        "periodic_metadata_interval_s",
        "periodic_repair_request_s",
        "pre_metadata_ttl_s",
        "primary_feedback_max_seconds",
        "refresh_interval_s",
        "repair_duplicate_suppression_s",
        "repair_request_cooldown_s",
        "repair_request_inflight_timeout_s",
        "repair_worker_poll_interval_s",
        "transfer_inactivity_timeout_s",
    }
)


def detect_cli_command(argv: list[str]) -> str | None:
    """Return the logical CLI command for config lookup, or None if argv is empty."""
    if not argv:
        return None
    if argv[0] == "sync":
        return None
    subcommands = {"receive", "server", "ssyncd", "send", "monitor"}
    if argv[0] in subcommands:
        return argv[0]
    return "sync"


def _toml_section_for_command(command: str) -> str:
    if command in {"server", "ssyncd"}:
        return "server"
    return command


def _allowed_keys_for_command(command: str) -> frozenset[str]:
    if command == "receive":
        return _GLOBAL_KEYS | _RECEIVE_KEYS
    if command == "send":
        return _GLOBAL_KEYS | _SEND_KEYS
    if command == "monitor":
        return _GLOBAL_KEYS | _MONITOR_KEYS
    if command in {"server", "ssyncd"}:
        return _GLOBAL_KEYS | _SERVER_KEYS
    if command == "sync":
        return _GLOBAL_KEYS | _SYNC_KEYS
    raise ValueError(f"unsupported command for config: {command!r}")


def _config_file_paths() -> list[Path]:
    """Lowest precedence first: XDG-style home config, then ~/.ssync.toml, then cwd."""
    home = Path.home()
    return [
        home / ".config" / "ssync" / "config.toml",
        home / ".ssync.toml",
        Path.cwd() / ".ssync.toml",
    ]


def _normalize_append_list(value: object, *, key: str, path: Path) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        out: list[str] = []
        for i, item in enumerate(value):
            if not isinstance(item, str):
                raise ValueError(
                    f"Invalid type for {key!r} entry {i} in {path}: expected string, got {type(item).__name__}"
                )
            out.append(item)
        return out
    raise ValueError(
        f"Invalid type for {key!r} in {path}: expected string or array of strings, got {type(value).__name__}"
    )


def _coerce_bool(key: str, value: object, *, path: Path) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(
        f"Invalid type for {key!r} in {path}: expected boolean, got {type(value).__name__}"
    )


def _coerce_int(key: str, value: object, *, path: Path, minimum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(
            f"Invalid type for {key!r} in {path}: expected integer, got {type(value).__name__}"
        )
    if value < minimum:
        raise ValueError(f"Invalid value for {key!r} in {path}: must be >= {minimum}")
    return value


def _coerce_float(key: str, value: object, *, path: Path, minimum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"Invalid type for {key!r} in {path}: expected number, got {type(value).__name__}"
        )
    coerced = float(value)
    if coerced < minimum:
        raise ValueError(f"Invalid value for {key!r} in {path}: must be >= {minimum:g}")
    return coerced


def _coerce_value(key: str, value: object, *, path: Path) -> object:
    if key in _APPEND_LIST_KEYS:
        return _normalize_append_list(value, key=key, path=path)
    if key in _PATH_KEYS:
        if value is None:
            raise ValueError(f"Invalid value for {key!r} in {path}: cannot be null")
        if isinstance(value, str):
            return Path(value)
        raise ValueError(
            f"Invalid type for {key!r} in {path}: expected string, got {type(value).__name__}"
        )
    if key in _STRING_KEYS:
        if isinstance(value, str):
            return value
        raise ValueError(
            f"Invalid type for {key!r} in {path}: expected string, got {type(value).__name__}"
        )
    if key == "log_level":
        if not isinstance(value, str):
            raise ValueError(
                f"Invalid type for log_level in {path}: expected string, got {type(value).__name__}"
            )
        upper = value.upper()
        if upper not in _LOG_LEVEL_CHOICES:
            raise ValueError(
                f"Invalid log_level {value!r} in {path}: must be one of {sorted(_LOG_LEVEL_CHOICES)}"
            )
        return upper
    if key in _BOOL_KEYS:
        return _coerce_bool(key, value, path=path)
    if key in _PORT_KEYS:
        port = _coerce_int(key, value, path=path, minimum=1)
        if port > 65535:
            raise ValueError(f"Invalid value for {key!r} in {path}: must be <= 65535")
        return port
    if key in _POSITIVE_INT_KEYS:
        return _coerce_int(key, value, path=path, minimum=1)
    if key in _NONNEGATIVE_INT_KEYS:
        return _coerce_int(key, value, path=path, minimum=0)
    if key in _NONNEGATIVE_FLOAT_KEYS:
        return _coerce_float(key, value, path=path, minimum=0.0)
    return value


def _validate_table_keys(table: dict[str, Any], *, allowed: frozenset[str], path: Path) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        joined = ", ".join(repr(k) for k in unknown)
        raise ValueError(f"Unknown config key(s) in {path}: {joined}")


def load_cli_config_defaults(command: str) -> dict[str, Any]:
    """
    Load merged TOML defaults for the given CLI command.

    File precedence (later overrides earlier): ~/.config/ssync/config.toml,
    ~/.ssync.toml, ./.ssync.toml

    Tables: only [global] and the table matching this command are read from each
    file (other known sections are ignored). Unknown top-level tables error.
    """
    section = _toml_section_for_command(command)
    allowed = _allowed_keys_for_command(command)

    merged: dict[str, Any] = {}
    for path in _config_file_paths():
        if not path.is_file():
            continue
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ValueError(f"Could not read config file {path}: {exc}") from exc
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"Invalid UTF-8 in config file {path}: {exc}") from exc
        try:
            doc = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"Invalid TOML in config file {path}: {exc}") from exc
        if not isinstance(doc, dict):
            raise ValueError(f"Invalid TOML root in {path}: expected table")

        for table_name, table in doc.items():
            if table_name not in _ALLOWED_SECTIONS:
                raise ValueError(
                    f"Unknown config section [{table_name}] in {path}: "
                    f"allowed sections are {sorted(_ALLOWED_SECTIONS)}"
                )
            if not isinstance(table, dict):
                raise ValueError(f"Invalid [{table_name}] in {path}: expected inline table")

            if table_name == "global":
                _validate_table_keys(table, allowed=_GLOBAL_KEYS, path=path)
                for key, value in table.items():
                    merged[key] = _coerce_value(key, value, path=path)
            elif table_name == section:
                _validate_table_keys(table, allowed=allowed - _GLOBAL_KEYS, path=path)
                for key, value in table.items():
                    merged[key] = _coerce_value(key, value, path=path)

    for key in merged:
        if key not in allowed:
            raise ValueError(f"Unknown merged config key {key!r} for command {command!r}")

    return merged
