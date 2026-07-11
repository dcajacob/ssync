from __future__ import annotations

import argparse
from typing import Any


def build_parser(config_defaults: dict[str, Any] | None = None) -> argparse.ArgumentParser:
    from .cli import _build_parser

    return _build_parser(config_defaults)


def build_rsync_parser(config_defaults: dict[str, Any] | None = None) -> argparse.ArgumentParser:
    from .cli import _build_rsync_parser

    return _build_rsync_parser(config_defaults)


def build_ssyncd_parser(config_defaults: dict[str, Any] | None = None) -> argparse.ArgumentParser:
    from .cli import _build_ssyncd_parser

    return _build_ssyncd_parser(config_defaults)
