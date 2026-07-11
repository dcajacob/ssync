from __future__ import annotations

import argparse


def run_sync(args: argparse.Namespace) -> int:
    from .cli import _run_sync_impl

    return _run_sync_impl(args)
