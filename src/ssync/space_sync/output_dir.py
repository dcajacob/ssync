from __future__ import annotations

import json
import logging
import shutil
import time
from pathlib import Path

_CLEAR_REQUEST_FILE = ".ssync-clear-request.json"


def clear_request_path(output_dir: Path) -> Path:
    return output_dir / _CLEAR_REQUEST_FILE


def write_clear_request(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = clear_request_path(output_dir)
    path.write_text(json.dumps({"ts_s": time.monotonic()}), encoding="utf-8")
    return path


def consume_clear_request(output_dir: Path) -> bool:
    path = clear_request_path(output_dir)
    if not path.exists():
        return False
    try:
        path.unlink()
    except OSError:
        return False
    return True


logger = logging.getLogger(__name__)


def clear_output_dir(output_dir: Path) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    resolved = output_dir.resolve()
    home_dir = Path.home().resolve()
    
    if resolved == Path(resolved.anchor):
        raise ValueError("refusing to clear filesystem root")
    if resolved == home_dir:
        raise ValueError("refusing to clear the home directory")
    
    removed_files = 0
    removed_dirs = 0
    
    try:
        for child in output_dir.iterdir():
            try:
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                    removed_dirs += 1
                    logger.debug(f"Removed directory: {child}")
                else:
                    child.unlink()
                    removed_files += 1
                    logger.debug(f"Removed file: {child}")
            except Exception as e:
                logger.warning(f"Failed to remove {child}: {e}")
                raise
    except Exception:
        # Re-raise any exceptions that occurred during iteration
        raise
    
    logger.info(
        f"Cleared output directory: {removed_files} files, {removed_dirs} directories removed"
    )
    return removed_files, removed_dirs
