from __future__ import annotations

import logging
import shutil
import stat
from pathlib import Path

logger = logging.getLogger(__name__)


def safe_output_path(output_dir: Path, file_name: str) -> Path | None:
    relative_path = Path(file_name)
    if relative_path.is_absolute():
        return None
    filtered_parts = [part for part in relative_path.parts if part not in ("", ".")]
    if not filtered_parts:
        return None
    if any(part == ".." for part in filtered_parts):
        return None

    root = output_dir.resolve()
    current = root
    for part in filtered_parts[:-1]:
        current = current / part
        try:
            stat_result = current.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError:
            return None
        if stat.S_ISLNK(stat_result.st_mode) or not stat.S_ISDIR(stat_result.st_mode):
            return None

    candidate = root / Path(*filtered_parts)
    try:
        candidate_parent = candidate.parent.resolve()
    except OSError:
        return None
    if not _is_relative_to(candidate_parent, root):
        return None
    if candidate.exists() or candidate.is_symlink():
        try:
            resolved_candidate = candidate.resolve(strict=True)
        except OSError:
            return None
        if not _is_relative_to(resolved_candidate, root):
            return None
        try:
            candidate_stat = candidate.stat(follow_symlinks=False)
        except OSError:
            return None
        if stat.S_ISLNK(candidate_stat.st_mode):
            return None
    return candidate


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


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
        "Cleared output directory: %d files, %d directories removed",
        removed_files,
        removed_dirs,
    )
    return removed_files, removed_dirs
