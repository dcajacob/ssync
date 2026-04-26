from __future__ import annotations

import shutil
from pathlib import Path


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
    for child in output_dir.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
            removed_dirs += 1
            continue
        child.unlink()
        removed_files += 1
    return removed_files, removed_dirs
