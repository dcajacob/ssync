from __future__ import annotations

import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make is not installed")


@pytest.mark.parametrize("target", ["wheel", "build"])
@pytest.mark.parametrize("output_dir", ["dist", "artifacts/custom wheels"])
def test_wheel_target_uses_sdist_pipeline_and_honors_overrides(
    target: str, output_dir: str
) -> None:
    result = subprocess.run(
        [
            "make", "--no-print-directory", "--dry-run", target,
            "UV=test-uv", "BUILD_FLAGS=--offline", f"DIST_DIR={output_dir}",
        ],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    commands = [line for line in result.stdout.splitlines() if not line.startswith("#")]
    assert len(commands) == 1
    assert shlex.split(commands[0]) == [
        "test-uv", "build", "--offline", "--out-dir", output_dir,
    ]
