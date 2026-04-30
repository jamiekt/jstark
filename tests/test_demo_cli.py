# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
"""Tests for the jstark-demo CLI."""

from __future__ import annotations

import subprocess
from importlib.resources import files
from pathlib import Path

NOTEBOOK_NAME = "GroceryFeatures_demo.ipynb"


def _bundled_notebook_bytes() -> bytes:
    return (files("jstark.demo") / NOTEBOOK_NAME).read_bytes()


def test_copies_notebook_when_target_absent(tmp_path: Path) -> None:
    result = subprocess.run(
        ["jstark-demo"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    target = tmp_path / NOTEBOOK_NAME
    assert target.exists()
    assert target.read_bytes() == _bundled_notebook_bytes()
    assert "Copied to" in result.stdout
