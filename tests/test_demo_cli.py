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
    """The CLI copies the bundled notebook to cwd when the target is absent."""
    result = subprocess.run(
        ["jstark-demo"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    target = tmp_path / NOTEBOOK_NAME
    assert target.exists()
    assert target.read_bytes() == _bundled_notebook_bytes()
    assert "Copied to" in result.stdout


def test_refuses_when_target_exists(tmp_path: Path) -> None:
    """The CLI refuses to overwrite an existing notebook without --force."""
    target = tmp_path / NOTEBOOK_NAME
    original = b"do not overwrite me"
    target.write_bytes(original)

    result = subprocess.run(
        ["jstark-demo"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert target.read_bytes() == original
    assert "already exists" in result.stderr
    assert "--force" in result.stderr


def test_force_overwrites_existing_target(tmp_path: Path) -> None:
    """The --force flag overrides the refuse-to-overwrite behaviour."""
    target = tmp_path / NOTEBOOK_NAME
    target.write_bytes(b"do not overwrite me")

    result = subprocess.run(
        ["jstark-demo", "--force"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == _bundled_notebook_bytes()
