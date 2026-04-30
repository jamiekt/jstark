# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
"""Execute the bundled demo notebook end-to-end."""

from __future__ import annotations

from pathlib import Path

import nbformat
import pytest
from nbclient import NotebookClient

NOTEBOOK_PATH = (
    Path(__file__).resolve().parent.parent
    / "jstark"
    / "demo"
    / "GroceryFeatures_demo.ipynb"
)


@pytest.mark.slow
def test_demo_notebook_executes() -> None:
    """Execute the committed demo notebook end-to-end.

    Any cell failure fails the test.
    """
    nb = nbformat.read(NOTEBOOK_PATH, as_version=4)
    client = NotebookClient(nb, timeout=600, kernel_name="python3")
    client.execute()
