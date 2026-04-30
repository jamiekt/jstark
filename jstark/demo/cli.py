# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
"""CLI that copies the bundled demo notebook into the user's cwd."""

from __future__ import annotations

import argparse
import shutil
import sys
from importlib.resources import files
from pathlib import Path

NOTEBOOK_NAME = "GroceryFeatures_demo.ipynb"


def _build_parser() -> argparse.ArgumentParser:
    """Create the argparse parser for jstark-demo."""
    parser = argparse.ArgumentParser(
        prog="jstark-demo",
        description=(
            f"Copy the bundled {NOTEBOOK_NAME} notebook into the current directory."
        ),
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite an existing file of the same name.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    source = Path(str(files("jstark.demo") / NOTEBOOK_NAME))
    target = Path.cwd() / NOTEBOOK_NAME

    if target.exists() and not args.force:
        print(
            f"{NOTEBOOK_NAME} already exists; refusing to overwrite. "
            "Use --force to overwrite.",
            file=sys.stderr,
        )
        return 1

    shutil.copyfile(source, target)
    print(
        f"Copied to ./{NOTEBOOK_NAME} — run "
        f"'jupyter notebook {NOTEBOOK_NAME}' to open it."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
