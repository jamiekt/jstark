# Bundled Demo Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bundle `GroceryFeatures_demo.ipynb` in the jstark wheel, ship a `jstark-demo` CLI that copies it into the user's cwd, and add a `jupyter` extra.

**Architecture:** New `jstark/demo/` subpackage contains the committed `.ipynb` and a stdlib-only `cli.py`. `pyproject.toml` gains a `[project.scripts]` entry and a `jupyter` optional-dependency. CI guards against drift via `nbclient` executing the notebook end-to-end.

**Tech Stack:** Python 3.10+ stdlib (`argparse`, `shutil`, `importlib.resources`, `pathlib`), hatchling (build), pytest + nbclient (tests), uv (env).

**Spec:** `docs/superpowers/specs/2026-04-30-bundled-demo-notebook-design.md`

---

## File Structure

Files to be created:

- `jstark/demo/__init__.py` — marks the subpackage, SPDX header only.
- `jstark/demo/cli.py` — the `jstark-demo` entry point. One `main()` function plus `argparse` setup.
- `jstark/demo/GroceryFeatures_demo.ipynb` — the demo notebook, generated once by a helper script and committed.
- `tests/test_demo_cli.py` — subprocess-based tests for the CLI.
- `tests/test_demo_notebook.py` — runs the notebook end-to-end via nbclient.
- `scripts/generate_demo_notebook.py` — one-shot generator kept under version control so the notebook can be rebuilt deterministically.

Files to be modified:

- `pyproject.toml` — add `jupyter` extra, `[project.scripts]`, dev deps, verify wheel include.
- `README.md` — add the "Try the demo notebook" section after Quick start.

Each file has one responsibility: `cli.py` is the CLI (nothing else), `__init__.py` is the package marker (nothing else), each test file targets one of the two artefacts.

---

## Task 1: Scaffold the `jstark/demo` subpackage

**Files:**

- Create: `jstark/demo/__init__.py`
- Create: `jstark/demo/cli.py`

- [ ] **Step 1: Create `jstark/demo/__init__.py`**

```python
# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
```

- [ ] **Step 2: Create `jstark/demo/cli.py` with a minimal non-functional stub**

This stub exists only so `pyproject.toml` can reference the entry point in the next task. It will fail its tests until Task 3. That's expected.

```python
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


def main(argv: list[str] | None = None) -> int:
    raise NotImplementedError("implemented in Task 3")


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Commit**

```bash
git add jstark/demo/__init__.py jstark/demo/cli.py
git commit -m "chore: scaffold jstark.demo subpackage"
```

---

## Task 2: Wire up `pyproject.toml` (extras, scripts, dev deps)

**Files:**

- Modify: `pyproject.toml`

- [ ] **Step 1: Add the `jupyter` extra alongside the existing `faker` extra**

Replace the existing `[project.optional-dependencies]` block:

```toml
[project.optional-dependencies]
faker = ["faker>=40.0.0"]
jupyter = ["notebook", "faker>=40.0.0"]
```

- [ ] **Step 2: Add the `jstark-demo` console script**

Insert a new top-level section (place it after `[project.optional-dependencies]`):

```toml
[project.scripts]
jstark-demo = "jstark.demo.cli:main"
```

- [ ] **Step 3: Add `nbclient` and `ipykernel` to the dev dependency group**

In the existing `[dependency-groups]` block, extend the `dev` list by appending two entries:

```toml
  "nbclient",
  "ipykernel",
```

After the edit, the whole `dev` list should end:

```toml
  "cogapp>=3.6.0",
  "faker>=40.4.0",
  "freezegun",
  "nbclient",
  "ipykernel",
]
```

- [ ] **Step 4: Sync the environment so the new script is installed**

Run: `uv sync --all-extras`
Expected: no errors, `.venv` is updated, `jstark-demo` becomes available.

- [ ] **Step 5: Confirm the script is registered**

Run: `uv run jstark-demo --help || true`
Expected: the command is found (exit code may be non-zero because `main()` raises NotImplementedError). If the shell prints `command not found: jstark-demo`, the wiring is wrong — revisit Step 2.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "build: add jupyter extra, jstark-demo script, notebook-test deps"
```

---

## Task 3: TDD — CLI happy path (copies when target absent)

**Files:**

- Create: `tests/test_demo_cli.py`
- Modify: `jstark/demo/cli.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_demo_cli.py`:

```python
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
```

This test also relies on the notebook existing at the bundled location. For now we will stage a placeholder so the test can assert byte equality; Task 7 replaces it with the real notebook.

- [ ] **Step 2: Create a placeholder notebook file so the test helper has something to read**

Create `jstark/demo/GroceryFeatures_demo.ipynb` with a trivially-valid empty notebook JSON. This placeholder is replaced in Task 7.

```json
{
 "cells": [],
 "metadata": {},
 "nbformat": 4,
 "nbformat_minor": 5
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/test_demo_cli.py::test_copies_notebook_when_target_absent -v`
Expected: FAIL — the CLI raises `NotImplementedError` (returncode != 0).

- [ ] **Step 4: Implement `main()`**

Replace the body of `jstark/demo/cli.py` with:

```python
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_demo_cli.py::test_copies_notebook_when_target_absent -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_demo_cli.py jstark/demo/cli.py jstark/demo/GroceryFeatures_demo.ipynb
git commit -m "feat: jstark-demo CLI copies bundled notebook to cwd"
```

---

## Task 4: TDD — CLI refuses to overwrite without `--force`

**Files:**

- Modify: `tests/test_demo_cli.py`

- [ ] **Step 1: Add the failing test**

Append to `tests/test_demo_cli.py`:

```python
def test_refuses_when_target_exists(tmp_path: Path) -> None:
    target = tmp_path / NOTEBOOK_NAME
    original = b"do not overwrite me"
    target.write_bytes(original)

    result = subprocess.run(
        ["jstark-demo"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert target.read_bytes() == original
    assert "already exists" in result.stderr
    assert "--force" in result.stderr
```

- [ ] **Step 2: Run the test to verify it already passes (behaviour was implemented in Task 3)**

Run: `uv run pytest tests/test_demo_cli.py::test_refuses_when_target_exists -v`
Expected: PASS. (If it fails, the refusal branch in `cli.py` is wrong — fix `main()`.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_demo_cli.py
git commit -m "test: CLI refuses to overwrite existing notebook"
```

---

## Task 5: TDD — CLI overwrites with `--force`

**Files:**

- Modify: `tests/test_demo_cli.py`

- [ ] **Step 1: Add the failing test**

Append to `tests/test_demo_cli.py`:

```python
def test_force_overwrites_existing_target(tmp_path: Path) -> None:
    target = tmp_path / NOTEBOOK_NAME
    target.write_bytes(b"do not overwrite me")

    result = subprocess.run(
        ["jstark-demo", "--force"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == _bundled_notebook_bytes()
```

- [ ] **Step 2: Run the test to verify it passes (behaviour implemented in Task 3)**

Run: `uv run pytest tests/test_demo_cli.py::test_force_overwrites_existing_target -v`
Expected: PASS.

- [ ] **Step 3: Run the whole CLI test module to confirm all three pass together**

Run: `uv run pytest tests/test_demo_cli.py -v`
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add tests/test_demo_cli.py
git commit -m "test: --force flag overwrites existing notebook"
```

---

## Task 6: Generator script for the notebook content

**Files:**

- Create: `scripts/generate_demo_notebook.py`

The script programmatically builds the `.ipynb` using `nbformat`. Keeping the generator under version control means the notebook can be rebuilt deterministically and diffs are reviewable (otherwise `.ipynb` JSON churn is painful).

- [ ] **Step 1: Create the generator**

```python
# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
"""Regenerate jstark/demo/GroceryFeatures_demo.ipynb.

Run from the repo root:
    uv run python scripts/generate_demo_notebook.py
"""
from __future__ import annotations

from pathlib import Path

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook

OUTPUT = (
    Path(__file__).resolve().parent.parent
    / "jstark"
    / "demo"
    / "GroceryFeatures_demo.ipynb"
)


def build() -> nbformat.NotebookNode:
    cells = [
        new_markdown_cell(
            "# jstark — GroceryFeatures demo\n"
            "\n"
            "[jstark](https://github.com/jamiekt/jstark) is a PySpark library for "
            "generating time-based features for machine learning. Features are "
            "calculated relative to an **as at** date, enabling point-in-time "
            "feature engineering over configurable time windows.\n"
            "\n"
            "This notebook walks through `GroceryFeatures` using the sample "
            "`FakeGroceryTransactions` data. For the full list of available "
            "features see the "
            "[README](https://github.com/jamiekt/jstark#features-reference)."
        ),
        new_code_cell(
            "from datetime import date\n"
            "\n"
            "from jstark.grocery import GroceryFeatures\n"
            "from jstark.sample.transactions import FakeGroceryTransactions"
        ),
        new_markdown_cell(
            "## Sample data\n"
            "\n"
            "`FakeGroceryTransactions` produces a deterministic PySpark "
            "DataFrame of synthetic grocery transactions (baskets, stores, "
            "products, spend) suitable for demonstration."
        ),
        new_code_cell(
            "input_df = FakeGroceryTransactions(seed=42, number_of_baskets=500).df\n"
            "input_df.printSchema()\n"
            "input_df.show(5)"
        ),
        new_markdown_cell(
            "## Feature period mnemonics\n"
            "\n"
            "Feature names end with a mnemonic `{start}{unit}{end}` where the "
            "unit is one of `d` (days), `w` (weeks), `m` (months), `q` "
            "(quarters) or `y` (years).\n"
            "\n"
            "Examples, all relative to the `as_at` date:\n"
            "\n"
            "- `3m1` — 3 months before to 1 month before\n"
            "- `6m4` — 6 months before to 4 months before\n"
            "- `1q1` — the single full quarter preceding `as_at`"
        ),
        new_code_cell(
            "gf = (\n"
            "    GroceryFeatures()\n"
            "    .with_as_at(date(2022, 1, 1))\n"
            "    .with_feature_periods(['3m1', '6m4'])\n"
            ")"
        ),
        new_code_cell(
            "output_df = input_df.groupBy('Store').agg(*gf.features)\n"
            "output_df.select(\n"
            "    'Store', 'BasketCount_3m1', 'BasketCount_6m4',\n"
            "    'GrossSpend_3m1', 'GrossSpend_6m4',\n"
            ").show()"
        ),
        new_markdown_cell(
            "## Feature descriptions\n"
            "\n"
            "Every feature carries a human-readable description in its "
            "PySpark column metadata."
        ),
        new_code_cell(
            "for c in output_df.schema:\n"
            "    if c.name.endswith('_3m1'):\n"
            "        print(f\"{c.name}: {c.metadata['description']}\")"
        ),
        new_markdown_cell(
            "## Feature input references\n"
            "\n"
            "`gf.references` tells you which input columns a feature depends on."
        ),
        new_code_cell(
            "print(gf.references['BasketCount_3m1'])\n"
            "print(gf.references['CustomerCount_3m1'])\n"
            "print(gf.references['AvgGrossSpendPerBasket_3m1'])"
        ),
        new_markdown_cell(
            "## Next steps\n"
            "\n"
            "See the "
            "[Features reference](https://github.com/jamiekt/jstark#features-reference) "
            "in the project README for the full catalogue of grocery features."
        ),
    ]

    nb = new_notebook(cells=cells)
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def main() -> None:
    nb = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as fh:
        nbformat.write(nb, fh)
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the generator**

Run: `uv run python scripts/generate_demo_notebook.py`
Expected output: `wrote .../jstark/demo/GroceryFeatures_demo.ipynb`

- [ ] **Step 3: Confirm the CLI tests still pass with the new notebook bytes**

Run: `uv run pytest tests/test_demo_cli.py -v`
Expected: 3 passed. (The helper reads bytes from the installed package, so rebuild if needed with `uv sync --all-extras`.)

- [ ] **Step 4: Commit**

```bash
git add scripts/generate_demo_notebook.py jstark/demo/GroceryFeatures_demo.ipynb
git commit -m "feat: generate GroceryFeatures_demo.ipynb via nbformat script"
```

---

## Task 7: Verify the wheel bundles the notebook

**Files:**

- Possibly modify: `pyproject.toml`

- [ ] **Step 1: Build the wheel**

Run: `uv build`
Expected: a `.whl` appears under `dist/`.

- [ ] **Step 2: List wheel contents to check for the notebook**

Run: `unzip -l dist/*.whl | grep GroceryFeatures_demo.ipynb`
Expected: a single line showing `jstark/demo/GroceryFeatures_demo.ipynb`. If the grep finds nothing, go to Step 3; otherwise skip to Step 4.

- [ ] **Step 3 (only if notebook missing): add an explicit hatchling include**

Edit `pyproject.toml`. After the existing `[tool.hatch.build.hooks.vcs]` block, add:

```toml
[tool.hatch.build.targets.wheel]
packages = ["jstark"]

[tool.hatch.build.targets.wheel.force-include]
"jstark/demo/GroceryFeatures_demo.ipynb" = "jstark/demo/GroceryFeatures_demo.ipynb"
```

Then rerun Steps 1 and 2 and confirm the notebook is now listed.

- [ ] **Step 4: Clean up the build artefact**

Run: `rm -rf dist`

- [ ] **Step 5: Commit (only if Step 3 ran)**

```bash
git add pyproject.toml
git commit -m "build: explicitly include demo notebook in wheel"
```

---

## Task 8: TDD — notebook executes end-to-end

**Files:**

- Create: `tests/test_demo_notebook.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_demo_notebook.py`:

```python
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
    nb = nbformat.read(NOTEBOOK_PATH, as_version=4)
    client = NotebookClient(nb, timeout=600, kernel_name="python3")
    client.execute()
```

Rationale: `NotebookClient.execute()` raises `CellExecutionError` on any cell failure, so no explicit assertion is needed — the test fails if any cell raises.

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_demo_notebook.py -v`
Expected: PASS. This test exercises a real Spark session and takes noticeably longer than the rest of the suite — that is expected.

- [ ] **Step 3: Commit**

```bash
git add tests/test_demo_notebook.py
git commit -m "test: execute GroceryFeatures demo notebook end-to-end"
```

---

## Task 9: README — "Try the demo notebook" section

**Files:**

- Modify: `README.md`

- [ ] **Step 1: Insert the new section immediately after the existing Quick start section**

In `README.md`, find the end of the Quick start section (the Spark output block ending with `+-----------+…+`). Immediately after the closing triple-backtick of that block, and before the next `## Feature descriptions and references` heading, insert:

````markdown
## Try the demo notebook

A runnable Jupyter notebook is bundled with the wheel:

```shell
pip install jstark[jupyter]
jstark-demo
jupyter notebook GroceryFeatures_demo.ipynb
```

`jstark-demo` copies `GroceryFeatures_demo.ipynb` into your current directory.
Use `jstark-demo --force` to overwrite an existing copy.

````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document jstark-demo and the jupyter extra"
```

---

## Task 10: Full verification pass

**Files:** none modified.

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest`
Expected: all tests pass, including the three new CLI tests and the notebook execution test.

- [ ] **Step 2: Run pylint as CI does**

Run: `uv run pylint --rcfile .github/linters/pylintrc jstark tests`
Expected: no lint errors introduced by new files. Fix any issues (e.g. missing docstrings) inline and re-run.

- [ ] **Step 3: Run pre-commit hooks across the whole change**

Run: `uv run pre-commit run --all-files`
Expected: all hooks pass.

- [ ] **Step 4: Sanity-check the CLI in a temp dir**

Run:
```bash
REPO="$PWD" && TMP=$(mktemp -d) && (cd "$TMP" && uv run --project "$REPO" jstark-demo) && ls "$TMP" && rm -rf "$TMP"
```
Expected: the CLI's success message appears, followed by `GroceryFeatures_demo.ipynb` in the `ls` output.

- [ ] **Step 5: Final commit if any fixups were made in Steps 2-3; otherwise nothing to do.**

---

## Review checklist (self-review before handoff)

- All five spec components are implemented: notebook (Task 6), CLI (Tasks 1, 3-5), packaging (Tasks 2, 7), tests (Tasks 3-5, 8), README (Task 9).
- No placeholders: all code and test bodies are inline.
- Names are consistent: `NOTEBOOK_NAME = "GroceryFeatures_demo.ipynb"`, `main()`, `--force` match across tasks.
- Non-goals respected: no auto-launch, no `--dest`, no interactive prompt, no `jstark.demo_notebook_path()` on the main package namespace.
- Error handling matches the spec: uncaught exceptions propagate for bundled-notebook-missing and read-only-cwd; refusal is the only hand-coded error path.
