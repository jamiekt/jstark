# SPDX-FileCopyrightText: 2022-present Jamie Thomson
#
# SPDX-License-Identifier: MIT
"""Regenerate jstark/demo/GroceryFeatures_demo.ipynb.

Run from the repo root:
    uv run python scripts/generate_demo_notebook.py
"""

from __future__ import annotations

import json
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
    """Build the demo notebook as an in-memory NotebookNode with stable cell IDs."""
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
            "gf = GroceryFeatures()"
            ".with_as_at(date(2022, 1, 1))"
            '.with_feature_periods(["3m1", "6m4"])'
        ),
        new_code_cell(
            'output_df = input_df.groupBy("Store").agg(*gf.features)\n'
            "output_df.select(\n"
            '    "Store",\n'
            '    "BasketCount_3m1",\n'
            '    "BasketCount_6m4",\n'
            '    "GrossSpend_3m1",\n'
            '    "GrossSpend_6m4",\n'
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
            '    if c.name.endswith("_3m1"):\n'
            "        print(f\"{c.name}: {c.metadata['description']}\")"
        ),
        new_markdown_cell(
            "## Feature input references\n"
            "\n"
            "`gf.references` tells you which input columns a feature depends on."
        ),
        new_code_cell(
            'print(gf.references["BasketCount_3m1"])\n'
            'print(gf.references["CustomerCount_3m1"])\n'
            'print(gf.references["AvgGrossSpendPerBasket_3m1"])'
        ),
        new_markdown_cell(
            "## Absolute period labels\n"
            "\n"
            "By default, feature names carry the mnemonic of their period "
            "(e.g. `BasketCount_1q1`). Calling `with_use_absolute_periods(True)` "
            "rewrites the suffix as the concrete calendar period the mnemonic "
            "resolves to, given `as_at`. For example, with `as_at=2022-01-01` "
            "the mnemonic `1q1` becomes `2021Q4` and `2q2` becomes `2021Q3`."
        ),
        new_code_cell(
            "gf_abs = (\n"
            "    GroceryFeatures()\n"
            "    .with_as_at(date(2022, 1, 1))\n"
            '    .with_feature_periods(["1q1", "2q2"])\n'
            "    .with_use_absolute_periods(True)\n"
            ")\n"
            'output_abs_df = input_df.groupBy("Store").agg(*gf_abs.features)\n'
            "output_abs_df.select(\n"
            '    "Store", "BasketCount_2021Q4", "BasketCount_2021Q3"\n'
            ").show()"
        ),
        new_markdown_cell(
            "## Next steps\n"
            "\n"
            "See the "
            "[Features reference]"
            "(https://github.com/jamiekt/jstark#features-reference) "
            "in the project README for the full catalogue of grocery features."
        ),
    ]

    for index, cell in enumerate(cells):
        cell["id"] = f"cell-{index}"

    nb = new_notebook(cells=cells)
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def main() -> None:
    """Build the demo notebook and write it to OUTPUT.

    The file is written using the same JSON formatting as the repository's
    ``pretty-format-json`` pre-commit hook (indent=2, ensure_ascii=True) so
    that regenerating does not produce spurious hook-driven diffs.
    """
    nb = build()
    nbformat.validate(nb)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as fh:
        json.dump(nb, fh, indent=2, ensure_ascii=True)
        fh.write("\n")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()
