# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is jstark?

jstark is a Python library for creating time-based features for machine learning using PySpark. It generates features calculated "as at" a specific date, enabling point-in-time feature engineering for grocery retail data (and similar transactional domains). Features are aggregated over configurable time periods (days, weeks, months, quarters, years) using a mnemonic syntax like `3m1` (3 months ago to 1 month ago).

## Build and Development Commands

jstark uses [Hatch](https://hatch.pypa.io/) for environment management and building.

```bash
# Run all tests
hatch run pytest

# Run tests with coverage
hatch run cov

# Run a single test
hatch run pytest tests/test_feature.py::test_start_date_days -vvv

# Run pylint
hatch run pylint-out

# Find the Hatch-managed Python interpreter (for IDE configuration)
hatch run python -c "import sys;print(sys.executable)"
```

**Prerequisites:** Java runtime required for PySpark. On macOS: `brew install openjdk@11`.

## Architecture

### Core Concepts

- **as_at date**: All features are calculated relative to this date, enabling historical point-in-time feature calculation.
- **Feature period mnemonics**: Format `{start}{unit}{end}` where unit is `d`/`w`/`m`/`q`/`y`. Example: `3m1` means from 3 months before as_at to 1 month before as_at. The start offset must be >= the end offset.
- **Feature naming**: `{FeatureClassName}_{mnemonic}`, e.g., `BasketCount_3m1`.

### Class Hierarchy

`FeatureGenerator` (abstract, `jstark/feature_generator.py`) is the base class that parses period mnemonics, holds the as_at date, and produces PySpark `Column` objects via its `features` property. `GroceryRetailerFeatureGenerator` (`jstark/grocery_retailer_feature_generator.py`) is the concrete implementation that defines ~37 feature classes in its `FEATURE_CLASSES` list.

Features themselves have a two-branch hierarchy in `jstark/features/feature.py`:
- `BaseFeature` — aggregates raw transactional data using an aggregator (sum, count, count_distinct, min, max, etc.) with date filtering via PySpark `when` clauses.
- `DerivedFeature` — computed from already-aggregated values (e.g., `AverageGrossSpendPerBasket` = `GrossSpend / BasketCount`).

Both branches inherit from `Feature`, which handles date range calculation (`start_date`/`end_date` properties), column metadata generation, and the abstract interface (`column`, `column_expression()`, `description_subject`, `default_value()`).

### Usage Pattern

Features are used by passing them to PySpark's `agg()`:
```python
grfg = GroceryRetailerFeatureGenerator(as_at=date(2022, 1, 1), feature_periods=["3m1", "6m4"])
output_df = input_df.groupBy("Store").agg(*grfg.features)
```

### Test Structure

Tests use pytest with session-scoped fixtures in `tests/conftest.py`. A shared `SparkSession` fixture and pre-built test DataFrames (both hand-crafted and Faker-generated via `jstark/sample/transactions.py`) are reused across tests for performance. The main test file is `tests/test_grocery_retailer_feature_generator.py`.

## Code Style

- Formatted with **black** (line length 88)
- Linted with **flake8** (max-line-length=88) and **pylint** (config at `.github/linters/pylintrc`)
- Type-checked with **mypy**
- Pre-commit hooks enforce formatting/linting (`.pre-commit-config.yaml`)
- Supports Python 3.8+ (avoids walrus operator and other 3.8+-only syntax for broader compat)
