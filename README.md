# jstark

[![PyPI - Version](https://img.shields.io/pypi/v/jstark.svg)](https://pypi.org/project/jstark)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jstark.svg)](https://pypi.org/project/jstark)
![build](https://github.com/jamiekt/jstark/actions/workflows/build.yml/badge.svg)
[![coverage](https://jamiekt.github.io/jstark/coverage.svg 'Click to see coverage report')](https://jamiekt.github.io/jstark/htmlcov/)
[![pylint](https://jamiekt.github.io/jstark/pylint.svg 'Click to view pylint report')](https://jamiekt.github.io/jstark/pylint.html)

Built with [![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

-----

A [PySpark](https://spark.apache.org/docs/latest/api/python/) library for generating time-based features for machine learning. All features are calculated relative to an **as at** date, enabling point-in-time feature engineering over configurable time periods.

## Feature period mnemonics

Feature names end with a mnemonic describing the time window. The format is `{start}{unit}{end}` where the unit is one of `d` (days), `w` (weeks), `m` (months), `q` (quarters) or `y` (years).

For example, `BasketCount_3m1` is the distinct count of baskets from 3 months before to 1 month before the as at date.

Multiple periods can be calculated in a single Spark job:

```python
from datetime import date
from jstark.grocery.grocery_retailer_feature_generator import GroceryRetailerFeatureGenerator

grfg = GroceryRetailerFeatureGenerator(as_at=date(2022, 1, 1), feature_periods=["3m1", "6m4"])
output_df = input_df.groupBy("Store").agg(*grfg.features)
```

This produces `BasketCount_3m1`, `BasketCount_6m4`, and every other feature for both periods.

## Quick start

**Prerequisites:** Java runtime required for PySpark. On macOS: `brew install openjdk@11`.

```shell
pip install jstark[sample]
```

The `sample` extra installs [Faker](https://faker.readthedocs.io/), which is needed for the sample data generator used below. If you don't need sample data, `pip install jstark` is sufficient.

```python
from datetime import date
from jstark.sample.transactions import FakeTransactions
from jstark.grocery.grocery_retailer_feature_generator import GroceryRetailerFeatureGenerator

input_df = FakeTransactions().get_df(seed=42, number_of_baskets=10000)
grfg = GroceryRetailerFeatureGenerator(date(2022, 1, 1), ["4q4", "3q3", "2q2", "1q1"])
output_df = input_df.groupBy("Store").agg(*grfg.features)
output_df.select(
    "Store", "BasketCount_4q4", "BasketCount_3q3", "BasketCount_2q2", "BasketCount_1q1"
).show()
```

```
+-----------+---------------+---------------+---------------+---------------+
|      Store|BasketCount_4q4|BasketCount_3q3|BasketCount_2q2|BasketCount_1q1|
+-----------+---------------+---------------+---------------+---------------+
|Hammersmith|            523|            529|            503|            492|
|    Staines|            461|            520|            493|            504|
|   Richmond|            500|            483|            503|            512|
|     Ealing|            490|            460|            485|            515|
| Twickenham|            484|            509|            514|            520|
+-----------+---------------+---------------+---------------+---------------+
```

## Feature descriptions and references

Every feature carries a description in its column metadata:

```python
from pprint import pprint
pprint([(c.name, c.metadata["description"]) for c in output_df.schema if c.name.endswith("1q1")])
```

```
[('BasketCount_1q1',
  'Distinct count of Baskets between 2021-10-01 and 2021-12-31'),
 ...]
```

You can also inspect what input columns each feature requires:

```python
grfg.references["BasketCount_1q1"]                  # ['Basket', 'Timestamp']
grfg.references["CustomerCount_1q1"]                 # ['Customer', 'Timestamp']
grfg.references["AvgGrossSpendPerBasket_1q1"]        # ['Basket', 'GrossSpend', 'Timestamp']
```

All features require a `Timestamp` column ([TimestampType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampType.html)). Most require additional columns depending on what they measure.

## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Why "jstark"?

The name is phonetically similar to PySpark, is a homage to [comic book character Jon Stark](https://www.worthpoint.com/worthopedia/football-picture-story-monthly-stark-423630034), and contains the initials of the original contributor (j, k & t).
