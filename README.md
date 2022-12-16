# jstark

[![PyPI - Version](https://img.shields.io/pypi/v/jstark.svg)](https://pypi.org/project/jstark)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jstark.svg)](https://pypi.org/project/jstark)
![build](https://github.com/jamiekt/jstark/actions/workflows/build.yml/badge.svg)
[![coverage](https://jamiekt.github.io/jstark/coverage.svg 'Click to see coverage report')](https://jamiekt.github.io/jstark/htmlcov/)
[![pylint](https://jamiekt.github.io/jstark/pylint.svg 'Click to view pylint report')](https://jamiekt.github.io/jstark/pylint.html)


Built with [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)

-----

## Installation

```console
pip install jstark
```

## Getting started

Create yourself a virtual environment and install pyspark followed by jstark

> **Note**
> As jstark is not yet properly released it is being hosted at [https://test.pypi.org/](https://test.pypi.org/) therefore
> it is required to install pyspark separately. When jstark is hosted at [https://pypi.org/](https://pypi.org/) this
> won't be necessary.

```shell
python3 -m venv venv && source venv/bin/activate && \
python -m pip install pyspark && \
python -m pip install faker && \
python -m pip install python-dateutil && \
python -m pip install -i https://test.pypi.org/simple/ jstark
```

Run python and ~type~ paste the following code

```python
from datetime import date
from jstark.sample.transactions import FakeTransactions
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure

input_df = FakeTransactions().get_df(seed=42, number_of_baskets=100)
pfg = PurchasingFeatureGenerator(
        date(2022, 1, 1),
        [
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 3),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 2, 2),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
        ],
    )
output_df = input_df.groupBy().agg(*pfg.features)
basket_counts_df = (output_df.
    select("BasketCount_4q4", "BasketCount_3q3", "BasketCount_2q2", "BasketCount_1q1"))
basket_counts_df.show()
```
```shell
+---------------+---------------+---------------+---------------+
|BasketCount_4q4|BasketCount_3q3|BasketCount_2q2|BasketCount_1q1|
+---------------+---------------+---------------+---------------+
|            285|            259|            277|            268|
+---------------+---------------+---------------+---------------+
```

One of the benefits of jstark is all the features have descriptions in their metadata.

```python
from pprint import pprint
pprint([(c.name, c.metadata["description"]) for c in basket_counts_df.schema])
```
```shell
[('BasketCount_4q4',
  'Distinct count of Baskets between 2021-01-01 and 2021-03-31'),
 ('BasketCount_3q3',
  'Distinct count of Baskets between 2021-04-01 and 2021-06-30'),
 ('BasketCount_2q2',
  'Distinct count of Baskets between 2021-07-01 and 2021-09-30'),
 ('BasketCount_1q1',
  'Distinct count of Baskets between 2021-10-01 and 2021-12-31')]
```

Typically you'll want to aggregate the data over some dimension, stores perhaps

```python
output_stores_df = input_df.groupBy("Store").agg(*pfg.features)
(output_stores_df.
    select(
        "Store", "BasketCount_4q4", "BasketCount_3q3", "BasketCount_2q2", "BasketCount_1q1"
    )
    .orderBy("BasketCount_1q1")).show()
```
```shell
+-----------+---------------+---------------+---------------+---------------+
|      Store|BasketCount_4q4|BasketCount_3q3|BasketCount_2q2|BasketCount_1q1|
+-----------+---------------+---------------+---------------+---------------+
|Hammersmith|             60|             44|             51|             45|
|   Richmond|             54|             50|             55|             51|
|    Staines|             51|             46|             62|             51|
|     Ealing|             58|             61|             45|             53|
| Twickenham|             62|             58|             64|             68|
+-----------+---------------+---------------+---------------+---------------+
```

And then perhaps examine different features, number of baskets and number of customers in the first half of 2021 versus the second half:

```python
pfg2 = PurchasingFeatureGenerator(
        date(2022, 1, 1),
        [
            FeaturePeriod(PeriodUnitOfMeasure.MONTH, 12, 7),
            FeaturePeriod(PeriodUnitOfMeasure.MONTH, 6, 1)
        ]
    )
output_stores_h1h2_df = input_df.groupBy("Store").agg(*pfg2.features)
(output_stores_h1h2_df.
    select(
        "Store", "BasketCount_12m7", "CustomerCount_12m7", "BasketCount_6m1", "CustomerCount_6m1"
    )).show()
```
```shell
+-----------+----------------+------------------+---------------+-----------------+
|      Store|BasketCount_12m7|CustomerCount_12m7|BasketCount_6m1|CustomerCount_6m1|
+-----------+----------------+------------------+---------------+-----------------+
|    Staines|              97|                97|            113|              113|
| Twickenham|             120|               120|            132|              131|
|     Ealing|             119|               119|             98|               97|
|Hammersmith|             104|               104|             96|               96|
|   Richmond|             104|               104|            106|              106|
+-----------+----------------+------------------+---------------+-----------------+
```

At this point you may wonder what other features are available other than BasketCount

```python
pprint([c.name for c in output_df.schema if c.name.endswith("1q1")])
```

returns:

```shell
['Count_1q1',
 'NetSpend_1q1',
 'GrossSpend_1q1',
 'RecencyDays_1q1',
 'BasketCount_1q1',
 'StoreCount_1q1',
 <snip>
 ...
 ...
```

Again, descriptions are available for all:

```python
pprint([(c.name,c.metadata["description"]) for c in output_df.schema if c.name.endswith("1q1")])
```

## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Frequently Asked Questions

### Why is it called jstark?

jstark is phonetically similar to PySpark (which jstark requires), is a homage to
[comic book character Jon Stark](https://www.worthpoint.com/worthopedia/football-picture-story-monthly-stark-423630034)
whom the original contributor was a fan of many many years ago and also contains all the initials of the original
contributor (j, k & t).
