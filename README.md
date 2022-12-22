# jstark

[![PyPI - Version](https://img.shields.io/pypi/v/jstark.svg)](https://pypi.org/project/jstark)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jstark.svg)](https://pypi.org/project/jstark)
![build](https://github.com/jamiekt/jstark/actions/workflows/build.yml/badge.svg)
[![coverage](https://jamiekt.github.io/jstark/coverage.svg 'Click to see coverage report')](https://jamiekt.github.io/jstark/htmlcov/)
[![pylint](https://jamiekt.github.io/jstark/pylint.svg 'Click to view pylint report')](https://jamiekt.github.io/jstark/pylint.html)


Built with [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)

-----
<!---
## Installation

```console
pip install jstark
```
-->
## Introduction

Data science relies on summarising data so it can be used in machine learning models, those summarised data values
are referred to as *features*. Some common requirements when summarising data as features are:

* ability to create features as they were at a certain point in time
* ability to create features that summarise different time periods (for example, each month of a year)
without having to run the feature creation multiple times
* a consistent naming convention
* feature descriptions that accurately describe how a feature is derived

A popular technology for producing features is [Apache Spark](https://spark.apache.org/), specifically Spark's
python-based implementation, [PySpark](https://spark.apache.org/docs/latest/api/python/). jstark uses PySpark
to address the requirements listed above.

## Getting started

Create yourself a virtual environment and install pyspark followed by jstark

> **Note**
> As jstark is still a work-in-progress ("alpha" stage) it is being made available at
> [https://test.pypi.org/](https://test.pypi.org/) therefore it is required to install pyspark
> separately. When jstark is hosted at [https://pypi.org/](https://pypi.org/) this won't be necessary.

```shell
python3 -m venv venv && source venv/bin/activate && \
python -m pip install pyspark && \
python -m pip install faker && \
python -m pip install python-dateutil && \
python -m pip install -i https://test.pypi.org/simple/ jstark
```

### Your first jstark features

Run the following code that summarises the number of baskets per quarter in 2021 for a mythical grocery retailer

```python
from datetime import date
from jstark.sample.transactions import FakeTransactions
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator

input_df = FakeTransactions().get_df(seed=42, number_of_baskets=10000)
pfg = PurchasingFeatureGenerator(date(2022, 1, 1), ["4q4", "3q3", "2q2", "1q1"])
output_df = input_df.groupBy().agg(*pfg.features)
basket_counts_df = (output_df.
    select("BasketCount_4q4", "BasketCount_3q3", "BasketCount_2q2", "BasketCount_1q1"))
basket_counts_df.show()
```
```shell
+---------------+---------------+---------------+---------------+
|BasketCount_4q4|BasketCount_3q3|BasketCount_2q2|BasketCount_1q1|
+---------------+---------------+---------------+---------------+
|           2458|           2501|           2498|           2543|
+---------------+---------------+---------------+---------------+
```

### Descriptions

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

### groupBy examples

Typically you'll want to aggregate the data over a dimension, stores perhaps

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
|Hammersmith|            523|            529|            503|            492|
|    Staines|            461|            520|            493|            504|
|   Richmond|            500|            483|            503|            512|
|     Ealing|            490|            460|            485|            515|
| Twickenham|            484|            509|            514|            520|
+-----------+---------------+---------------+---------------+---------------+
```

And then perhaps examine different features, number of baskets and number of customers in the first half of 2021 versus the second half:

```python
pfg2 = PurchasingFeatureGenerator(date(2022, 1, 1),["12m7","6m1"])
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
|    Staines|             981|               974|            997|              986|
| Twickenham|             993|               986|           1034|             1023|
|     Ealing|             950|               937|           1000|              993|
|Hammersmith|            1052|              1043|            995|              990|
|   Richmond|             983|               976|           1015|             1006|
+-----------+----------------+------------------+---------------+-----------------+
```
*Some customers shop more than once hence CustomerCount is lower than BasketCount.*

At this point you may wonder what other features are available other than BasketCount & CustomerCount

```python
pprint({c.metadata["name-stem"] for c in df.schema})
```
```shell
{'ApproxBasketCount',
 'ApproxCustomerCount',
 'AverageGrossSpendPerBasket',
 'AvgQuantityPerBasket',
 'BasketCount',
 'ChannelCount',
 'Count',
 'CustomerCount',
 'Discount',
 <snip>
 ...
 ...
```

Again, descriptions are available for all:

```python
pprint([(c.name,c.metadata["description"]) for c in output_df.schema if c.name.endswith("1q1")])
```

### References

All of these features expect certain columns to be in the input dataframe. All of them require a column called Timestamp (of type [TimestampType()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampType.html)) to indicate when the activity occurred and most expect additional columns too. BasketCount for example expects a column called Basket.

jstark can provide this information

```python
pfg.references["BasketCount_1q1"]                  # ['Basket', 'Timestamp']
pfg.references["CustomerCount_1q1"]                # ['Customer', 'Timestamp']
pfg.references["AverageGrossSpendPerBasket_1q1"]   # ['Basket', 'GrossSpend', 'Timestamp']
```

To find out what all columns are required by all features

```python
pprint({k.split("_")[0]:v for (k,v) in pfg.references.items()})
```
```shell
{'ApproxBasketCount': ['Basket', 'Timestamp'],
 'ApproxCustomerCount': ['Customer', 'Timestamp'],
 'AverageGrossSpendPerBasket': ['Basket', 'GrossSpend', 'Timestamp'],
 'AvgQuantityPerBasket': ['Basket', 'Quantity', 'Timestamp'],
 'BasketCount': ['Basket', 'Timestamp'],
 'ChannelCount': ['Channel', 'Timestamp'],
 'Count': ['Timestamp'],
 'CustomerCount': ['Customer', 'Timestamp'],
 'Discount': ['Discount', 'Timestamp'],
 <snip>
 ...
 ...
```


## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Frequently Asked Questions

### Why is it called jstark?

jstark:
* is phonetically similar to PySpark (which jstark requires)
* is a homage to
[comic book character Jon Stark](https://www.worthpoint.com/worthopedia/football-picture-story-monthly-stark-423630034)
whom the original contributor was a fan of many many years ago
* contains all the initials of the original contributor (j, k & t).
