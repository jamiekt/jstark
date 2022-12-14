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
python -m pip install python-dateutil && \
python -m pip install -i https://test.pypi.org/simple/ jstark
```

Run python and ~type~ paste the following code:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType, TimestampType
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
spark = SparkSession.builder.getOrCreate()
as_at_timestamp = datetime(2022, 11, 30, 10, 12, 13)
purchases_schema = StructType(
        [
            StructField("Timestamp", TimestampType(), True), StructField("Customer", StringType(), True),
            StructField("Store", StringType(), True), StructField("Channel", StringType(), True),
            StructField("Product", StringType(), True), StructField("Quantity", IntegerType(), True),
            StructField("Basket", StringType(), True), StructField("GrossSpend", DecimalType(10, 2), True),
            StructField("NetSpend", DecimalType(10, 2), True), StructField("Discount", DecimalType(10, 2), True),
        ]
transactions = [
        {
            "Timestamp": as_at_timestamp - timedelta(days=1),
            "Customer": "Luke",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [{"Product": "Custard Creams", "Quantity": 1, "GrossSpend": Decimal(4.00), "NetSpend": Decimal(3.75), "Discount": Decimal(0.0),}],
        },
        {
            "Timestamp": as_at_timestamp,
            "Customer": "Leia",
            "Store": "Hammersmith",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {"Product": "Cheddar", "Quantity": 2, "GrossSpend": Decimal(2.50), "NetSpend": Decimal(2.25), "Discount": Decimal(0.1),},
                {"Product": "Grapes", "Quantity": 1, "GrossSpend": Decimal(3.00), "NetSpend": Decimal(2.75), "Discount": Decimal(0.1),},
            ],
        },
    ]
    flattened_transactions: Iterable[Dict[str, Any]] = [
        {
            "Customer": d["Customer"], "Store": d["Store"],
            "Basket": d["Basket"], "Channel": d["Channel"],
            "Timestamp": d["Timestamp"], **d2,
        }
        for d in transactions
        for d2 in d["items"]
    ]
df = spark.createDataFrame(
        flattened_transactions, schema=purchases_schema
    )
purchasing_feature_generator = PurchasingFeatureGenerator(
        as_at=as_at_timestamp.date(),
        feature_periods=[
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 0),
        ],
    )
df = df.groupBy().agg(*purchasing_feature_generator.features)
df.collect()
```


## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Frequently Asked Questions


### Why is it called jstark?

jstark is phonetically similar to PySpark (which jstark requires) and is a homage to [comic book character Jon Stark](https://www.worthpoint.com/worthopedia/football-picture-story-monthly-stark-423630034) who I was a fan of many many years ago.
