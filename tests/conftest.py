import uuid
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, Any, Iterable
from dateutil.relativedelta import relativedelta


import pytest
from pyspark.sql import DataFrame, SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def as_at_timestamp() -> datetime:
    """return a datetime to be used as as_at. This can be used in other fixtures to
    generate datetimes relative to this datetime
    """
    # This is a Wednesday. That's important to know because
    # it affects week calculations.
    return datetime(2022, 11, 30, 10, 12, 13)


@pytest.fixture(scope="session")
def purchases_schema() -> StructType:
    return StructType(
        [
            StructField("Timestamp", TimestampType(), True),
            StructField("Customer", StringType(), True),
            StructField("Store", StringType(), True),
            StructField("Channel", StringType(), True),
            StructField("Product", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Basket", StringType(), True),
            StructField("GrossSpend", DecimalType(10, 2), True),
            StructField("NetSpend", DecimalType(10, 2), True),
            StructField("Discount", DecimalType(10, 2), True),
        ]
    )


@pytest.fixture(scope="session")
def dataframe_of_purchases(
    spark_session: SparkSession, as_at_timestamp: datetime, purchases_schema: StructType
) -> DataFrame:
    transactions = [
        {
            "Timestamp": as_at_timestamp - relativedelta(months=12),
            "Customer": "Luke",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "Kleenex",
                    "Quantity": 1,
                    "GrossSpend": Decimal(8),
                    "NetSpend": Decimal(7.75),
                    "Discount": Decimal(0.5),
                }
            ],
        },
        {
            "Timestamp": as_at_timestamp - relativedelta(months=2),
            "Customer": "Luke",
            "Store": "Twickenham",
            "Channel": "Online",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "WD40",
                    "Quantity": 2,
                    "GrossSpend": Decimal(7),
                    "NetSpend": Decimal(6.75),
                    "Discount": Decimal(0.0),
                }
            ],
        },
        {
            "Timestamp": as_at_timestamp - relativedelta(months=1),
            "Customer": "Leia",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "Tiger Bread",
                    "Quantity": 3,
                    "GrossSpend": Decimal(6),
                    "NetSpend": Decimal(5.75),
                    "Discount": Decimal(0.1),
                }
            ],
        },
        {
            "Timestamp": as_at_timestamp - timedelta(days=4),
            "Customer": "Luke",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "Apples",
                    "Quantity": 6,
                    "GrossSpend": Decimal(3.25),
                    "NetSpend": Decimal(3.0),
                    "Discount": Decimal(0.75),
                }
            ],
        },
        {
            "Timestamp": as_at_timestamp - timedelta(days=1),
            "Customer": "Luke",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "Custard Creams",
                    "Quantity": 1,
                    "GrossSpend": Decimal(4.00),
                    "NetSpend": Decimal(3.75),
                    "Discount": Decimal(0.0),
                }
            ],
        },
        {
            "Timestamp": as_at_timestamp,
            "Customer": "Leia",
            "Store": "Hammersmith",
            "Channel": "Instore",
            "Basket": uuid.uuid4(),
            "items": [
                {
                    "Product": "Cheddar",
                    "Quantity": 2,
                    "GrossSpend": Decimal(2.50),
                    "NetSpend": Decimal(2.25),
                    "Discount": Decimal(0.1),
                },
                {
                    "Product": "Grapes",
                    "Quantity": 1,
                    "GrossSpend": Decimal(3.00),
                    "NetSpend": Decimal(2.75),
                    "Discount": Decimal(0.1),
                },
            ],
        },
    ]
    flattened_transactions: Iterable[Dict[str, Any]] = [
        {
            "Customer": d["Customer"],
            "Store": d["Store"],
            "Basket": d["Basket"],
            "Channel": d["Channel"],
            "Timestamp": d["Timestamp"],
            **d2,
        }
        for d in transactions
        for d2 in d["items"]  # type: ignore
    ]
    return spark_session.createDataFrame(
        flattened_transactions, schema=purchases_schema  # type: ignore
    )


@pytest.fixture(scope="session")
def luke_and_leia_purchases(dataframe_of_purchases: DataFrame) -> DataFrame:
    return dataframe_of_purchases.where(
        (f.col("Customer") == "Leia") | (f.col("Customer") == "Luke")
    )


@pytest.fixture(scope="session")
def purchasing_feature_generator(
    as_at_timestamp: datetime,
) -> PurchasingFeatureGenerator:
    return PurchasingFeatureGenerator(
        as_at=as_at_timestamp.date(),
        feature_periods=[
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 2),
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 0),
            FeaturePeriod(PeriodUnitOfMeasure.MONTH, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 0),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 0),
            FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0),
            FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 0),
            FeaturePeriod(PeriodUnitOfMeasure.YEAR, 2, 0),
        ],
    )


@pytest.fixture(scope="session")
def luke_and_leia_purchases_first(
    luke_and_leia_purchases: DataFrame,
    purchasing_feature_generator: PurchasingFeatureGenerator,
) -> Row:
    """
    If we only collect once, the tests should run quicker
    """
    df = luke_and_leia_purchases.groupBy().agg(*purchasing_feature_generator.features)
    first = df.first()
    assert first is not None
    return first
