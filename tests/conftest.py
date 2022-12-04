import uuid
from decimal import Decimal
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, Iterable

import pytest
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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
        ]
    )


@pytest.fixture(scope="session")
def dataframe_of_purchases(
    spark_session: SparkSession, as_at_timestamp: datetime, purchases_schema: StructType
) -> DataFrame:
    transactions = [
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
                {"Product": "Cheddar", "Quantity": 2, "GrossSpend": Decimal(2.50)},
                {"Product": "Grapes", "Quantity": 1, "GrossSpend": Decimal(3.00)},
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
def luke_and_leia_purchases(dataframe_of_purchases):
    return dataframe_of_purchases.where(
        (f.col("Customer") == "Leia") | (f.col("Customer") == "Luke")
    )
