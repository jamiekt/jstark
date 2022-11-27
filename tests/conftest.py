from decimal import Decimal
from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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
def dataframe_of_purchases(purchases_schema) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(
        data=[
            (
                datetime.now(),
                "Leia",
                "Hammersmith",
                "Instore",
                "Cheddar",
                2,
                "Basket1",
                Decimal(2.50),
            ),
            (
                datetime.now(),
                "Leia",
                "Hammersmith",
                "Instore",
                "Grapes",
                1,
                "Basket1",
                Decimal(3.00),
            ),
        ],
        schema=purchases_schema,
    )
