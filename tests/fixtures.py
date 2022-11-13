from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType
)


@pytest.fixture
def purchases_schema():
    return StructType(
        [
            StructField("Customer", StringType(), True),
            StructField("Store", StringType(), True),
            StructField("Channel", StringType(), True),
            StructField("Product", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Basket", StringType(), True),
            StructField("GrossSpend", DecimalType(10, 2), True),
        ]
    )


@pytest.fixture
def dataframe_of_purchases(purchases_schema) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(
        data=[
            ("Leia", "Hammersmith", "Instore", "Cheddar", 2, "Basket1", Decimal(2.50))
        ],
        schema=purchases_schema,
    )
