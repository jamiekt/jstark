from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any

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
    transactions = [
        {
            "Timestamp": datetime.now(),
            "Customer": "Leia",
            "Store": "Hammersmith",
            "Channel": "Instore",
            "Basket": "basket1",
            "items": [
                {"Product": "Cheddar", "Quantity": 2, "GrossSpend": Decimal(2.50)},
                {"Product": "Grapes", "Quantity": 1, "GrossSpend": Decimal(3.00)},
            ],
        },
        {
            "Timestamp": datetime.now(),
            "Customer": "Luke",
            "Store": "Ealing",
            "Channel": "Instore",
            "Basket": "basket2",
            "items": [
                {
                    "Product": "Custard Creams",
                    "Quantity": 1,
                    "GrossSpend": Decimal(3.00),
                }
            ],
        },
    ]
    flattened_transactions: List[Dict[str, Any]] = []
    for transaction in transactions:
        flattened_transactions.extend(
            {
                "Timestamp": transaction["Timestamp"],
                "Customer": transaction["Customer"],
                "Store": transaction["Store"],
                "Channel": transaction["Channel"],
                "Basket": transaction["Basket"],
                "Product": item["Product"],
                "Quantity": item["Quantity"],
                "GrossSpend": item["GrossSpend"],
            }
            for item in transaction["items"]  # type: ignore
        )
    return spark.createDataFrame(
        flattened_transactions, schema=purchases_schema  # type: ignore
    )
