import random
import uuid
from datetime import date
from typing import Dict, Any, Iterable
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from faker import Faker
from faker.providers import DynamicProvider


class FakeTransactions:
    def get_df(self, seed: int = 42, number_of_baskets: int = 1000) -> DataFrame:
        purchases_schema = StructType(
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

        stores_provider = DynamicProvider(
            provider_name="store",
            elements=["Hammersmith", "Ealing", "Richmond", "Twickenham", "Staines"],
        )
        products_provider = DynamicProvider(
            provider_name="product",
            elements=[
                ("Custard Creams", "Ambient", 1.00),
                ("Carrots", "Fresh", 0.69),
                ("Cheddar", "Dairy", 3.43),
                ("Ice Cream", "Frozen", 5.32),
                ("Milk 1l", "Dairy", 1.70),
                ("Apples", "Fresh", 1.50),
                ("Beer", "BWS", 8.50),
                ("Wine", "BWS", 7.50),
                ("Yoghurt", "Dairy", 0.99),
                ("Bananas", "Fresh", 0.79),
                ("Nappies", "Baby", 15.00),
                ("Baby formula", "Baby", 5.99),
                ("Whisky", "BWS", 23.00),
            ],
        )
        channels_provider = DynamicProvider(
            provider_name="channel",
            elements=["Instore", "Online", "Click and Collect"],
        )

        fake = Faker()
        Faker.seed(seed)
        fake.add_provider(stores_provider)
        fake.add_provider(channels_provider)

        products_fake = Faker()
        products_fake.add_provider(products_provider)

        transactions = []

        for _ in range(number_of_baskets):
            items = []
            random.seed(seed)
            for _ in range(random.randint(1, len(products_provider.elements))):
                p = products_fake.unique.product()
                random.seed(seed)
                quantity = random.choices(
                    [1, 2, 3, 4, 5], weights=[100, 40, 20, 10, 3]
                )[0]
                gross_spend: float = round(p[2] * quantity, 2)
                net_spend = round((gross_spend * random.random()), 2)
                discount = round((gross_spend - net_spend) * random.random(), 2)
                items.append(
                    {
                        "Product": p[0],
                        "ProductCategory": p[1],
                        "Quantity": quantity,
                        "GrossSpend": Decimal(gross_spend),
                        "NetSpend": Decimal(net_spend),
                        "Discount": Decimal(discount),
                    }
                )
                transactions.append(
                    {
                        "Customer": fake.name(),
                        "Homecity": fake.city(),
                        "Store": fake.store(),
                        "Timestamp": fake.date_time_between(
                            start_date=date(2021, 1, 1), end_date=date(2021, 12, 31)
                        ),
                        "Basket": str(uuid.uuid4()),
                        "Channel": fake.channel(),
                        "items": items,
                    }
                )
            products_fake.unique.clear()

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
            for d2 in d["items"]
        ]
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(
            flattened_transactions, schema=purchases_schema  # type: ignore
        )
