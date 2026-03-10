import random
from functools import cached_property
import uuid
from datetime import date
from typing import Any, Iterable
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType,
)
from faker import Faker
from faker.providers import DynamicProvider


class FakeMealkitOrders:
    def __init__(self, seed: int | None = None, number_of_orders: int = 1000):
        self.seed = seed
        self.number_of_orders = number_of_orders

    @property
    def mealkit_orders_schema(self) -> StructType:
        return StructType(
            [
                StructField("Timestamp", TimestampType(), True),
                StructField("Customer", StringType(), True),
                StructField("Product", StringType(), True),
                StructField("Recipe", StringType(), True),
                StructField("Quantity", IntegerType(), True),
                StructField("Order", StringType(), True),
                StructField("Discount", DecimalType(10, 2), True),
            ]
        )

    @staticmethod
    def flatten_mealkit_orders(mealkit_orders: list[Any]) -> Iterable[dict[str, Any]]:
        return [
            {
                "Customer": d["Customer"],
                "Product": d["Product"],
                "Order": d["Order"],
                "Timestamp": d["Timestamp"],
                **d2,
            }
            for d in mealkit_orders
            for d2 in d["Recipes"]
        ]

    @cached_property
    def df(self) -> DataFrame:

        products_provider = DynamicProvider(
            provider_name="product",
            elements=[
                "classic-plan",
                "preset-box-bc",
                "balanced-living-t1",
                "dinner-box",
                "classic-plan-t11",
                "classic-plan-t12",
                "classic-plan-t13",
                "classic-plan-t14",
                "classic-plan-t15",
            ],
        )
        recipes_provider = DynamicProvider(
            provider_name="recipe",
            elements=[
                "Banging bangers and mash",
                "Fish and chips",
                "Pizza and salad",
                "Chicken curry",
                "Beef stew",
                "Vegetable lasagna",
                "Salad and bread",
                "Soup and bread",
                "Pasta and sauce",
            ],
        )

        fake = Faker()
        if self.seed:
            Faker.seed(self.seed)

        products_fake = Faker()
        products_fake.add_provider(products_provider)

        recipes_fake = Faker()
        recipes_fake.add_provider(recipes_provider)

        mealkit_orders = []

        possible_quantities = [1, 2]
        if self.seed:
            random.seed(self.seed)
        quantities = random.choices(
            possible_quantities,
            weights=[100, 1],
            k=self.number_of_orders * len(recipes_provider.elements),
        )
        for order in range(self.number_of_orders):
            recipes = []
            # if self.seed:
            #     random.seed(self.seed)
            for recipe_index in range(random.randint(2, 5)):
                r = recipes_fake.unique.recipe()
                quantity = quantities[(order * len(possible_quantities)) + recipe_index]
                recipes.append(
                    {
                        "Recipe": r,
                        "Quantity": quantity,
                    }
                )
            mealkit_orders.append(
                {
                    "Customer": fake.name(),
                    "Timestamp": fake.date_time_between(
                        start_date=date(2021, 1, 1), end_date=date(2021, 12, 31)
                    ),
                    "Order": str(uuid.uuid4()),
                    "Product": products_fake.product(),
                    "Recipes": recipes,
                    "Discount": Decimal(random.uniform(0, 5)),
                }
            )
            recipes_fake.unique.clear()

        flattened_mealkit_orders = self.flatten_mealkit_orders(mealkit_orders)
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(
            flattened_mealkit_orders,
            schema=self.mealkit_orders_schema,  # type: ignore
        )
