from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import Feature, FeaturePeriod


class CustomerCount(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def aggregator(self) -> Callable[[Column], Column]:
        return self.count_distinct_aggregator

    def column_expression(self) -> Column:
        return f.col("Customer")

    def default_value(self) -> Column:
        return f.lit(0)

    @property
    def description_subject(self) -> str:
        return "Distinct count of Customers"

    @property
    def commentary(self) -> str:
        return (
            "The number of customers. Typically the dataframe supplied "
            + "to this feature will have many transactions for many baskets each bought"
            + " a different customer, this feature allows you to determine how many "
            + "disinct customerss bought those baskets."
        )
