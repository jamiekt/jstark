"""AvgQuantityPerBasket feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.feature import DerivedFeature
from jstark.features.quantity import Quantity
from .order_count import OrderCount


class AvgQuantityPerOrder(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            Quantity(
                self.as_at,
                self.feature_period,
                first_day_of_week=self._first_day_of_week,
            ).column,
            OrderCount(
                self.as_at,
                self.feature_period,
                first_day_of_week=self._first_day_of_week,
            ).column,
        )

    @property
    def description_subject(self) -> str:
        return "Average Quantity per Order"

    @property
    def commentary(self) -> str:
        return (
            "Total Quantity divided by the number of orders. "
            + "Very useful to know how large, on average, each order is."
        )

    def default_value(self) -> Column:
        return f.lit(None)
