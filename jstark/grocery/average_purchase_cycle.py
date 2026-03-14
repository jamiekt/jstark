"""AvgPurchaseCycle feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.feature import DerivedFeature
from jstark.features.earliest_purchase_date import EarliestPurchaseDate
from jstark.features.most_recent_purchase_date import MostRecentPurchaseDate
from .basket_count import BasketCount


class AvgPurchaseCycle(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            f.datediff(
                MostRecentPurchaseDate(
                    self.as_at,
                    self.feature_period,
                    first_day_of_week=self._first_day_of_week,
                ).column,
                EarliestPurchaseDate(
                    self.as_at,
                    self.feature_period,
                    first_day_of_week=self._first_day_of_week,
                ).column,
            ),
            BasketCount(
                self.as_at,
                self.feature_period,
                first_day_of_week=self._first_day_of_week,
            ).column,
        )

    @property
    def description_subject(self) -> str:
        return "Average purchase cycle"

    @property
    def commentary(self) -> str:
        return (
            "How often (measured in days) is a purchase made. This "
            + "is very useful to determine how often a customer buys "
            + "a particular product"
        )

    def default_value(self) -> Column:
        return f.lit(None)
