"""CyclesSinceLastPurchase feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.feature import DerivedFeature
from .average_purchase_cycle import AvgPurchaseCycle
from jstark.features.recency_days import RecencyDays


class CyclesSinceLastOrder(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            RecencyDays(
                self.as_at,
                self.feature_period,
                first_day_of_week=self._first_day_of_week,
            ).column,
            AvgPurchaseCycle(
                self.as_at,
                self.feature_period,
                first_day_of_week=self._first_day_of_week,
            ).column,
        )

    @property
    def description_subject(self) -> str:
        return "Cycles since last order"

    @property
    def commentary(self) -> str:
        return (
            "Days since last order divided by average purchase cycle. "
            + "This may be a predictor of when a customer is "
            + "likely to next order something."
        )

    def default_value(self) -> Column:
        return f.lit(None)
