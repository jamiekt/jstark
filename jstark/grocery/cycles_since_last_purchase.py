"""CyclesSinceLastPurchase feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.feature import DerivedFeature
from .average_purchase_cycle import AvgPurchaseCycle
from .recency_days import RecencyDays


class CyclesSinceLastPurchase(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            RecencyDays(self.as_at, self.feature_period).column,
            AvgPurchaseCycle(self.as_at, self.feature_period).column,
        )

    @property
    def description_subject(self) -> str:
        return "Cycles since last purchase"

    @property
    def commentary(self) -> str:
        return (
            "Days since last purchase divided by average purchase cycle. "
            + "This may be a predictor of when a customer is "
            + "likely to next buy a particular product."
        )

    def default_value(self) -> Column:
        return f.lit(None)
