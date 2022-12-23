"""CyclesSinceLastPurchase feature"""
import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import DerivedFeature
from .earliest_purchase_date import EarliestPurchaseDate
from .most_recent_purchase_date import MostRecentPurchaseDate
from .basket_count import BasketCount


class CyclesSinceLastPurchase(DerivedFeature):
    def column_expression(self) -> Column:
        return (
            f.datediff(
                MostRecentPurchaseDate(self.as_at, self.feature_period).column,
                EarliestPurchaseDate(self.as_at, self.feature_period).column,
            )
        ) / BasketCount(self.as_at, self.feature_period).column

    @property
    def description_subject(self) -> str:
        return "Cycles since last purchase"

    @property
    def commentary(self) -> str:
        return (
            "Days since last purchase divided by average purchase cycle. "
            + "This may be a very good predictor of when a customer is "
            + "likely to next buy a particular product."
        )

    def default_value(self) -> Column:
        return f.lit(None)
