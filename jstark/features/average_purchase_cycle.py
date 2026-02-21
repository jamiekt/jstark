"""AveragePurchaseCycle feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import DerivedFeature
from .earliest_purchase_date import EarliestPurchaseDate
from .most_recent_purchase_date import MostRecentPurchaseDate
from .basket_count import BasketCount


class AvgPurchaseCycle(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            f.datediff(
                MostRecentPurchaseDate(self.as_at, self.feature_period).column,
                EarliestPurchaseDate(self.as_at, self.feature_period).column,
            ),
            BasketCount(self.as_at, self.feature_period).column,
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
