"""AvgGrossSpendPerBasket feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import DerivedFeature
from .gross_spend import GrossSpend
from .basket_count import BasketCount


class AvgGrossSpendPerBasket(DerivedFeature):
    def column_expression(self) -> Column:
        return f.try_divide(
            GrossSpend(self.as_at, self.feature_period).column,
            BasketCount(self.as_at, self.feature_period).column,
        )

    @property
    def description_subject(self) -> str:
        return "Average GrossSpend per Basket"

    @property
    def commentary(self) -> str:
        return "Total GrossSpend divided by the number of baskets"

    def default_value(self) -> Column:
        return f.lit(None)
