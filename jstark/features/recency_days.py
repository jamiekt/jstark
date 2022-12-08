from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import Feature, FeaturePeriod


class RecencyDays(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def aggregator(self) -> Callable[[Column], Column]:
        return self.sum_aggregator

    def column_expression(self) -> Column:
        return f.datediff(f.lit(self.as_at), f.col("Timestamp"))

    def default_value(self) -> Column:
        return f.lit(0)

    @property
    def description_subject(self) -> str:
        return "Minimum number of days since occurrence"

    @property
    def commentary(self) -> str:
        return (
            "This could be particularly useful (for example) in a grocery retailer "
            + "for determining when a customer most recently bought a product or "
            + " when a product was most recently bought in a store"
        )
