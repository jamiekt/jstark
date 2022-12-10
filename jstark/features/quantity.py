from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .sum_feature import Sum


class Quantity(Sum):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.col("Quantity")

    @property
    def description_subject(self) -> str:
        return "Sum of Quantity"

    def default_value(self) -> Column:
        return f.lit(0)
