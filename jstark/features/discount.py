from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .sum_feature import Sum


class Discount(Sum):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.col("Discount")

    @property
    def description_subject(self) -> str:
        return "Sum of Discount"

    @property
    def commentary(self) -> str:
        return "Requires a field called `Discount` in the input data."
