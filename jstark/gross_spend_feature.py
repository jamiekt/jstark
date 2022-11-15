from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame

from .feature import Feature, FeaturePeriod


class GrossSpend(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def columnExpression(self, df: DataFrame) -> Column:
        return f.sum(df["GrossSpend"]).alias(self.feature_name)
