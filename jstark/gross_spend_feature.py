from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame

from .feature import Feature, FeaturePeriod


class GrossSpend(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod, df: DataFrame) -> None:
        super().__init__(as_at, feature_period, df)

    def columnExpression(self) -> Column:
        return f.sum(self.df["GrossSpend"])
