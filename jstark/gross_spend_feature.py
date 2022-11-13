from .feature import Feature
from datetime import date
from .feature import FeaturePeriod
from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as f


class GrossSpendFeature(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def columnExpression(self, df: DataFrame) -> Column:
        return f.sum(df["GrossSpend"]).alias("GrossSpend_2d1")
