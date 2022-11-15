from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame

from .feature import Feature, FeaturePeriod


class GrossSpendFeature(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def columnExpression(self, df: DataFrame) -> Column:
        uom_abbreviation = (
            "d"
            if self.feature_period.period_unit_of_measure.name == "DAY"
            else "w"
            if self.feature_period.period_unit_of_measure.name == "WEEK"
            else "m"
            if self.feature_period.period_unit_of_measure.name == "MONTH"
            else "q"
            if self.feature_period.period_unit_of_measure.name == "QUARTER"
            else "y"
        )
        return f.sum(df["GrossSpend"]).alias(
            f"GrossSpend_{self.feature_period.start}"
            + f"{uom_abbreviation}"
            + f"{self.feature_period.end}"
        )
