from .feature import Feature
from datetime import date
from .feature import FeaturePeriod
from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as f
from jstark.feature import PeriodUnitOfMeasure


class GrossSpendFeature(Feature):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def columnExpression(self, df: DataFrame) -> Column:
        return f.sum(df["GrossSpend"]).alias(
            f"GrossSpend_{self.feature_period.start}{'d' if self.feature_period.period_unit_of_measure.name == 'DAY' else 'w' if self.feature_period.period_unit_of_measure.name == 'WEEK' else 'm' if self.feature_period.period_unit_of_measure.name == 'MONTH' else 'q' if self.feature_period.period_unit_of_measure.name == 'QUARTER' else 'y'}{self.feature_period.end}"
        )
