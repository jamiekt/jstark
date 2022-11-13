from pyspark.sql import DataFrame
from datetime import date

from jstark.gross_spend_feature import GrossSpendFeature
from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure


class PurchasingFeatureGenerator(object):
    def __init__(self, as_at: date, df: DataFrame) -> None:
        self.__df = df
        self.__as_at = as_at

    def get_df(self):
        gross_spend_feature = GrossSpendFeature(
            as_at=self.__as_at,
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
        )
        expressions = [gross_spend_feature.columnExpression(df=self.__df)]
        return self.__df.agg(*expressions)
