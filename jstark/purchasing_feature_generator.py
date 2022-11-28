from datetime import date
from typing import List

from pyspark.sql import Column
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.gross_spend_feature import GrossSpend


class PurchasingFeatureGenerator(object):
    def __init__(self, as_at: date) -> None:
        self.__as_at = as_at

    def get_features(self) -> List[Column]:
        gross_spend = GrossSpend(
            as_at=self.__as_at,
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
        )
        return [feature.column for feature in [gross_spend]]
