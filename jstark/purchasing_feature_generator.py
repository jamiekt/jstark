from datetime import date
from typing import List

from pyspark.sql import Column
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.feature import Feature


class PurchasingFeatureGenerator(object):
    def __init__(self, as_at: date) -> None:
        self.__as_at = as_at

    def get_features(self) -> List[Column]:
        feature_periods = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ]
        feature_classes_and_periods = (
            (cls, fp) for cls in Feature.get_subclasses() for fp in feature_periods
        )
        features = [
            f[0](as_at=self.__as_at, feature_period=f[1])
            for f in feature_classes_and_periods
        ]
        return [f.column for f in features]
