from datetime import date
from typing import List

from pyspark.sql import Column
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.feature import Feature


class PurchasingFeatureGenerator(object):
    def __init__(
        self,
        as_at: date,
        feature_periods: List[FeaturePeriod] = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ],
    ) -> None:
        self.as_at = as_at
        self.feature_periods = feature_periods

    @property
    def as_at(self) -> date:
        return self.__as_at

    @as_at.setter
    def as_at(self, value: date) -> None:
        self.__as_at = value

    @property
    def feature_periods(self) -> List[FeaturePeriod]:
        return self.__feature_periods

    @feature_periods.setter
    def feature_periods(self, value: List[FeaturePeriod]) -> None:
        self.__feature_periods = value

    def get_features(self) -> List[Column]:
        feature_classes_and_periods = (
            (cls, fp) for cls in Feature.get_subclasses() for fp in self.feature_periods
        )
        features = [
            f[0](as_at=self.as_at, feature_period=f[1])
            for f in feature_classes_and_periods
        ]
        return [f.column for f in features]
