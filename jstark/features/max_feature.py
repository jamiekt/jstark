from abc import ABCMeta
from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import BaseFeature, FeaturePeriod


class Max(BaseFeature, metaclass=ABCMeta):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def aggregator(self) -> Callable[[Column], Column]:
        return self.max_aggregator

    def default_value(self) -> Column:
        return f.lit(0.0)
