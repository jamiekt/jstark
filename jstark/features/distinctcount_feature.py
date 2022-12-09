from abc import ABCMeta
from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import Feature, FeaturePeriod


class DistinctCount(Feature, metaclass=ABCMeta):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def aggregator(self) -> Callable[[Column], Column]:
        return self.count_distinct_aggregator

    def default_value(self) -> Column:
        return f.lit(0)
