from abc import ABCMeta

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import BaseFeature


class DistinctCount(BaseFeature, metaclass=ABCMeta):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.count_distinct_aggregator

    def default_value(self) -> Column:
        return f.lit(0)
