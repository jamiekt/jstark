"""Min abstract base class"""

from abc import ABCMeta
from typing import Callable

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import BaseFeature


class Min(BaseFeature, metaclass=ABCMeta):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.min_aggregator

    def default_value(self) -> Column:
        return f.lit(0.0)
