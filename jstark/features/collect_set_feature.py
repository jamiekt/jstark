"""CollectSet abstract base class"""

from abc import ABCMeta
from typing import Callable

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import BaseFeature


class CollectSet(BaseFeature, metaclass=ABCMeta):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.collect_set_aggregator

    def default_value(self) -> Column:
        return f.lit([])
