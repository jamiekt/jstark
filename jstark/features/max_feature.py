from abc import ABCMeta

import pyspark.sql.functions as f
from pyspark.sql import Column
from typing import Callable

from .feature import BaseFeature


class Max(BaseFeature, metaclass=ABCMeta):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.max_aggregator

    def default_value(self) -> Column:
        return f.lit(None)
