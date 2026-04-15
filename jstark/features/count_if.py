"""CountIf feature"""

from typing import Callable
import pyspark.sql.functions as f
from pyspark.sql import Column


from jstark.features.feature import BaseFeature


class CountIf(BaseFeature):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.count_if_aggregator

    def default_value(self) -> Column:
        return f.lit(0)
