"""Count feature"""

from typing import Callable
import pyspark.sql.functions as f
from pyspark.sql import Column


from jstark.features.feature import BaseFeature


class Count(BaseFeature):
    def aggregator(self) -> Callable[[Column], Column]:
        return self.count_aggregator

    def column_expression(self) -> Column:
        return f.lit(1)

    def default_value(self) -> Column:
        return f.lit(0)

    @property
    def description_subject(self) -> str:
        return "Count of rows"
