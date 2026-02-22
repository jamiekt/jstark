"""MinNetSpend feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .min_feature import Min


class MinNetSpend(Min):
    def column_expression(self) -> Column:
        return f.col("NetSpend")

    @property
    def description_subject(self) -> str:
        return "Minimum of NetSpend value"
