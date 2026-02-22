"""MinGrossSpen feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .min_feature import Min


class MinGrossSpend(Min):
    def column_expression(self) -> Column:
        return f.col("GrossSpend")

    @property
    def description_subject(self) -> str:
        return "Minimum GrossSpend value"
