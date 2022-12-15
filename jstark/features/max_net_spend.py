"""MaxNetSpend feature"""
import pyspark.sql.functions as f
from pyspark.sql import Column

from .max_feature import Max


class MaxNetSpend(Max):
    def column_expression(self) -> Column:
        return f.col("NetSpend")

    @property
    def description_subject(self) -> str:
        return "Maximum of NetSpend value"
