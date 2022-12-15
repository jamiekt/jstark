"""MaxGrossSpend feature"""
import pyspark.sql.functions as f
from pyspark.sql import Column

from .max_feature import Max


class MaxGrossSpend(Max):
    def column_expression(self) -> Column:
        return f.col("GrossSpend")

    @property
    def description_subject(self) -> str:
        return "Maximum GrossSpend value"
