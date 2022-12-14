import pyspark.sql.functions as f
from pyspark.sql import Column

from .sum_feature import Sum


class Quantity(Sum):
    def column_expression(self) -> Column:
        return f.col("Quantity")

    @property
    def description_subject(self) -> str:
        return "Sum of Quantity"

    def default_value(self) -> Column:
        return f.lit(0)
