import pyspark.sql.functions as f
from pyspark.sql import Column

from .sum_feature import Sum


class Discount(Sum):
    def column_expression(self) -> Column:
        return f.col("Discount")

    @property
    def description_subject(self) -> str:
        return "Sum of Discount"

    @property
    def commentary(self) -> str:
        return "Requires a field called `Discount` in the input data."
