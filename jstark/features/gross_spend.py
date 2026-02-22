"""GrossSpend feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .sum_feature import Sum


class GrossSpend(Sum):
    def column_expression(self) -> Column:
        return f.col("GrossSpend")

    @property
    def description_subject(self) -> str:
        return "Sum of GrossSpend"

    @property
    def commentary(self) -> str:
        return (
            "The definition of GrossSpend can be whatever you want "
            + "it to be though typically its the price inclusive of any tax paid"
        )
