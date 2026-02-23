"""NetSpend feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.sum_feature import Sum


class NetSpend(Sum):
    def column_expression(self) -> Column:
        return f.col("NetSpend")

    @property
    def description_subject(self) -> str:
        return "Sum of NetSpend"

    @property
    def commentary(self) -> str:
        return (
            "The definition of NetSpend can be whatever you want "
            + "it to be though typically its the price exclusive of any tax paid"
        )
