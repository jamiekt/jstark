from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .sum_feature import Sum


class NetSpend(Sum):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

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
