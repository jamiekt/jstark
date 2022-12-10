from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .max_feature import Max


class MaxNetPrice(Max):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.col("NetSpend") / f.col("Quantity")

    @property
    def description_subject(self) -> str:
        return "Maximum of (NetSpend / Quantity)"

    @property
    def commentary(self) -> str:
        return (
            "The net price is calculated as the net "
            + "spend divided by how many were bought."
        )
