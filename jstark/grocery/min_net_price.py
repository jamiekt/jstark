"""MinNetPrice feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.min_feature import Min


class MinNetPrice(Min):
    def column_expression(self) -> Column:
        return f.try_divide(f.col("NetSpend"), f.col("Quantity"))

    @property
    def description_subject(self) -> str:
        return "Minimum of (NetSpend / Quantity)"

    @property
    def commentary(self) -> str:
        return (
            "The net price is calculated as the net "
            + "spend dividied by how many were bought."
        )
