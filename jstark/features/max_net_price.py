import pyspark.sql.functions as f
from pyspark.sql import Column

from .max_feature import Max


class MaxNetPrice(Max):
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
