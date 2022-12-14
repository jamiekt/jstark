import pyspark.sql.functions as f
from pyspark.sql import Column

from .min_feature import Min


class MinGrossPrice(Min):
    def column_expression(self) -> Column:
        return f.col("GrossSpend") / f.col("Quantity")

    @property
    def description_subject(self) -> str:
        return "Minimum of (GrossSpend / Quantity)"

    @property
    def commentary(self) -> str:
        return (
            "The net price is calculated as the gross "
            + "spend divided by how many were bought."
        )
