"""MaxGrossPrice feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.max_feature import Max


class MaxGrossPrice(Max):
    def column_expression(self) -> Column:
        return f.try_divide(f.col("GrossSpend"), f.col("Quantity"))

    @property
    def description_subject(self) -> str:
        return "Maximum of (GrossSpend / Quantity)"

    @property
    def commentary(self) -> str:
        return (
            "The gross price is calculated as the gross "
            + "spend divided by how many were bought."
        )
