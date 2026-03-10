"""OrderCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class OrderCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Order")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Orders"

    @property
    def commentary(self) -> str:
        return (
            "The number of orders. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same order, "
            + "this feature allows you to determine how many distinct orders "
            + "existed in that collection of orders."
        )
