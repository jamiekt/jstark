"""BasketCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class BasketCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Basket")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Baskets"

    @property
    def commentary(self) -> str:
        return (
            "The number of baskets. Typically the dataframe supplied "
            + "to this feature will have many transactions for the same basket, "
            + "this feature allows you to determine how many shopping baskets "
            + "existed in that collection of transactions."
        )
