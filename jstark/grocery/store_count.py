"""StoreCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class StoreCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Store")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Stores"

    @property
    def commentary(self) -> str:
        return (
            "The number of stores. Typically the dataframe supplied "
            + "to this feature will have many transactions for many baskets for "
            + "many stores, this feature allows you to determine how many "
            + "stores those baskets were purchased in."
        )
