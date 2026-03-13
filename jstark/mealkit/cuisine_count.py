"""CuisineCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class CuisineCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Cuisine")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Cuisines"

    @property
    def commentary(self) -> str:
        return (
            "The number of cuisines. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same cuisine, "
            + "this feature allows you to determine how many distinct cuisines "
            + "have been ordered."
        )
