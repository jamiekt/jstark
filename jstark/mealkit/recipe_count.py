"""RecipeCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class RecipeCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Recipe")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Recipes"

    @property
    def commentary(self) -> str:
        return (
            "The number of recipes. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same order, "
            + "this feature allows you to determine how many distinct recipes "
            + "existed in that collection of recipes."
        )
