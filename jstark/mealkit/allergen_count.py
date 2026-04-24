"""AllergenCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class AllergenCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Allergen")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Allergens"

    @property
    def commentary(self) -> str:
        return (
            "The number of allergens. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same allergen, "
            + "this feature allows you to determine how many distinct allergens "
            + "have been ordered."
        )
