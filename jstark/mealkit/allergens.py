"""Allergens feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.collect_set_feature import CollectSet


class Allergens(CollectSet):
    def column_expression(self) -> Column:
        return f.col("Allergen")

    @property
    def description_subject(self) -> str:
        return "Set of Allergens"

    @property
    def commentary(self) -> str:
        return (
            "The set of allergens. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same allergen, "
            + "this feature allows you to determine the set of allergens "
            + "that have been ordered."
        )
