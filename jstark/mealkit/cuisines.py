"""Cuisines feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.collect_set_feature import CollectSet


class Cuisines(CollectSet):
    def column_expression(self) -> Column:
        return f.col("Cuisine")

    @property
    def description_subject(self) -> str:
        return "Set of Cuisines"

    @property
    def commentary(self) -> str:
        return (
            "The set of cuisines. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same cuisine, "
            + "this feature allows you to determine the set of cuisines "
            + "that have been ordered."
        )
