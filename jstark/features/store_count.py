from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .distinctcount_feature import DistinctCount


class StoreCount(DistinctCount):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

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
