from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .distinctcount_feature import DistinctCount


class BasketCount(DistinctCount):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

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