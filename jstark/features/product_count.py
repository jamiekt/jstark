from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .distinctcount_feature import DistinctCount


class ProductCount(DistinctCount):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.col("Product")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Products"

    @property
    def commentary(self) -> str:
        return (
            "The number of products. Typically the dataframe supplied "
            + "to this feature will have many transactions for many baskets with "
            + "many products bought, this feature allows you to determine how many "
            + "disinct products were bought in those baskets."
        )
