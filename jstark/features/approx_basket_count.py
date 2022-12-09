from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .approxdistinctcount_feature import ApproxDistinctCount


class ApproxBasketCount(ApproxDistinctCount):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.col("Basket")

    @property
    def description_subject(self) -> str:
        return "Approximate distinct count of Baskets"

    @property
    def commentary(self) -> str:
        return (
            "The approximate number of baskets. Similar to "
            + f"BasketCount_{self.feature_period.code} "
            + "except that it uses an approximation algorithm which "
            + "will not be as accurate as "
            + f"BasketCount_{self.feature_period.code} but will be a lot "
            + 'quicker to compute and in many cases will be "close enough".'
        )
