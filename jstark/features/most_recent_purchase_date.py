from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .max_feature import Max


class MostRecentPurchaseDate(Max):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

    def column_expression(self) -> Column:
        return f.to_date(f.col("Timestamp"))

    @property
    def description_subject(self) -> str:
        return "Most recent purchase date"

    @property
    def commentary(self) -> str:
        return (
            "It is useful to be able to see when something was most recently "
            + "purchased.For example, grouping by Store and filtering where "
            + "MostRecentPurchaseDate is more than 2 days ago could be a useful "
            + "indicator of things which might not be available for purchase."
            + "Also note that this is very similar to "
            + f"RecencyDays_{self.feature_period.code} so consider which of these "
            + "features is most useful to you."
        )
