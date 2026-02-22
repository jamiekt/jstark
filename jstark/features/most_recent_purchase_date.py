"""MostRecentPurchaseDate feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from .max_feature import Max


class MostRecentPurchaseDate(Max):
    def column_expression(self) -> Column:
        return f.to_date(f.col("Timestamp"))

    @property
    def description_subject(self) -> str:
        return "Most recent purchase date"

    def default_value(self) -> Column:
        return f.lit(None)

    @property
    def commentary(self) -> str:
        return (
            "It is useful to be able to see when something was most recently "
            + "purchased.For example, grouping by Store and filtering where "
            + "MostRecentPurchaseDate is more than 2 days ago could be a useful "
            + "indicator of things which might not be available for purchase."
            + "Also note that this is very similar to "
            + f"RecencyDays_{self.feature_period.mnemonic} so consider which of these "
            + "features is most useful to you."
        )
