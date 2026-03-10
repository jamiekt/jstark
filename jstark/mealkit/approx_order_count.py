"""ApproxOrderCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.approxdistinctcount_feature import ApproxDistinctCount


class ApproxOrderCount(ApproxDistinctCount):
    def column_expression(self) -> Column:
        return f.col("Order")

    @property
    def description_subject(self) -> str:
        return "Approximate distinct count of Orders"

    @property
    def commentary(self) -> str:
        return (
            "The approximate number of orders. Similar to "
            + f"OrderCount_{self.feature_period.mnemonic} "
            + "except that it uses an approximation algorithm which "
            + "will not be as accurate as "
            + f"OrderCount_{self.feature_period.mnemonic} but will be a lot "
            + 'quicker to compute and in many cases will be "close enough".'
        )
