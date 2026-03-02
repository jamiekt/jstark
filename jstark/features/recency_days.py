"""RecencyDays feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.min_feature import Min


class RecencyDays(Min):
    def column_expression(self) -> Column:
        return f.datediff(f.lit(self.as_at), f.col("Timestamp"))

    def default_value(self) -> Column:
        return f.lit(0)

    @property
    def description_subject(self) -> str:
        return "Minimum number of days since occurrence"

    @property
    def commentary(self) -> str:
        return (
            "This could be particularly useful (for example) in a grocery retailer "
            + "for determining when a customer most recently bought a product or "
            + " when a product was most recently bought in a store"
            + "Also note that this is very similar to "
            + f"MostRecentPurchaseDate_{self.feature_period.mnemonic} "
            + "so consider which of these "
            + "features is most useful to you."
        )
