"""ChannelCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount


class ChannelCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Channel")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Channels"

    @property
    def commentary(self) -> str:
        return (
            "Channels may be things like 'instore' or 'online', although you can supply"
            + " whatever values you want. Perhaps they are marketing channels. This "
            + " feature is the number of distinct channels in which activity occurred."
        )
