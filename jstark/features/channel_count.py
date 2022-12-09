from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import Column

from .feature import FeaturePeriod
from .distinctcount_feature import DistinctCount


class ChannelCount(DistinctCount):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period)

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
