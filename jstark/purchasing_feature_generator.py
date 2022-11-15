from datetime import date

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType

from jstark.exceptions import DataFrameDoesNotIncludeTimestampColumn
from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from jstark.gross_spend_feature import GrossSpend


class PurchasingFeatureGenerator(object):
    def __init__(
        self,
        as_at: date,
        df: DataFrame,
        customer_attr: str = "All",
        product_attr: str = "All",
        store_attr: str = "All",
        channel_attr: str = "All",
    ) -> None:
        if (
            "Timestamp" not in df.schema.fieldNames()
            or df.schema["Timestamp"].dataType is not TimestampType()
        ):
            raise DataFrameDoesNotIncludeTimestampColumn()
        # Need a column containing the date of the transaction.
        df = df.withColumn("~date~", f.to_date("Timestamp"))
        self.__df = df
        self.__as_at = as_at
        self.__customer_attr = customer_attr
        self.__product_attr = product_attr
        self.__store_attr = store_attr
        self.__channel_attr = channel_attr

    @property
    def grain(self) -> list[str]:
        grain = []
        if self.__customer_attr != "All":
            grain.append(self.__customer_attr)
        if self.__product_attr != "All":
            grain.append(self.__product_attr)
        if self.__store_attr != "All":
            grain.append(self.__store_attr)
        if self.__channel_attr != "All":
            grain.append(self.__channel_attr)
        return grain

    def get_df(self):
        gross_spend = GrossSpend(
            as_at=self.__as_at,
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
            df=self.__df,
        )
        expressions = [gross_spend.column]
        output_df = self.__df.groupBy(self.grain).agg(*expressions)
        return output_df
