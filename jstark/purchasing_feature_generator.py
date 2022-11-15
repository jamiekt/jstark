from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType
from datetime import date
import pyspark.sql.functions as f

from jstark.gross_spend_feature import GrossSpend
from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import DataFrameDoesNotIncludeTimestampColumn


class PurchasingFeatureGenerator(object):
    def __init__(self, as_at: date, df: DataFrame) -> None:
        if (
            "Timestamp" not in df.schema.fieldNames()
            or df.schema["Timestamp"].dataType is not TimestampType()
        ):
            raise DataFrameDoesNotIncludeTimestampColumn()
        # Need a column containing the date of the transaction.
        df = df.withColumn("~date~", f.to_date("Timestamp"))
        self.__df = df
        self.__as_at = as_at

    def get_df(self):
        gross_spend = GrossSpend(
            as_at=self.__as_at,
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
            df=self.__df,
        )
        expressions = [gross_spend.column]
        output_df = self.__df.groupBy(["Customer", "Product", "Store", "Channel"]).agg(
            *expressions
        )
        return output_df
