from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType
from datetime import date
import pyspark.sql.functions as f

from jstark.gross_spend_feature import GrossSpendFeature
from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import DataFrameDoesNotIncludeTimestampColumn


class PurchasingFeatureGenerator(object):
    def __init__(self, as_at: date, df: DataFrame) -> None:
        self.__df = df
        self.__as_at = as_at
        if (
            "Timestamp" not in df.schema.fieldNames()
            or df.schema["Timestamp"].dataType is not TimestampType()
        ):
            raise DataFrameDoesNotIncludeTimestampColumn()
        # Need a column containing the date of the transaction.
        df = df.withColumn("~date~", f.to_date("Timestamp"))

    def get_df(self):
        gross_spend_feature = GrossSpendFeature(
            as_at=self.__as_at,
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
        )
        expressions = [gross_spend_feature.columnExpression(df=self.__df)]
        return self.__df.agg(*expressions)
