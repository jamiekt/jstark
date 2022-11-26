import pytest

from pyspark.sql import DataFrame
from datetime import date
import pyspark.sql.functions as f

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.exceptions import DataFrameDoesNotIncludeTimestampColumn, AsAtIsNotADate


def test_generator_returns_dataframe(dataframe_of_purchases):
    df_out = PurchasingFeatureGenerator(
        as_at=date.today(), df=dataframe_of_purchases
    ).get_df()
    assert isinstance(df_out, DataFrame)


def test_errors_if_as_at_is_not_a_date(dataframe_of_purchases: DataFrame):
    with pytest.raises(AsAtIsNotADate):
        PurchasingFeatureGenerator(as_at="2000-01-01", df=dataframe_of_purchases)  # type: ignore


def test_input_df_without_a_field_called_timestamp_raises_error(
    dataframe_of_purchases: DataFrame,
):
    dataframe_of_purchases = dataframe_of_purchases.drop("Timestamp")
    with pytest.raises(DataFrameDoesNotIncludeTimestampColumn):
        PurchasingFeatureGenerator(as_at=date.today(), df=dataframe_of_purchases)


def test_input_df_field_timestamp_is_not_of_type_timestamp(
    dataframe_of_purchases: DataFrame,
):
    dataframe_of_purchases = dataframe_of_purchases.withColumn("Timestamp", f.lit(""))
    with pytest.raises(DataFrameDoesNotIncludeTimestampColumn):
        PurchasingFeatureGenerator(as_at=date.today(), df=dataframe_of_purchases)
