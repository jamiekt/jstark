from pyspark.sql import DataFrame
from datetime import date

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator


def test_generator_returns_dataframe(dataframe_of_purchases):
    df_out = PurchasingFeatureGenerator(
        as_at=date(2023, 11, 13), df=dataframe_of_purchases
    ).get_df()
    assert isinstance(df_out, DataFrame)
