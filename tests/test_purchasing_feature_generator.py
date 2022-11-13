from pyspark.sql import DataFrame

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator


def test_generator_returns_dataframe(dataframe_of_purchases):
    ret = PurchasingFeatureGenerator(dataframe_of_purchases).go()
    assert isinstance(ret, DataFrame)
