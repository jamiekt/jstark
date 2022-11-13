from pyspark.sql import DataFrame

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator

from .fixtures import dataframe_of_purchases, purchases_schema


def test_generator_returns_dataframe(dataframe_of_purchases):
    ret = PurchasingFeatureGenerator(dataframe_of_purchases).go()
    assert isinstance(ret, DataFrame)
