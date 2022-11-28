from pyspark.sql import DataFrame
from datetime import date

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator


def test_gross_spend(dataframe_of_purchases: DataFrame):
    first = (
        dataframe_of_purchases.groupBy()
        .agg(*PurchasingFeatureGenerator(as_at=date.today()).features)
        .first()
    )
    assert first is not None
    assert float(first["GrossSpend_2d1"]) == 5.5


def test_count(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    assert "Count_4d3" in df.schema.fieldNames()
