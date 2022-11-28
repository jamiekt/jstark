from pyspark.sql import DataFrame
from datetime import date

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator


def test_gross_spend(dataframe_of_purchases: DataFrame):
    first = (
        dataframe_of_purchases.groupBy()
        .agg(*PurchasingFeatureGenerator(as_at=date.today()).get_features())
        .first()
    )
    assert first is not None
    assert float(first["GrossSpend_2d1"]) == 5.5
