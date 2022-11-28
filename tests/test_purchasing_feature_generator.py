from pyspark.sql import DataFrame
from datetime import date
import pyspark.sql.functions as f

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator


def test_gross_spend(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_2d0"]) == 8.5


def test_count_exists(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    assert "Count_4d3" in df.schema.fieldNames()


def test_count(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy("Store").agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    df = df.where(f.col("Store") == "Hammersmith")
    first = df.first()
    assert first is not None
    assert first["Count_2d0"] == 2
