from pyspark.sql import DataFrame
from datetime import date
import pyspark.sql.functions as f

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_gross_spend(dataframe_of_purchases: DataFrame):
    dataframe_of_purchases = dataframe_of_purchases.where(
        (f.col("Customer") == "Leia") | (f.col("Customer") == "Luke")
    )
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=date.today(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0),
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 2),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_0d0"]) == 5.5
    assert float(first["GrossSpend_1d0"]) == 8.5
    assert float(first["GrossSpend_2d0"]) == 8.5
    assert float(first["GrossSpend_2d2"]) == 0


def test_count_exists(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    assert "Count_4d3" in df.schema.fieldNames()


def test_count(dataframe_of_purchases: DataFrame):
    dataframe_of_purchases = dataframe_of_purchases.where(
        f.col("Store") == "Hammersmith"
    )
    df = dataframe_of_purchases.groupBy("Store").agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    first = df.first()
    assert first is not None
    assert first["Count_2d0"] == 2
