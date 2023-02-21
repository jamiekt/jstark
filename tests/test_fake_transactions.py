"""Test FakeTransactions
"""
from datetime import date
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from jstark.sample.transactions import FakeTransactions
from jstark.grocery_retailer_feature_generator import GroceryRetailerFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_fake_transactions_returns_a_dataframe():
    """In order to get 100% code coverage we need to call
    FakeTransactions().get_df() without a seed
    """
    assert isinstance(FakeTransactions().get_df(), DataFrame)


def test_number_of_baskets_is_correct():
    """
    Had a bug where FakeTransactions was returning the wrong number of baskets.
    First wrote this test to verify the correct behaviour and then made the test pass.
    Might as well leave the test here.
    """
    number_of_baskets = 1234
    first = (
        FakeTransactions()
        .get_df(number_of_baskets=number_of_baskets)
        .groupBy()
        .agg(f.countDistinct("Basket").alias("baskets"))
        .first()
    )
    assert first is not None
    assert first["baskets"] == number_of_baskets


def test_fake_transactions_returns_same_data_with_same_seed():
    """FakeTransactions has a seed which is used to make sure it returns
    the same data every time.
    """
    pfg = GroceryRetailerFeatureGenerator(
        date(2022, 1, 1),
        [
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 2, 2),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 3),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4),
        ],
    )
    df = FakeTransactions().get_df(seed=42, number_of_baskets=10)
    df = df.agg(*pfg.features)
    collected = df.collect()
    assert collected[0]["GrossSpend_1q1"] == 334.38
