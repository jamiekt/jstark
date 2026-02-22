"""Test FakeTransactions"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from jstark.sample.transactions import FakeTransactions


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
