"""Test FakeTransactions
"""
from datetime import date
from jstark.sample.transactions import FakeTransactions
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_fake_transactions_returns_same_data_with_same_seed():
    """FakeTransactions has a seed which is used to make sure it returns
    the same data every time.
    """
    pfg = PurchasingFeatureGenerator(
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
    assert collected[0]["GrossSpend_1q1"] == 2263.58
