from datetime import date
from jstark.sample.transactions import FakeTransactions
from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_sample():
    pfg = PurchasingFeatureGenerator(
        date(2022, 1, 1),
        [
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 2, 2),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 3),
            FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4),
        ],
    )
    df = FakeTransactions().get_df(number_of_baskets=10)
    df = df.agg(*pfg.features)
    collected = df.collect()
    assert collected[0]["GrossSpend_1q1"] == 2263.58
