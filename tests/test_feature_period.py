from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure


def test_feature_period_description():
    assert (
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3).description
        == "Between 2 and 3 days ago"
    )
