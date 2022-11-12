import pytest

from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodEndGreaterThanStartError

def test_feature_period_description():
    assert (
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).description
        == "Between 3 and 2 days ago"
    )

def test_feature_period_start_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.start = 10
    assert  "can't set attribute" in  str(excInfo.value)

def test_feature_period_end_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.end = 10
    assert  "can't set attribute" in  str(excInfo.value)

def test_feature_period_periodunitofmeasure_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.period_unit_of_measure = PeriodUnitOfMeasure.MONTH
    assert  "can't set attribute" in  str(excInfo.value)

def test_feature_period_end_greater_than_start_raises_exception():
    with pytest.raises(FeaturePeriodEndGreaterThanStartError) as excInfo:
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3)
    assert str(excInfo.value) == "End of the feature period (3) cannot be before the start of the feature period (2)"