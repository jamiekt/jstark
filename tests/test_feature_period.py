from platform import python_version

import pytest
from jstark.exceptions import FeaturePeriodEndGreaterThanStartError
from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from packaging import version

def test_feature_period_description():
    assert (
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).description
        == "Between 3 and 2 days ago"
    )


def test_feature_period_start_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.start = 10
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert (
            str(excInfo.value)
            == "property 'start' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.end = 10
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert (
            str(excInfo.value)
            == "property 'end' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_periodunitofmeasure_is_immutable():
    feature_period = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        feature_period.period_unit_of_measure = PeriodUnitOfMeasure.MONTH
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert (
            str(excInfo.value)
            == "property 'period_unit_of_measure' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_greater_than_start_raises_exception():
    with pytest.raises(FeaturePeriodEndGreaterThanStartError) as excInfo:
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3)
    assert (
        str(excInfo.value)
        == "End of the feature period (3) cannot be before the start of the feature period (2)"
    )
