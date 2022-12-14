from platform import python_version

import pytest
from packaging import version
from jstark.exceptions import FeaturePeriodEndGreaterThanStartError
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_feature_period_description():
    assert (
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).description
        == "Between 3 and 2 days ago"
    )


def test_feature_period_start_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        fp.start = 10  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert (
            str(excInfo.value)
            == "property 'start' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        fp.end = 10  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert (
            str(excInfo.value)
            == "property 'end' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_periodunitofmeasure_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as excInfo:
        fp.period_unit_of_measure = PeriodUnitOfMeasure.YEAR  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(excInfo.value)
    else:
        assert str(excInfo.value) == (
            "property 'period_unit_of_measure' of "
            + "'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_greater_than_start_raises_exception():
    with pytest.raises(FeaturePeriodEndGreaterThanStartError) as excInfo:
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3)
    assert (
        str(excInfo.value)
        == "End of the feature period (3) cannot be "
        + "before the start of the feature period (2)"
    )


def test_feature_period_code_for_day():
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).code == "3d2"


def test_feature_period_code_for_week():
    assert FeaturePeriod(PeriodUnitOfMeasure.WEEK, 3, 2).code == "3w2"


def test_feature_period_code_for_month():
    assert FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 2).code == "3m2"


def test_feature_period_code_for_quarter():
    assert FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 2).code == "3q2"


def test_feature_period_code_for_year():
    assert FeaturePeriod(PeriodUnitOfMeasure.YEAR, 3, 2).code == "3y2"
