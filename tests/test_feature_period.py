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


def test_repr():
    assert (
        repr(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2))
        == "FeaturePeriod(period_unit_of_measure=PeriodUnitOfMeasure.DAY, start=3, end=2)"  # noqa: E501
    )


def test_eq():
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) == FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 3, 2
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 3, 3
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.WEEK, 3, 2
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 4, 2
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 3, 3
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 3, 1
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != FeaturePeriod(
        PeriodUnitOfMeasure.DAY, 2, 2
    )
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2) != "SomeOtherObject"


def test_str():
    assert (
        str(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) == "Between 3 and 2 days ago"
    )


def test_hash():
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) == hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 3)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.WEEK, 3, 2)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 2)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 3)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 1)
    )
    assert hash(FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)) != hash(
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 2)
    )


def test_feature_period_start_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as exc_info:
        fp.start = 10  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(exc_info.value)
    else:
        assert (
            str(exc_info.value)
            == "property 'start' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as exc_info:
        fp.end = 10  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(exc_info.value)
    else:
        assert (
            str(exc_info.value)
            == "property 'end' of 'FeaturePeriod' object has no setter"
        )


def test_feature_period_periodunitofmeasure_is_immutable():
    fp = FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2)
    with pytest.raises(AttributeError) as exc_info:
        fp.period_unit_of_measure = PeriodUnitOfMeasure.YEAR  # type: ignore
    if version.parse(python_version()) < version.parse("3.11"):
        assert "can't set attribute" in str(exc_info.value)
    else:
        assert str(exc_info.value) == (
            "property 'period_unit_of_measure' of "
            + "'FeaturePeriod' object has no setter"
        )


def test_feature_period_end_greater_than_start_raises_exception():
    with pytest.raises(FeaturePeriodEndGreaterThanStartError) as exc_info:
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3)
    assert (
        str(exc_info.value)
        == "End of the feature period (3) cannot be "
        + "before the start of the feature period (2)"
    )


def test_feature_period_code_for_day():
    assert FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).mnemonic == "3d2"


def test_feature_period_code_for_week():
    assert FeaturePeriod(PeriodUnitOfMeasure.WEEK, 3, 2).mnemonic == "3w2"


def test_feature_period_code_for_month():
    assert FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 2).mnemonic == "3m2"


def test_feature_period_code_for_quarter():
    assert FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 2).mnemonic == "3q2"


def test_feature_period_code_for_year():
    assert FeaturePeriod(PeriodUnitOfMeasure.YEAR, 3, 2).mnemonic == "3y2"


def test_argument_of_wrong_types_raises():
    with pytest.raises(TypeError) as exc_info:
        FeaturePeriod("YEAR", 3, 2)
    assert str(exc_info.value) == (
        "period_unit_of_measure needs to be of type "
        + "PeriodUnitOfMeasure, not <class 'str'>"
    )
