import pytest
from datetime import date, timedelta

from jstark.exceptions import AsAtIsNotADate
from jstark.features.gross_spend_feature import GrossSpend
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_errors_if_as_at_is_not_a_date():
    with pytest.raises(AsAtIsNotADate):
        GrossSpend(
            as_at="2000-01-01",  # type: ignore
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
        )


def test_start_date_days():
    as_at = date(2022, 11, 30)
    _0d0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0))
    assert _0d0.start_date == as_at
    assert _0d0.end_date == as_at
    _2d0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0))
    assert _2d0.start_date == date(2022, 11, 28)
    assert _2d0.end_date == as_at


def test_start_and_end_date_when_period_unit_of_measure_is_weeks():
    first_day_of_week = date(2022, 11, 27)  # this is a Sunday
    for i in range(7):
        as_at = first_day_of_week + timedelta(days=i)
        # all days in the same week should return the same results
        _0w0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0))
        _1w0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 0))
        _1w1 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1))
        _2w0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.WEEK, 2, 0))
        _2w1 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.WEEK, 2, 1))
        assert _0w0.start_date == first_day_of_week
        assert _0w0.end_date == as_at
        assert _1w0.start_date == date(2022, 11, 20)
        assert _1w0.end_date == as_at
        assert _1w1.start_date == date(2022, 11, 20)
        assert _1w1.end_date == date(2022, 11, 26)
        assert _2w0.start_date == date(2022, 11, 13)
        assert _2w0.end_date == as_at
        assert _2w1.start_date == date(2022, 11, 13)
        assert _2w1.end_date == date(2022, 11, 26)


def test_start_and_end_date_when_period_unit_of_measure_is_months():
    first_day_of_month = date(2022, 11, 1)
    for i in range(30):
        as_at = first_day_of_month + timedelta(days=i)
        _0m0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.MONTH, 0, 0))
        _1m0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 0))
        _1m1 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1))
        _12m0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.MONTH, 12, 0))
        _12m12 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.MONTH, 12, 12))
        assert _0m0.start_date == first_day_of_month
        assert _0m0.end_date == as_at
        assert _1m0.start_date == date(2022, 10, 1)
        assert _0m0.end_date == as_at
        assert _1m1.start_date == date(2022, 10, 1)
        assert _1m1.end_date == date(2022, 10, 31)
        assert _12m0.start_date == date(2021, 11, 1)
        assert _12m0.end_date == as_at
        assert _12m12.start_date == date(2021, 11, 1)
        assert _12m12.end_date == date(2021, 11, 30)


def test_start_and_end_date_when_period_unit_of_measure_is_quarter():
    first_day_of_quarter = date(2022, 10, 1)
    for i in range(92):
        as_at = first_day_of_quarter + timedelta(days=i)
        _0q0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0))
        _1q1 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1))
        _2q2 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 2, 2))
        _3q3 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 3, 3))
        _4q4 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4))
        _4q0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 0))
        assert _0q0.start_date == date(2022, 10, 1)
        assert _0q0.end_date == as_at
        assert _1q1.start_date == date(2022, 7, 1)
        assert _1q1.end_date == date(2022, 9, 30)
        assert _2q2.start_date == date(2022, 4, 1)
        assert _2q2.end_date == date(2022, 6, 30)
        assert _3q3.start_date == date(2022, 1, 1)
        assert _3q3.end_date == date(2022, 3, 31)
        assert _4q4.start_date == date(2021, 10, 1)
        assert _4q4.end_date == date(2021, 12, 31)
        assert _4q0.start_date == date(2021, 10, 1)
        assert _4q0.end_date == as_at


def test_start_and_end_date_when_period_unit_of_measure_is_year():
    first_day_of_year = date(2022, 1, 1)
    for i in range(365):
        as_at = first_day_of_year + timedelta(days=i)
        _0y0 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0))
        _1y1 = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 1))
        assert _0y0.start_date == date(2022, 1, 1)
        assert _0y0.end_date == as_at
        assert _1y1.start_date == date(2021, 1, 1)
        assert _1y1.end_date == date(2021, 12, 31)


# def test_metadata():
#     f = GrossSpend(as_at, FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0))
#     f.
