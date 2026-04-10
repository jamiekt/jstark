from platform import python_version
from datetime import date, datetime, timedelta
import pendulum
import pytest
from packaging import version
from pyspark.sql import DataFrame
from jstark.exceptions import FeaturePeriodEndGreaterThanStartError
from jstark.feature_period import (
    FeaturePeriod,
    PeriodUnitOfMeasure,
    ALL_MONTHS_LAST_YEAR,
    ALL_QUARTERS_LAST_YEAR,
    ALL_MONTHS_THIS_YEAR,
    ALL_QUARTERS_THIS_YEAR,
    TODAY,
    YESTERDAY,
    THIS_WEEK,
    LAST_WEEK,
    THIS_MONTH,
    LAST_MONTH,
    THIS_QUARTER,
    LAST_QUARTER,
    THIS_YEAR,
    LAST_YEAR,
    THIS_MONTH_VS_ONE_YEAR_PRIOR,
    LAST_MONTH_VS_ONE_YEAR_PRIOR,
    THIS_QUARTER_VS_ONE_YEAR_PRIOR,
    LAST_QUARTER_VS_ONE_YEAR_PRIOR,
    LAST_FIVE_YEARS,
)
from jstark.grocery import GroceryFeatures


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


def test_all_months_last_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**ALL_MONTHS_LAST_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    last_year = date.today().year - 1
    expected_start_dates = {date(last_year, i, 1) for i in range(1, 13)}
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == expected_start_dates


def test_all_months_this_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**ALL_MONTHS_THIS_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    expected_start_dates = {
        date(date.today().year, i, 1) for i in range(1, date.today().month + 1)
    }
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == expected_start_dates


def test_all_quarters_last_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**ALL_QUARTERS_LAST_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    last_year = date.today().year - 1
    expected_start_dates = {
        date(last_year, 1, 1),
        date(last_year, 4, 1),
        date(last_year, 7, 1),
        date(last_year, 10, 1),
    }
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == expected_start_dates


def test_all_quarters_this_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**ALL_QUARTERS_THIS_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    match date.today().month:
        case 1 | 2 | 3:
            first_month_of_quarters = [1]
        case 4 | 5 | 6:
            first_month_of_quarters = [1, 4]
        case 7 | 8 | 9:
            first_month_of_quarters = [1, 4, 7]
        case _:  # all other months:
            first_month_of_quarters = [1, 4, 7, 10]
    expected_start_dates = {
        date(date.today().year, i, 1) for i in first_month_of_quarters
    }
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == expected_start_dates


def test_today(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**TODAY, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today()}


def test_yesterday(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**YESTERDAY, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today() - timedelta(days=1)}


def test_this_week(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**THIS_WEEK, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    today_day_of_the_week = date.today().weekday()
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today() - timedelta(days=today_day_of_the_week)}


def test_last_week(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_WEEK, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    today_day_of_the_week = date.today().weekday()
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today() - timedelta(days=today_day_of_the_week) - timedelta(days=7)}


def test_this_month(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**THIS_MONTH, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today().replace(day=1)}


def test_last_month(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_MONTH, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {pendulum.date(date.today().year, date.today().month, 1).subtract(months=1)}


def test_this_quarter(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**THIS_QUARTER, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    match date.today().month:
        case 1 | 2 | 3:
            first_day_of_quarter = date.today().replace(day=1).replace(month=1)
        case 4 | 5 | 6:
            first_day_of_quarter = date.today().replace(day=1).replace(month=4)
        case 7 | 8 | 9:
            first_day_of_quarter = date.today().replace(day=1).replace(month=7)
        case _:  # all other months:
            first_day_of_quarter = date.today().replace(day=1).replace(month=10)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {first_day_of_quarter}


def test_last_quarter(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_QUARTER, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    match date.today().month:
        case 1 | 2 | 3:
            first_day_of_quarter = pendulum.date(date.today().year, 1, 1).subtract(
                months=3
            )
        case 4 | 5 | 6:
            first_day_of_quarter = pendulum.date(date.today().year, 4, 1).subtract(
                months=3
            )
        case 7 | 8 | 9:
            first_day_of_quarter = pendulum.date(date.today().year, 7, 1).subtract(
                months=3
            )
        case _:  # all other months:
            first_day_of_quarter = pendulum.date(date.today().year, 10, 1).subtract(
                months=3
            )
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {first_day_of_quarter}


def test_this_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**THIS_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {date.today().replace(day=1).replace(month=1)}


def test_last_year(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_YEAR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {pendulum.date(date.today().year, 1, 1).subtract(years=1)}


def test_this_month_vs_one_year_prior(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**THIS_MONTH_VS_ONE_YEAR_PRIOR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {
        date.today().replace(day=1),
        pendulum.date(date.today().year, date.today().month, 1).subtract(years=1),
    }


def test_last_month_vs_one_year_prior(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_MONTH_VS_ONE_YEAR_PRIOR, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    today_first = pendulum.date(date.today().year, date.today().month, 1)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {
        today_first.subtract(months=1),
        today_first.subtract(months=1).subtract(years=1),
    }


def test_this_quarter_vs_one_year_prior(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(
        **THIS_QUARTER_VS_ONE_YEAR_PRIOR,  # type: ignore[arg-type]
        feature_stems={"BasketCount"},
    )
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    match date.today().month:
        case 1 | 2 | 3:
            first_day_of_quarter = date.today().replace(day=1).replace(month=1)
        case 4 | 5 | 6:
            first_day_of_quarter = date.today().replace(day=1).replace(month=4)
        case 7 | 8 | 9:
            first_day_of_quarter = date.today().replace(day=1).replace(month=7)
        case _:  # all other months:
            first_day_of_quarter = date.today().replace(day=1).replace(month=10)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {
        first_day_of_quarter,
        pendulum.date(
            first_day_of_quarter.year, first_day_of_quarter.month, 1
        ).subtract(years=1),
    }


def test_last_quarter_vs_one_year_prior(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(
        **LAST_QUARTER_VS_ONE_YEAR_PRIOR,  # type: ignore[arg-type]
        feature_stems={"BasketCount"},
    )
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    match date.today().month:
        case 1 | 2 | 3:
            first_day_of_quarter = pendulum.date(date.today().year, 1, 1).subtract(
                months=3
            )
        case 4 | 5 | 6:
            first_day_of_quarter = pendulum.date(date.today().year, 4, 1).subtract(
                months=3
            )
        case 7 | 8 | 9:
            first_day_of_quarter = pendulum.date(date.today().year, 7, 1).subtract(
                months=3
            )
        case _:  # all other months:
            first_day_of_quarter = pendulum.date(date.today().year, 10, 1).subtract(
                months=3
            )
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {
        first_day_of_quarter,
        pendulum.date(
            first_day_of_quarter.year, first_day_of_quarter.month, 1
        ).subtract(years=1),
    }


def test_last_five_years(luke_and_leia_purchases: DataFrame):
    gf = GroceryFeatures(**LAST_FIVE_YEARS, feature_stems={"BasketCount"})  # type: ignore[arg-type]
    df = luke_and_leia_purchases.groupBy().agg(*gf.features)
    assert {
        datetime.strptime(c.metadata["start-date"], "%Y-%m-%d").date()
        for c in df.schema
    } == {
        pendulum.date(date.today().year, 1, 1).subtract(years=1),
        pendulum.date(date.today().year, 1, 1).subtract(years=2),
        pendulum.date(date.today().year, 1, 1).subtract(years=3),
        pendulum.date(date.today().year, 1, 1).subtract(years=4),
        pendulum.date(date.today().year, 1, 1).subtract(years=5),
    }
