from datetime import date
import pytest
from jstark.features.first_and_last_date_of_period import FirstAndLastDateOfPeriod


def test_invalid_first_day_of_week():
    assert pytest.raises(
        ValueError, FirstAndLastDateOfPeriod, date(2022, 12, 8), first_day_of_week="Sat"
    )


def test_first_date_in_week_for_default_first_day_of_week():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).first_date_in_week == date(
        2022, 12, 5
    )


def test_last_date_in_week_for_default_first_day_of_week():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).last_date_in_week == date(
        2022, 12, 11
    )


def test_first_date_in_week_with_saturday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Saturday"
    ).first_date_in_week == date(2022, 12, 3)


def test_last_date_in_week_with_saturday_as_first_day_of_week():
    last_date_in_week = FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Saturday"
    ).last_date_in_week
    assert last_date_in_week == date(2022, 12, 9)


def test_first_date_in_week_with_sunday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Sunday"
    ).first_date_in_week == date(2022, 12, 4)


def test_first_date_in_week_with_monday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Monday"
    ).first_date_in_week == date(2022, 12, 5)


def test_first_date_in_week_with_tuesday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Tuesday"
    ).first_date_in_week == date(2022, 12, 6)


def test_first_date_in_week_with_wednesday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Wednesday"
    ).first_date_in_week == date(2022, 12, 7)


def test_first_date_in_week_with_thursday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Thursday"
    ).first_date_in_week == date(2022, 12, 8)


def test_first_date_in_week_with_friday_as_first_day_of_week():
    assert FirstAndLastDateOfPeriod(
        date(2022, 12, 8), first_day_of_week="Friday"
    ).first_date_in_week == date(2022, 12, 2)


def test_first_date_in_month():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).first_date_in_month == date(
        2022, 12, 1
    )


def test_last_date_in_month():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).last_date_in_month == date(
        2022, 12, 31
    )


def test_first_date_in_quarter():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).first_date_in_quarter == date(
        2022, 10, 1
    )


def test_last_date_in_quarter():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).last_date_in_quarter == date(
        2022, 12, 31
    )


def test_first_date_in_year():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).first_date_in_year == date(
        2022, 1, 1
    )


def test_last_date_in_year():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).last_date_in_year == date(
        2022, 12, 31
    )
