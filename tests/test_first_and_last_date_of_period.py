from datetime import date
from jstark.features.first_and_last_date_of_period import FirstAndLastDateOfPeriod


def test_first_date_in_week():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).first_date_in_week == date(
        2022, 12, 4
    )


def test_last_date_in_week():
    assert FirstAndLastDateOfPeriod(date(2022, 12, 8)).last_date_in_week == date(
        2022, 12, 10
    )


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
