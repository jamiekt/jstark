from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


class FirstAndLastDateOfPeriod:
    """Encapsulate all the logic to determine first and last date
    of a period that includes the supplied date
    """

    def __init__(self, date_in_period: date) -> None:
        self.__date_in_period = date_in_period

    @property
    def first_date_in_week(self) -> date:
        # Use strftime because we want Sunday to be first day of the week.
        # date.DayOfWeek() has different behaviour
        return self.__date_in_period - timedelta(
            days=int(self.__date_in_period.strftime("%w"))
        )

    @property
    def last_date_in_week(self) -> date:
        return self.__date_in_period + timedelta(
            days=6 - int(self.__date_in_period.strftime("%w"))
        )

    @property
    def first_date_in_month(self) -> date:
        return date(self.__date_in_period.year, self.__date_in_period.month, 1)

    @property
    def last_date_in_month(self) -> date:
        return (
            self.__date_in_period
            + relativedelta(months=1, day=1)
            - relativedelta(days=1)
        )

    @property
    def first_date_in_quarter(self) -> date:
        return (
            date(self.__date_in_period.year, 1, 1)
            if self.__date_in_period.month in [1, 2, 3]
            else date(self.__date_in_period.year, 4, 1)
            if self.__date_in_period.month in [4, 5, 6]
            else date(self.__date_in_period.year, 7, 1)
            if self.__date_in_period.month in [7, 8, 9]
            else date(self.__date_in_period.year, 10, 1)
        )

    @property
    def last_date_in_quarter(self) -> date:
        return (
            date(self.__date_in_period.year, 3, 31)
            if self.__date_in_period.month in [1, 2, 3]
            else date(self.__date_in_period.year, 6, 30)
            if self.__date_in_period.month in [4, 5, 6]
            else date(self.__date_in_period.year, 9, 30)
            if self.__date_in_period.month in [7, 8, 9]
            else date(self.__date_in_period.year, 12, 31)
        )

    @property
    def first_date_in_year(self) -> date:
        return date(self.__date_in_period.year, 1, 1)

    @property
    def last_date_in_year(self) -> date:
        return date(self.__date_in_period.year, 12, 31)
