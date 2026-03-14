"""FirstAndLastDateOfPeriod class

Helper class for figuring out dates relative to a given date
"""

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


class FirstAndLastDateOfPeriod:
    """Encapsulate all the logic to determine first and last date
    of a period that includes the supplied date
    """

    def __init__(
        self, date_in_period: date, first_day_of_week: str | None = None
    ) -> None:
        self.__date_in_period = date_in_period
        self.__weekdays = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]
        if first_day_of_week is None:
            first_day_of_week = "Sunday"
        if first_day_of_week not in self.__weekdays:
            raise ValueError(f"first_day_of_week must be one of {self.__weekdays}")
        self._first_day_of_week = first_day_of_week

    @property
    def first_date_in_week(self) -> date:
        current_weekday_index = self.__weekdays.index(
            self.__date_in_period.strftime("%A")
        )
        first_day_index = self.__weekdays.index(self._first_day_of_week)
        # Number of days to subtract to get to the first day of this week (may be 0)
        days_to_subtract = (current_weekday_index - first_day_index) % 7
        return self.__date_in_period - timedelta(days=days_to_subtract)

    @property
    def last_date_in_week(self) -> date:
        return self.first_date_in_week + timedelta(days=6)

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
        match self.__date_in_period.month:
            case 1 | 2 | 3:
                return date(self.__date_in_period.year, 1, 1)
            case 4 | 5 | 6:
                return date(self.__date_in_period.year, 4, 1)
            case 7 | 8 | 9:
                return date(self.__date_in_period.year, 7, 1)
            case _:  # all other months:
                return date(self.__date_in_period.year, 10, 1)

    @property
    def last_date_in_quarter(self) -> date:
        match self.__date_in_period.month:
            case 1 | 2 | 3:
                return date(self.__date_in_period.year, 3, 31)
            case 4 | 5 | 6:
                return date(self.__date_in_period.year, 6, 30)
            case 7 | 8 | 9:
                return date(self.__date_in_period.year, 9, 30)
            case _:  # all other months:
                return date(self.__date_in_period.year, 12, 31)

    @property
    def first_date_in_year(self) -> date:
        return date(self.__date_in_period.year, 1, 1)

    @property
    def last_date_in_year(self) -> date:
        return date(self.__date_in_period.year, 12, 31)
