from abc import ABC, abstractmethod
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Callable
import calendar

from pyspark.sql import Column
import pyspark.sql.functions as f

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import AsAtIsNotADate


class Feature(ABC):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        self.feature_period = feature_period
        self.as_at = as_at

    def sum_aggregator(self, column: Column) -> Column:
        return f.sum(column)

    def count_aggregator(self, column: Column) -> Column:
        return f.count(column)

    def max_aggregator(self, column: Column) -> Column:
        return f.max(column)

    def min_aggregator(self, column: Column) -> Column:
        return f.min(column)

    @abstractmethod
    def aggregator(self) -> Callable[[Column], Column]:
        pass

    @property
    def feature_period(self) -> FeaturePeriod:
        return self.__feature_period

    @feature_period.setter
    def feature_period(self, value) -> None:
        self.__feature_period = value

    @property
    def as_at(self) -> date:
        return self.__as_at

    @as_at.setter
    def as_at(self, value) -> None:
        if not isinstance(value, date):
            raise AsAtIsNotADate
        self.__as_at = value

    @abstractmethod
    def column_expression(self) -> Column:
        pass

    @abstractmethod
    def default_value(self) -> Column:
        """Default value of the feature, typically used when zero rows match
        the feature's feature_period
        """
        pass

    @property
    def feature_name(self) -> str:
        return f"{type(self).__name__}_{self.feature_period.code}"

    @property
    def column(self) -> Column:
        metadata = {"createdBy": "jstark"}
        return f.coalesce(
            self.aggregator()(
                f.when(
                    (f.to_date(f.col("Timestamp")) >= f.lit(self.start_date))
                    & (f.to_date(f.col("Timestamp")) <= f.lit(self.end_date)),
                    self.column_expression(),
                )
            ),
            self.default_value(),
        ).alias(self.feature_name, metadata=metadata)

    @staticmethod
    def first_or_last_day_of_quarter(n_quarters_ago: date, start_or_end: str):
        if start_or_end in {"start", "end"}:
            return (
                (
                    date(n_quarters_ago.year, 1, 1)
                    if n_quarters_ago.month in [1, 2, 3]
                    else date(n_quarters_ago.year, 4, 1)
                    if n_quarters_ago.month in [4, 5, 6]
                    else date(n_quarters_ago.year, 7, 1)
                    if n_quarters_ago.month in [7, 8, 9]
                    else date(n_quarters_ago.year, 10, 1)
                )
                if start_or_end == "start"
                else (
                    date(n_quarters_ago.year, 3, 31)
                    if n_quarters_ago.month in [1, 2, 3]
                    else date(n_quarters_ago.year, 6, 30)
                    if n_quarters_ago.month in [4, 5, 6]
                    else date(n_quarters_ago.year, 9, 30)
                    if n_quarters_ago.month in [7, 8, 9]
                    else date(n_quarters_ago.year, 12, 31)
                )
            )
        else:
            raise RuntimeError(f"Don't know what to do with {start_or_end}")

    @staticmethod
    def get_n_weeks_ago(as_at: date, n: int):
        return as_at - timedelta(weeks=n)

    @staticmethod
    def get_n_quarters_ago(as_at: date, n: int):
        return as_at - relativedelta(months=n * 3)

    @property
    def start_date(self) -> date:
        n_weeks_ago = self.get_n_weeks_ago(self.as_at, self.feature_period.start)
        n_quarters_ago = self.get_n_quarters_ago(self.as_at, self.feature_period.start)
        first_day_of_quarter = self.first_or_last_day_of_quarter(
            n_quarters_ago, start_or_end="start"
        )
        return (
            self.as_at - timedelta(days=self.feature_period.start)
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.DAY
            # Use strftime because we want Sunday to be first day of the week.
            # date.DayOfWeek() has different behaviour
            else n_weeks_ago - timedelta(days=int(n_weeks_ago.strftime("%w")))
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.WEEK
            else self.as_at - relativedelta(months=self.feature_period.start, day=1)
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.MONTH
            else first_day_of_quarter
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.QUARTER
            else (
                self.as_at
                - relativedelta(years=self.feature_period.start, month=1, day=1)
            )
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.YEAR
            else self.as_at
        )

    @property
    def end_date(self) -> date:
        n_weeks_ago = self.get_n_weeks_ago(self.as_at, self.feature_period.end)
        n_months_ago = self.as_at - relativedelta(months=self.feature_period.end)
        n_quarters_ago = self.get_n_quarters_ago(self.as_at, self.feature_period.end)
        last_day_of_quarter = self.first_or_last_day_of_quarter(
            n_quarters_ago, start_or_end="end"
        )
        # min() is used to ensure we don't return a date later than self.as_at
        return min(
            (
                self.as_at - timedelta(days=self.feature_period.end)
                if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.DAY
                # Use strftime because we want Sunday to be first day of the week.
                # date.DayOfWeek() has different behaviour
                else n_weeks_ago + timedelta(days=6 - int(n_weeks_ago.strftime("%w")))
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.WEEK
                else n_months_ago.replace(
                    day=calendar.monthrange(n_months_ago.year, n_months_ago.month)[1]
                )
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.MONTH
                else last_day_of_quarter
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.QUARTER
                else (
                    self.as_at
                    - relativedelta(years=self.feature_period.end, month=12, day=31)
                )
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.YEAR
                else self.as_at
            ),
            self.as_at,
        )
