"""Feature abstract base class

All feature classes are derived from here
"""

from abc import ABCMeta, abstractmethod
from datetime import date, timedelta, datetime
from typing import Callable
from dateutil.relativedelta import relativedelta


from pyspark.sql import Column
import pyspark.sql.functions as f
import pendulum

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features.first_and_last_date_of_period import FirstAndLastDateOfPeriod
from jstark.exceptions import AsAtIsNotADate


class Feature(metaclass=ABCMeta):
    def __init__(
        self,
        as_at: date,
        feature_period: FeaturePeriod,
        first_day_of_week: str | None = None,
        use_absolute_periods: bool = False,
    ) -> None:
        self.feature_period = feature_period
        if isinstance(as_at, datetime):
            import warnings

            warnings.warn(f"as_at={as_at!r} was converted to a date")
            as_at = as_at.date()
        if not isinstance(as_at, date):
            raise AsAtIsNotADate
        self._as_at = as_at
        self._first_day_of_week = first_day_of_week
        self._use_absolute_periods = use_absolute_periods

    @property
    def feature_period(self) -> FeaturePeriod:
        return self._feature_period

    @feature_period.setter
    def feature_period(self, value) -> None:
        self._feature_period = value

    @property
    def as_at(self) -> date:
        return self._as_at

    @property
    def feature_name(self) -> str:
        suffix = (
            self.column_metadata["period-absolute"]
            if self._use_absolute_periods
            else self.feature_period.mnemonic
        )
        return f"{type(self).__name__}_{suffix}"

    @property
    @abstractmethod
    def column(self) -> Column:
        """Complete definition of the column returned by this feature,
        replete with feature period filtering, metadata, default value
        and alias"""

    @property
    @abstractmethod
    def description_subject(self) -> str:
        """Desciption of the feature that will be concatenated
        with an explanation of the feature period.
        """

    @property
    def commentary(self) -> str:
        return "No commentary supplied"

    @abstractmethod
    def default_value(self) -> Column:
        """Default value of the feature, typically used when zero rows match
        the feature's feature_period
        """

    @abstractmethod
    def column_expression(self) -> Column:
        """The expression that defines the feature"""

    @property
    def start_date(self) -> date:
        n_days_ago = self.as_at - timedelta(days=self.feature_period.start)
        n_weeks_ago = self.as_at - timedelta(weeks=self.feature_period.start)
        n_months_ago = self.as_at - relativedelta(months=self.feature_period.start)
        n_quarters_ago = self.as_at - relativedelta(
            months=self.feature_period.start * 3
        )
        n_years_ago = self.as_at - relativedelta(years=self.feature_period.start)
        match self.feature_period.period_unit_of_measure:
            case PeriodUnitOfMeasure.DAY:
                return n_days_ago
            case PeriodUnitOfMeasure.WEEK:
                return FirstAndLastDateOfPeriod(
                    n_weeks_ago, self._first_day_of_week
                ).first_date_in_week
            case PeriodUnitOfMeasure.MONTH:
                return FirstAndLastDateOfPeriod(n_months_ago).first_date_in_month
            case PeriodUnitOfMeasure.QUARTER:
                return FirstAndLastDateOfPeriod(n_quarters_ago).first_date_in_quarter
            case _:  # PeriodUnitOfMeasure.YEAR:
                return FirstAndLastDateOfPeriod(n_years_ago).first_date_in_year

    @property
    def end_date(self) -> date:
        n_days_ago = self.as_at - timedelta(days=self.feature_period.end)
        n_weeks_ago = self.as_at - timedelta(weeks=self.feature_period.end)
        n_months_ago = self.as_at - relativedelta(months=self.feature_period.end)
        n_quarters_ago = self.as_at - relativedelta(months=self.feature_period.end * 3)
        n_years_ago = self.as_at - relativedelta(years=self.feature_period.end)
        match self.feature_period.period_unit_of_measure:
            case PeriodUnitOfMeasure.DAY:
                last_day_of_period = n_days_ago
            case PeriodUnitOfMeasure.WEEK:
                last_day_of_period = FirstAndLastDateOfPeriod(
                    n_weeks_ago, self._first_day_of_week
                ).last_date_in_week
            case PeriodUnitOfMeasure.MONTH:
                last_day_of_period = FirstAndLastDateOfPeriod(
                    n_months_ago
                ).last_date_in_month
            case PeriodUnitOfMeasure.QUARTER:
                last_day_of_period = FirstAndLastDateOfPeriod(
                    n_quarters_ago
                ).last_date_in_quarter
            case _:  # PeriodUnitOfMeasure.YEAR:
                last_day_of_period = FirstAndLastDateOfPeriod(
                    n_years_ago
                ).last_date_in_year
        # min() is used to ensure we don't return a date later than self.as_at
        return min(last_day_of_period, self.as_at)

    @property
    def column_metadata(self) -> dict[str, str]:
        period_absolute_start_period: str = ""
        period_absolute_end_period: str = ""
        match self.feature_period.period_unit_of_measure:
            case PeriodUnitOfMeasure.DAY:
                period_absolute_start_period = (
                    pendulum.instance(self.as_at)
                    .subtract(days=self.feature_period.start)
                    .format("YYYYMMDD")
                )
                period_absolute_end_period = (
                    pendulum.instance(self.as_at)
                    .subtract(days=self.feature_period.end)
                    .format("YYYYMMDD")
                )
            case PeriodUnitOfMeasure.WEEK:
                period_absolute_start_period = self._week_label(self.start_date)
                end_week_start = FirstAndLastDateOfPeriod(
                    pendulum.instance(self.as_at).subtract(
                        weeks=self.feature_period.end
                    ),
                    self._first_day_of_week,
                ).first_date_in_week
                period_absolute_end_period = self._week_label(end_week_start)
            case PeriodUnitOfMeasure.MONTH:
                period_absolute_start_period = (
                    pendulum.instance(self.as_at)
                    .subtract(months=self.feature_period.start)
                    .format("YYYYMMM")
                )
                period_absolute_end_period = (
                    pendulum.instance(self.as_at)
                    .subtract(months=self.feature_period.end)
                    .format("YYYYMMM")
                )
            case PeriodUnitOfMeasure.QUARTER:
                dt_start = pendulum.instance(self.as_at).subtract(
                    months=self.feature_period.start * 3
                )
                period_absolute_start_period = f"{dt_start.year}Q{dt_start.quarter}"
                dt_end = pendulum.instance(self.as_at).subtract(
                    months=self.feature_period.end * 3
                )
                period_absolute_end_period = f"{dt_end.year}Q{dt_end.quarter}"
            case _:  # PeriodUnitOfMeasure.YEAR:
                period_absolute_start_period = str(
                    pendulum.instance(self.as_at)
                    .subtract(years=self.feature_period.start)
                    .year
                )
                period_absolute_end_period = str(
                    pendulum.instance(self.as_at)
                    .subtract(years=self.feature_period.end)
                    .year
                )
        return {
            "created-with-love-by": "https://github.com/jamiekt/jstark",
            "start-date": self.start_date.strftime("%Y-%m-%d"),
            "end-date": self.end_date.strftime("%Y-%m-%d"),
            "description": (
                f"{self.description_subject} between "
                + f"{self.start_date.strftime('%Y-%m-%d')} and "
                + f"{self.end_date.strftime('%Y-%m-%d')}"
            ),
            "generated-at": datetime.now().strftime("%Y-%m-%d"),
            "commentary": self.commentary,
            "name-stem": str(type(self).__name__),
            "period-absolute-start": period_absolute_start_period,
            "period-absolute-end": period_absolute_end_period,
            "period-absolute": period_absolute_start_period
            if period_absolute_start_period == period_absolute_end_period
            else f"{period_absolute_start_period}-{period_absolute_end_period}",
        }

    def _week_label(self, week_start_date: date) -> str:
        """Convert the first day of a week to a label like 2026W13.

        W01 of a year starts on the first occurrence of first_day_of_week
        on or before Jan 1 of that year.
        """
        weekdays = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        first_day_of_week = self._first_day_of_week or "Monday"
        target_weekday = weekdays.index(first_day_of_week)
        year = week_start_date.year

        jan1 = date(year, 1, 1)
        days_back = (jan1.weekday() - target_weekday) % 7
        w01_start = pendulum.instance(jan1).subtract(days=days_back)

        jan1_next = date(year + 1, 1, 1)
        days_back_next = (jan1_next.weekday() - target_weekday) % 7
        w01_start_next = pendulum.instance(jan1_next).subtract(days=days_back_next)

        if week_start_date >= w01_start_next:
            w01_start = w01_start_next
            year = year + 1

        week_number = (week_start_date - w01_start).days // 7 + 1
        return f"{year}W{week_number:02d}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(as_at={self.as_at}"
            f", feature_period='{self.feature_period.mnemonic}'"
            f", first_day_of_week={self._first_day_of_week!r})"
        )


class DerivedFeature(Feature, metaclass=ABCMeta):
    """A DerivedFeature is a feature that is calculated by combining
    data that has already been aggregated. For example, a derived
    feature called 'Average Gross Spend Per Basket' would be calculated
    by dividing the total GrossSpend by number of baskets (BasketCount)
    """

    @property
    def column(self) -> Column:
        return f.coalesce(self.column_expression(), self.default_value()).alias(
            self.feature_name, metadata=self.column_metadata
        )


class BaseFeature(Feature, metaclass=ABCMeta):
    """A BaseFeature is a feature that is calculated by aggregating
    raw source data. That data may have been cleaned and transformed in
    some way, but typically the grain of that data is real occurrences
    of some activity. Examples of such data are lists of grocery
    transactions, phone calls or journeys.
    """

    def sum_aggregator(self, column: Column) -> Column:
        return f.sum(column)

    def count_aggregator(self, column: Column) -> Column:
        return f.count(column)

    def count_if_aggregator(self, column: Column) -> Column:
        return f.count_if(column)

    def count_distinct_aggregator(self, column: Column) -> Column:
        return f.countDistinct(column)

    def approx_count_distinct_aggregator(self, column: Column) -> Column:
        return f.approx_count_distinct(column)

    def max_aggregator(self, column: Column) -> Column:
        return f.max(column)

    def min_aggregator(self, column: Column) -> Column:
        return f.min(column)

    def collect_set_aggregator(self, column: Column) -> Column:
        return f.collect_set(column)

    @abstractmethod
    def aggregator(self) -> Callable[[Column], Column]:
        """Aggregator function"""

    @property
    def column(self) -> Column:
        return f.coalesce(
            self.aggregator()(
                f.when(
                    (f.to_date(f.col("Timestamp")) >= f.lit(self.start_date))
                    & (f.to_date(f.col("Timestamp")) <= f.lit(self.end_date)),
                    self.column_expression(),
                )
            ),
            self.default_value(),
        ).alias(self.feature_name, metadata=self.column_metadata)
