"""Feature abstract base class

All feature classes are derived from here
"""

from abc import ABCMeta, abstractmethod
from datetime import date, timedelta, datetime
from typing import Callable, Dict
from dateutil.relativedelta import relativedelta


from pyspark.sql import Column
import pyspark.sql.functions as f

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features.first_and_last_date_of_period import FirstAndLastDateOfPeriod
from jstark.exceptions import AsAtIsNotADate


class Feature(metaclass=ABCMeta):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        self.feature_period = feature_period
        self.as_at = as_at

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

    @property
    def feature_name(self) -> str:
        return f"{type(self).__name__}_{self.feature_period.mnemonic}"

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
        return (
            n_days_ago
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.DAY
            else (
                FirstAndLastDateOfPeriod(n_weeks_ago).first_date_in_week
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.WEEK
                else (
                    FirstAndLastDateOfPeriod(n_months_ago).first_date_in_month
                    if self.feature_period.period_unit_of_measure
                    == PeriodUnitOfMeasure.MONTH
                    else (
                        FirstAndLastDateOfPeriod(n_quarters_ago).first_date_in_quarter
                        if self.feature_period.period_unit_of_measure
                        == PeriodUnitOfMeasure.QUARTER
                        else FirstAndLastDateOfPeriod(n_years_ago).first_date_in_year
                    )
                )
            )
        )

    @property
    def end_date(self) -> date:
        n_days_ago = self.as_at - timedelta(days=self.feature_period.end)
        n_weeks_ago = self.as_at - timedelta(weeks=self.feature_period.end)
        n_months_ago = self.as_at - relativedelta(months=self.feature_period.end)
        n_quarters_ago = self.as_at - relativedelta(months=self.feature_period.end * 3)
        n_years_ago = self.as_at - relativedelta(years=self.feature_period.end)
        last_day_of_period = (
            n_days_ago
            if self.feature_period.period_unit_of_measure == PeriodUnitOfMeasure.DAY
            else (
                FirstAndLastDateOfPeriod(n_weeks_ago).last_date_in_week
                if self.feature_period.period_unit_of_measure
                == PeriodUnitOfMeasure.WEEK
                else (
                    FirstAndLastDateOfPeriod(n_months_ago).last_date_in_month
                    if self.feature_period.period_unit_of_measure
                    == PeriodUnitOfMeasure.MONTH
                    else (
                        FirstAndLastDateOfPeriod(n_quarters_ago).last_date_in_quarter
                        if self.feature_period.period_unit_of_measure
                        == PeriodUnitOfMeasure.QUARTER
                        else FirstAndLastDateOfPeriod(n_years_ago).last_date_in_year
                    )
                )
            )
        )
        # min() is used to ensure we don't return a date later than self.as_at
        return min(last_day_of_period, self.as_at)

    @property
    def column_metadata(self) -> Dict[str, str]:
        return {
            "created-with-love-by": "https://github.com/jamiekt/jstark",
            "start-date": self.start_date.strftime("%Y-%m-%d"),
            "end-date": self.end_date.strftime("%Y-%m-%d"),
            "description": (
                f"{self.description_subject} between "
                + f'{self.start_date.strftime("%Y-%m-%d")} and '
                + f'{self.end_date.strftime("%Y-%m-%d")}'
            ),
            "generated-at": datetime.now().strftime("%Y-%m-%d"),
            "commentary": self.commentary,
            "name-stem": str(type(self).__name__),
        }


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

    def count_distinct_aggregator(self, column: Column) -> Column:
        return f.countDistinct(column)

    def approx_count_distinct_aggregator(self, column: Column) -> Column:
        return f.approx_count_distinct(column)

    def max_aggregator(self, column: Column) -> Column:
        return f.max(column)

    def min_aggregator(self, column: Column) -> Column:
        return f.min(column)

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
