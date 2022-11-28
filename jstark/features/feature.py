from abc import ABC, abstractmethod
from datetime import date
from typing import Callable

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
    def columnExpression(self) -> Column:
        pass

    @property
    def feature_name(self) -> str:
        return f"{type(self).__name__}_{self.feature_period.code}"

    @property
    def column(self) -> Column:
        as_at_col = f.lit(self.as_at)
        date_of_occurrence_col = f.col("Timestamp")
        days_since_occurrence = f.datediff(f.to_date(date_of_occurrence_col), as_at_col)
        weeks_since_occurrence = f.ceil(days_since_occurrence / 7)
        months_since_occurrence = f.ceil(
            f.months_between(as_at_col, date_of_occurrence_col)
        )
        quarters_since_occurrence = f.ceil(months_since_occurrence / 3)
        years_since_occurrence = f.ceil(months_since_occurrence / 12)
        puom = self.feature_period.period_unit_of_measure
        start = self.feature_period.start
        end = self.feature_period.end
        periods_since_occurrence = (
            days_since_occurrence
            if puom == PeriodUnitOfMeasure.DAY
            else weeks_since_occurrence
            if puom == PeriodUnitOfMeasure.WEEK
            else months_since_occurrence
            if puom == PeriodUnitOfMeasure.MONTH
            else quarters_since_occurrence
            if puom == PeriodUnitOfMeasure.QUARTER
            else years_since_occurrence
        )
        return self.aggregator()(
            f.when(
                (periods_since_occurrence <= start) & (periods_since_occurrence >= end),
                self.columnExpression(),
            )
        ).alias(self.feature_name)
