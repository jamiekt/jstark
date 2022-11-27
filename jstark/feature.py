from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Callable

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as f

from .exceptions import FeaturePeriodEndGreaterThanStartError


class PeriodUnitOfMeasure(Enum):
    DAY = "d"
    WEEK = "w"
    MONTH = "m"
    QUARTER = "q"
    YEAR = "y"


class FeaturePeriod(object):
    """
    Encapsulate the period of a feature, defined by a unit of time
    measure, a start and an end
    """

    def __init__(
        self, period_unit_of_measure: PeriodUnitOfMeasure, start: int, end: int
    ) -> None:
        if end > start:
            raise FeaturePeriodEndGreaterThanStartError(start=start, end=end)
        self.__period_unit_of_measure = period_unit_of_measure
        self.__start = start
        self.__end = end

    @property
    def start(self) -> int:
        return self.__start

    @property
    def end(self) -> int:
        return self.__end

    @property
    def period_unit_of_measure(self) -> PeriodUnitOfMeasure:
        return self.__period_unit_of_measure

    @property
    def code(self) -> str:
        return f"{self.start}{self.period_unit_of_measure.value}{self.end}"

    @property
    def description(self) -> str:
        """Description of the feature period

        Pretty sure this will change in time, but this initial implementation
        will do for now

        Returns:
            str: description
        """
        return (
            f"Between {self.start} and {self.end} "
            + f"{self.period_unit_of_measure.name.lower()}s ago"
        )


class Feature(ABC):
    def __init__(
        self, as_at: date, feature_period: FeaturePeriod, df: DataFrame
    ) -> None:
        self.__feature_period = feature_period
        self.__as_at = as_at
        self.__df = df

    def sum_aggregator(self, column: Column) -> Column:
        return f.sum(column)

    def count_aggregator(self, column: Column) -> Column:
        return f.count(column)

    @property
    @abstractmethod
    def aggregator(self) -> Callable[[Column], Column]:
        pass

    @property
    def feature_period(self) -> FeaturePeriod:
        return self.__feature_period

    @property
    def as_at(self) -> date:
        return self.__as_at

    @property
    def df(self) -> DataFrame:
        return self.__df

    @abstractmethod
    def columnExpression(self) -> Column:
        pass

    @property
    def feature_name(self) -> str:
        return f"{type(self).__name__}_{self.feature_period.code}"

    @property
    def column(self) -> Column:
        return self.aggregator()(self.columnExpression()).alias(self.feature_name)
