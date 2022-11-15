from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Literal

from pyspark.sql import Column, DataFrame

from .exceptions import FeaturePeriodEndGreaterThanStartError


class PeriodUnitOfMeasure(Enum):
    DAY = 1
    WEEK = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5


class FeaturePeriod(object):
    """Encapsulate the period of a feature, defined by a unit of time measure, a start and an end"""

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
        uom_abbreviation = self.__uom_abbreviation()
        return f"{self.start}{uom_abbreviation}{self.end}"

    def __uom_abbreviation(self) -> Literal["d", "w", "m", "q", "y"]:
        uom_name = self.period_unit_of_measure.name
        return (
            "d"
            if uom_name == "DAY"
            else "w"
            if uom_name == "WEEK"
            else "m"
            if uom_name == "MONTH"
            else "q"
            if uom_name == "QUARTER"
            else "y"
        )

    @property
    def description(self) -> str:
        """Description of the feature period

        Pretty sure this will change in time, but this initial implementation will do for now

        Returns:
            str: description
        """
        return f"Between {self.start} and {self.end} {self.period_unit_of_measure.name.lower()}s ago"


class Feature(ABC):
    def __init__(
        self, as_at: date, feature_period: FeaturePeriod, df: DataFrame
    ) -> None:
        self.__feature_period = feature_period
        self.__as_at = as_at
        self.__df = df

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
        return self.columnExpression().alias(self.feature_name)
