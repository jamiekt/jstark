from abc import ABC
from datetime import date
from enum import Enum
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
    def description(self) -> str:
        """Description of the feature period

        Pretty sure this will change in time, but this initial implementation will do for now

        Returns:
            str: description
        """
        return f"Between {self.start} and {self.end} {self.period_unit_of_measure.name.lower()}s ago"


class Feature(ABC):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        pass
