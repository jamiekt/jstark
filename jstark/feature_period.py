"""
Encapsulate the period of a feature, defined by a unit of time
measure, a start and an end
"""
from jstark.period_unit_of_measure import PeriodUnitOfMeasure
from .exceptions import FeaturePeriodEndGreaterThanStartError


class FeaturePeriod:
    """
    Encapsulate the period of a feature, defined by a unit of time
    measure, a start and an end
    """

    def __init__(
        self, period_unit_of_measure: PeriodUnitOfMeasure, start: int, end: int
    ) -> None:
        if not isinstance(period_unit_of_measure, PeriodUnitOfMeasure):
            raise TypeError(
                (
                    "period_unit_of_measure needs to be of type "
                    + f"PeriodUnitOfMeasure, not {type(period_unit_of_measure)}"
                )
            )
        if end > start:
            raise FeaturePeriodEndGreaterThanStartError(start=start, end=end)
        self.__period_unit_of_measure = period_unit_of_measure
        self.__start = start
        self.__end = end

    @property
    def start(self) -> int:
        "Number of periods ago that the FeaturePeriod begins at"
        return self.__start

    @property
    def end(self) -> int:
        "Number of periods ago that the FeaturePeriod ends at"
        return self.__end

    @property
    def period_unit_of_measure(self) -> PeriodUnitOfMeasure:
        "Period unit of measure"
        return self.__period_unit_of_measure

    @property
    def mnemonic(self) -> str:
        "Mnemonic for the feature period"
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
