"""
Encapsulate the period of a feature, defined by a unit of time
measure, a start and an end
"""

from datetime import date

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

    @property
    def number_of_periods(self) -> int:
        "Number of periods between start and end (inclusive)"
        return self.start - self.end + 1

    def __str__(self) -> str:
        return self.description

    def __repr__(self) -> str:
        return (
            f"FeaturePeriod("
            f"period_unit_of_measure={self.period_unit_of_measure}, "
            f"start={self.start}, end={self.end})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FeaturePeriod):
            return False
        return (
            self.period_unit_of_measure == other.period_unit_of_measure
            and self.start == other.start
            and self.end == other.end
        )

    def __hash__(self) -> int:
        return hash((self.period_unit_of_measure, self.start, self.end))


TODAY = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0)],
}
YESTERDAY = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 1)],
}
THIS_WEEK = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0)],
}
LAST_WEEK = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1)],
}
THIS_MONTH = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.MONTH, 0, 0)],
}
LAST_MONTH = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1)],
}
THIS_QUARTER = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0)],
}
LAST_QUARTER = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1)],
}
THIS_YEAR = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0)],
}
LAST_YEAR = {
    "as_at": date.today(),
    "feature_periods": [FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 1)],
}
current_year = date.today().year
current_month = date.today().month
match current_month:
    case 1 | 2 | 3:
        quarters_this_year = [1]
    case 4 | 5 | 6:
        quarters_this_year = [1, 2]
    case 7 | 8 | 9:
        quarters_this_year = [1, 2, 3]
    case _:  # all other months:
        quarters_this_year = [1, 2, 3, 4]
ALL_MONTHS_LAST_YEAR = {
    "as_at": date(current_year, 1, 1),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, i, i) for i in range(1, 13)
    ],
}
ALL_MONTHS_THIS_YEAR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, i, i) for i in range(current_month)
    ],
}
ALL_QUARTERS_LAST_YEAR = {
    "as_at": date(current_year, 1, 1),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, i, i) for i in range(1, 5)
    ],
}
ALL_QUARTERS_THIS_YEAR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, i - 1, i - 1)
        for i in quarters_this_year
    ],
}
THIS_MONTH_VS_ONE_YEAR_PRIOR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 0, 0),
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 12, 12),
    ],
}
LAST_MONTH_VS_ONE_YEAR_PRIOR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1),
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 13, 13),
    ],
}
THIS_QUARTER_VS_ONE_YEAR_PRIOR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0),
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4),
    ],
}

LAST_QUARTER_VS_ONE_YEAR_PRIOR = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
        FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 5, 5),
    ],
}

LAST_FIVE_YEARS = {
    "as_at": date.today(),
    "feature_periods": [
        FeaturePeriod(PeriodUnitOfMeasure.YEAR, i, i) for i in range(1, 6)
    ],
}
