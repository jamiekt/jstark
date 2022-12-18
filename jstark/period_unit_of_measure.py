from enum import Enum


class PeriodUnitOfMeasure(Enum):
    """Units in which periods can be measured"""

    DAY = "d"
    WEEK = "w"
    MONTH = "m"
    QUARTER = "q"
    YEAR = "y"
