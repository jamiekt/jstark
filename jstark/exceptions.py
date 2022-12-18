"""jstark exceptions
"""
from jstark.period_unit_of_measure import PeriodUnitOfMeasure


class FeaturePeriodMnemonicIsInvalid(Exception):
    """FeaturePeriod mnemonic doesn't match X[dqmqy]Y"""

    def __str__(self) -> str:
        return (
            "FeaturePeriod mnemonic must be an integer followed by a letter "
            + f"from {[e.value for e in PeriodUnitOfMeasure]} followed by an integer"
        )


class FeaturePeriodEndGreaterThanStartError(Exception):
    """Exception indicating end of a feature period cannot be before the start"""

    def __init__(self, start: int, end: int, *args: object) -> None:
        super().__init__(*args)
        self.start = start
        self.end = end

    def __str__(self) -> str:
        return (
            f"End of the feature period ({self.end}) cannot be "
            + f"before the start of the feature period ({self.start})"
        )


class AsAtIsNotADate(Exception):
    """Exception indicating a value must be of type date"""

    def __str__(self) -> str:
        return "as_at value must be of type date"
