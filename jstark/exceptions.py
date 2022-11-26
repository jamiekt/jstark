class FeaturePeriodEndGreaterThanStartError(Exception):
    """Exception indicating end of a feature period cannot be before the start"""

    def __init__(self, start: int, end: int, *args: object) -> None:
        super().__init__(*args)
        self.start = start
        self.end = end

    def __str__(self) -> str:
        return f"End of the feature period ({self.end}) cannot be before the start of the feature period ({self.start})"


class DataFrameDoesNotIncludeTimestampColumn(Exception):
    "Exception indicating DataFrame does not include a column called Timestamp of type Timestamp"

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return (
            "DataFrame does not include column called Timestamp of type TimestampType"
        )


class AsAtIsNotADate(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return "as_at value must be of type Date"
