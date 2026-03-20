from jstark.grocery import GroceryFeatures
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from datetime import date
from pyspark.sql import DataFrame
from pytest import mark


# fmt: off
@mark.parametrize(
    "as_at, feature_period, first_day_of_the_week, period_absolute, period_absolute_start, period_absolute_end, start_date, end_date",
    [
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0), "Saturday", "20260321", "20260321", "20260321", "2026-03-21", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 1), "Saturday", "20260320", "20260320", "20260320", "2026-03-20", "2026-03-20",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0), "Saturday", "20260320-20260321", "20260320", "20260321", "2026-03-20", "2026-03-21",),

        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0), "Saturday", "2026W13", "2026W13", "2026W13", "2026-03-21", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1), "Saturday", "2026W12", "2026W12", "2026W12", "2026-03-14", "2026-03-20",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 2, 2), "Saturday", "2026W11", "2026W11", "2026W11", "2026-03-07", "2026-03-13",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 3, 3), "Saturday", "2026W10", "2026W10", "2026W10", "2026-02-28", "2026-03-06",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 4, 4), "Saturday", "2026W09", "2026W09", "2026W09", "2026-02-21", "2026-02-27",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 5, 5), "Saturday", "2026W08", "2026W08", "2026W08", "2026-02-14", "2026-02-20",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 6, 6), "Saturday", "2026W07", "2026W07", "2026W07", "2026-02-07", "2026-02-13",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 7, 7), "Saturday", "2026W06", "2026W06", "2026W06", "2026-01-31", "2026-02-06",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 8, 8), "Saturday", "2026W05", "2026W05", "2026W05", "2026-01-24", "2026-01-30",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 9, 9), "Saturday", "2026W04", "2026W04", "2026W04", "2026-01-17", "2026-01-23",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 10, 10), "Saturday", "2026W03", "2026W03", "2026W03", "2026-01-10", "2026-01-16",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 11, 11), "Saturday", "2026W02", "2026W02", "2026W02", "2026-01-03", "2026-01-09",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 12, 12), "Saturday", "2026W01", "2026W01", "2026W01", "2025-12-27", "2026-01-02",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 13, 13), "Saturday", "2025W52", "2025W52", "2025W52", "2025-12-20", "2025-12-26",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 13, 0), "Saturday", "2025W52-2026W13", "2025W52", "2026W13", "2025-12-20", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 13, 1), "Saturday", "2025W52-2026W12", "2025W52", "2026W12", "2025-12-20", "2026-03-20",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0), "Thursday", "2026W12", "2026W12", "2026W12", "2026-03-19", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1), "Thursday", "2026W11", "2026W11", "2026W11", "2026-03-12", "2026-03-18",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 2, 2), "Thursday", "2026W10", "2026W10", "2026W10", "2026-03-05", "2026-03-11",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 3, 3), "Thursday", "2026W09", "2026W09", "2026W09", "2026-02-26", "2026-03-04",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 4, 4), "Thursday", "2026W08", "2026W08", "2026W08", "2026-02-19", "2026-02-25",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 5, 5), "Thursday", "2026W07", "2026W07", "2026W07", "2026-02-12", "2026-02-18",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 6, 6), "Thursday", "2026W06", "2026W06", "2026W06", "2026-02-05", "2026-02-11",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 7, 7), "Thursday", "2026W05", "2026W05", "2026W05", "2026-01-29", "2026-02-04",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 8, 8), "Thursday", "2026W04", "2026W04", "2026W04", "2026-01-22", "2026-01-28",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 9, 9), "Thursday", "2026W03", "2026W03", "2026W03", "2026-01-15", "2026-01-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 10, 10), "Thursday", "2026W02", "2026W02", "2026W02", "2026-01-08", "2026-01-14",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 11, 11), "Thursday", "2026W01", "2026W01", "2026W01", "2026-01-01", "2026-01-07",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.WEEK, 12, 12), "Thursday", "2025W53", "2025W53", "2025W53", "2025-12-25", "2025-12-31",),

        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 3), "Saturday", "2025Dec", "2025Dec", "2025Dec", "2025-12-01", "2025-12-31",),
        (date(2024, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1), "Saturday", "2024Feb", "2024Feb", "2024Feb", "2024-02-01", "2024-02-29",),
        (date(2024, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1), "Saturday", "2023Dec-2024Feb", "2023Dec", "2024Feb", "2023-12-01", "2024-02-29",),

        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0), "Saturday", "2026Q1", "2026Q1", "2026Q1", "2026-01-01", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 4), "Saturday", "2025Q1", "2025Q1", "2025Q1", "2025-01-01", "2025-03-31",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 4, 0), "Saturday", "2025Q1-2026Q1", "2025Q1", "2026Q1", "2025-01-01", "2026-03-21",),

        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0), "Saturday", "2026", "2026", "2026", "2026-01-01", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.YEAR, 5, 0), "Saturday", "2021-2026", "2021", "2026", "2021-01-01", "2026-03-21",),
        (date(2026, 3, 21),FeaturePeriod(PeriodUnitOfMeasure.YEAR, 5, 1), "Saturday", "2021-2025", "2021", "2025", "2021-01-01", "2025-12-31",),
    ],
)
# fmt: on
def test_period_absolute(
    luke_and_leia_purchases: DataFrame,
    as_at: date,
    feature_period: FeaturePeriod,
    first_day_of_the_week: str,
    period_absolute: str,
    period_absolute_start: str,
    period_absolute_end: str,
    start_date: str,
    end_date: str,
):
    fg = GroceryFeatures(
        as_at=as_at,
        feature_periods=[feature_period],
        feature_stems=["Count"],
        first_day_of_week=first_day_of_the_week,
    )

    df = luke_and_leia_purchases.groupBy().agg(*fg.features)
    assert (
        df.schema[f"Count_{feature_period.mnemonic}"].metadata["start-date"]
        == start_date
    )
    assert (
        df.schema[f"Count_{feature_period.mnemonic}"].metadata["end-date"] == end_date
    )
    assert (
        df.schema[f"Count_{feature_period.mnemonic}"].metadata["period-absolute-start"]
        == period_absolute_start
    )
    assert (
        df.schema[f"Count_{feature_period.mnemonic}"].metadata["period-absolute-end"]
        == period_absolute_end
    )
    assert (
        df.schema[f"Count_{feature_period.mnemonic}"].metadata["period-absolute"]
        == period_absolute
    )
