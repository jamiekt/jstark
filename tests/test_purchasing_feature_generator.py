from pyspark.sql import DataFrame, Row
from datetime import date, datetime, timedelta
import pyspark.sql.functions as f

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_parse_references_grossspend():
    """
    PurchasingFeatureGenerator.parse_references() is only an internal helper function
    but still a good idea to have a couple of little tests here to verify and
    document its behaviour
    """
    assert PurchasingFeatureGenerator.parse_references(
        "List('Timestamp, 'GrossSpend)"
    ) == ["GrossSpend", "Timestamp"]


def test_parse_references_count():
    assert PurchasingFeatureGenerator.parse_references("List('Timestamp)") == [
        "Timestamp"
    ]


def test_references_count(as_at_timestamp: datetime):
    feature_generator = PurchasingFeatureGenerator(
        as_at=as_at_timestamp,
        feature_periods=[
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
        ],
    )
    assert feature_generator.references["Count_0d0"] == ["Timestamp"]


def test_references_grossspend(as_at_timestamp: datetime):
    feature_generator = PurchasingFeatureGenerator(
        as_at=as_at_timestamp,
        feature_periods=[
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
        ],
    )
    assert feature_generator.references["GrossSpend_0d0"] == ["GrossSpend", "Timestamp"]


def test_count_today(as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            ],
        ).features
    )
    result = df.first()
    assert result is not None
    assert float(result["Count_0d0"]) == 2


def test_gross_spend_today(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    def _validate_expected_value(df, expected):
        result = df.first()
        assert result is not None
        assert float(result["GrossSpend_0d0"]) == expected
        return result

    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 5.5)
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date() - timedelta(days=1),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 4.0)


def test_gross_spend_today_and_yesterday(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    def _validate_expected_value(df, expected):
        result = df.first()
        assert result is not None
        assert float(result["GrossSpend_1d0"]) == expected
        return result

    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 9.5)
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date() - timedelta(days=1),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 4.0)


def test_gross_spend_today_yesterday_and_day_before_yesterday(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            ],
        ).features
    )
    first = df.first()
    """This test is being used to verify the logic that filters the
    input dataframe according to each the respective feature period
    """
    assert first is not None
    assert float(first["GrossSpend_2d0"]) == 9.5


def test_gross_spend_day_before_yesterday(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 2),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_2d2"]) == 0


def test_gross_spend_this_week(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.WEEK, 0, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_0w0"]) == 9.5


def test_gross_spend_last_week(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 1),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1w1"]) == 3.25


def test_gross_spend_this_week_and_last_week(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.WEEK, 1, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1w0"]) == 12.75


def test_gross_spend_this_month(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.MONTH, 0, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_0m0"]) == 12.75


def test_gross_spend_last_month(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 1),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1m1"]) == 6.0


def test_gross_spend_this_month_and_last_month(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.MONTH, 1, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1m0"]) == 18.75


def test_gross_spend_this_quarter(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 0, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_0q0"]) == 18.75


def test_gross_spend_last_quarter(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 1),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1q1"]) == 7


def test_gross_spend_this_quarter_and_last_quarter(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.QUARTER, 1, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1q0"]) == 25.75


def test_gross_spend_this_year(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.YEAR, 0, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_0y0"]) == 25.75


def test_gross_spend_last_year(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 1),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1y1"]) == 8


def test_gross_spend_this_year_and_last_year(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.YEAR, 1, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert float(first["GrossSpend_1y0"]) == 33.75


def test_count_exists(dataframe_of_purchases: DataFrame):
    df = dataframe_of_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(as_at=date.today()).features
    )
    assert "Count_4d3" in df.schema.fieldNames()


def test_count(as_at_timestamp: datetime, dataframe_of_purchases: DataFrame):
    dataframe_of_purchases = dataframe_of_purchases.where(
        f.col("Store") == "Hammersmith"
    )
    df = dataframe_of_purchases.groupBy("Store").agg(
        *PurchasingFeatureGenerator(as_at=as_at_timestamp.date()).features
    )
    first = df.first()
    assert first is not None
    assert first["Count_2d0"] == 2


def test_grossspend_metadata_description(
    as_at_timestamp: datetime, dataframe_of_purchases: DataFrame
):
    df = dataframe_of_purchases.agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            ],
        ).features
    )
    assert [c.metadata["description"] for c in df.schema if c.name == "GrossSpend_2d0"][
        0
    ] == "Sum of GrossSpend between 2022-11-28 and 2022-11-30 (inclusive)"


def test_recencydays(as_at_timestamp: datetime, dataframe_of_purchases: DataFrame):
    dataframe_of_purchases = dataframe_of_purchases.where(
        f.col("Timestamp") < f.lit(date(2022, 1, 1))
    )
    df = dataframe_of_purchases.agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.YEAR, 2, 0),
            ],
        ).features
    )
    first = df.first()
    assert first is not None
    assert first["RecencyDays_2y0"] == 365


def test_basket_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["BasketCount_0y0"]) == 5


def test_store_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["StoreCount_0y0"]) == 3


def test_product_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["ProductCount_0y0"]) == 6


def test_customer_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["CustomerCount_0y0"]) == 2


def test_channel_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["ChannelCount_0y0"]) == 2


def test_approx_basket_count_luke_and_leia_0y0(luke_and_leia_purchases_0y0_first: Row):
    assert float(luke_and_leia_purchases_0y0_first["ApproxBasketCount_0y0"]) == 5


def test_approx_customer_count_luke_and_leia_0y0(
    luke_and_leia_purchases_0y0_first: Row,
):
    assert float(luke_and_leia_purchases_0y0_first["ApproxCustomerCount_0y0"]) == 2
