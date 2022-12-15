from datetime import date, datetime, timedelta
from pyspark.sql import DataFrame, Row
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
    """Basic test that static method parse_references works"""
    assert PurchasingFeatureGenerator.parse_references("List('Timestamp)") == [
        "Timestamp"
    ]


def test_references_count(purchasing_feature_generator: PurchasingFeatureGenerator):
    """Verify that Count_0d0 requires field Timestamp"""
    assert purchasing_feature_generator.references["Count_0d0"] == ["Timestamp"]


def test_references_grossspend(
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Verify that Count_0d0 requires fields Timestamp,GrossSpend"""
    assert purchasing_feature_generator.references["GrossSpend_0d0"] == [
        "GrossSpend",
        "Timestamp",
    ]


def test_count_today(luke_and_leia_purchases_first: Row):
    """Test Count_0d0"""
    assert float(luke_and_leia_purchases_first["Count_0d0"]) == 2


def test_gross_spend_today(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    """Test GrossSpend_0d0"""

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
    """Test GrossSpend_1d0"""

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
    luke_and_leia_purchases_first: Row,
):
    """This test is being used to verify the logic that filters the
    input dataframe according to each the respective feature period
    """
    assert float(luke_and_leia_purchases_first["GrossSpend_2d0"]) == 9.5


def test_gross_spend_day_before_yesterday(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_2d2"""
    assert float(luke_and_leia_purchases_first["GrossSpend_2d2"]) == 0


def test_gross_spend_this_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0w0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0w0"]) == 9.5


def test_gross_spend_last_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1w1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1w1"]) == 3.25


def test_gross_spend_this_week_and_last_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1w0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1w0"]) == 12.75


def test_gross_spend_this_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0m0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0m0"]) == 12.75


def test_gross_spend_last_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1m1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1m1"]) == 6.0


def test_gross_spend_this_month_and_last_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1m0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1m0"]) == 18.75


def test_gross_spend_this_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0q0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0q0"]) == 18.75


def test_gross_spend_last_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1q1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1q1"]) == 7


def test_gross_spend_this_quarter_and_last_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1q0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1q0"]) == 25.75


def test_gross_spend_this_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0y0"]) == 25.75


def test_gross_spend_last_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1y1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1y1"]) == 8


def test_gross_spend_this_year_and_last_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1y0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1y0"]) == 33.75


def test_grossspend_metadata_description(
    dataframe_of_purchases: DataFrame,
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Test GrossSpend_2d0 metadata"""
    df = dataframe_of_purchases.agg(*purchasing_feature_generator.features)
    assert [c.metadata["description"] for c in df.schema if c.name == "GrossSpend_2d0"][
        0
    ] == "Sum of GrossSpend between 2022-11-28 and 2022-11-30 (inclusive)"


def test_recencydays_and_mostrecentpurchasedate(
    dataframe_of_purchases: DataFrame,
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Test RecencyDays_2y0 & MostRecentPurchaseDate_2y0"""
    dataframe_of_purchases = dataframe_of_purchases.where(
        f.col("Timestamp") < f.lit(date(2022, 1, 1))
    )
    df = dataframe_of_purchases.agg(*purchasing_feature_generator.features)
    first = df.select("RecencyDays_2y0", "MostRecentPurchaseDate_2y0").first()
    assert first is not None
    assert first["RecencyDays_2y0"] == 365
    assert first["MostRecentPurchaseDate_2y0"] == date(2021, 11, 30)


def test_basket_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test BasketCount_0y0"""
    assert float(luke_and_leia_purchases_first["BasketCount_0y0"]) == 5


def test_store_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test StoreCount_0y0"""
    assert float(luke_and_leia_purchases_first["StoreCount_0y0"]) == 3


def test_product_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test ProductCount_0y0"""
    assert float(luke_and_leia_purchases_first["ProductCount_0y0"]) == 6


def test_customer_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test CustomerCount_0y0"""
    assert float(luke_and_leia_purchases_first["CustomerCount_0y0"]) == 2


def test_channel_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test ChannelCount_0y0"""
    assert float(luke_and_leia_purchases_first["ChannelCount_0y0"]) == 2


def test_discount_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test Discount_0y0"""
    assert float(luke_and_leia_purchases_first["Discount_0y0"]) == 1.05


def test_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test NetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["NetSpend_0y0"]) == 24.25


def test_max_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxNetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MaxNetSpend_0y0"]) == 6.75


def test_min_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinNetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MinNetSpend_0y0"]) == 2.25


def test_max_gross_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxGrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MaxGrossSpend_0y0"]) == 7.0


def test_min_gross_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinGrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MinGrossSpend_0y0"]) == 2.5


def test_avg_gross_spend_per_basket_luke_and_leia_0y0(
    luke_and_leia_purchases_first: Row,
):
    """Test AverageGrossSpendPerBasket_0y0"""
    assert (
        float(luke_and_leia_purchases_first["AverageGrossSpendPerBasket_0y0"]) == 5.15
    )


def test_quantity_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test Quantity_0y0"""
    assert float(luke_and_leia_purchases_first["Quantity_0y0"]) == 15


def test_avg_quantity_per_basket_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test AvgQuantityPerBasket_0y0"""
    assert float(luke_and_leia_purchases_first["AvgQuantityPerBasket_0y0"]) == 3.0


def test_min_net_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinNetPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MinNetPrice_0y0"]) == 0.5


def test_max_net_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxNetPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MaxNetPrice_0y0"]) == 3.75


def test_min_gross_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinGrossPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MinGrossPrice_0y0"]) == 0.5416666666667


def test_max_gross_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxGrossPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MaxGrossPrice_0y0"]) == 4.0
