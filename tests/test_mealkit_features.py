from datetime import datetime, date
import pytest
from pyspark.sql import DataFrame, SparkSession

from jstark.mealkit.mealkit_features import MealkitFeatures
from jstark.features.feature import FeaturePeriod, PeriodUnitOfMeasure


def test_orderweeks(
    as_at_timestamp: datetime, dataframe_of_faker_mealkit_orders: DataFrame
):
    """Test OrderWeeks

    OrderWeeks is the number of weeks in which at least one order was placed

    """
    mf = MealkitFeatures(as_at=as_at_timestamp, feature_periods=["52w0"])
    output_df = (
        dataframe_of_faker_mealkit_orders.groupBy()
        .agg(*mf.features)
        .select("OrderCount_52w0")
    )
    first = output_df.first()
    assert first is not None
    assert first["OrderCount_52w0"] == 931


def test_as_at_timestamp(dataframe_of_faker_mealkit_orders: DataFrame):
    """
    We had a situation where as_at was being passed as a datetime (which we support)
    however an error was still occurring:
    > TypeError: can't compare datetime.datetime to datetime.date
    Following TDD this was written as a failing test which was then fixed,
    the fix is in the same commit as this test.
    """
    as_at_timestamp = datetime(2022, 11, 30, 10, 12, 13)
    mf = MealkitFeatures(
        as_at=as_at_timestamp, feature_periods=["1q1", "2q2", "3q3", "4q4"]
    )
    dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)


def test_desired_features(dataframe_of_faker_mealkit_orders: DataFrame):
    mf = MealkitFeatures(
        as_at=date(2022, 11, 30),
        feature_periods=["1q1", "2q2", "3q3", "4q4"],
        feature_stems=["OrderCount"],
    )
    output_df = dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)
    assert output_df.columns == [
        "OrderCount_1q1",
        "OrderCount_2q2",
        "OrderCount_3q3",
        "OrderCount_4q4",
    ]


def test_non_existent_desired_features(dataframe_of_faker_mealkit_orders: DataFrame):
    mf = MealkitFeatures(
        as_at=date(2022, 11, 30),
        feature_periods=["1q1", "2q2", "3q3", "4q4"],
        feature_stems=["NonExistentFeature", "NonExistentFeature2"],
    )
    with pytest.raises(Exception) as exc_info:
        dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)
    assert (
        str(exc_info.value)
        == "Feature(s) ['NonExistentFeature', 'NonExistentFeature2'] not found"
    )


def test_cuisines(dataframe_of_faker_mealkit_orders: DataFrame):
    mf = MealkitFeatures(
        as_at=date(2022, 1, 1),
        feature_periods=["1q1", "2q2", "3q3", "4q4"],
        feature_stems=["Cuisines"],
    )
    output_df = dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)
    assert output_df.columns == [
        "Cuisines_1q1",
        "Cuisines_2q2",
        "Cuisines_3q3",
        "Cuisines_4q4",
    ]
    first = output_df.first()
    assert first is not None
    assert first["Cuisines_1q1"] == ["Italian", "French", "Spanish"]
    assert first["Cuisines_2q2"] == ["Italian", "French", "Spanish"]
    assert first["Cuisines_3q3"] == ["Italian", "French", "Spanish"]
    assert first["Cuisines_4q4"] == ["Italian", "French", "Spanish"]


def test_cuisine(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            ("Italian", datetime(2022, 1, 10, 1, 2, 3)),
            ("Italian", datetime(2022, 1, 10, 1, 2, 3)),
            ("Italian", datetime(2022, 1, 10, 1, 2, 3)),
            ("French", datetime(2022, 1, 10, 1, 2, 3)),
            ("French", datetime(2022, 1, 10, 1, 2, 3)),
            ("Spanish", datetime(2022, 1, 10, 1, 2, 3)),
        ],
        ["cuisine", "timestamp"],
    )
    mf = MealkitFeatures(
        as_at=date(2022, 2, 1),
        feature_periods=["1m1"],
        feature_stems=[
            "ItalianCuisineCount",
            "FrenchCuisineCount",
            "SpanishCuisineCount",
        ],
    )
    output_df = df.groupBy().agg(*mf.features)
    assert (
        output_df.schema["ItalianCuisineCount_1m1"].metadata["description"]
        == "Count of Italian recipes between 2022-01-01 and 2022-01-31"
    )
    assert output_df.schema["ItalianCuisineCount_1m1"].metadata["commentary"] == (
        "The number of Italian recipes. Typically the dataframe supplied to this "
        + "feature will have many recipes for the same cuisine, this feature allows "
        + "you to determine how many Italian recipes have been ordered."
    )
    first = output_df.first()
    assert first is not None
    assert first["ItalianCuisineCount_1m1"] == 3
    assert first["FrenchCuisineCount_1m1"] == 2
    assert first["SpanishCuisineCount_1m1"] == 1
    assert [
        c.metadata["description"]
        for c in output_df.schema
        if c.name == "ItalianCuisineCount_1m1"
    ][0] == "Count of Italian recipes between 2022-01-01 and 2022-01-31"


def test_none_args(spark_session: SparkSession):
    mf = MealkitFeatures(as_at=date(2022, 2, 1))
    assert mf.feature_periods == [FeaturePeriod(PeriodUnitOfMeasure.WEEK, 52, 0)]
    assert mf.feature_stems == set[str]()
