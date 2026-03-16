# jstark

[![PyPI - Version](https://img.shields.io/pypi/v/jstark.svg)](https://pypi.org/project/jstark)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jstark.svg)](https://pypi.org/project/jstark)
![build](https://github.com/jamiekt/jstark/actions/workflows/build.yml/badge.svg)
[![coverage](https://jamiekt.github.io/jstark/coverage.svg 'Click to see coverage report')](https://jamiekt.github.io/jstark/htmlcov/)
[![pylint](https://jamiekt.github.io/jstark/pylint.svg 'Click to view pylint report')](https://jamiekt.github.io/jstark/pylint.html)

Built with [![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

-----

A [PySpark](https://spark.apache.org/docs/latest/api/python/) library for generating time-based features for machine learning. All features are calculated relative to an **as at** date, enabling point-in-time feature engineering over configurable time periods.

## Feature period mnemonics

Feature names end with a mnemonic describing the time window. The format is `{start}{unit}{end}` where the unit is one
of `d` (days), `w` (weeks), `m` (months), `q` (quarters) or `y` (years).

For example, `BasketCount_3m1` is the distinct count of baskets from 3 months before to 1 month before the as at date.

Multiple periods can be calculated in a single Spark job:

```python
from datetime import date
from jstark.grocery import GroceryFeatures

gf = GroceryFeatures(as_at=date(2022, 1, 1), feature_periods=["3m1", "6m4"])
output_df = input_df.groupBy("Store").agg(*gf.features)
```

This produces `BasketCount_3m1`, `BasketCount_6m4`, and every other feature for both periods. See the
[Features reference](#features-reference) for a list of all available features.

## Quick start

**Prerequisites:** Java runtime required for PySpark. On macOS: `brew install openjdk@11`.

```shell
pip install jstark[faker]
```

The `faker` extra installs [Faker](https://faker.readthedocs.io/), which is needed for the sample data generator used
below. If you don't need sample data, `pip install jstark` is sufficient.

```python
from datetime import date
from jstark.sample.transactions import FakeGroceryTransactions
from jstark.grocery import GroceryFeatures

input_df = FakeGroceryTransactions().df
gf = GroceryFeatures(date(2022, 1, 1), ["4q4", "3q3", "2q2", "1q1"])
output_df = input_df.groupBy("Store").agg(*gf.features)
output_df.select(
    "Store", "BasketCount_4q4", "BasketCount_3q3", "BasketCount_2q2", "BasketCount_1q1"
).show()
```

```shell
+-----------+---------------+---------------+---------------+---------------+
|      Store|BasketCount_4q4|BasketCount_3q3|BasketCount_2q2|BasketCount_1q1|
+-----------+---------------+---------------+---------------+---------------+
|    Staines|             47|             46|             48|             51|
| Twickenham|             55|             57|             48|             49|
|     Ealing|             52|             51|             50|             54|
|Hammersmith|             47|             40|             43|             51|
|   Richmond|             54|             40|             64|             53|
+-----------+---------------+---------------+---------------+---------------+
```

## Feature descriptions and references

Every feature carries a description in its column metadata:

```python
from pprint import pprint
pprint([(c.name, c.metadata["description"]) for c in output_df.schema if c.name.endswith("1q1")])
```

```python
[('BasketCount_1q1',
  'Distinct count of Baskets between 2021-10-01 and 2021-12-31'),
 ...]
```

You can also inspect what input columns each feature requires:

```python
gf.references["BasketCount_1q1"]                   # ['Basket', 'Timestamp']
gf.references["CustomerCount_1q1"]                 # ['Customer', 'Timestamp']
gf.references["AvgGrossSpendPerBasket_1q1"]        # ['Basket', 'GrossSpend', 'Timestamp']
```

All features require a `Timestamp` column ([TimestampType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampType.html)). Most require additional columns depending on what they measure.

## Features reference


<details><summary>Grocery features</summary>

A list of all Grocery features available if one were to call:

```python
GroceryFeatures(date(2026, 1, 1), ["3m1"])
```

<!--[[[cog
from datetime import date, datetime, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.grocery.grocery_features import GroceryFeatures

d = date(2022, 1, 1)
spark = SparkSession.builder.getOrCreate()
schema = StructType(
    [
        StructField("Timestamp", TimestampType(), True),
        StructField("Basket", StringType(), True),
        StructField("Store", StringType(), True),
        StructField("Customer", StringType(), True),
        StructField("Channel", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("NetSpend", FloatType(), True),
        StructField("GrossSpend", FloatType(), True),
        StructField("Discount", FloatType(), True),
    ]
)
df = spark.createDataFrame([(datetime.combine(d, time.min), "1234567890", "Store1", "Customer1", "Channel1", "Product1", 1, 1.2, 1.5, 0.3)], schema)
period = FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1)
gf = GroceryFeatures(d, [period])
output_df = df.groupBy().agg(*gf.features)

cog.outl("| Feature | Description |")
cog.outl("| --- | --- |")
for name, description in sorted(
    [(c.name, c.metadata["description"]) for c in output_df.schema],
    key=lambda x: x[0]
):
    cog.outl(f"| {name} | {description} |")
]]]-->
| Feature | Description |
| --- | --- |
| ApproxBasketCount_3m1 | Approximate distinct count of Baskets between 2021-10-01 and 2021-12-31 |
| ApproxCustomerCount_3m1 | Approximate distinct count of Customers between 2021-10-01 and 2021-12-31 |
| ApproxProductCount_3m1 | Approximate distinct count of Products between 2021-10-01 and 2021-12-31 |
| AverageBasketsPerMonth_3m1 | Average number of baskets per month between 2021-10-01 and 2021-12-31 |
| AvgDiscountPerBasket_3m1 | Average Discount per Basket between 2021-10-01 and 2021-12-31 |
| AvgGrossSpendPerBasket_3m1 | Average GrossSpend per Basket between 2021-10-01 and 2021-12-31 |
| AvgPurchaseCycle_3m1 | Average purchase cycle between 2021-10-01 and 2021-12-31 |
| AvgQuantityPerBasket_3m1 | Average Quantity per Basket between 2021-10-01 and 2021-12-31 |
| BasketCount_3m1 | Distinct count of Baskets between 2021-10-01 and 2021-12-31 |
| BasketMonths_3m1 | Number of months in which at least one basket was purchased between 2021-10-01 and 2021-12-31 |
| ChannelCount_3m1 | Distinct count of Channels between 2021-10-01 and 2021-12-31 |
| Count_3m1 | Count of rows between 2021-10-01 and 2021-12-31 |
| CustomerCount_3m1 | Distinct count of Customers between 2021-10-01 and 2021-12-31 |
| CyclesSinceLastPurchase_3m1 | Cycles since last purchase between 2021-10-01 and 2021-12-31 |
| Discount_3m1 | Sum of Discount between 2021-10-01 and 2021-12-31 |
| EarliestPurchaseDate_3m1 | Earliest purchase date between 2021-10-01 and 2021-12-31 |
| GrossSpend_3m1 | Sum of GrossSpend between 2021-10-01 and 2021-12-31 |
| MaxGrossPrice_3m1 | Maximum of (GrossSpend / Quantity) between 2021-10-01 and 2021-12-31 |
| MaxGrossSpend_3m1 | Maximum GrossSpend value between 2021-10-01 and 2021-12-31 |
| MaxNetPrice_3m1 | Maximum of (NetSpend / Quantity) between 2021-10-01 and 2021-12-31 |
| MaxNetSpend_3m1 | Maximum of NetSpend value between 2021-10-01 and 2021-12-31 |
| MinGrossPrice_3m1 | Minimum of (GrossSpend / Quantity) between 2021-10-01 and 2021-12-31 |
| MinGrossSpend_3m1 | Minimum GrossSpend value between 2021-10-01 and 2021-12-31 |
| MinNetPrice_3m1 | Minimum of (NetSpend / Quantity) between 2021-10-01 and 2021-12-31 |
| MinNetSpend_3m1 | Minimum of NetSpend value between 2021-10-01 and 2021-12-31 |
| MostRecentPurchaseDate_3m1 | Most recent purchase date between 2021-10-01 and 2021-12-31 |
| NetSpend_3m1 | Sum of NetSpend between 2021-10-01 and 2021-12-31 |
| ProductCount_3m1 | Distinct count of Products between 2021-10-01 and 2021-12-31 |
| Quantity_3m1 | Sum of Quantity between 2021-10-01 and 2021-12-31 |
| RecencyDays_3m1 | Minimum number of days since occurrence between 2021-10-01 and 2021-12-31 |
| RecencyWeightedApproxBasketMonths90_3m1 | Exponentially weighted moving average, with smoothing factor of 0.9, of the approximate number of baskets per month between 2021-10-01 and 2021-12-31 |
| RecencyWeightedApproxBasketMonths95_3m1 | Exponentially weighted moving average, with smoothing factor of 0.95, of the approximate number of baskets per month between 2021-10-01 and 2021-12-31 |
| RecencyWeightedApproxBasketMonths99_3m1 | Exponentially weighted moving average, with smoothing factor of 0.99, of the approximate number of baskets per month between 2021-10-01 and 2021-12-31 |
| RecencyWeightedBasketMonths90_3m1 | Exponentially weighted moving average, with smoothing factor of 0.9, of the number of baskets per month between 2021-10-01 and 2021-12-31 |
| RecencyWeightedBasketMonths95_3m1 | Exponentially weighted moving average, with smoothing factor of 0.95, of the number of baskets per month between 2021-10-01 and 2021-12-31 |
| RecencyWeightedBasketMonths99_3m1 | Exponentially weighted moving average, with smoothing factor of 0.99, of the number of baskets per month between 2021-10-01 and 2021-12-31 |
| StoreCount_3m1 | Distinct count of Stores between 2021-10-01 and 2021-12-31 |
<!--[[[end]]]-->
</details>

<details><summary>Mealkit features</summary>

A list of all Mealkit features available if one were to call:

```python
MealkitFeatures(date(2026, 1, 1), ["3m1"])
```

<!--[[[cog
from datetime import date, datetime, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.mealkit.mealkit_features import MealkitFeatures

d = date(2022, 1, 1)
spark = SparkSession.builder.getOrCreate()
schema = StructType(
    [
        StructField("Timestamp", TimestampType(), True),
        StructField("Order", StringType(), True),
        StructField("Cuisine", StringType(), True),
        StructField("Customer", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Recipe", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("NetSpend", FloatType(), True),
        StructField("GrossSpend", FloatType(), True),
        StructField("Discount", FloatType(), True),
    ]
)
df = spark.createDataFrame([(datetime.combine(d, time.min), "1234567890", "Cuisine1", "Customer1",  "Product1", "Recipe1", 1, 1.2, 1.5, 0.3)], schema)
period = FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1)
mf = MealkitFeatures(d, [period])
output_df = df.groupBy().agg(*mf.features)

cog.outl("| Feature | Description |")
cog.outl("| --- | --- |")
for name, description in sorted(
    [(c.name, c.metadata["description"]) for c in output_df.schema],
    key=lambda x: x[0]
):
    cog.outl(f"| {name} | {description} |")
]]]-->
| Feature | Description |
| --- | --- |
| AfricanCuisineCount_3m1 | Count of African recipes between 2021-10-01 and 2021-12-31 |
| AmericanCuisineCount_3m1 | Count of American recipes between 2021-10-01 and 2021-12-31 |
| ApproxCustomerCount_3m1 | Approximate distinct count of Customers between 2021-10-01 and 2021-12-31 |
| ApproxOrderCount_3m1 | Approximate distinct count of Orders between 2021-10-01 and 2021-12-31 |
| ApproxProductCount_3m1 | Approximate distinct count of Products between 2021-10-01 and 2021-12-31 |
| ApproxRecipeCount_3m1 | Approximate distinct count of Recipes between 2021-10-01 and 2021-12-31 |
| ArgentinianCuisineCount_3m1 | Count of Argentinian recipes between 2021-10-01 and 2021-12-31 |
| AsianCuisineCount_3m1 | Count of Asian recipes between 2021-10-01 and 2021-12-31 |
| AustralianCuisineCount_3m1 | Count of Australian recipes between 2021-10-01 and 2021-12-31 |
| AustrianCuisineCount_3m1 | Count of Austrian recipes between 2021-10-01 and 2021-12-31 |
| AverageOrdersPerMonth_3m1 | Average number of orders per month between 2021-10-01 and 2021-12-31 |
| AvgPurchaseCycle_3m1 | Average purchase cycle between 2021-10-01 and 2021-12-31 |
| AvgQuantityPerOrder_3m1 | Average Quantity per Order between 2021-10-01 and 2021-12-31 |
| BelgianCuisineCount_3m1 | Count of Belgian recipes between 2021-10-01 and 2021-12-31 |
| BrazilianCuisineCount_3m1 | Count of Brazilian recipes between 2021-10-01 and 2021-12-31 |
| BritishCuisineCount_3m1 | Count of British recipes between 2021-10-01 and 2021-12-31 |
| BulgarianCuisineCount_3m1 | Count of Bulgarian recipes between 2021-10-01 and 2021-12-31 |
| CajunCuisineCount_3m1 | Count of Cajun recipes between 2021-10-01 and 2021-12-31 |
| CambodianCuisineCount_3m1 | Count of Cambodian recipes between 2021-10-01 and 2021-12-31 |
| CanadianCuisineCount_3m1 | Count of Canadian recipes between 2021-10-01 and 2021-12-31 |
| CaribbeanCuisineCount_3m1 | Count of Caribbean recipes between 2021-10-01 and 2021-12-31 |
| CentralAmericaCuisineCount_3m1 | Count of Central america recipes between 2021-10-01 and 2021-12-31 |
| CentralAsiaCuisineCount_3m1 | Count of Centralasia recipes between 2021-10-01 and 2021-12-31 |
| ChineseCuisineCount_3m1 | Count of Chinese recipes between 2021-10-01 and 2021-12-31 |
| Count_3m1 | Count of rows between 2021-10-01 and 2021-12-31 |
| CubanCuisineCount_3m1 | Count of Cuban recipes between 2021-10-01 and 2021-12-31 |
| CuisineCount_3m1 | Distinct count of Cuisines between 2021-10-01 and 2021-12-31 |
| Cuisines_3m1 | Set of Cuisines between 2021-10-01 and 2021-12-31 |
| CustomerCount_3m1 | Distinct count of Customers between 2021-10-01 and 2021-12-31 |
| CyclesSinceLastOrder_3m1 | Cycles since last order between 2021-10-01 and 2021-12-31 |
| DanishCuisineCount_3m1 | Count of Danish recipes between 2021-10-01 and 2021-12-31 |
| Discount_3m1 | Sum of Discount between 2021-10-01 and 2021-12-31 |
| DutchCuisineCount_3m1 | Count of Dutch recipes between 2021-10-01 and 2021-12-31 |
| EarliestPurchaseDate_3m1 | Earliest purchase date between 2021-10-01 and 2021-12-31 |
| EastAfricanCuisineCount_3m1 | Count of East african recipes between 2021-10-01 and 2021-12-31 |
| EastAsiaCuisineCount_3m1 | Count of East asia recipes between 2021-10-01 and 2021-12-31 |
| EasteuropeanCuisineCount_3m1 | Count of Easteuropean recipes between 2021-10-01 and 2021-12-31 |
| EgyptianCuisineCount_3m1 | Count of Egyptian recipes between 2021-10-01 and 2021-12-31 |
| EuropeanCuisineCount_3m1 | Count of European recipes between 2021-10-01 and 2021-12-31 |
| FilipinoCuisineCount_3m1 | Count of Filipino recipes between 2021-10-01 and 2021-12-31 |
| FrenchCuisineCount_3m1 | Count of French recipes between 2021-10-01 and 2021-12-31 |
| FusionCuisineCount_3m1 | Count of Fusion recipes between 2021-10-01 and 2021-12-31 |
| FusionCuisineCusiineCount_3m1 | Count of Fusion-cuisine recipes between 2021-10-01 and 2021-12-31 |
| GeorgianCuisineCount_3m1 | Count of Georgian recipes between 2021-10-01 and 2021-12-31 |
| GermanCuisineCount_3m1 | Count of German recipes between 2021-10-01 and 2021-12-31 |
| GreekCuisineCount_3m1 | Count of Greek recipes between 2021-10-01 and 2021-12-31 |
| HawaiianCuisineCount_3m1 | Count of Hawaiian recipes between 2021-10-01 and 2021-12-31 |
| HungarianCuisineCount_3m1 | Count of Hungarian recipes between 2021-10-01 and 2021-12-31 |
| IndianCuisineCount_3m1 | Count of Indian recipes between 2021-10-01 and 2021-12-31 |
| IndonesianCuisineCount_3m1 | Count of Indonesian recipes between 2021-10-01 and 2021-12-31 |
| IranianCuisineCount_3m1 | Count of Iranian recipes between 2021-10-01 and 2021-12-31 |
| IrishCuisineCount_3m1 | Count of Irish recipes between 2021-10-01 and 2021-12-31 |
| IsrealiCuisineCount_3m1 | Count of Israeli recipes between 2021-10-01 and 2021-12-31 |
| ItalianCuisineCount_3m1 | Count of Italian recipes between 2021-10-01 and 2021-12-31 |
| JamaicanCuisineCount_3m1 | Count of Jamaican recipes between 2021-10-01 and 2021-12-31 |
| JapaneseCuisineCount_3m1 | Count of Japanese recipes between 2021-10-01 and 2021-12-31 |
| KoreanCuisineCount_3m1 | Count of Korean recipes between 2021-10-01 and 2021-12-31 |
| LatinAmericanCuisineCount_3m1 | Count of Latin american recipes between 2021-10-01 and 2021-12-31 |
| LatinCuisineCount_3m1 | Count of Latin recipes between 2021-10-01 and 2021-12-31 |
| LebaneseCuisineCount_3m1 | Count of Lebanese recipes between 2021-10-01 and 2021-12-31 |
| MalaysianCuisineCount_3m1 | Count of Malay recipes between 2021-10-01 and 2021-12-31 |
| MediterraneanCuisineCount_3m1 | Count of Mediterranean recipes between 2021-10-01 and 2021-12-31 |
| MexicanCuisineCount_3m1 | Count of Mexican recipes between 2021-10-01 and 2021-12-31 |
| MiddleEasternCuisineCount_3m1 | Count of Middleeastern recipes between 2021-10-01 and 2021-12-31 |
| MidwestCuisineCount_3m1 | Count of Midwest recipes between 2021-10-01 and 2021-12-31 |
| MongolianCuisineCount_3m1 | Count of Mongolian recipes between 2021-10-01 and 2021-12-31 |
| MoroccanCuisineCount_3m1 | Count of Moroccan recipes between 2021-10-01 and 2021-12-31 |
| MostRecentPurchaseDate_3m1 | Most recent purchase date between 2021-10-01 and 2021-12-31 |
| NewZealandCuisineCount_3m1 | Count of New zealand recipes between 2021-10-01 and 2021-12-31 |
| NordicCuisineCount_3m1 | Count of Nordic recipes between 2021-10-01 and 2021-12-31 |
| NorthAfricanCuisineCount_3m1 | Count of North african recipes between 2021-10-01 and 2021-12-31 |
| NorthAmericanCuisineCount_3m1 | Count of North american recipes between 2021-10-01 and 2021-12-31 |
| NorthamericaCuisineCount_3m1 | Count of Northamerica recipes between 2021-10-01 and 2021-12-31 |
| NortheastCuisineCount_3m1 | Count of Northeast recipes between 2021-10-01 and 2021-12-31 |
| NorthernEuropeanCuisineCount_3m1 | Count of Northern europe recipes between 2021-10-01 and 2021-12-31 |
| OrderCount_3m1 | Distinct count of Orders between 2021-10-01 and 2021-12-31 |
| OrderMonths_3m1 | Number of months in which at least one order was placed between 2021-10-01 and 2021-12-31 |
| PacificIslandsCuisineCount_3m1 | Count of Pacific-islands recipes between 2021-10-01 and 2021-12-31 |
| PacificislandsCuisineCount_3m1 | Count of Pacificislands recipes between 2021-10-01 and 2021-12-31 |
| PeruvianCuisineCount_3m1 | Count of Peruvian recipes between 2021-10-01 and 2021-12-31 |
| PortugueseCuisineCount_3m1 | Count of Portuguese recipes between 2021-10-01 and 2021-12-31 |
| ProductCount_3m1 | Distinct count of Products between 2021-10-01 and 2021-12-31 |
| Quantity_3m1 | Sum of Quantity between 2021-10-01 and 2021-12-31 |
| RecencyDays_3m1 | Minimum number of days since occurrence between 2021-10-01 and 2021-12-31 |
| RecipeCount_3m1 | Distinct count of Recipes between 2021-10-01 and 2021-12-31 |
| RussianCuisineCount_3m1 | Count of Russian recipes between 2021-10-01 and 2021-12-31 |
| ScandinavianCuisineCount_3m1 | Count of Scandinavian recipes between 2021-10-01 and 2021-12-31 |
| SingaporeanCuisineCount_3m1 | Count of Singaporean recipes between 2021-10-01 and 2021-12-31 |
| SouthAfricanCuisineCount_3m1 | Count of South african recipes between 2021-10-01 and 2021-12-31 |
| SouthAmericanCuisineCount_3m1 | Count of South american recipes between 2021-10-01 and 2021-12-31 |
| SouthAsiaCuisineCount_3m1 | Count of South asia recipes between 2021-10-01 and 2021-12-31 |
| SouthAsianCuisineCount_3m1 | Count of South-asian recipes between 2021-10-01 and 2021-12-31 |
| SouthEastAsianCuisineCount_3m1 | Count of Southeast-asian recipes between 2021-10-01 and 2021-12-31 |
| SouthHyphenAfricanCuisineCount_3m1 | Count of South-african recipes between 2021-10-01 and 2021-12-31 |
| SoutheastAsiaCuisineCount_3m1 | Count of Southeast asia recipes between 2021-10-01 and 2021-12-31 |
| SoutheastCuisineCount_3m1 | Count of Southeast recipes between 2021-10-01 and 2021-12-31 |
| SouthernEuropeCuisineCount_3m1 | Count of Southern europe recipes between 2021-10-01 and 2021-12-31 |
| SouthwestCuisineCount_3m1 | Count of Southwest recipes between 2021-10-01 and 2021-12-31 |
| SpanishCuisineCount_3m1 | Count of Spanish recipes between 2021-10-01 and 2021-12-31 |
| SrilankanCuisineCount_3m1 | Count of Srilankan recipes between 2021-10-01 and 2021-12-31 |
| SteakhouseCuisineCount_3m1 | Count of Steakhouse recipes between 2021-10-01 and 2021-12-31 |
| SwedishCuisineCount_3m1 | Count of Swedish recipes between 2021-10-01 and 2021-12-31 |
| ThaiCuisineCount_3m1 | Count of Thai recipes between 2021-10-01 and 2021-12-31 |
| TonganCuisineCount_3m1 | Count of Tongan recipes between 2021-10-01 and 2021-12-31 |
| TraditionalCuisineCount_3m1 | Count of Traditional recipes between 2021-10-01 and 2021-12-31 |
| TurkishCuisineCount_3m1 | Count of Turkish recipes between 2021-10-01 and 2021-12-31 |
| VietnameseCuisineCount_3m1 | Count of Vietnamese recipes between 2021-10-01 and 2021-12-31 |
| WestAfricanCuisineCount_3m1 | Count of West african recipes between 2021-10-01 and 2021-12-31 |
| WestHyphenAfricanCuisineCount_3m1 | Count of West-african recipes between 2021-10-01 and 2021-12-31 |
| WestafricaCuisineCount_3m1 | Count of Westafrica recipes between 2021-10-01 and 2021-12-31 |
| WesternEuropeCuisineCount_3m1 | Count of Westerneurope recipes between 2021-10-01 and 2021-12-31 |
| WesternEuropeanCuisineCount_3m1 | Count of Western-european recipes between 2021-10-01 and 2021-12-31 |
| ZanzibarianCuisineCount_3m1 | Count of Zanzibarian recipes between 2021-10-01 and 2021-12-31 |
<!--[[[end]]]-->
</details>

## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Why "jstark"?

The name is phonetically similar to PySpark, is a homage to [comic book character Jon Stark](https://www.worthpoint.com/worthopedia/football-picture-story-monthly-stark-423630034), and contains the initials of the original contributor (j, k & t).
