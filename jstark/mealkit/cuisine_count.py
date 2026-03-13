"""CuisineCount feature"""

import pyspark.sql.functions as f
from pyspark.sql import Column

from jstark.features.distinctcount_feature import DistinctCount
from jstark.features.count_if import CountIf


class CuisineCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Cuisine")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Cuisines"

    @property
    def commentary(self) -> str:
        return (
            "The number of cuisines. Typically the dataframe supplied "
            + "to this feature will have many recipes for the same cuisine, "
            + "this feature allows you to determine how many distinct cuisines "
            + "have been ordered."
        )


class CuisineCountIf(CountIf):
    CUISINE_NAME: str = ""

    def column_expression(self) -> Column:
        # Compare lowercase cuisine to class's CUISINE_NAME (also lowercase)
        return f.lower(f.col("Cuisine")) == self.CUISINE_NAME.lower()

    @property
    def description_subject(self) -> str:
        return f"Count of {self.CUISINE_NAME.capitalize()} recipes"

    @property
    def commentary(self) -> str:
        name = self.CUISINE_NAME.capitalize()
        return (
            f"The number of {name} recipes. Typically the "
            + "dataframe supplied to this feature will have "
            + "many recipes for the same cuisine, this feature "
            + f"allows you to determine how many {name} "
            + "recipes have been ordered."
        )


# This list of classes was based upon all the observed cuisines in
# all recipes from a well-known mealkit provider in 2025
class ItalianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "italian"


class FrenchCuisineCount(CuisineCountIf):
    CUISINE_NAME = "french"


class SpanishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "spanish"


class SrilankanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "srilankan"


class LebaneseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "lebanese"


class GreekCuisineCount(CuisineCountIf):
    CUISINE_NAME = "greek"


class VietnameseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "vietnamese"


class DanishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "danish"


class WesternEuropeCuisineCount(CuisineCountIf):
    CUISINE_NAME = "westerneurope"


class FusionCuisineCount(CuisineCountIf):
    CUISINE_NAME = "fusion"


class DutchCuisineCount(CuisineCountIf):
    CUISINE_NAME = "dutch"


class KoreanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "korean"


class SouthEastAsianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "southeast-asian"


class MalaysianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "malay"


class AmericanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "american"


class AsianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "asian"


class IndianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "indian"


class GermanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "german"


class CentralAmericaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "central america"


class ArgentinianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "argentinian"


class NorthAmericanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "north american"


class NewZealandCuisineCount(CuisineCountIf):
    CUISINE_NAME = "new zealand"


class CanadianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "canadian"


class SwedishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "swedish"


class EgyptianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "egyptian"


class NorthernEuropeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "northern europe"


class CajunCuisineCount(CuisineCountIf):
    CUISINE_NAME = "cajun"


class AustrianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "austrian"


class MexicanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "mexican"


class MediterraneanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "mediterranean"


class ThaiCuisineCount(CuisineCountIf):
    CUISINE_NAME = "thai"


class ChineseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "chinese"


class LatinAmericanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "latin american"


class SouthAsiaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "south asia"


class MiddleEasternCuisineCount(CuisineCountIf):
    CUISINE_NAME = "middleeastern"


class TraditionalCuisineCount(CuisineCountIf):
    CUISINE_NAME = "traditional"


class SteakhouseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "steakhouse"


class PacificislandsCuisineCount(CuisineCountIf):
    CUISINE_NAME = "pacificislands"


class EastAsiaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "east asia"


class BritishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "british"


class CaribbeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "caribbean"


class FilipinoCuisineCount(CuisineCountIf):
    CUISINE_NAME = "filipino"


class TurkishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "turkish"


class BelgianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "belgian"


class SouthAmericanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "south american"


class NorthAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "north african"


class SouthAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "south african"


class WestAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "west african"


class EastAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "east african"


class WesternEuropeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "western-european"


class PortugueseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "portuguese"


class PeruvianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "peruvian"


class JapaneseCuisineCount(CuisineCountIf):
    CUISINE_NAME = "japanese"


class PacificIslandsCuisineCount(CuisineCountIf):
    CUISINE_NAME = "pacific-islands"


class SouthernEuropeCuisineCount(CuisineCountIf):
    CUISINE_NAME = "southern europe"


class AfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "african"


class CentralAsiaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "centralasia"


class NorthamericaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "northamerica"


class EuropeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "european"


class MoroccanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "moroccan"


class AustralianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "australian"


class HungarianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "hungarian"


class IranianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "iranian"


class SoutheastAsiaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "southeast asia"


class HawaiianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "hawaiian"


class ScandinavianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "scandinavian"


class BrazilianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "brazilian"


class IndonesianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "indonesian"


class MongolianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "mongolian"


class RussianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "russian"


class SouthAsianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "south-asian"


class BulgarianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "bulgarian"


class FusionCuisineCusiineCount(CuisineCountIf):
    CUISINE_NAME = "fusion-cuisine"


class IrishCuisineCount(CuisineCountIf):
    CUISINE_NAME = "irish"


class GeorgianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "georgian"


class SouthwestCuisineCount(CuisineCountIf):
    CUISINE_NAME = "southwest"


class CambodianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "cambodian"


class LatinCuisineCount(CuisineCountIf):
    CUISINE_NAME = "latin"


class CubanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "cuban"


class SoutheastCuisineCount(CuisineCountIf):
    CUISINE_NAME = "southeast"


class SouthHyphenAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "south-african"


class JamaicanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "jamaican"


class IsrealiCuisineCount(CuisineCountIf):
    CUISINE_NAME = "israeli"


class EasteuropeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "easteuropean"


class SingaporeanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "singaporean"


class NordicCuisineCount(CuisineCountIf):
    CUISINE_NAME = "nordic"


class WestHyphenAfricanCuisineCount(CuisineCountIf):
    CUISINE_NAME = "west-african"


class NortheastCuisineCount(CuisineCountIf):
    CUISINE_NAME = "northeast"


class TonganCuisineCount(CuisineCountIf):
    CUISINE_NAME = "tongan"


class WestafricaCuisineCount(CuisineCountIf):
    CUISINE_NAME = "westafrica"


class ZanzibarianCuisineCount(CuisineCountIf):
    CUISINE_NAME = "zanzibarian"


class MidwestCuisineCount(CuisineCountIf):
    CUISINE_NAME = "midwest"
