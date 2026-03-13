from datetime import date

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features.count import Count
from jstark.features.recency_days import RecencyDays
from jstark.features.customer_count import CustomerCount
from jstark.features.approx_customer_count import ApproxCustomerCount
from jstark.features.quantity import Quantity
from jstark.features.product_count import ProductCount
from jstark.features.most_recent_purchase_date import MostRecentPurchaseDate
from jstark.features.earliest_purchase_date import EarliestPurchaseDate
from jstark.mealkit.order_periods import OrderPeriods
from jstark.mealkit.order_count import OrderCount
from jstark.mealkit.approx_order_count import ApproxOrderCount
from jstark.features.approx_product_count import ApproxProductCount
from jstark.mealkit.recipe_count import RecipeCount
from jstark.mealkit.approx_recipe_count import ApproxRecipeCount
from jstark.mealkit.average_order import AvgOrder
from jstark.features.discount import Discount
from jstark.features.feature import Feature
from jstark.feature_generator import FeatureGenerator
from jstark.mealkit.average_quantity_per_order import AvgQuantityPerOrder
from jstark.mealkit.cycles_since_last_order import CyclesSinceLastOrder
from jstark.mealkit.average_purchase_cycle import AvgPurchaseCycle
from jstark.mealkit.cuisines import Cuisines
from jstark.mealkit.cuisine_count import CuisineCount
from jstark.mealkit.cuisine_count import (
    ItalianCuisineCount,
    FrenchCuisineCount,
    SpanishCuisineCount,
    SrilankanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    MalaysianCuisineCount,
    AmericanCuisineCount,
    AsianCuisineCount,
    GermanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    CentralAmericaCuisineCount,
    ArgentinianCuisineCount,
    NorthAmericanCuisineCount,
    NewZealandCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    CanadianCuisineCount,
    SwedishCuisineCount,
    EgyptianCuisineCount,
    NorthernEuropeanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    CajunCuisineCount,
    AustrianCuisineCount,
    MexicanCuisineCount,
    MediterraneanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    ThaiCuisineCount,
    ChineseCuisineCount,
    LatinAmericanCuisineCount,
    SouthAsiaCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    MiddleEasternCuisineCount,
    TraditionalCuisineCount,
    SteakhouseCuisineCount,
    PacificislandsCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    EastAsiaCuisineCount,
    BritishCuisineCount,
    CaribbeanCuisineCount,
    FilipinoCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    TurkishCuisineCount,
    BelgianCuisineCount,
    SouthAmericanCuisineCount,
    NorthAfricanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    SouthAfricanCuisineCount,
    WestAfricanCuisineCount,
    EastAfricanCuisineCount,
    WesternEuropeanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    PortugueseCuisineCount,
    PeruvianCuisineCount,
    JapaneseCuisineCount,
    PacificIslandsCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    SouthernEuropeCuisineCount,
    AfricanCuisineCount,
    CentralAsiaCuisineCount,
    NorthamericaCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    EuropeanCuisineCount,
    MoroccanCuisineCount,
    AustralianCuisineCount,
    HungarianCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    IranianCuisineCount,
    SoutheastAsiaCuisineCount,
    HawaiianCuisineCount,
    ScandinavianCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    BrazilianCuisineCount,
    IndonesianCuisineCount,
    MongolianCuisineCount,
    RussianCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    SouthAsianCuisineCount,
    BulgarianCuisineCount,
    FusionCuisineCusiineCount,
    IrishCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    GeorgianCuisineCount,
    SouthwestCuisineCount,
    CambodianCuisineCount,
    LatinCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    CubanCuisineCount,
    SoutheastCuisineCount,
    SouthHyphenAfricanCuisineCount,
    JamaicanCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    IsrealiCuisineCount,
    EasteuropeanCuisineCount,
    SingaporeanCuisineCount,
    NordicCuisineCount,
)
from jstark.mealkit.cuisine_count import (
    WestHyphenAfricanCuisineCount,
    NortheastCuisineCount,
    TonganCuisineCount,
    WestafricaCuisineCount,
)
from jstark.mealkit.cuisine_count import ZanzibarianCuisineCount, MidwestCuisineCount


class MealkitFeatures(FeatureGenerator):
    def __init__(
        self,
        as_at: date,
        feature_periods: list[FeaturePeriod] | list[str] = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ],
        feature_stems: set[str] | list[str] = set[str](),
    ) -> None:
        super().__init__(as_at, feature_periods, feature_stems)

    FEATURE_CLASSES: set[type[Feature]] = {
        Count,
        # NetSpend,
        # GrossSpend,
        RecencyDays,
        CustomerCount,
        ApproxCustomerCount,
        Quantity,
        MostRecentPurchaseDate,
        EarliestPurchaseDate,
        OrderPeriods,
        OrderCount,
        ApproxOrderCount,
        ProductCount,
        ApproxProductCount,
        RecipeCount,
        ApproxRecipeCount,
        AvgOrder,
        Discount,
        AvgQuantityPerOrder,
        CyclesSinceLastOrder,
        AvgPurchaseCycle,
        Cuisines,
        CuisineCount,
        ItalianCuisineCount,
        FrenchCuisineCount,
        SpanishCuisineCount,
        SrilankanCuisineCount,
        MalaysianCuisineCount,
        AmericanCuisineCount,
        AsianCuisineCount,
        GermanCuisineCount,
        CentralAmericaCuisineCount,
        ArgentinianCuisineCount,
        NorthAmericanCuisineCount,
        NewZealandCuisineCount,
        CanadianCuisineCount,
        SwedishCuisineCount,
        EgyptianCuisineCount,
        NorthernEuropeanCuisineCount,
        CajunCuisineCount,
        AustrianCuisineCount,
        MexicanCuisineCount,
        MediterraneanCuisineCount,
        ThaiCuisineCount,
        ChineseCuisineCount,
        LatinAmericanCuisineCount,
        SouthAsiaCuisineCount,
        MiddleEasternCuisineCount,
        TraditionalCuisineCount,
        SteakhouseCuisineCount,
        PacificislandsCuisineCount,
        EastAsiaCuisineCount,
        BritishCuisineCount,
        CaribbeanCuisineCount,
        FilipinoCuisineCount,
        TurkishCuisineCount,
        BelgianCuisineCount,
        SouthAmericanCuisineCount,
        NorthAfricanCuisineCount,
        SouthAfricanCuisineCount,
        WestAfricanCuisineCount,
        EastAfricanCuisineCount,
        WesternEuropeanCuisineCount,
        PortugueseCuisineCount,
        PeruvianCuisineCount,
        JapaneseCuisineCount,
        PacificIslandsCuisineCount,
        SouthernEuropeCuisineCount,
        AfricanCuisineCount,
        CentralAsiaCuisineCount,
        NorthamericaCuisineCount,
        EuropeanCuisineCount,
        MoroccanCuisineCount,
        AustralianCuisineCount,
        HungarianCuisineCount,
        IranianCuisineCount,
        SoutheastAsiaCuisineCount,
        HawaiianCuisineCount,
        ScandinavianCuisineCount,
        BrazilianCuisineCount,
        IndonesianCuisineCount,
        MongolianCuisineCount,
        RussianCuisineCount,
        SouthAsianCuisineCount,
        BulgarianCuisineCount,
        FusionCuisineCusiineCount,
        IrishCuisineCount,
        GeorgianCuisineCount,
        SouthwestCuisineCount,
        CambodianCuisineCount,
        LatinCuisineCount,
        CubanCuisineCount,
        SoutheastCuisineCount,
        SouthHyphenAfricanCuisineCount,
        JamaicanCuisineCount,
        IsrealiCuisineCount,
        EasteuropeanCuisineCount,
        SingaporeanCuisineCount,
        NordicCuisineCount,
        WestHyphenAfricanCuisineCount,
        NortheastCuisineCount,
        TonganCuisineCount,
        WestafricaCuisineCount,
        ZanzibarianCuisineCount,
        MidwestCuisineCount,
    }
