from pyspark.sql import DataFrame


class PurchasingFeatureGenerator(object):
    def __init__(self, df: DataFrame) -> None:
        self.__df = df

    def get_df(self):
        return self.__df
