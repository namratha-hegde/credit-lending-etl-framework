import pandas as pd


class DataReader:
    """Read datasets into Spark."""

    def __init__(self, spark):
        self.spark = spark

    def read(self, path: str, fmt: str):
        """
        Read dataset from source.

        Parameters
        ----------
        path : str
            Input file path.
        fmt : str
            File format.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        if fmt == "excel":
            pdf = pd.read_excel(path)
            return self.spark.createDataFrame(pdf)

        return self.spark.read.format(fmt).load(path)