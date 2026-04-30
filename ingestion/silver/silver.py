"""
Silver layer processing.

Applies standardization and data quality checks.
"""

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SilverProcessor:
    """Processes Bronze data into Silver layer."""

    def __init__(
        self,
        adls_base_path: str,
        datasets_cfg: Dict,
    ):
        """
        Initialize SilverProcessor.

        Parameters
        ----------
        adls_base_path : str
            Base ADLS Gen2 path.
        datasets_cfg : dict
            Dataset configuration.
        """
        self.adls_base_path = adls_base_path.rstrip("/")
        self.datasets_cfg = datasets_cfg

    @staticmethod
    def standardize_string_columns(df: DataFrame) -> DataFrame:
        """
        Convert all string columns to uppercase.

        Parameters
        ----------
        df : pyspark.sql.DataFrame

        Returns
        -------
        pyspark.sql.DataFrame
        """
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string":
                df = df.withColumn(
                    field.name,
                    upper(col(field.name))
                )
        return df

    @staticmethod
    def enforce_not_null(df: DataFrame, columns: List[str]) -> None:
        """
        Enforce not-null constraint.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        columns : list[str]

        Raises
        ------
        ValueError
        """
        for column in columns:
            if df.filter(col(column).isNull()).count() > 0:
                raise ValueError(
                    f"NULL values detected in column: {column}"
                )

    def write_silver(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> None:
        """
        Write dataframe to Silver Delta layer.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        dataset_name : str
        """
        silver_path = (
            f"{self.adls_base_path}/"
            f"{self.datasets_cfg[dataset_name]['silver']['path']}"
        )

        df = df.withColumn("_silver_ts", current_timestamp())

        (
            df.write.format("delta")
            .mode("overwriteSchema")
            .save(silver_path)
        )