"""
Gold layer processing.

Creates business aggregates and curated datasets.
"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum


class GoldProcessor:
    """Builds Gold analytical datasets."""

    def __init__(
        self,
        adls_base_path: str,
        datasets_cfg: Dict,
    ):
        """
        Initialize GoldProcessor.

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
    def build_client_credit_exposure(
        clients_df: DataFrame,
        credits_df: DataFrame,
    ) -> DataFrame:
        """
        Build client-level credit exposure.

        Parameters
        ----------
        clients_df : pyspark.sql.DataFrame
            Silver clients dataset.
        credits_df : pyspark.sql.DataFrame
            Silver credits dataset.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        return (
            clients_df.join(
                credits_df,
                on="client_id",
                how="left",
            )
            .groupBy("client_id", "client_name")
            .agg(
                spark_sum("credit_amount").alias(
                    "total_credit_exposure"
                )
            )
        )

    def write_gold(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> None:
        """
        Write dataframe to Gold Delta layer.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        dataset_name : str
        """
        gold_path = (
            f"{self.adls_base_path}/"
            f"{self.datasets_cfg[dataset_name]['gold']['path']}"
        )

        df = df.withColumn("_gold_ts", current_timestamp())

        (
            df.write.format("delta")
            .mode("overwriteschema")
            .save(gold_path)
        )
