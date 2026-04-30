"""
Raw ingestion layer (Bronze).

Reads Excel source files from ADLS Gen2 using abfss
and writes Delta Bronze tables with technical metadata.
"""

from typing import Dict

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit


class RawIngestion:
    """Handles raw ingestion of source datasets."""

    def __init__(
        self,
        spark: SparkSession,
        adls_base_path: str,
        datasets_cfg: Dict,
    ):
        """
        Initialize RawIngestion.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        adls_base_path : str
            Base ADLS Gen2 path (abfss://...).
        datasets_cfg : dict
            Datasets configuration.
        """
        self.spark = spark
        self.adls_base_path = adls_base_path.rstrip("/")
        self.datasets_cfg = datasets_cfg

    def ingest_dataset(self, dataset_name: str) -> DataFrame:
        """
        Ingest a raw dataset into Bronze Delta.

        Parameters
        ----------
        dataset_name : str
            Dataset key as defined in datasets.yml.

        Returns
        -------
        pyspark.sql.DataFrame
            Ingested dataframe.
        """
        dataset = self.datasets_cfg[dataset_name]
        source_cfg = dataset["source"]

        source_path = (
            f"{self.adls_base_path}/{source_cfg['path']}"
        )
        target_path = (
            f"{self.adls_base_path}/bronze/{dataset_name}"
        )

        # Excel → Pandas → Spark (standard pattern)
        pdf = pd.read_excel(source_path)
        df = self.spark.createDataFrame(pdf)

        df = (
            df.withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_dataset_name", lit(dataset_name))
        )

        (
            df.write.format("delta")
            .mode("append")
            .save(target_path)
        )

        return df
