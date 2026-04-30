"""
Unit tests for Spark transformations.
"""

import pytest
from pyspark.sql import SparkSession

from framework.etl.transformer import Transformer


@pytest.fixture(scope="session")
def spark():
    """
    Provide a SparkSession for tests.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )


def test_standardize_string_columns(spark):
    """
    Verify that string columns are converted to uppercase.
    """
    data = [("abc", 1), ("Def", 2)]
    df = spark.createDataFrame(data, ["name", "value"])

    result_df = Transformer.standardize_string_columns(df)
    result = [row["name"] for row in result_df.collect()]

    assert result == ["ABC", "DEF"]