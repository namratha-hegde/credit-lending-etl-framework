"""
Unit tests for Data Quality rules.
"""

import pytest
from pyspark.sql import SparkSession

from framework.dq.rules import DQRules


@pytest.fixture(scope="session")
def spark():
    """
    Provide SparkSession for DQ tests.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-dq")
        .getOrCreate()
    )


def test_completeness_rule(spark):
    """
    Validate completeness rule metrics.
    """
    data = [(1,), (None,), (2,)]
    df = spark.createDataFrame(data, ["client_id"])

    result = DQRules.completeness(
        df=df,
        column="client_id",
    )

    assert result["total_count"] == 3
    assert result["invalid_count"] == 1
    assert result["dq_score"] == pytest.approx(2 / 3)