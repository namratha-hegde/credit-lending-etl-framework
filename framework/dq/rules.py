"""
Atomic Data Quality rule definitions.

Rules return metrics only and never
control pipeline execution.
"""

from typing import Iterable, Set
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DQRules:
    """Reusable data quality rules."""

    @staticmethod
    def completeness(df: DataFrame, column: str) -> dict:
        """
        Measure completeness (non-null ratio).

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        column : str

        Returns
        -------
        dict
            Completeness metrics.
        """
        total_count = df.count()
        invalid_count = df.filter(col(column).isNull()).count()

        return {
            "rule": "completeness",
            "dimension": "COMPLETENESS",
            "column": column,
            "total_count": total_count,
            "invalid_count": invalid_count,
            "dq_score": (
                1.0 - invalid_count / total_count
                if total_count > 0
                else 1.0
            ),
        }

    @staticmethod
    def uniqueness(df: DataFrame, column: str) -> dict:
        """
        Measure uniqueness of values.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        column : str

        Returns
        -------
        dict
            Uniqueness metrics.
        """
        total_count = df.count()
        distinct_count = df.select(column).distinct().count()
        duplicate_count = total_count - distinct_count

        return {
            "rule": "uniqueness",
            "dimension": "UNIQUENESS",
            "column": column,
            "total_count": total_count,
            "invalid_count": duplicate_count,
            "dq_score": (
                distinct_count / total_count
                if total_count > 0
                else 1.0
            ),
        }

    @staticmethod
    def validity(
        df: DataFrame,
        column: str,
        allowed_values: Iterable,
    ) -> dict:
        """
        Measure validity against allowed values.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        column : str
        allowed_values : Iterable
            Set of allowed values.

        Returns
        -------
        dict
            Validity metrics.
        """
        allowed: Set = set(allowed_values)
        total_count = df.count()

        invalid_count = df.filter(
            ~col(column).isin(allowed)
        ).count()

        return {
            "rule": "validity",
            "dimension": "VALIDITY",
            "column": column,
            "total_count": total_count,
            "invalid_count": invalid_count,
            "dq_score": (
                1.0 - invalid_count / total_count
                if total_count > 0
                else 1.0
            ),
        }