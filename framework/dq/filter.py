"""
DQ threshold-based filtering logic.
"""

import polars as pl
from typing import Mapping


def filter_results_by_thresholds(
    df_table_overall: pl.DataFrame,
    dq_thresholds: Mapping[str, float],
) -> pl.DataFrame:
    """Filter results according to DQ thresholds per dimension.

    Applies:
    `DQ_Score >= threshold(dq_dimension)` with default threshold = 0.

    Parameters
    ----------
    df_table_overall : polars.DataFrame
     https://westeurope.azuredatabricks.net/editor/files/3573814719266690?o=7405613713857225$0   Table-level DQ results.
    dq_thresholds : Mapping[str, float]
        Thresholds per DQ dimension.

    Returns
    -------
    polars.DataFrame
        Filtered results.
    """
    if df_table_overall.is_empty():
        return df_table_overall

    return df_table_overall.filter(
        pl.col("dq_score")
        >= pl.col("dimension").replace(
            dict(dq_thresholds), default=0.0
        )
    )