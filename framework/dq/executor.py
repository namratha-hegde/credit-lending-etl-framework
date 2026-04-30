"""
Generic Data Quality execution engine.
"""

from datetime import datetime
from typing import Dict, List

from pyspark.sql import DataFrame

from framework.dq.rules import DQRules
from framework.dq.results import DQStatusEvaluator


class DQExecutor:
    """Executes DQ rules and produces audit records."""

    def __init__(self, rules_cfg: List[Dict]):
        """
        Initialize DQExecutor.

        Parameters
        ----------
        rules_cfg : list[dict]
            Rules configuration.
        """
        self.rules_cfg = rules_cfg

    def run(
        self,
        df: DataFrame,
        dataset_name: str,
    ) -> List"""
        Execute DQ rules.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        dataset_name : str

        Returns
        -------
        list[dict]
            Structured DQ results.
        """
        audit_results = []

        for cfg in self.rules_cfg:
            rule_name = cfg["rule"]
            params = cfg.get("params", {})
            threshold = cfg.get("threshold", 0.0)
            severity = cfg.get("severity", "FAIL")

            metrics = getattr(DQRules, rule_name)(
                df, **params
            )

            status = DQStatusEvaluator.evaluate(
                metrics["dq_score"],
                threshold,
                severity,
            )

            result = {
                "dataset": dataset_name,
                "rule": rule_name,
                "dimension": metrics["dimension"],
                "column": metrics["column"],
                "dq_score": metrics["dq_score"],
                "threshold": threshold,
                "severity": severity,
                "status": status,
                "invalid_count": metrics["invalid_count"],
                "total_count": metrics["total_count"],
                "execution_ts": datetime.utcnow(),
            }

            audit_results.append(result)

            if status == "FAIL":
                raise ValueError(
                    f"DQ FAILED: {dataset_name} | "
                    f"{rule_name} | {metrics['column']}"
                )

        return audit_results