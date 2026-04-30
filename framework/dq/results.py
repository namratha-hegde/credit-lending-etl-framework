"""
DQ result evaluation logic.
"""


class DQStatusEvaluator:
    """Evaluates DQ rule results."""

    @staticmethod
    def evaluate(
        dq_score: float,
        threshold: float,
        severity: str,
    ) -> str:
        """
        Evaluate DQ status.

        Parameters
        ----------
        dq_score : float
            Computed DQ score.
        threshold : float
            Threshold value.
        severity : str
            FAIL | WARN | CONTINUE.

        Returns
        -------
        str
            PASS | WARN | FAIL.
        """
        if dq_score >= threshold:
            return "PASS"

        if severity == "FAIL":
            return "FAIL"

        if severity == "WARN":
            return "WARN"

        return "PASS"