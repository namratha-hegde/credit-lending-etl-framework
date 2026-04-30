from framework.etl.pipeline_registry import PipelineRegistry


def run_risk_pipeline(context: dict) -> None:
    """
    Execute credit risk pipeline.

    Parameters
    ----------
    context : dict
        Runtime context.
    """
    # existing logic currently in main.py
    pass


PipelineRegistry.register(
    name="risk_pipeline",
    runner=run_risk_pipeline,
)
