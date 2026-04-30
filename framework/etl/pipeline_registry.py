"""
Pipeline registry for plug-in based pipeline execution.
"""

from typing import Dict, Callable


class PipelineRegistry:
    """Registry for available pipelines."""

    _pipelines: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str, runner: Callable) -> None:
        """
        Register a pipeline runner.

        Parameters
        ----------
        name : str
            Pipeline name.
        runner : Callable
            Pipeline execution function.
        """
        cls._pipelines[name] = runner

    @classmethod
    def get(cls, name: str) -> Callable:
        """
        Retrieve a pipeline runner.

        Parameters
        ----------
        name : str

        Returns
        -------
        Callable
            Pipeline runner.
        """
        return cls._pipelines[name]