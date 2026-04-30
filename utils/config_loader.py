# utils/config_loader.py
import yaml
from pathlib import Path


class ConfigLoader:
    """Utility class for loading YAML configuration files."""

    @staticmethod
    def load(path: str) -> dict:
        """
        Load a YAML configuration file.

        Parameters
        ----------
        path : str
            Path to the YAML file.

        Returns
        -------
        dict
            Parsed configuration dictionary.
        """
        with Path(path).open(encoding="utf-8") as file:
            return yaml.safe_load(file)