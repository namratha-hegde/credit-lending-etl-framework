"""
Unit tests for configuration loading.
"""

from utils.config_loader import ConfigLoader


def test_load_datasets_config():
    """
    Validate datasets.yml can be loaded and contains datasets key.
    """
    cfg = ConfigLoader.load("config/datasets.yml")

    assert isinstance(cfg, dict)
    assert "datasets" in cfg
    assert "clients" in cfg["datasets"]


def test_load_environments_config():
    """
    Validate environments.yml contains required environments.
    """
    cfg = ConfigLoader.load("config/environments.yml")

    assert "environments" in cfg
    assert "dev" in cfg["environments"]
    assert "adls_base_path" in cfg["environments"]["dev"]