"""
SQL template rendering utilities for Data Quality validations.
"""

from pathlib import Path
from typing import Dict

from jinja2 import Environment, FileSystemLoader


class DQSqlTemplateRenderer:
    """Renders SQL-based DQ templates using Jinja2."""

    def __init__(self, template_dir: str):
        """
        Initialize the renderer.

        Parameters
        ----------
        template_dir : str
            Base directory for SQL templates.
        """
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render(
        self,
        template_name: str,
        context: Dict,
    ) -> str:
        """
        Render a SQL template.

        Parameters
        ----------
        template_name : str
            Template filename.
        context : dict
            Template variables.

        Returns
        -------
        str
            Rendered SQL string.
        """
        template = self.env.get_template(template_name)
        return template.render(**context)