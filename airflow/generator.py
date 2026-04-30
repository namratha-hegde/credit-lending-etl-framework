"""
Airflow DAG generator using Jinja2 templates.
"""

from pathlib import Path
from typing import Dict, List

from jinja2 import Environment, FileSystemLoader


class AirflowDagGenerator:
    """Generates Airflow DAGs from Jinja2 templates."""

    def __init__(self, template_dir: str):
        """
        Initialize DAG generator.

        Parameters
        ----------
        template_dir : str
            Directory containing Jinja2 templates.
        """
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render_dag(
        self,
        template_name: str,
        context: Dict,
        output_path: str,
    ) -> None:
        """
        Render an Airflow DAG and write it to disk.

        Parameters
        ----------
        template_name : str
            Jinja2 template filename.
        context : dict
            Template variables.
        output_path : str
            Path to write rendered DAG.
        """
        template = self.env.get_template(template_name)
        rendered = template.render(**context)

        Path(output_path).write_text(rendered, encoding="utf-8")
``