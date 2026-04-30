"""
Main entrypoint for the DQ-driven ETL platform.

Execution flow:
- Pipeline selection via configuration
- Raw ingestion (Bronze)
- Silver transformations
- Data Quality validation
- Gold aggregation
- Control & audit metadata capture
"""

from uuid import uuid4
from typing import List, Dict

from pyspark.sql import SparkSession

from utils.config_loader import ConfigLoader
from ingestion.raw import RawLayer
from ingestion.silver import SilverLayer
from ingestion.gold import GoldLayer
from framework.dq.executor import DQExecutor
from framework.control.control_record import build_control_record


def main(environment: str) -> None:
    """
    Execute the configured ETL pipeline with integrated
    Data Quality and Control & Audit metadata.

    Parameters
    ----------
    environment : str
        Execution environment (dev / acc / prod).
    """
    # --------------------------------------------------
    # 1. Spark session
    # --------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("DQ_ETL_PLATFORM")
        .getOrCreate()
    )

    # --------------------------------------------------
    # 2. Load configurations
    # --------------------------------------------------
    env_cfg = ConfigLoader.load(
        "config/environments.yml"
    )["environments"][environment]

    datasets_cfg = ConfigLoader.load(
        "config/datasets.yml"
    )["datasets"]

    pipelines_cfg = ConfigLoader.load(
        "config/pipelines.yml"
    )["pipelines"]

    dq_rules_cfg = ConfigLoader.load(
        "config/dq_rules.yml"
    )["dq_rules"]

    # --------------------------------------------------
    # 3. Resolve pipeline (config-driven, no hardcoding)
    # --------------------------------------------------
    pipeline_name: str = pipelines_cfg["default_pipeline"]
    pipeline_cfg: Dict = pipelines_cfg[pipeline_name]

    # --------------------------------------------------
    # 4. Initialize core components
    # --------------------------------------------------
    raw_layer = RawLayer(
        spark=spark,
        adls_base_path=env_cfg["adls_base_path"],
        datasets_cfg=datasets_cfg,
    )

    silver_layer = SilverLayer(
        adls_base_path=env_cfg["adls_base_path"],
        datasets_cfg=datasets_cfg,
    )

    gold_layer = GoldLayer(
        adls_base_path=env_cfg["adls_base_path"],
        datasets_cfg=datasets_cfg,
    )

    dq_executor = DQExecutor(dq_rules_cfg)

    # --------------------------------------------------
    # 5. Control & audit context
    # --------------------------------------------------
    run_id: str = str(uuid4())
    control_records: List[Dict] = []

    # --------------------------------------------------
    # 6. Raw → Silver → DQ per dataset
    # --------------------------------------------------
    silver_outputs: Dict[str, object] = {}

    for dataset_name in pipeline_cfg["inputs"]:
        try:
            # ------------------------------
            # Raw (Bronze)
            # ------------------------------
            bronze_df = raw_layer.run(dataset_name)

            control_records.append(
                build_control_record(
                    dataset=dataset_name,
                    layer="raw",
                    input_row_count=bronze_df.count(),
                    output_row_count=bronze_df.count(),
                    run_id=run_id,
                )
            )

            # ------------------------------
            # Silver
            # ------------------------------
            silver_df = silver_layer.run(
                bronze_df,
                pipeline_cfg["silver"],
                dataset_name,
            )

            control_records.append(
                build_control_record(
                    dataset=dataset_name,
                    layer="silver",
                    input_row_count=bronze_df.count(),
                    output_row_count=silver_df.count(),
                    run_id=run_id,
                )
            )

            # ------------------------------
            # Data Quality
            # ------------------------------
            dq_executor.run(
                df=silver_df,
                dataset_name=dataset_name,
            )

            silver_outputs[dataset_name] = silver_df

        except Exception as exc:
            # Failure isolation at dataset level
            raise RuntimeError(
                f"Pipeline failed for dataset '{dataset_name}'"
            ) from exc

    # --------------------------------------------------
    # 7. Gold layer
    # --------------------------------------------------
    gold_df = gold_layer.run(
        silver_outputs,
        pipeline_cfg["gold"],
    )

    control_records.append(
        build_control_record(
            dataset=pipeline_name,
            layer="gold",
            input_row_count=sum(
                df.count() for df in silver_outputs.values()
            ),
            output_row_count=gold_df.count(),
            run_id=run_id,
        )
    )

    # --------------------------------------------------
    # 8. Persist control metadata (audit)
    # --------------------------------------------------
    control_df = spark.createDataFrame(control_records)

    (
        control_df.write
        .format("delta")
        .mode("append")
        .save(
            f"{env_cfg['adls_base_path']}/audit/control_metadata"
        )
    )

    gold_df.show(truncate=False)


if __name__ == "__main__":
    main(environment="dev")