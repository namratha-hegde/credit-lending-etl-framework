from datetime import datetime
from typing import Dict


def build_control_record(
    *,
    dataset: str,
    layer: str,
    input_row_count: int,
    output_row_count: int,
    run_id: str,
) -> Dict:
    """Build a control metadata record.

    Parameters
    ----------
    dataset : str
        Logical dataset name.
    layer : str
        Processing layer (raw / silver / gold).
    input_row_count : int
        Number of input rows.
    output_row_count : int
        Number of output rows.
    run_id : str
        Unique pipeline execution identifier.

    Returns
    -------
    dict
        Control metadata record.
    """
    return {
        "dataset": dataset,
        "layer": layer,
        "input_row_count": input_row_count,
        "output_row_count": output_row_count,
        "run_id": run_id,
        "processed_ts": datetime.utcnow(),
    }
