import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from modules.logging_utils import get_logger

logger = get_logger()


def generate_success_report(
    out_dir: str,
    batch_id: str,
    filename: str,
    env: str,
    sr_number: str,
    total_records: int,
    loaded_records: int,
    step_timestamps: Dict[str, str],
) -> str:
    """
    Generate a single-sheet Excel success report and return the file path.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{batch_id}_success_report.xlsx")

    df = pd.DataFrame(
        [
            {
                "Batch ID": batch_id,
                "Filename": filename,
                "Environment": env,
                "SR Number": sr_number,
                "Total Records Submitted": total_records,
                "Records Successfully Loaded": loaded_records,
                "Step 3 Completed At": step_timestamps.get("step3"),
                "Step 4 Completed At": step_timestamps.get("step4"),
                "Step 5 Completed At": step_timestamps.get("step5"),
                "Step 6 Completed At": step_timestamps.get("step6"),
                "Step 7 Completed At": step_timestamps.get("step7"),
            }
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Summary")

    logger.info("Success report generated at %s", path)
    return path


def generate_reject_report(
    out_dir: str,
    batch_id: str,
    rejects: List[Dict[str, Any]],
) -> str:
    """
    Generate an Excel reject report from a list of dictionaries.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{batch_id}_reject_report.xlsx")

    if rejects:
        df = pd.DataFrame(rejects)
    else:
        df = pd.DataFrame(
            columns=[
                "Batch ID",
                "Row Number",
                "BP Record Identifier",
                "Rejection Reason",
                "Step",
                "Timestamp",
            ]
        )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Rejects")

    logger.info("Reject report generated at %s", path)
    return path

