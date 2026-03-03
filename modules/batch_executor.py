from typing import Callable, Tuple

import ibm_db

from config.settings import EnvConfig
from modules.dih_executor import (
    _fetch_single_int,
    _open_db2,
    _poll_job_status,
    _run_remote,
    _ssh_client,
)
from modules.logging_utils import get_logger

logger = get_logger()


def run_step6_batch(
    env_cfg: EnvConfig,
    batch_id: str,
    abort_check: Callable[[], bool] | None = None,
) -> Tuple[bool, int, int]:
    """
    Step 6 — Batch jobs and DB2 validations.
    1) landing_to_staging.sh <batch_id>
    2) staging_to_bo.sh <batch_id>
    3) validate counts in STAGING and BO tables.
    """
    logger.info("Step 6: Batch jobs for batch %s", batch_id)

    client = _ssh_client(env_cfg.batch_host, env_cfg.batch_user, env_cfg.ssh_key_path)
    conn = _open_db2(env_cfg.db2_dsn)

    try:
        # Batch Job 1
        exit_code, out, _ = _run_remote(
            client, f"sh /batch/scripts/landing_to_staging.sh {batch_id}"
        )
        job_id1 = out.strip()
        status1 = _poll_job_status(client, job_id1, abort_check=abort_check)
        if status1 != "COMPLETED":
            logger.error("Job landing_to_staging FAILED: %s", job_id1)
            return False, 0, 0

        staging_count = _fetch_single_int(
            conn,
            "SELECT COUNT(*) FROM BPDH.STAGING WHERE BATCH_ID = ?",
            (batch_id,),
        )

        # Batch Job 2
        exit_code, out, _ = _run_remote(
            client, f"sh /batch/scripts/staging_to_bo.sh {batch_id}"
        )
        job_id2 = out.strip()
        status2 = _poll_job_status(client, job_id2, abort_check=abort_check)
        if status2 != "COMPLETED":
            logger.error("Job staging_to_bo FAILED: %s", job_id2)
            return False, staging_count, 0

        bo_count = _fetch_single_int(
            conn,
            "SELECT COUNT(*) FROM BPDH.BO_TABLE WHERE BATCH_ID = ?",
            (batch_id,),
        )

        return True, staging_count, bo_count
    finally:
        try:
            ibm_db.close(conn)
        except Exception:
            pass
        client.close()

