import time
from typing import Callable, List, Tuple

import ibm_db
import paramiko

from config.settings import EnvConfig
from modules.logging_utils import get_logger

logger = get_logger()

POLL_INTERVAL_SECONDS = 30


def _ssh_client(host: str, user: str, key_path: str) -> paramiko.SSHClient:
    key = paramiko.RSAKey.from_private_key_file(key_path)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, pkey=key)
    return client


def _run_remote(client: paramiko.SSHClient, command: str) -> tuple[int, str, str]:
    logger.info("Running remote command: %s", command)
    stdin, stdout, stderr = client.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    logger.info("Command exit=%s, stdout=%s, stderr=%s", exit_code, out.strip(), err.strip())
    return exit_code, out, err


def _open_db2(dsn: str):
    conn = ibm_db.connect(dsn, "", "")
    return conn


def _fetch_single_int(conn, sql: str, params: tuple) -> int:
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.execute(stmt, params)
    row = ibm_db.fetch_tuple(stmt)
    return int(row[0]) if row else 0


def _fetch_counts_grouped(conn, sql: str, params: tuple) -> List[tuple]:
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.execute(stmt, params)
    rows: List[tuple] = []
    row = ibm_db.fetch_tuple(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_tuple(stmt)
    return rows


def _poll_job_status(
    client: paramiko.SSHClient,
    job_id: str,
    abort_check: Callable[[], bool] | None = None,
) -> str:
    """
    Poll DIH job status until COMPLETED or FAILED, or until aborted.
    """
    while True:
        if abort_check and abort_check():
            logger.warning("Job %s polling aborted by user.", job_id)
            return "FAILED"

        exit_code, out, _ = _run_remote(client, f"sh /dih/scripts/check_job_status.sh {job_id}")
        status = out.strip()
        logger.info("Job %s status: %s", job_id, status)

        if status in {"COMPLETED", "FAILED"}:
            return status

        time.sleep(POLL_INTERVAL_SECONDS)


def run_step4_pre_landing(
    env_cfg: EnvConfig,
    batch_id: str,
    csv_filename: str,
    abort_check: Callable[[], bool] | None = None,
) -> tuple[bool, int]:
    """
    Step 4 — Pre-landing load.
    1) clear_landing_tables.sh
    2) load_prelanding.sh <csv_filename>
    3) validate record count in PRE_LANDING.
    """
    logger.info("Step 4: Pre-landing load for batch %s", batch_id)

    client = _ssh_client(env_cfg.dih_host, env_cfg.dih_user, env_cfg.ssh_key_path)
    conn = _open_db2(env_cfg.db2_dsn)

    try:
        # Clear landing tables
        exit_code, out, _ = _run_remote(client, "sh /dih/scripts/clear_landing_tables.sh")
        job_id = out.strip()
        status = _poll_job_status(client, job_id, abort_check=abort_check)
        if status != "COMPLETED":
            logger.error("Clear landing tables FAILED for job %s", job_id)
            return False, 0

        # Load pre-landing
        exit_code, out, _ = _run_remote(client, f"sh /dih/scripts/load_prelanding.sh {csv_filename}")
        job_id2 = out.strip()
        status2 = _poll_job_status(client, job_id2, abort_check=abort_check)
        if status2 != "COMPLETED":
            logger.error("Pre-landing load FAILED for job %s", job_id2)
            return False, 0

        # DB2 validation
        cnt = _fetch_single_int(
            conn,
            "SELECT COUNT(*) FROM BPDH.PRE_LANDING WHERE BATCH_ID = ?",
            (batch_id,),
        )
        logger.info("Pre-landing record count for batch %s: %s", batch_id, cnt)
        return True, cnt
    finally:
        try:
            ibm_db.close(conn)
        except Exception:
            pass
        client.close()


def run_step5_landing(
    env_cfg: EnvConfig,
    batch_id: str,
    abort_check: Callable[[], bool] | None = None,
) -> tuple[bool, List[tuple]]:
    """
    Step 5 — Landing load and validation.
    1) load_landing.sh <batch_id>
    2) validate counts grouped by STATUS in LANDING.
    """
    logger.info("Step 5: Landing load for batch %s", batch_id)

    client = _ssh_client(env_cfg.dih_host, env_cfg.dih_user, env_cfg.ssh_key_path)
    conn = _open_db2(env_cfg.db2_dsn)

    try:
        exit_code, out, _ = _run_remote(client, f"sh /dih/scripts/load_landing.sh {batch_id}")
        job_id = out.strip()
        status = _poll_job_status(client, job_id, abort_check=abort_check)
        if status != "COMPLETED":
            logger.error("Landing load FAILED for job %s", job_id)
            return False, []

        rows = _fetch_counts_grouped(
            conn,
            "SELECT COUNT(*), STATUS FROM BPDH.LANDING WHERE BATCH_ID = ? GROUP BY STATUS",
            (batch_id,),
        )
        logger.info("Landing counts for %s: %s", batch_id, rows)
        return True, rows
    finally:
        try:
            ibm_db.close(conn)
        except Exception:
            pass
        client.close()


def run_step7_downstream(
    env_cfg: EnvConfig,
    batch_id: str,
    log_callback: Callable[[str], None] | None = None,
    abort_check: Callable[[], bool] | None = None,
) -> tuple[bool, int]:
    """
    Step 7 — Downstream workflow and validation.
    """
    logger.info("Step 7: Downstream trigger for batch %s", batch_id)

    client = _ssh_client(env_cfg.dih_host, env_cfg.dih_user, env_cfg.ssh_key_path)
    conn = _open_db2(env_cfg.db2_dsn)

    try:
        stdin, stdout, stderr = client.exec_command(
            f"sh /dih/scripts/trigger_downstream.sh {batch_id}"
        )

        for line in iter(lambda: stdout.readline(2048), ""):
            if abort_check and abort_check():
                logger.warning("Downstream log streaming aborted by user.")
                break
            line = line.rstrip()
            if not line:
                continue
            logger.info("[DOWNSTREAM LOG] %s", line)
            if log_callback:
                log_callback(line)

        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            logger.error("Downstream script exit code %s", exit_code)
            return False, 0

        cnt = _fetch_single_int(
            conn,
            "SELECT COUNT(*) FROM BPDH.DOWNSTREAM_LOG WHERE BATCH_ID = ? AND STATUS = 'SUCCESS'",
            (batch_id,),
        )
        logger.info("Downstream success count for %s: %s", batch_id, cnt)
        return True, cnt
    finally:
        try:
            ibm_db.close(conn)
        except Exception:
            pass
        client.close()

