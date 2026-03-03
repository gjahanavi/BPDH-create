import os
import time
from typing import Callable, Tuple

import paramiko

from config.settings import EnvConfig
from modules.logging_utils import get_logger

logger = get_logger()


def _connect_sftp(host: str, username: str, key_path: str, port: int = 22) -> paramiko.SFTPClient:
    key = paramiko.RSAKey.from_private_key_file(key_path)
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, pkey=key)
    return paramiko.SFTPClient.from_transport(transport)


def upload_with_retry(
    env_cfg: EnvConfig,
    local_csv_path: str,
    max_retries: int = 2,
    abort_check: Callable[[], bool] | None = None,
) -> Tuple[bool, str]:
    """
    Upload CSV to environment-specific landing directory with retries.

    Returns (success, remote_path).
    """
    logger.info("Step 3: starting SFTP upload for %s", local_csv_path)

    if not os.path.exists(local_csv_path):
        raise FileNotFoundError(f"Local CSV not found: {local_csv_path}")

    host = env_cfg.dih_host
    user = env_cfg.dih_user
    key_path = env_cfg.sftp_key_path
    landing_dir = env_cfg.sftp_landing_dir

    if not key_path or not os.path.exists(key_path):
        raise RuntimeError("SFTP key path not configured or does not exist.")

    attempt = 0
    last_error: Exception | None = None
    remote_path = ""

    while attempt <= max_retries:
        if abort_check and abort_check():
            logger.warning("SFTP upload aborted by user request.")
            return False, remote_path

        attempt += 1
        try:
            sftp = _connect_sftp(host, user, key_path)
            filename = os.path.basename(local_csv_path)
            remote_path = os.path.join(landing_dir, filename)

            logger.info("Uploading to %s (attempt %d)", remote_path, attempt)
            sftp.put(local_csv_path, remote_path)

            local_size = os.path.getsize(local_csv_path)
            remote_stat = sftp.stat(remote_path)

            if local_size != remote_stat.st_size:
                raise IOError(
                    f"Size mismatch after upload: local={local_size}, remote={remote_stat.st_size}"
                )

            logger.info("SFTP upload success: %s", remote_path)
            sftp.close()
            return True, remote_path

        except Exception as exc:  # pragma: no cover - environment dependent
            last_error = exc
            logger.error("SFTP upload attempt %d failed: %s", attempt, exc)
            time.sleep(3)

    logger.error("SFTP upload failed after %d attempts: %s", max_retries + 1, last_error)
    return False, remote_path

