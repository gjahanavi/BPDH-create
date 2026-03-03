from typing import List

import requests

from config.settings import EnvConfig
from modules.logging_utils import get_logger

logger = get_logger()


def update_ticket(
    env_cfg: EnvConfig,
    sys_id: str,
    state: str,
    work_notes: str,
    close_notes: str,
) -> None:
    """
    Update a ServiceNow incident/record via REST API.
    """
    base_url = f"https://{env_cfg.snow_instance}.service-now.com"
    url = f"{base_url}/api/now/table/incident/{sys_id}"
    auth = (env_cfg.snow_user, env_cfg.snow_password)

    payload: dict = {
        "work_notes": work_notes,
        "close_notes": close_notes,
    }

    if state == "success":
        payload["state"] = "6"  # Resolved
    elif state == "failure":
        payload["state"] = "2"  # In progress / Work in progress

    logger.info("Updating ServiceNow ticket %s", sys_id)
    resp = requests.patch(url, auth=auth, json=payload)
    resp.raise_for_status()


def attach_files(
    env_cfg: EnvConfig,
    sys_id: str,
    file_paths: List[str],
) -> None:
    """
    Attach local files to a ServiceNow record.
    """
    base_url = f"https://{env_cfg.snow_instance}.service-now.com"
    auth = (env_cfg.snow_user, env_cfg.snow_password)

    for path in file_paths:
        with open(path, "rb") as f:
            files = {"file": (path, f)}
            data = {"table_name": "incident", "table_sys_id": sys_id}
            url = f"{base_url}/api/now/attachment/file"
            logger.info("Attaching %s to ServiceNow ticket %s", path, sys_id)
            resp = requests.post(url, auth=auth, files=files, data=data)
            resp.raise_for_status()

