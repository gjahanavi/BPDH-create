import os
from dataclasses import dataclass

import streamlit as st


@dataclass
class EnvConfig:
    name: str
    sftp_landing_dir: str
    dih_host: str
    dih_user: str
    batch_host: str
    batch_user: str
    db2_dsn: str
    snow_instance: str
    snow_user: str
    snow_password: str
    ssh_key_path: str
    sftp_key_path: str


def _get_secret(key: str, default: str = "") -> str:
    """
    Read configuration from Streamlit secrets first, then from environment variables.
    """
    if hasattr(st, "secrets") and key in st.secrets:
        return str(st.secrets[key])
    return os.getenv(key, default)


def get_env_config(env: str) -> EnvConfig:
    """
    Build environment configuration without hardcoding hostnames or credentials.
    All sensitive values must come from st.secrets or environment variables.
    """
    env = env.upper()
    if env not in {"DEV", "UAT", "PROD"}:
        raise ValueError(f"Unsupported environment: {env}")

    landing_dirs = {
        "DEV": "/data/bpdh/dev/landing/",
        "UAT": "/data/bpdh/uat/landing/",
        "PROD": "/data/bpdh/prod/landing/",
    }

    prefix = f"BPDH_{env}_"

    return EnvConfig(
        name=env,
        sftp_landing_dir=landing_dirs[env],
        dih_host=_get_secret(prefix + "DIH_HOST"),
        dih_user=_get_secret(prefix + "DIH_USER"),
        batch_host=_get_secret(prefix + "BATCH_HOST"),
        batch_user=_get_secret(prefix + "BATCH_USER"),
        db2_dsn=_get_secret(prefix + "DB2_DSN"),
        snow_instance=_get_secret(prefix + "SNOW_INSTANCE"),
        snow_user=_get_secret(prefix + "SNOW_USER"),
        snow_password=_get_secret(prefix + "SNOW_PASSWORD"),
        ssh_key_path=_get_secret(prefix + "SSH_KEY_PATH"),
        sftp_key_path=_get_secret(prefix + "SFTP_KEY_PATH"),
    )

