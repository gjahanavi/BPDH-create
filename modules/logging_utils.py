import logging
import os
from datetime import datetime

_LOGGER = None


def get_logger() -> logging.Logger:
    """
    Return a module-level logger that logs to both console and a timestamped file.
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    os.makedirs("logs", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join("logs", f"bpdh_pipeline_{ts}.log")

    logger = logging.getLogger("bpdh_pipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    logger.info("Logger initialized, writing to %s", log_path)
    _LOGGER = logger
    return logger

