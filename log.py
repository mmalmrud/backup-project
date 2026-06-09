import datetime
import logging
import os
from pathlib import Path
import shutil

from rclone_python import rclone


def setup_logging(
    log_file_name: str | None = None,
    log_path: str | None = None,
    max_logfiles: int = 100,
) -> tuple[logging.Logger, str, str]:
    if not log_file_name:
        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_name = f"{datestring}_backup.log"

    if not log_path:
        log_path = os.path.join(os.path.expanduser("~"), ".backup_logs")

    path = Path(log_path)
    path.mkdir(parents=True, exist_ok=True)

    if len(list(path.glob("*.log"))) > max_logfiles - 1:
        paths = sorted(path.iterdir(), key=os.path.getmtime, reverse=True)
        for p in paths[max_logfiles:]:
            os.remove(p)

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )
    log_file_path = f"{log_path}/{log_file_name}"

    file_handler = logging.FileHandler(log_file_path, "a")
    file_handler.setFormatter(log_formatter)
    log.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    log.addHandler(console_handler)

    rclone.set_log_level(logging.INFO)
    return log, log_path, log_file_name


def _bytes_to_human_readable(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{size} B"


def log_disk_usage(
    path: str,
    log: logging.Logger,
    label: str = "Disk usage",
) -> tuple[int, int, int]:
    total, used, free = shutil.disk_usage(path)
    used_percent = (used / total * 100) if total else 0.0
    log.info(
        "%s for %s: total=%s used=%s free=%s used_percent=%.2f%%",
        label,
        path,
        _bytes_to_human_readable(total),
        _bytes_to_human_readable(used),
        _bytes_to_human_readable(free),
        used_percent,
    )
    return total, used, free


def format_size(size: int) -> str:
    return _bytes_to_human_readable(size)
