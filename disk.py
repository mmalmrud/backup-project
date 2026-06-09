import logging
import os
from pathlib import Path


def ensure_mounted(
    mount_device: str,
    mount_point: str,
    log: logging.Logger,
) -> bool:
    path = Path(mount_point)
    if not path.exists():
        raise ValueError(f"{path} does not exist")

    with open("/proc/mounts", "r", encoding="utf-8") as mounts_file:
        if any(mount_point in line for line in mounts_file):
            log.info(f"{mount_point} is already mounted")
            return True

    exit_code = os.system(f"mount {mount_device} {mount_point}")
    if exit_code != 0:
        log.error(f"Failed to mount {mount_device} to {mount_point}")
        raise Exception(f"Mount exit status {exit_code}")

    log.info(f"Mounted {mount_device} to {mount_point}")
    return True


def unmount(
    mount_device: str,
    mount_point: str,
    log: logging.Logger,
) -> None:
    exit_code = os.system(f"umount {mount_point}")
    log.info(f"Unmounted {mount_device} from {mount_point}")
    if exit_code != 0:
        raise Exception(f"Unmount exit status {exit_code}")
