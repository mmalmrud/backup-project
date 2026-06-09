import datetime
import logging
from pathlib import Path
import runpy
import sys
from typing import Any

from rclone_python import rclone
from rclone_python.remote_types import RemoteTypes
from rclone_python.utils import run_rclone_cmd
from requests import get

from disk import ensure_mounted, unmount
from log import format_size, log_disk_usage, setup_logging
from rclone import prepare_rclone_args

send_email_notification = runpy.run_path(
    str(Path(__file__).with_name("email.py"))
)["send_email_notification"]

def main(config: dict[str, Any]):
    log, log_path, log_file_name = setup_logging(
        log_path=config.get("log_path"), log_file_name=config.get("log_file_name")
    )
    backup_success = True
    log.info("Starting backup")
    log.debug(config)

    try:
        ip = get("https://api.ipify.org").content.decode("utf8")
        log.info(f"Public IP address: {ip}")
    except Exception as e:
        log.error("Failed to get public IP address")
        log.error(e)

    run_rclone_cmd("config delete", [config["remote_name"]])
    log.info("Creating remote")
    rclone.create_remote(
        remote_name=config["remote_name"],
        remote_type=RemoteTypes[config["remote_type"]],
        key_file=config["key_file"],
        host=config["host"],
        port=config["port"],
        user=config["user"],
    )

    mounted = False
    session_usage_path = config.get("mount_point") or config.get("local_path")
    initial_used_bytes: int | None = None

    try:
        about = rclone.about(config["remote_name"])
        log.debug(about)
        if config.get("mount_device") and config.get("mount_point"):
            mounted = ensure_mounted(
                config["mount_device"],
                config["mount_point"],
                log,
            )

        args = prepare_rclone_args(config)

        if session_usage_path:
            try:
                _, initial_used_bytes, _ = log_disk_usage(
                    session_usage_path,
                    log,
                    "Disk usage before backup",
                )
            except Exception as e:
                log.error(f"Failed to read pre-backup disk usage for {session_usage_path}")
                log.error(e)

        start_time = datetime.datetime.now()
        log.info("Starting copy")
        rclone.copy(
            f"{config['remote_name']}:{config['remote_path']}",
            config["local_path"],
            ignore_existing=config.get("ignore_existing", "true") != "false",
            args=args,
        )
        duration = datetime.datetime.now() - start_time
        log.info(f"Copy completed, took {duration}")
    except Exception as e:
        backup_success = False
        log.error(e)
    finally:
        if session_usage_path and initial_used_bytes is not None:
            try:
                _, final_used_bytes, _ = log_disk_usage(
                    session_usage_path,
                    log,
                    "Disk usage after backup",
                )
                added_bytes = final_used_bytes - initial_used_bytes
                if added_bytes >= 0:
                    log.info(
                        "Backup session added %s on %s",
                        format_size(added_bytes),
                        session_usage_path,
                    )
                else:
                    log.info(
                        "Backup session freed %s on %s",
                        format_size(abs(added_bytes)),
                        session_usage_path,
                    )
            except Exception as e:
                log.error(f"Failed to read post-backup disk usage for {session_usage_path}")
                log.error(e)

        if mounted:
            try:
                unmount(config["mount_device"], config["mount_point"], log)
            except Exception as e:
                backup_success = False
                log.error(
                    f"Failed to unmount {config['mount_device']} from {config['mount_point']}"
                )
                log.error(e)
        log.info("Copy completed")
        if config.get("remote_log_path"):
            local_log_path = f"{log_path}/{log_file_name}"
            remote_log_path = f"{config['remote_name']}:{config['remote_log_path']}"
            try:
                log.info(f"Copying {local_log_path} to {remote_log_path}")
                rclone.copy(local_log_path, remote_log_path, ignore_existing=True)
                log.info(f"Copied {local_log_path} to {remote_log_path}")
            except Exception as e:
                backup_success = False
                log.error(f"Failed to copy {local_log_path} to {remote_log_path}")
                log.error(e)

        log_file_path = f"{log_path}/{log_file_name}"
        for handler in logging.getLogger().handlers:
            handler.flush()

        subject = "Successful backup" if backup_success else "Failed backup"
        try:
            with open(log_file_path, "r", encoding="utf-8", errors="replace") as f:
                log_content = f.read()
            send_email_notification(config, subject, log_content, log)
            log.info("Backup email notification sent")
        except Exception as e:
            log.error("Failed to send backup email notification")
            log.error(e)

        log.info("Done")


def read_config(path: str) -> dict[str, Any]:
    with open(path, "r") as file:
        return {
            line.split("=")[0].strip().lower(): "=".join(line.split("=")[1:]).strip()
            for line in file
            if "=" in line and line.strip()[0] != "#"
        }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Using config file: ", sys.argv[1])
        config = read_config(sys.argv[1])
        main(config)
    else:
        print("No config file specified")
