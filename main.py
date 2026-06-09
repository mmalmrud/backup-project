import datetime
import logging
import os
from pathlib import Path
import shutil
import smtplib
import ssl
import sys
from typing import Any
from email.message import EmailMessage
from rclone_python import rclone
from rclone_python.remote_types import RemoteTypes
from rclone_python.utils import run_rclone_cmd
from requests import get


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

    logFormatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )
    log_file_path = f"{log_path}/{log_file_name}"
    fileHandler = logging.FileHandler(log_file_path, "a")
    fileHandler.setFormatter(logFormatter)
    log.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    log.addHandler(consoleHandler)
    rclone.set_log_level(logging.INFO)
    return log, log_path, log_file_name


def send_email_notification(
    config: dict[str, Any],
    subject: str,
    body: str,
    log: logging.Logger,
) -> None:
    required_keys = ["smtp_username", "smtp_password", "smtp_to"]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        log.info(
            "Email notification skipped. Missing SMTP config keys: %s",
            ", ".join(missing),
        )
        return

    smtp_host = config.get("smtp_host", "smtp.gmail.com")
    smtp_port = int(config.get("smtp_port", "587"))
    smtp_username = config["smtp_username"]
    smtp_password = config["smtp_password"]
    smtp_from = config.get("smtp_from", smtp_username)
    recipients = [recipient.strip() for recipient in config["smtp_to"].split(",") if recipient.strip()]

    if not recipients:
        log.info("Email notification skipped. No recipients configured in smtp_to.")
        return

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = smtp_from
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_username, smtp_password)
        server.send_message(message)


def _bytes_to_human_readable(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{size} B"


def log_disk_usage(path: str, log: logging.Logger, label: str = "Disk usage") -> tuple[int, int, int]:
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
            path = Path(config["mount_point"])
            if not path.exists():
                raise ValueError(f"{path} does not exist")
            # Check if the mount point is already mounted
            with open("/proc/mounts", "r") as mounts_file:
                if any(config["mount_point"] in line for line in mounts_file):
                    log.info(f"{config['mount_point']} is already mounted")
                    mounted = True
                else:
                    exit_code = os.system(
                        f"mount {config['mount_device']} {config['mount_point']}"
                    )
                    if exit_code != 0:
                        log.error(
                            f"Failed to mount {config['mount_device']} to {config['mount_point']}"
                        )
                        raise Exception(f"Mount exit status {exit_code}")
                    mounted = True
            log.info(f"Mounted {config['mount_device']} to {config['mount_point']}")

        args = [*config.get("extra_rclone_args", "").split(",")]
        if config.get("backup_dir"):
            args.append(f"--backup-dir={config['backup_dir']}")
        if config.get("exclude"):
            args.append(f"--exclude={config['exclude']}")
        if config.get("bwlimit"):
            args.append(f"--bwlimit={config['bwlimit']}")
        if config.get("suffix"):
            args.append(f"--suffix={config['suffix']}")
        if config.get("dry_run") == "true":
            args.append("--dry-run")

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
                        _bytes_to_human_readable(added_bytes),
                        session_usage_path,
                    )
                else:
                    log.info(
                        "Backup session freed %s on %s",
                        _bytes_to_human_readable(abs(added_bytes)),
                        session_usage_path,
                    )
            except Exception as e:
                log.error(f"Failed to read post-backup disk usage for {session_usage_path}")
                log.error(e)

        if mounted:
            try:
                exit_code = os.system(f"umount {config['mount_point']}")
                log.info(
                    f"Unmounted {config['mount_device']} from {config['mount_point']}"
                )
                if exit_code != 0:
                    raise Exception(f"Unmount exit status {exit_code}")
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
