import datetime
import logging
import os
from pathlib import Path
import sys
from typing import Any
from rclone_python import rclone
from rclone_python.remote_types import RemoteTypes


def setup_logging(
    log_file_name: str | None = None,
    log_path: str | None = None,
    max_logfiles: int = 100,
) -> logging.Logger:
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
    fileHandler = logging.FileHandler(f"{log_path}/{log_file_name}", "a")
    fileHandler.setFormatter(logFormatter)
    log.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    log.addHandler(consoleHandler)
    rclone.set_log_level(logging.INFO)
    return log


def main(config: dict[str, Any]):
    log = setup_logging(
        log_path=config.get("log_path"), log_file_name=config.get("log_file_name")
    )
    log.info("Starting backup")
    log.debug(config)

    if not rclone.check_remote_existing(config["remote_name"]):
        log.info("Creating remote")
        rclone.create_remote(
            remote_name=config["remote_name"],
            remote_type=RemoteTypes[config["remote_type"]],
            key_file=config["key_file"],
            host=config["host"],
            user=config["user"],
        )

    about = rclone.about(config["remote_name"])
    log.debug(about)

    mounted = False
    if config.get("mount_device") and config.get("mountpoint"):
        mounted = (
            os.system(f"mount {config['mount_device']} {config['mountpoint']}") == 0
        )
        if mounted:
            log.info(f"Mounted {config['mount_device']} to {config['mountpoint']}")
        else:
            log.error(
                f"Failed to mount {config['mount_device']} to {config['mountpoint']}"
            )
            return

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

    try:
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
        log.error(e)
        raise e
    finally:
        if mounted:
            os.system(f"umount {config['mountpoint']}")
            log.info(f"Unmounted {config['mount_device']} from {config['mountpoint']}")
        log.info("Done!")


def read_config(path: str) -> dict[str, Any]:
    with open(path, "r") as file:
        return {
            line.split("=")[0].strip().lower(): "".join(line.split("=")[1:]).strip()
            for line in file
            if "=" in line
        }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Using config file: ", sys.argv[1])
        config = read_config(sys.argv[1])
        main(config)
    else:
        print("No config file specified")
