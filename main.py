import logging
import os
import pprint
import sys
from typing import Any
from rclone_python import rclone
from rclone_python.remote_types import RemoteTypes
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TransferSpeedColumn,
)


def main(config: dict[str, Any]):
    pprint.pp(config)
    rclone.set_log_level(logging.DEBUG)
    if not rclone.check_remote_existing(config["remote_name"]):
        rclone.create_remote(
            remote_name=config["remote_name"],
            remote_type=RemoteTypes[config["remote_type"]],
            key_file=config["key_file"],
            host=config["host"],
            user=config["user"],
        )

    about = rclone.about(config["remote_name"])
    pprint.pp(about)

    mounted = False
    if config.get("mount_device") and config.get("mountpoint"):
        mounted = os.system(f"mount {config['mount_device']} {config['mountpoint']}") == 0
        if mounted:
            print(f"Mounted {config['mount_device']} to {config['mountpoint']}")
        else:
            print(f"Failed to mount {config['mount_device']} to {config['mountpoint']}")
            return

    pbar = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TransferSpeedColumn(),
    )

    args = [
        *config.get("extra_rclone_args", "").split(",")
    ]
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
        rclone.copy(
            f"{config['remote_name']}:{config['remote_path']}",
            config["local_path"],
            ignore_existing=True,
            args=args,
            pbar=pbar,
        )
    except Exception as e:
        raise e
    finally:
        if mounted:
            os.system(f"umount {config['mountpoint']}")
            print(f"Unmounted {config['mount_device']} from {config['mountpoint']}")
        print("Done!")

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
        print("Error: Missing config file")
