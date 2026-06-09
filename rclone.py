from typing import Any


def prepare_rclone_args(config: dict[str, Any]) -> list[str]:
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

    return args
