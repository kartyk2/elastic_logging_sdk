from dataclasses import dataclass
import os

@dataclass
class LoggingConfig:
    project: str
    environment: str = "dev"
    log_dir: str = "./logs"
    level: str = "INFO"
    rotation: str = "midnight"  # or "size"
    max_bytes: int = 50 * 1024 * 1024
    backup_count: int = 14
    utc: bool = True

    def log_file_path(self):
        os.makedirs(self.log_dir, exist_ok=True)
        return os.path.join(
            self.log_dir,
            f"{self.project}-{self.environment}.log"
        )