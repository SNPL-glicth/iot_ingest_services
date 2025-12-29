from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _default_env_file() -> str:
    # Reuse the Nest backend .env by default, so credentials are not duplicated.
    repo_root = Path(__file__).resolve().parents[2]
    return str(repo_root / "iot_monitor_backend" / ".env")


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

    odbc_driver: str


def get_settings() -> Settings:
    # Load env file (if present) but still allow overriding via real environment variables.
    env_file = os.getenv("IOT_ENV_FILE", _default_env_file())
    if env_file and Path(env_file).exists():
        load_dotenv(env_file, override=False)

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "1434"))
    db_user = os.getenv("DB_USER", "sa")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "iot_monitoring_system")

    # Driver name depends on the OS image.
    # Common values:
    # - ODBC Driver 17 for SQL Server
    # - ODBC Driver 18 for SQL Server
    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

    return Settings(
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        odbc_driver=odbc_driver,
    )
