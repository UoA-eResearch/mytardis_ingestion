"""Common functions for config"""

import logging
import sys
from pathlib import Path
from typing import Annotated, Optional, TypeAlias

import typer
from pydantic import ValidationError

from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig

logger = logging.getLogger(__name__)

SourceDataPathArg: TypeAlias = Annotated[
    Path,
    typer.Argument(
        help="Path to the source data (either a directory or file)",
        exists=True,
        dir_okay=True,
        file_okay=True,
    ),
]

ProfileNameOption: TypeAlias = Annotated[
    str,
    typer.Option(
        "--profile",
        "-p",
        help="Name of the ingestion profile to be used to extract the data",
    ),
]

ProfileVersionOption = Annotated[
    Optional[str],
    typer.Option(
        help="Version of the profile to be used. If left unspecified, the latest will be used",
    ),
]

LogFileOption: TypeAlias = Annotated[
    Path,
    typer.Option(help="Path to be used for the log file"),
]

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def validate_log_level(value: str) -> str:
    """Validate the log level passed in as a string."""
    if value.upper() not in LOG_LEVELS:
        raise typer.BadParameter(message=f"'{value}'. Valid values are {LOG_LEVELS}.")
    return value.upper()


LogLevelOption: TypeAlias = Annotated[
    str,
    typer.Option(
        help=f"Logging verbosity level. Options: {LOG_LEVELS}",
        callback=validate_log_level,
    ),
]

StorageBoxOption: TypeAlias = Annotated[
    Optional[tuple[str, Path]],
    typer.Option(
        help="Name and filesystem directory of staging StorageBox to transfer datafiles into."
    ),
]


def get_config(storage: StorageBoxOption) -> ConfigFromEnv:
    """Returns ingestion configuration parsed from the.env file.

    Args:
        storage (StorageBoxOption): Optional storage details if the ones parsed from .env
        file need to be overridden.

    Returns:
        ConfigFromEnv: Ingestion configuration.
    """
    try:
        config = ConfigFromEnv()
        if storage is not None:
            # Create storagebox config based on passed in argument.
            store = FilesystemStorageBoxConfig(
                storage_name=storage[0], target_root_dir=storage[1]
            )
            config.storage = store
        return config
    except ValidationError as error:
        logger.error(
            (
                "An error occurred while validating the environment "
                "configuration. Make sure all required variables are set "
                "or pass your own configuration instance. Error: %s"
            ),
            error,
        )
        sys.exit(1)
